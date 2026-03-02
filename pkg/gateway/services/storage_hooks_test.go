package services

import (
	"context"
	"sync"
	"testing"

	"github.com/beam-cloud/airstore/pkg/auth"
	"github.com/beam-cloud/airstore/pkg/clients"
	"github.com/beam-cloud/airstore/pkg/common"
	"github.com/beam-cloud/airstore/pkg/hooks"
	"github.com/beam-cloud/airstore/pkg/types"
)

type captureHookEmitter struct {
	mu     sync.Mutex
	events []map[string]any
}

func (e *captureHookEmitter) Emit(_ context.Context, data map[string]any) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	cloned := make(map[string]any, len(data))
	for k, v := range data {
		cloned[k] = v
	}
	e.events = append(e.events, cloned)
	return nil
}

func (e *captureHookEmitter) snapshot() []map[string]any {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]map[string]any, len(e.events))
	for i, event := range e.events {
		cloned := make(map[string]any, len(event))
		for k, v := range event {
			cloned[k] = v
		}
		out[i] = cloned
	}
	return out
}

func workspaceCtx(wsID uint, wsExtID string) context.Context {
	return auth.WithAuthInfo(context.Background(), &types.AuthInfo{
		TokenType: types.TokenTypeWorkspaceService,
		Workspace: &types.WorkspaceInfo{
			Id:         wsID,
			ExternalId: wsExtID,
			Name:       "test",
		},
	})
}

func TestStorageService_EmitHookMoveEvents_FileRename(t *testing.T) {
	emitter := &captureHookEmitter{}
	svc := &StorageService{hookStream: emitter}

	svc.emitHookMoveEvents(workspaceCtx(124, "ws-124"), "/inbox/file.pdf", "/pdfs/file.pdf")

	events := emitter.snapshot()
	if len(events) != 2 {
		t.Fatalf("expected 2 events, got %d", len(events))
	}

	first := events[0]
	second := events[1]

	if got := first["event"]; got != hooks.EventFsDelete {
		t.Fatalf("expected first event %q, got %v", hooks.EventFsDelete, got)
	}
	if got := first["path"]; got != "/inbox/file.pdf" {
		t.Fatalf("expected first path /inbox/file.pdf, got %v", got)
	}
	if got := second["event"]; got != hooks.EventFsWrite {
		t.Fatalf("expected second event %q, got %v", hooks.EventFsWrite, got)
	}
	if got := second["path"]; got != "/pdfs/file.pdf" {
		t.Fatalf("expected second path /pdfs/file.pdf, got %v", got)
	}

	moveID1, _ := first["move_op_id"].(string)
	moveID2, _ := second["move_op_id"].(string)
	if moveID1 == "" || moveID2 == "" {
		t.Fatalf("expected move_op_id on both events, got %q and %q", moveID1, moveID2)
	}
	if moveID1 != moveID2 {
		t.Fatalf("expected same move_op_id on both events, got %q and %q", moveID1, moveID2)
	}

	if got := first["old_path"]; got != "/inbox/file.pdf" {
		t.Fatalf("expected old_path on first event, got %v", got)
	}
	if got := first["new_path"]; got != "/pdfs/file.pdf" {
		t.Fatalf("expected new_path on first event, got %v", got)
	}
	if got := second["old_path"]; got != "/inbox/file.pdf" {
		t.Fatalf("expected old_path on second event, got %v", got)
	}
	if got := second["new_path"]; got != "/pdfs/file.pdf" {
		t.Fatalf("expected new_path on second event, got %v", got)
	}
}

func TestStorageService_EmitHookMoveEvents_PrefixRenameRoots(t *testing.T) {
	emitter := &captureHookEmitter{}
	svc := &StorageService{hookStream: emitter}

	svc.emitHookMoveEvents(workspaceCtx(124, "ws-124"), "/inbox", "/pdfs")

	events := emitter.snapshot()
	if len(events) != 2 {
		t.Fatalf("expected 2 events, got %d", len(events))
	}
	if got := events[0]["path"]; got != "/inbox" {
		t.Fatalf("expected source root path /inbox, got %v", got)
	}
	if got := events[1]["path"]; got != "/pdfs" {
		t.Fatalf("expected destination root path /pdfs, got %v", got)
	}
}

func TestStorageService_InvalidateBroadcastsChildAndParentKeys(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	bus := common.NewEventBus(ctx, nil)
	seen := map[string]int{}
	var mu sync.Mutex
	bus.On(common.EventCacheInvalidate, func(e common.Event) {
		key, _ := e.Data["key"].(string)
		if key == "" {
			return
		}
		mu.Lock()
		seen[key]++
		mu.Unlock()
	})

	svc := &StorageService{
		cache:    newMetadataCache(cacheTTL, cacheMaxEntries),
		eventBus: bus,
	}
	svc.invalidate("bucket-a", "pdfs/file.pdf")

	mu.Lock()
	defer mu.Unlock()
	if len(seen) != 2 {
		t.Fatalf("expected exactly 2 broadcast keys, got %d (%v)", len(seen), seen)
	}
	if seen["bucket-a:pdfs/file.pdf"] != 1 {
		t.Fatalf("expected child invalidation key, got map: %v", seen)
	}
	if seen["bucket-a:pdfs"] != 1 {
		t.Fatalf("expected parent invalidation key, got map: %v", seen)
	}
}

func TestStorageService_NotifyUploadComplete_EmitsHookEvent(t *testing.T) {
	emitter := &captureHookEmitter{}

	svc := &StorageService{
		client:     &clients.StorageClient{}, // nil internal S3 client -> readiness probe no-ops
		cache:      newMetadataCache(cacheTTL, cacheMaxEntries),
		hookStream: emitter,
	}

	err := svc.NotifyUploadComplete(workspaceCtx(124, "ws-124"), "/pdfs/file.pdf")
	if err != nil {
		t.Fatalf("notify upload complete returned error: %v", err)
	}

	events := emitter.snapshot()
	if len(events) != 1 {
		t.Fatalf("expected 1 hook event, got %d", len(events))
	}
	if got := events[0]["event"]; got != hooks.EventFsCreate {
		t.Fatalf("expected event %q, got %v", hooks.EventFsCreate, got)
	}
}
