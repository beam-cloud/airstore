package services

import (
	"context"
	"sync"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/beam-cloud/airstore/pkg/auth"
	"github.com/beam-cloud/airstore/pkg/common"
	"github.com/beam-cloud/airstore/pkg/instrumentation"
	"github.com/beam-cloud/airstore/pkg/types"
	pb "github.com/beam-cloud/airstore/proto"
)

type collectingRecorder struct {
	mu     sync.Mutex
	events []instrumentation.AccessEvent
}

func (r *collectingRecorder) Record(_ context.Context, event instrumentation.AccessEvent) error {
	r.mu.Lock()
	r.events = append(r.events, event)
	r.mu.Unlock()
	return nil
}

func (r *collectingRecorder) Flush() error { return nil }

func (r *collectingRecorder) snapshot() []instrumentation.AccessEvent {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]instrumentation.AccessEvent, len(r.events))
	copy(out, r.events)
	return out
}

func authCtx(workspaceExtID string) context.Context {
	return auth.WithAuthInfo(context.Background(), &types.AuthInfo{
		TokenType: types.TokenTypeWorkspaceMember,
		Workspace: &types.WorkspaceInfo{ExternalId: workspaceExtID},
	})
}

func newTestRedis(t *testing.T) *common.RedisClient {
	t.Helper()
	s, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)

	rdb, err := common.NewRedisClient(types.RedisConfig{
		Addrs: []string{s.Addr()},
		Mode:  types.RedisModeSingle,
	})
	if err != nil {
		t.Fatal(err)
	}
	return rdb
}

func TestAccessServiceRequiresWorkspaceContext(t *testing.T) {
	rec := &collectingRecorder{}
	svc := NewAccessService(rec, nil)

	resp, err := svc.IngestAccessEvents(context.Background(), &pb.IngestAccessEventsRequest{
		Events: []*pb.AccessLogEvent{{EventId: "e1", Path: "sources/a.txt"}},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Ok {
		t.Fatalf("expected not ok without workspace context")
	}
}

func TestAccessServiceNormalizesWorkspaceAndSession(t *testing.T) {
	rec := &collectingRecorder{}
	svc := NewAccessService(rec, nil)

	resp, err := svc.IngestAccessEvents(authCtx("ws-ext"), &pb.IngestAccessEventsRequest{
		Events: []*pb.AccessLogEvent{{
			EventId:          "e1",
			Ts:               12345,
			Path:             "sources/gmail/inbox/msg.txt",
			CacheSource:      "open_content",
			OriginalTokens:   1000,
			CompressedTokens: 200,
			Strategy:         "strip",
			Outcome:          "cache_hit",
			AccessOrigin:     "",
		}},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !resp.Ok || resp.Accepted != 1 {
		t.Fatalf("expected accepted=1, got ok=%v accepted=%d", resp.Ok, resp.Accepted)
	}

	events := rec.snapshot()
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	ev := events[0]
	if ev.WorkspaceID != "ws-ext" {
		t.Fatalf("expected workspace_id=ws-ext, got %q", ev.WorkspaceID)
	}
	if ev.SessionID != "ws-ext" {
		t.Fatalf("expected session fallback to workspace ID, got %q", ev.SessionID)
	}
	if ev.AccessOrigin != "fuse" {
		t.Fatalf("expected default access_origin=fuse, got %q", ev.AccessOrigin)
	}
	if ev.OriginalTokens != 1000 || ev.CompressedTokens != 200 {
		t.Fatalf("unexpected token mapping: %#v", ev)
	}
}

func TestAccessServiceDedupesByEventIDWhenRedisConfigured(t *testing.T) {
	rec := &collectingRecorder{}
	svc := NewAccessService(rec, newTestRedis(t))

	ctx := authCtx("ws-ext")
	req := &pb.IngestAccessEventsRequest{
		Events: []*pb.AccessLogEvent{
			{EventId: "dup-1", Path: "sources/a.txt"},
			{EventId: "dup-1", Path: "sources/a.txt"},
		},
	}

	resp1, err := svc.IngestAccessEvents(ctx, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !resp1.Ok || resp1.Accepted != 1 {
		t.Fatalf("expected first ingest accepted=1, got ok=%v accepted=%d", resp1.Ok, resp1.Accepted)
	}

	resp2, err := svc.IngestAccessEvents(ctx, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !resp2.Ok || resp2.Accepted != 0 {
		t.Fatalf("expected second ingest accepted=0, got ok=%v accepted=%d", resp2.Ok, resp2.Accepted)
	}
}

func TestAccessServiceSkipsHiddenDotPaths(t *testing.T) {
	rec := &collectingRecorder{}
	svc := NewAccessService(rec, nil)

	resp, err := svc.IngestAccessEvents(authCtx("ws-ext"), &pb.IngestAccessEventsRequest{
		Events: []*pb.AccessLogEvent{
			{EventId: "hidden-1", Path: ".claude/.claude.json"},
			{EventId: "hidden-2", Path: "/skills/.cache/index.json"},
			{EventId: "visible-1", Path: "sources/gmail/inbox/msg.txt"},
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !resp.Ok || resp.Accepted != 1 {
		t.Fatalf("expected accepted=1 for visible event only, got ok=%v accepted=%d", resp.Ok, resp.Accepted)
	}

	events := rec.snapshot()
	if len(events) != 1 {
		t.Fatalf("expected 1 recorded event, got %d", len(events))
	}
	if events[0].Path != "sources/gmail/inbox/msg.txt" {
		t.Fatalf("unexpected recorded path: %q", events[0].Path)
	}
}
