package hooks

import (
	"strings"
	"testing"

	"github.com/beam-cloud/airstore/pkg/repository"
	"github.com/beam-cloud/airstore/pkg/types"
)

func TestPreferSourceWatchMatchesPrefersChildTaskForSameCorrelationKey(t *testing.T) {
	matches := []repository.TaskSourceWatchMatch{
		{
			WorkspaceID:    7,
			TaskID:         "parent-task",
			CorrelationKey: "thread-123",
			Reason:         "Aggregate monitoring",
		},
		{
			WorkspaceID:    7,
			TaskID:         "child-task",
			CorrelationKey: "thread-123",
			Reason:         "Watch Luke thread",
			ParentTaskID:   "parent-task",
		},
		{
			WorkspaceID:    7,
			TaskID:         "other-thread-task",
			CorrelationKey: "thread-456",
			Reason:         "Another thread",
		},
	}

	selected := preferSourceWatchMatches(matches)
	if got, want := len(selected), 2; got != want {
		t.Fatalf("selected count = %d, want %d", got, want)
	}
	if got, want := selected[0].TaskID, "child-task"; got != want {
		t.Fatalf("selected task for thread-123 = %q, want %q", got, want)
	}
	if got, want := selected[0].CorrelationKey, "thread-123"; got != want {
		t.Fatalf("selected key = %q, want %q", got, want)
	}
	if got, want := selected[1].TaskID, "other-thread-task"; got != want {
		t.Fatalf("selected task for thread-456 = %q, want %q", got, want)
	}
}

func TestIsSystemManagedSourceWatchHookRecognizesFollowupHooks(t *testing.T) {
	targetTaskID := "task-123"
	hook := &types.Hook{
		SystemManaged: true,
		Path:          "/sources/gmail/__followup__task-123__hash",
		TargetTaskID:  &targetTaskID,
	}
	if !isSystemManagedSourceWatchHook(hook) {
		t.Fatal("expected follow-up hook to be treated as system-managed source watch hook")
	}
	if got, want := sourceWatchTaskInputIdempotencyKey("hook:abc"), "source_watch_hook:hook:abc"; got != want {
		t.Fatalf("source watch idempotency key = %q, want %q", got, want)
	}
}

func TestHookIdempotencyKey_SourceCreateStableAcrossEventIDs(t *testing.T) {
	data := map[string]any{
		"path":           "/sources/github/airstore-prs",
		"integration":    "github",
		"new_items_hash": "abc123",
	}

	keyA := hookIdempotencyKey("hook-1", "1700000000-0", data)
	keyB := hookIdempotencyKey("hook-1", "1700000001-0", data)
	if keyA != keyB {
		t.Fatalf("expected source fs.create key to ignore stream event ID, got %q vs %q", keyA, keyB)
	}
}

func TestHookIdempotencyKey_SourceCreateFallbackCanonicalizesNewItems(t *testing.T) {
	dataA := map[string]any{
		"path":        "/sources/github/airstore-prs",
		"integration": "github",
		"new_items":   "pr-2, pr-1, pr-2",
	}
	dataB := map[string]any{
		"path":        "/sources/github/airstore-prs",
		"integration": "github",
		"new_items":   "pr-1,pr-2",
	}

	keyA := hookIdempotencyKey("hook-1", "evt-a", dataA)
	keyB := hookIdempotencyKey("hook-1", "evt-b", dataB)
	if keyA != keyB {
		t.Fatalf("expected canonicalized new_items to dedupe, got %q vs %q", keyA, keyB)
	}
}

func TestHookIdempotencyKey_SourceCreateDifferentItemsDifferentKeys(t *testing.T) {
	dataA := map[string]any{
		"path":        "/sources/github/airstore-prs",
		"integration": "github",
		"new_items":   "pr-1,pr-2",
	}
	dataB := map[string]any{
		"path":        "/sources/github/airstore-prs",
		"integration": "github",
		"new_items":   "pr-3",
	}

	keyA := hookIdempotencyKey("hook-1", "evt-a", dataA)
	keyB := hookIdempotencyKey("hook-1", "evt-b", dataB)
	if keyA == keyB {
		t.Fatalf("expected different item sets to produce different keys, got %q", keyA)
	}
}

func TestHookIdempotencyKey_FsWriteStillUsesEventID(t *testing.T) {
	keyA := hookIdempotencyKey("hook-1", "evt-a", map[string]any{"path": "/foo.txt"})
	keyB := hookIdempotencyKey("hook-1", "evt-b", map[string]any{"path": "/foo.txt"})
	if keyA == keyB {
		t.Fatalf("expected fs.write idempotency to remain event-scoped, got %q", keyA)
	}
}

func TestBuildHookAttachment_IncludesMoveContext(t *testing.T) {
	hook := &types.Hook{
		Id:         1,
		ExternalId: "hook-1",
		Path:       "/pdfs",
	}
	data := map[string]any{
		"path":         "/pdfs/UHC_letter_jan12026.pdf",
		"workspace_id": "124",
		"old_path":     "/UHC_letter_jan12026.pdf",
		"new_path":     "/pdfs/UHC_letter_jan12026.pdf",
		"move_op_id":   "mv-123",
	}

	attachment := buildHookAttachment(hook, EventFsWrite, data)
	if got := attachment["old_path"]; got != data["old_path"] {
		t.Fatalf("expected old_path %v, got %v", data["old_path"], got)
	}
	if got := attachment["new_path"]; got != data["new_path"] {
		t.Fatalf("expected new_path %v, got %v", data["new_path"], got)
	}
	if got := attachment["move_op_id"]; got != data["move_op_id"] {
		t.Fatalf("expected move_op_id %v, got %v", data["move_op_id"], got)
	}
}

func TestHookSessionID_StableForSameHookAndEvent(t *testing.T) {
	sessionA := hookSessionID("hook-1", "evt-1")
	sessionB := hookSessionID("hook-1", "evt-1")
	if sessionA != sessionB {
		t.Fatalf("expected stable session id for same hook, got %q vs %q", sessionA, sessionB)
	}
}

func TestHookSessionID_IsolatedAcrossEvents(t *testing.T) {
	sessionA := hookSessionID("hook-1", "evt-1")
	sessionB := hookSessionID("hook-1", "evt-2")
	if sessionA == sessionB {
		t.Fatalf("expected distinct session ids across events, got %q", sessionA)
	}
}

func TestHookLane_StableAndIsolatedAcrossEvents(t *testing.T) {
	laneA := hookLane("hook-1", "evt-1")
	laneB := hookLane("hook-1", "evt-1")
	laneC := hookLane("hook-1", "evt-2")
	if laneA != laneB {
		t.Fatalf("expected stable lane for same hook/event, got %q vs %q", laneA, laneB)
	}
	if laneA == laneC {
		t.Fatalf("expected isolated lane across events, got %q", laneA)
	}
}

func TestHookIsolationIDs_CompressLongSeed(t *testing.T) {
	seedHook := strings.Repeat("x", 512)
	seedEvent := strings.Repeat("y", 512)

	sessionID := hookSessionID(seedHook, seedEvent)
	lane := hookLane(seedHook, seedEvent)
	if len(sessionID) > 180 {
		t.Fatalf("expected compressed session id length <= 180, got %d", len(sessionID))
	}
	if len(lane) > 180 {
		t.Fatalf("expected compressed lane length <= 180, got %d", len(lane))
	}
	if !strings.HasPrefix(sessionID, "hook-session:") {
		t.Fatalf("expected compressed session id to retain prefix, got %q", sessionID)
	}
	if !strings.HasPrefix(lane, "hook-lane:") {
		t.Fatalf("expected compressed lane to retain prefix, got %q", lane)
	}
}
