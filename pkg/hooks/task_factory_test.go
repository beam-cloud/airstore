package hooks

import (
	"testing"

	"github.com/beam-cloud/airstore/pkg/types"
)

func TestHookIdempotencyKey_SourceChangeStableAcrossEventIDs(t *testing.T) {
	data := map[string]any{
		"path":           "/sources/github/airstore-prs",
		"integration":    "github",
		"new_items_hash": "abc123",
	}

	keyA := hookIdempotencyKey("hook-1", "1700000000-0", EventSourceChange, data)
	keyB := hookIdempotencyKey("hook-1", "1700000001-0", EventSourceChange, data)
	if keyA != keyB {
		t.Fatalf("expected source.change key to ignore stream event ID, got %q vs %q", keyA, keyB)
	}
}

func TestHookIdempotencyKey_SourceChangeFallbackCanonicalizesNewItems(t *testing.T) {
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

	keyA := hookIdempotencyKey("hook-1", "evt-a", EventSourceChange, dataA)
	keyB := hookIdempotencyKey("hook-1", "evt-b", EventSourceChange, dataB)
	if keyA != keyB {
		t.Fatalf("expected canonicalized new_items to dedupe, got %q vs %q", keyA, keyB)
	}
}

func TestHookIdempotencyKey_SourceChangeDifferentItemsDifferentKeys(t *testing.T) {
	base := map[string]any{
		"path":        "/sources/github/airstore-prs",
		"integration": "github",
	}
	dataA := map[string]any{
		"path":        base["path"],
		"integration": base["integration"],
		"new_items":   "pr-1,pr-2",
	}
	dataB := map[string]any{
		"path":        base["path"],
		"integration": base["integration"],
		"new_items":   "pr-3",
	}

	keyA := hookIdempotencyKey("hook-1", "evt-a", EventSourceChange, dataA)
	keyB := hookIdempotencyKey("hook-1", "evt-b", EventSourceChange, dataB)
	if keyA == keyB {
		t.Fatalf("expected different item sets to produce different keys, got %q", keyA)
	}
}

func TestHookIdempotencyKey_FsWriteStillUsesEventID(t *testing.T) {
	keyA := hookIdempotencyKey("hook-1", "evt-a", EventFsWrite, map[string]any{"path": "/foo.txt"})
	keyB := hookIdempotencyKey("hook-1", "evt-b", EventFsWrite, map[string]any{"path": "/foo.txt"})
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
