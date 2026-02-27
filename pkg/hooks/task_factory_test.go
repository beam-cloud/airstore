package hooks

import "testing"

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
