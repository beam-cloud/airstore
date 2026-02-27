package hooks

import (
	"context"
	"testing"
	"time"

	"github.com/beam-cloud/airstore/pkg/common"
	"github.com/beam-cloud/airstore/pkg/repository"
	"github.com/beam-cloud/airstore/pkg/types"
)

func newTestTracker(t *testing.T) *SeenTracker {
	t.Helper()
	rdb, err := repository.NewRedisClientForTest()
	if err != nil {
		t.Fatalf("failed to create test redis: %v", err)
	}
	return NewSeenTracker(rdb)
}

func TestSeenTracker_FirstCall_SeedsBaseline(t *testing.T) {
	tracker := newTestTracker(t)
	ctx := context.Background()
	key := "test:seen:first"

	// First call with IDs should return all IDs as new so hooks can bootstrap.
	newIDs, err := tracker.Compare(ctx, key, []string{"a", "b", "c"})
	if err != nil {
		t.Fatalf("compare: %v", err)
	}
	if len(newIDs) != 3 {
		t.Fatalf("expected 3 IDs on first compare, got %d: %v", len(newIDs), newIDs)
	}
	got := make(map[string]bool, len(newIDs))
	for _, id := range newIDs {
		got[id] = true
	}
	if !got["a"] || !got["b"] || !got["c"] {
		t.Errorf("expected [a b c], got %v", newIDs)
	}
}

func TestSeenTracker_FirstCall_ThenCommit_ThenNoChange(t *testing.T) {
	tracker := newTestTracker(t)
	ctx := context.Background()
	key := "test:seen:nochange"

	ids := []string{"a", "b", "c"}

	// Seed
	tracker.Compare(ctx, key, ids)
	if err := tracker.Commit(ctx, key, ids); err != nil {
		t.Fatalf("commit: %v", err)
	}

	// Second call with same IDs → empty (no new)
	newIDs, err := tracker.Compare(ctx, key, ids)
	if err != nil {
		t.Fatalf("compare: %v", err)
	}
	if len(newIDs) != 0 {
		t.Errorf("expected no new IDs, got %v", newIDs)
	}
}

func TestSeenTracker_DetectsNewIDs(t *testing.T) {
	tracker := newTestTracker(t)
	ctx := context.Background()
	key := "test:seen:new"

	// Seed with {a, b}
	tracker.Compare(ctx, key, []string{"a", "b"})
	tracker.Commit(ctx, key, []string{"a", "b"})

	// Now {a, b, c, d} → should detect c, d as new
	newIDs, err := tracker.Compare(ctx, key, []string{"a", "b", "c", "d"})
	if err != nil {
		t.Fatalf("compare: %v", err)
	}
	if len(newIDs) != 2 {
		t.Fatalf("expected 2 new IDs, got %d: %v", len(newIDs), newIDs)
	}

	got := make(map[string]bool)
	for _, id := range newIDs {
		got[id] = true
	}
	if !got["c"] || !got["d"] {
		t.Errorf("expected c and d as new, got %v", newIDs)
	}
}

func TestSeenTracker_CommitAdvancesSet(t *testing.T) {
	tracker := newTestTracker(t)
	ctx := context.Background()
	key := "test:seen:advance"

	// Seed with {a, b}
	tracker.Compare(ctx, key, []string{"a", "b"})
	tracker.Commit(ctx, key, []string{"a", "b"})

	// Detect {c} as new
	newIDs, _ := tracker.Compare(ctx, key, []string{"a", "b", "c"})
	if len(newIDs) != 1 || newIDs[0] != "c" {
		t.Fatalf("expected [c], got %v", newIDs)
	}

	// Commit with {a, b, c}
	tracker.Commit(ctx, key, []string{"a", "b", "c"})

	// Now {a, b, c} again → no new
	newIDs, _ = tracker.Compare(ctx, key, []string{"a", "b", "c"})
	if len(newIDs) != 0 {
		t.Errorf("expected no new after commit, got %v", newIDs)
	}
}

func TestSeenTracker_WithoutCommit_RetryDetectsSameNew(t *testing.T) {
	tracker := newTestTracker(t)
	ctx := context.Background()
	key := "test:seen:nocommit"

	// Seed with {a}
	tracker.Compare(ctx, key, []string{"a"})
	tracker.Commit(ctx, key, []string{"a"})

	// Detect {b} as new — but DON'T commit (simulating failed emit)
	newIDs, _ := tracker.Compare(ctx, key, []string{"a", "b"})
	if len(newIDs) != 1 || newIDs[0] != "b" {
		t.Fatalf("expected [b], got %v", newIDs)
	}
	// Intentionally skip Commit

	// Retry: should still detect {b} as new since we didn't commit
	newIDs, _ = tracker.Compare(ctx, key, []string{"a", "b"})
	if len(newIDs) != 1 || newIDs[0] != "b" {
		t.Errorf("expected [b] on retry (no commit), got %v", newIDs)
	}
}

func TestSeenTracker_EmptyCurrentReturnsNil(t *testing.T) {
	tracker := newTestTracker(t)
	ctx := context.Background()
	key := "test:seen:empty"

	newIDs, err := tracker.Compare(ctx, key, nil)
	if err != nil {
		t.Fatalf("compare: %v", err)
	}
	if newIDs != nil {
		t.Errorf("expected nil for empty current, got %v", newIDs)
	}

	newIDs, err = tracker.Compare(ctx, key, []string{})
	if err != nil {
		t.Fatalf("compare: %v", err)
	}
	if newIDs != nil {
		t.Errorf("expected nil for empty current, got %v", newIDs)
	}
}

func TestSeenTracker_CommitEmpty_ClearsSet(t *testing.T) {
	tracker := newTestTracker(t)
	ctx := context.Background()
	key := "test:seen:clearset"

	// Seed with {a, b}
	tracker.Compare(ctx, key, []string{"a", "b"})
	tracker.Commit(ctx, key, []string{"a", "b"})

	// Commit empty → clears the set
	if err := tracker.Commit(ctx, key, []string{}); err != nil {
		t.Fatalf("commit empty: %v", err)
	}

	// After clearing, the key is still initialized, so {a} is new.
	newIDs, err := tracker.Compare(ctx, key, []string{"a"})
	if err != nil {
		t.Fatalf("compare: %v", err)
	}
	if len(newIDs) != 1 || newIDs[0] != "a" {
		t.Errorf("expected [a] after clear, got %v", newIDs)
	}
}

func TestSeenTracker_DetectsRemovedAndReaddedIDs(t *testing.T) {
	tracker := newTestTracker(t)
	ctx := context.Background()
	key := "test:seen:readd"

	// Seed {a, b, c}
	tracker.Compare(ctx, key, []string{"a", "b", "c"})
	tracker.Commit(ctx, key, []string{"a", "b", "c"})

	// Items rotate: {b, c, d} (a removed, d added)
	newIDs, _ := tracker.Compare(ctx, key, []string{"b", "c", "d"})
	if len(newIDs) != 1 || newIDs[0] != "d" {
		t.Errorf("expected [d], got %v", newIDs)
	}
	tracker.Commit(ctx, key, []string{"b", "c", "d"})

	// a reappears: {a, b, c, d}
	newIDs, _ = tracker.Compare(ctx, key, []string{"a", "b", "c", "d"})
	if len(newIDs) != 1 || newIDs[0] != "a" {
		t.Errorf("expected [a] (re-added), got %v", newIDs)
	}
}

func TestSeenTracker_NoExpiry(t *testing.T) {
	rdb, err := repository.NewRedisClientForTest()
	if err != nil {
		t.Fatalf("failed to create test redis: %v", err)
	}
	tracker := NewSeenTracker(rdb)
	ctx := context.Background()
	key := "test:seen:ttl"

	tracker.Compare(ctx, key, []string{"a"})
	tracker.Commit(ctx, key, []string{"a"})

	// Verify set key does not expire.
	ttl, err := rdb.TTL(ctx, key).Result()
	if err != nil {
		t.Fatalf("ttl: %v", err)
	}
	if ttl != -1*time.Nanosecond {
		t.Errorf("expected no expiry (-1), got %v", ttl)
	}

	// Verify the init marker also does not expire.
	initTTL, err := rdb.TTL(ctx, key+":init").Result()
	if err != nil {
		t.Fatalf("init ttl: %v", err)
	}
	if initTTL != -1*time.Nanosecond {
		t.Errorf("expected no expiry on init marker (-1), got %v", initTTL)
	}
}

func TestSeenTracker_ResetPath_ReinitializesFirstObservation(t *testing.T) {
	tracker := newTestTracker(t)
	ctx := context.Background()
	workspaceID := uint(321)
	path := "/sources/github/repo-prs"
	key := common.Keys.HookSeen(workspaceID, types.GeneratePathID(path))
	ids := []string{"a", "b"}

	// Seed baseline and verify unchanged snapshots produce no new IDs.
	_, _ = tracker.Compare(ctx, key, ids)
	if err := tracker.Commit(ctx, key, ids); err != nil {
		t.Fatalf("commit: %v", err)
	}
	newIDs, err := tracker.Compare(ctx, key, ids)
	if err != nil {
		t.Fatalf("compare before reset: %v", err)
	}
	if len(newIDs) != 0 {
		t.Fatalf("expected no new IDs before reset, got %v", newIDs)
	}

	// Reset should force next compare for this path to bootstrap.
	if err := tracker.ResetPath(ctx, workspaceID, path); err != nil {
		t.Fatalf("reset path: %v", err)
	}
	newIDs, err = tracker.Compare(ctx, key, ids)
	if err != nil {
		t.Fatalf("compare after reset: %v", err)
	}
	if len(newIDs) != len(ids) {
		t.Fatalf("expected %d new IDs after reset, got %d (%v)", len(ids), len(newIDs), newIDs)
	}
}
