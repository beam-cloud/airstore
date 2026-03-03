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

	// First call with IDs should return all IDs as added so hooks can bootstrap.
	result, err := tracker.Compare(ctx, key, []string{"a", "b", "c"})
	if err != nil {
		t.Fatalf("compare: %v", err)
	}
	if len(result.Added) != 3 {
		t.Fatalf("expected 3 IDs on first compare, got %d: %v", len(result.Added), result.Added)
	}
	got := make(map[string]bool, len(result.Added))
	for _, id := range result.Added {
		got[id] = true
	}
	if !got["a"] || !got["b"] || !got["c"] {
		t.Errorf("expected [a b c], got %v", result.Added)
	}
	if len(result.Removed) != 0 {
		t.Errorf("expected no removed on first call, got %v", result.Removed)
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

	// Second call with same IDs → nil (no changes)
	result, err := tracker.Compare(ctx, key, ids)
	if err != nil {
		t.Fatalf("compare: %v", err)
	}
	if result != nil {
		t.Errorf("expected nil for unchanged set, got added=%v removed=%v", result.Added, result.Removed)
	}
}

func TestSeenTracker_DetectsNewIDs(t *testing.T) {
	tracker := newTestTracker(t)
	ctx := context.Background()
	key := "test:seen:new"

	// Seed with {a, b}
	tracker.Compare(ctx, key, []string{"a", "b"})
	tracker.Commit(ctx, key, []string{"a", "b"})

	// Now {a, b, c, d} → should detect c, d as added
	result, err := tracker.Compare(ctx, key, []string{"a", "b", "c", "d"})
	if err != nil {
		t.Fatalf("compare: %v", err)
	}
	if len(result.Added) != 2 {
		t.Fatalf("expected 2 added IDs, got %d: %v", len(result.Added), result.Added)
	}

	got := make(map[string]bool)
	for _, id := range result.Added {
		got[id] = true
	}
	if !got["c"] || !got["d"] {
		t.Errorf("expected c and d as added, got %v", result.Added)
	}
	if len(result.Removed) != 0 {
		t.Errorf("expected no removed IDs, got %v", result.Removed)
	}
}

func TestSeenTracker_CommitAdvancesSet(t *testing.T) {
	tracker := newTestTracker(t)
	ctx := context.Background()
	key := "test:seen:advance"

	// Seed with {a, b}
	tracker.Compare(ctx, key, []string{"a", "b"})
	tracker.Commit(ctx, key, []string{"a", "b"})

	// Detect {c} as added
	result, _ := tracker.Compare(ctx, key, []string{"a", "b", "c"})
	if len(result.Added) != 1 || result.Added[0] != "c" {
		t.Fatalf("expected [c], got %v", result.Added)
	}

	// Commit with {a, b, c}
	tracker.Commit(ctx, key, []string{"a", "b", "c"})

	// Now {a, b, c} again → nil (no changes)
	result, _ = tracker.Compare(ctx, key, []string{"a", "b", "c"})
	if result != nil {
		t.Errorf("expected nil for unchanged set, got added=%v removed=%v", result.Added, result.Removed)
	}
}

func TestSeenTracker_WithoutCommit_RetryDetectsSameNew(t *testing.T) {
	tracker := newTestTracker(t)
	ctx := context.Background()
	key := "test:seen:nocommit"

	// Seed with {a}
	tracker.Compare(ctx, key, []string{"a"})
	tracker.Commit(ctx, key, []string{"a"})

	// Detect {b} as added — but DON'T commit (simulating failed emit)
	result, _ := tracker.Compare(ctx, key, []string{"a", "b"})
	if len(result.Added) != 1 || result.Added[0] != "b" {
		t.Fatalf("expected [b], got %v", result.Added)
	}
	// Intentionally skip Commit

	// Retry: should still detect {b} as added since we didn't commit
	result, _ = tracker.Compare(ctx, key, []string{"a", "b"})
	if len(result.Added) != 1 || result.Added[0] != "b" {
		t.Errorf("expected [b] on retry (no commit), got %v", result.Added)
	}
}

func TestSeenTracker_EmptyCurrentReturnsNil(t *testing.T) {
	tracker := newTestTracker(t)
	ctx := context.Background()
	key := "test:seen:empty"

	result, err := tracker.Compare(ctx, key, nil)
	if err != nil {
		t.Fatalf("compare: %v", err)
	}
	if result != nil {
		t.Errorf("expected nil for empty current, got %v", result)
	}

	result, err = tracker.Compare(ctx, key, []string{})
	if err != nil {
		t.Fatalf("compare: %v", err)
	}
	if result != nil {
		t.Errorf("expected nil for empty current, got %v", result)
	}
}

func TestSeenTracker_EmptyCurrentAfterInit_ReportsAllRemoved(t *testing.T) {
	tracker := newTestTracker(t)
	ctx := context.Background()
	key := "test:seen:allempty"

	// Seed with {a, b, c} and commit
	tracker.Compare(ctx, key, []string{"a", "b", "c"})
	tracker.Commit(ctx, key, []string{"a", "b", "c"})

	// Query returns empty → all items should be reported as removed
	result, err := tracker.Compare(ctx, key, []string{})
	if err != nil {
		t.Fatalf("compare: %v", err)
	}
	if result == nil {
		t.Fatal("expected non-nil result when all items removed, got nil")
	}
	if len(result.Added) != 0 {
		t.Errorf("expected no added, got %v", result.Added)
	}
	if len(result.Removed) != 3 {
		t.Fatalf("expected 3 removed, got %d: %v", len(result.Removed), result.Removed)
	}
	got := make(map[string]bool)
	for _, id := range result.Removed {
		got[id] = true
	}
	if !got["a"] || !got["b"] || !got["c"] {
		t.Errorf("expected a, b, c removed, got %v", result.Removed)
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

	// After clearing, the key is still initialized, so {a} is added.
	result, err := tracker.Compare(ctx, key, []string{"a"})
	if err != nil {
		t.Fatalf("compare: %v", err)
	}
	if len(result.Added) != 1 || result.Added[0] != "a" {
		t.Errorf("expected [a] after clear, got %v", result.Added)
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
	result, _ := tracker.Compare(ctx, key, []string{"b", "c", "d"})
	if len(result.Added) != 1 || result.Added[0] != "d" {
		t.Errorf("expected added [d], got %v", result.Added)
	}
	if len(result.Removed) != 1 || result.Removed[0] != "a" {
		t.Errorf("expected removed [a], got %v", result.Removed)
	}
	tracker.Commit(ctx, key, []string{"b", "c", "d"})

	// a reappears: {a, b, c, d}
	result, _ = tracker.Compare(ctx, key, []string{"a", "b", "c", "d"})
	if len(result.Added) != 1 || result.Added[0] != "a" {
		t.Errorf("expected [a] (re-added), got %v", result.Added)
	}
	if len(result.Removed) != 0 {
		t.Errorf("expected no removed, got %v", result.Removed)
	}
}

func TestSeenTracker_DetectsRemovedIDs(t *testing.T) {
	tracker := newTestTracker(t)
	ctx := context.Background()
	key := "test:seen:removed"

	// Seed {a, b, c}
	tracker.Compare(ctx, key, []string{"a", "b", "c"})
	tracker.Commit(ctx, key, []string{"a", "b", "c"})

	// Now {a} only → b,c removed
	result, err := tracker.Compare(ctx, key, []string{"a"})
	if err != nil {
		t.Fatalf("compare: %v", err)
	}
	if len(result.Added) != 0 {
		t.Errorf("expected no added, got %v", result.Added)
	}
	if len(result.Removed) != 2 {
		t.Fatalf("expected 2 removed, got %d: %v", len(result.Removed), result.Removed)
	}
	got := make(map[string]bool)
	for _, id := range result.Removed {
		got[id] = true
	}
	if !got["b"] || !got["c"] {
		t.Errorf("expected b and c removed, got %v", result.Removed)
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

	// Seed baseline and verify unchanged snapshots produce no changes.
	_, _ = tracker.Compare(ctx, key, ids)
	if err := tracker.Commit(ctx, key, ids); err != nil {
		t.Fatalf("commit: %v", err)
	}
	result, err := tracker.Compare(ctx, key, ids)
	if err != nil {
		t.Fatalf("compare before reset: %v", err)
	}
	if result != nil {
		t.Fatalf("expected nil for unchanged set before reset, got added=%v removed=%v", result.Added, result.Removed)
	}

	// Reset should force next compare for this path to bootstrap.
	if err := tracker.ResetPath(ctx, workspaceID, path); err != nil {
		t.Fatalf("reset path: %v", err)
	}
	result, err = tracker.Compare(ctx, key, ids)
	if err != nil {
		t.Fatalf("compare after reset: %v", err)
	}
	if len(result.Added) != len(ids) {
		t.Fatalf("expected %d added IDs after reset, got %d (%v)", len(ids), len(result.Added), result.Added)
	}
}
