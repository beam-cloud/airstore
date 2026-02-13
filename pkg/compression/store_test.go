package compression

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/beam-cloud/airstore/pkg/common"
	"github.com/beam-cloud/airstore/pkg/types"
)

func newTestRedis(t *testing.T) (*common.RedisClient, *miniredis.Miniredis) {
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
	return rdb, s
}

func testStore(t *testing.T) (*CompressedStore, *miniredis.Miniredis) {
	t.Helper()
	rdb, s := newTestRedis(t)
	store := NewCompressedStore(rdb, Config{
		ContentCacheMaxBytes: 1024, // 1KB budget for testing
		ContentCacheTTL:      1 * time.Minute,
	})
	return store, s
}

// ---------------------------------------------------------------------------
// Pointer tests
// ---------------------------------------------------------------------------

func TestStore_PointerRoundTrip(t *testing.T) {
	store, _ := testStore(t)
	ctx := context.Background()

	ptr := &CompressedPointer{
		OriginalTokens:   1000,
		CompressedTokens: 200,
		Strategy:         "strip",
		CreatedAt:        time.Now().Unix(),
		Size:             5000,
	}

	// Write
	if err := store.SetPointer(ctx, 1, "/sources/gmail/inbox", "result-1", "strip", ptr); err != nil {
		t.Fatal(err)
	}

	// Read back
	got := store.GetPointer(ctx, 1, "/sources/gmail/inbox", "result-1", "strip")
	if got == nil {
		t.Fatal("pointer not found after write")
	}
	if got.OriginalTokens != ptr.OriginalTokens {
		t.Errorf("OriginalTokens: got %d, want %d", got.OriginalTokens, ptr.OriginalTokens)
	}
	if got.CompressedTokens != ptr.CompressedTokens {
		t.Errorf("CompressedTokens: got %d, want %d", got.CompressedTokens, ptr.CompressedTokens)
	}
	if got.Size != ptr.Size {
		t.Errorf("Size: got %d, want %d", got.Size, ptr.Size)
	}
}

func TestStore_PointerMiss(t *testing.T) {
	store, _ := testStore(t)
	ctx := context.Background()

	got := store.GetPointer(ctx, 1, "/sources/gmail/inbox", "nonexistent", "strip")
	if got != nil {
		t.Error("expected nil for missing pointer")
	}
}

func TestStore_PointerStrategyIsolation(t *testing.T) {
	store, _ := testStore(t)
	ctx := context.Background()

	stripPtr := &CompressedPointer{Strategy: "strip", CompressedTokens: 100}
	chainPtr := &CompressedPointer{Strategy: "chain", CompressedTokens: 50}

	store.SetPointer(ctx, 1, "/q", "r1", "strip", stripPtr)
	store.SetPointer(ctx, 1, "/q", "r1", "chain", chainPtr)

	// Each strategy gets its own pointer
	gotStrip := store.GetPointer(ctx, 1, "/q", "r1", "strip")
	gotChain := store.GetPointer(ctx, 1, "/q", "r1", "chain")

	if gotStrip == nil || gotStrip.CompressedTokens != 100 {
		t.Errorf("strip pointer: got %+v", gotStrip)
	}
	if gotChain == nil || gotChain.CompressedTokens != 50 {
		t.Errorf("chain pointer: got %+v", gotChain)
	}
}

func TestStore_PointerTTL(t *testing.T) {
	store, mini := testStore(t)
	ctx := context.Background()

	store.SetPointer(ctx, 1, "/q", "r1", "strip", &CompressedPointer{Strategy: "strip"})

	// Should exist before TTL
	if got := store.GetPointer(ctx, 1, "/q", "r1", "strip"); got == nil {
		t.Error("pointer should exist before TTL")
	}

	// Fast-forward past TTL (1 minute configured in testStore)
	mini.FastForward(2 * time.Minute)

	// Should be gone
	if got := store.GetPointer(ctx, 1, "/q", "r1", "strip"); got != nil {
		t.Error("pointer should expire after TTL")
	}
}

// ---------------------------------------------------------------------------
// Content cache tests
// ---------------------------------------------------------------------------

func TestStore_ContentRoundTrip(t *testing.T) {
	store, _ := testStore(t)
	ctx := context.Background()

	content := []byte("compressed email content here")

	if err := store.SetContent(ctx, 1, "/q", "r1", "strip", content); err != nil {
		t.Fatal(err)
	}

	got := store.GetContent(ctx, 1, "/q", "r1", "strip")
	if got == nil {
		t.Fatal("content not found after write")
	}
	if string(got) != string(content) {
		t.Errorf("content mismatch: got %q, want %q", got, content)
	}
}

func TestStore_ContentMiss(t *testing.T) {
	store, _ := testStore(t)
	ctx := context.Background()

	got := store.GetContent(ctx, 1, "/q", "nonexistent", "strip")
	if got != nil {
		t.Error("expected nil for missing content")
	}
}

func TestStore_ContentStrategyIsolation(t *testing.T) {
	store, _ := testStore(t)
	ctx := context.Background()

	store.SetContent(ctx, 1, "/q", "r1", "strip", []byte("strip-data"))
	store.SetContent(ctx, 1, "/q", "r1", "chain", []byte("chain-data"))

	gotStrip := store.GetContent(ctx, 1, "/q", "r1", "strip")
	gotChain := store.GetContent(ctx, 1, "/q", "r1", "chain")

	if string(gotStrip) != "strip-data" {
		t.Errorf("strip content: got %q", gotStrip)
	}
	if string(gotChain) != "chain-data" {
		t.Errorf("chain content: got %q", gotChain)
	}
}

func TestStore_ContentTTL(t *testing.T) {
	store, mini := testStore(t)
	ctx := context.Background()

	store.SetContent(ctx, 1, "/q", "r1", "strip", []byte("will expire"))

	// Should exist now
	if got := store.GetContent(ctx, 1, "/q", "r1", "strip"); got == nil {
		t.Fatal("content should exist before TTL")
	}

	// Fast-forward past TTL (1 minute configured in testStore)
	mini.FastForward(2 * time.Minute)

	// Should be gone
	if got := store.GetContent(ctx, 1, "/q", "r1", "strip"); got != nil {
		t.Error("content should expire after TTL")
	}
}

// ---------------------------------------------------------------------------
// Budget enforcement
// ---------------------------------------------------------------------------

func TestStore_ContentBudgetEnforced(t *testing.T) {
	store, _ := testStore(t)
	ctx := context.Background()

	// Budget is 1024 bytes. Write 600 bytes — should succeed.
	data600 := make([]byte, 600)
	for i := range data600 {
		data600[i] = 'a'
	}
	if err := store.SetContent(ctx, 1, "/q", "r1", "strip", data600); err != nil {
		t.Fatal(err)
	}
	if got := store.GetContent(ctx, 1, "/q", "r1", "strip"); got == nil {
		t.Fatal("first write should succeed (under budget)")
	}

	// Write another 600 bytes — should be silently skipped (600 + 600 > 1024).
	data600b := make([]byte, 600)
	for i := range data600b {
		data600b[i] = 'b'
	}
	if err := store.SetContent(ctx, 1, "/q", "r2", "strip", data600b); err != nil {
		t.Fatal("SetContent should not return error on budget skip")
	}

	// Second content should NOT be cached
	if got := store.GetContent(ctx, 1, "/q", "r2", "strip"); got != nil {
		t.Error("second write should be skipped (over budget)")
	}

	// First content should still be there
	if got := store.GetContent(ctx, 1, "/q", "r1", "strip"); got == nil {
		t.Error("first content should still exist")
	}
}

func TestStore_ContentBudgetPerWorkspace(t *testing.T) {
	store, _ := testStore(t)
	ctx := context.Background()

	data := make([]byte, 800)

	// Workspace 1: write 800 bytes (under 1024 budget)
	store.SetContent(ctx, 1, "/q", "r1", "strip", data)
	if got := store.GetContent(ctx, 1, "/q", "r1", "strip"); got == nil {
		t.Fatal("workspace 1 write should succeed")
	}

	// Workspace 2: should have its own budget, so 800 bytes should also succeed
	store.SetContent(ctx, 2, "/q", "r1", "strip", data)
	if got := store.GetContent(ctx, 2, "/q", "r1", "strip"); got == nil {
		t.Error("workspace 2 should have independent budget")
	}
}

func TestStore_BudgetUsageCounterExpires(t *testing.T) {
	store, mini := testStore(t)
	ctx := context.Background()

	// Fill most of the budget
	data := make([]byte, 900)
	store.SetContent(ctx, 1, "/q", "r1", "strip", data)

	// Budget is now ~900/1024. Second write of 200 would exceed.
	over := make([]byte, 200)
	store.SetContent(ctx, 1, "/q", "r2", "strip", over)
	if got := store.GetContent(ctx, 1, "/q", "r2", "strip"); got != nil {
		t.Error("should be over budget before expiry")
	}

	// Fast-forward past usage counter TTL (2x content TTL = 2 minutes)
	mini.FastForward(3 * time.Minute)

	// Usage counter expired, so budget resets. New write should succeed.
	store.SetContent(ctx, 1, "/q", "r3", "strip", over)
	if got := store.GetContent(ctx, 1, "/q", "r3", "strip"); got == nil {
		t.Error("should succeed after usage counter expires (budget reset)")
	}
}

// ---------------------------------------------------------------------------
// Nil safety
// ---------------------------------------------------------------------------

func TestStore_NilRedis(t *testing.T) {
	store := NewCompressedStore(nil, DefaultConfig())
	ctx := context.Background()

	// All operations should be no-ops, not panics
	if got := store.GetPointer(ctx, 1, "/q", "r1", "strip"); got != nil {
		t.Error("nil redis GetPointer should return nil")
	}
	if err := store.SetPointer(ctx, 1, "/q", "r1", "strip", &CompressedPointer{}); err != nil {
		t.Error("nil redis SetPointer should return nil error")
	}
	if got := store.GetContent(ctx, 1, "/q", "r1", "strip"); got != nil {
		t.Error("nil redis GetContent should return nil")
	}
	if err := store.SetContent(ctx, 1, "/q", "r1", "strip", []byte("data")); err != nil {
		t.Error("nil redis SetContent should return nil error")
	}
}

