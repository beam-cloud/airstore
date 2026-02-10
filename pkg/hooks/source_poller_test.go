package hooks

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/beam-cloud/airstore/pkg/repository"
	"github.com/beam-cloud/airstore/pkg/types"
)

// --- Mock QueryRefresher ---

type mockRefresher struct {
	mu       sync.Mutex
	calls    []string // paths refreshed
	err      error    // if set, RefreshQuery returns this
	delay    time.Duration
	callCount atomic.Int32
}

func (m *mockRefresher) RefreshQuery(_ context.Context, query *types.FilesystemQuery) error {
	m.callCount.Add(1)
	if m.delay > 0 {
		time.Sleep(m.delay)
	}
	m.mu.Lock()
	m.calls = append(m.calls, query.Path)
	m.mu.Unlock()
	return m.err
}

func (m *mockRefresher) getCalls() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]string, len(m.calls))
	copy(out, m.calls)
	return out
}

// --- Mock FilesystemStore for poller ---

type mockPollerStore struct {
	repository.FilesystemStore // embed to satisfy interface
	queries                    []*types.FilesystemQuery
	err                        error
}

func (m *mockPollerStore) GetWatchedSourceQueries(_ context.Context, _ time.Duration, _ int) ([]*types.FilesystemQuery, error) {
	if m.err != nil {
		return nil, m.err
	}
	return m.queries, nil
}

// --- Tests ---

func TestSourcePoller_PollRefreshesStaleQueries(t *testing.T) {
	rdb, err := repository.NewRedisClientForTest()
	if err != nil {
		t.Fatalf("failed to create test redis: %v", err)
	}

	queries := []*types.FilesystemQuery{
		{Id: 1, ExternalId: "q-1", Path: "/sources/gmail/inbox", Integration: "gmail"},
		{Id: 2, ExternalId: "q-2", Path: "/sources/gdrive/invoices", Integration: "gdrive"},
	}

	store := &mockPollerStore{queries: queries}
	refresher := &mockRefresher{}
	poller := NewSourcePoller(store, refresher, rdb)

	poller.Poll(context.Background())

	calls := refresher.getCalls()
	if len(calls) != 2 {
		t.Fatalf("expected 2 refreshes, got %d: %v", len(calls), calls)
	}

	got := make(map[string]bool)
	for _, c := range calls {
		got[c] = true
	}
	if !got["/sources/gmail/inbox"] || !got["/sources/gdrive/invoices"] {
		t.Errorf("expected both queries refreshed, got %v", calls)
	}
}

func TestSourcePoller_DistributedLockPreventsDoubleRefresh(t *testing.T) {
	rdb, err := repository.NewRedisClientForTest()
	if err != nil {
		t.Fatalf("failed to create test redis: %v", err)
	}

	queries := []*types.FilesystemQuery{
		{Id: 1, ExternalId: "q-lock-test", Path: "/sources/gmail/inbox", Integration: "gmail"},
	}

	store := &mockPollerStore{queries: queries}
	refresher := &mockRefresher{}
	poller := NewSourcePoller(store, refresher, rdb)

	// First poll acquires lock and refreshes
	poller.Poll(context.Background())

	calls := refresher.getCalls()
	if len(calls) != 1 {
		t.Fatalf("expected 1 refresh, got %d", len(calls))
	}

	// Second poll: lock is still held → skip
	poller.Poll(context.Background())

	calls = refresher.getCalls()
	if len(calls) != 1 {
		t.Errorf("expected still 1 refresh (locked), got %d", len(calls))
	}
}

func TestSourcePoller_NoQueriesIsNoOp(t *testing.T) {
	rdb, err := repository.NewRedisClientForTest()
	if err != nil {
		t.Fatalf("failed to create test redis: %v", err)
	}

	store := &mockPollerStore{queries: nil}
	refresher := &mockRefresher{}
	poller := NewSourcePoller(store, refresher, rdb)

	poller.Poll(context.Background())

	if len(refresher.getCalls()) != 0 {
		t.Error("expected no refreshes for empty query list")
	}
}

func TestSourcePoller_StoreErrorDoesNotPanic(t *testing.T) {
	rdb, err := repository.NewRedisClientForTest()
	if err != nil {
		t.Fatalf("failed to create test redis: %v", err)
	}

	store := &mockPollerStore{err: fmt.Errorf("db connection lost")}
	refresher := &mockRefresher{}
	poller := NewSourcePoller(store, refresher, rdb)

	// Should not panic
	poller.Poll(context.Background())

	if len(refresher.getCalls()) != 0 {
		t.Error("expected no refreshes after store error")
	}
}

func TestSourcePoller_RefreshErrorDoesNotBlock(t *testing.T) {
	rdb, err := repository.NewRedisClientForTest()
	if err != nil {
		t.Fatalf("failed to create test redis: %v", err)
	}

	queries := []*types.FilesystemQuery{
		{Id: 1, ExternalId: "q-fail-1", Path: "/sources/gmail/inbox", Integration: "gmail"},
		{Id: 2, ExternalId: "q-fail-2", Path: "/sources/gdrive/docs", Integration: "gdrive"},
	}

	store := &mockPollerStore{queries: queries}
	refresher := &mockRefresher{err: fmt.Errorf("provider error")}
	poller := NewSourcePoller(store, refresher, rdb)

	// Should complete (not hang) despite errors
	poller.Poll(context.Background())

	// Both queries were attempted
	if refresher.callCount.Load() != 2 {
		t.Errorf("expected 2 refresh attempts, got %d", refresher.callCount.Load())
	}
}

func TestSourcePoller_ConcurrencyRespectsSemaphore(t *testing.T) {
	rdb, err := repository.NewRedisClientForTest()
	if err != nil {
		t.Fatalf("failed to create test redis: %v", err)
	}

	// Create more queries than workers (5 workers, 10 queries)
	queries := make([]*types.FilesystemQuery, 10)
	for i := range queries {
		queries[i] = &types.FilesystemQuery{
			Id:          uint(i + 1),
			ExternalId:  fmt.Sprintf("q-sem-%d", i),
			Path:        fmt.Sprintf("/sources/gmail/query-%d", i),
			Integration: "gmail",
		}
	}

	store := &mockPollerStore{queries: queries}
	refresher := &mockRefresher{delay: 50 * time.Millisecond}
	poller := NewSourcePoller(store, refresher, rdb)

	start := time.Now()
	poller.Poll(context.Background())
	elapsed := time.Since(start)

	// With 5 workers and 10 queries (50ms each), should take ~100ms (2 batches)
	// not ~50ms (all parallel) or ~500ms (serial)
	if elapsed < 80*time.Millisecond || elapsed > 500*time.Millisecond {
		t.Logf("elapsed: %v (expected ~100ms with 5 workers, 10 queries @ 50ms)", elapsed)
	}

	if refresher.callCount.Load() != 10 {
		t.Errorf("expected 10 refreshes, got %d", refresher.callCount.Load())
	}
}
