package vnode

import (
	"runtime"
	"testing"
	"time"
)

func TestStorageVNodeWriteInvalidatesContentCache(t *testing.T) {
	path := "/workspace/demo.txt"
	s := &StorageVNode{
		content: NewContentCache(),
		handles: make(map[FileHandle]*handleState),
		writes:  make(map[string]map[FileHandle]*handleState),
		nextFH:  1,
	}

	s.content.Set(path, []byte("old"), 123)
	if _, ok := s.content.Get(path, 123); !ok {
		t.Fatal("expected cached content before write")
	}

	fh := s.allocHandle(path)
	if _, err := s.Write(path, []byte("new"), 0, fh); err != nil {
		t.Fatalf("Write returned error: %v", err)
	}

	if _, ok := s.content.Get(path, 123); ok {
		t.Fatal("expected content cache to be invalidated after write")
	}
}

func TestStaleHandleEviction(t *testing.T) {
	s := &StorageVNode{
		content:    NewContentCache(),
		handles:    make(map[FileHandle]*handleState),
		writes:     make(map[string]map[FileHandle]*handleState),
		nextFH:     1,
		warmupDirs: make(map[string]time.Time),
		stopWarmup: make(chan struct{}),
		asyncWriter: NewAsyncWriter(func(path string, off int64, data []byte) error {
			return nil
		}),
	}

	for i := 0; i < 500; i++ {
		fh := s.allocHandle("/test/file.txt")
		state := s.getHandleState(fh)
		state.mu.Lock()
		state.writeBuf = make([]byte, 1024)
		state.mu.Unlock()
	}

	if got := s.OpenHandleCount(); got != 500 {
		t.Fatalf("expected 500 handles, got %d", got)
	}

	s.evictStaleHandles()
	if got := s.OpenHandleCount(); got != 500 {
		t.Fatalf("expected 500 handles after eviction (none stale), got %d", got)
	}

	s.mu.Lock()
	staleTime := time.Now().Add(-handleStaleTimeout - time.Minute)
	for _, state := range s.handles {
		state.mu.Lock()
		state.lastActivity = staleTime
		state.mu.Unlock()
	}
	s.mu.Unlock()

	s.evictStaleHandles()
	if got := s.OpenHandleCount(); got != 0 {
		t.Fatalf("expected 0 handles after eviction, got %d", got)
	}
}

func TestActiveHandlesNotEvicted(t *testing.T) {
	s := &StorageVNode{
		content:    NewContentCache(),
		handles:    make(map[FileHandle]*handleState),
		writes:     make(map[string]map[FileHandle]*handleState),
		nextFH:     1,
		warmupDirs: make(map[string]time.Time),
		stopWarmup: make(chan struct{}),
		asyncWriter: NewAsyncWriter(func(path string, off int64, data []byte) error {
			return nil
		}),
	}

	staleFH := s.allocHandle("/stale.txt")
	activeFH := s.allocHandle("/active.txt")

	s.mu.Lock()
	s.handles[staleFH].mu.Lock()
	s.handles[staleFH].lastActivity = time.Now().Add(-handleStaleTimeout - time.Minute)
	s.handles[staleFH].mu.Unlock()
	s.mu.Unlock()

	state := s.getHandleState(activeFH)
	state.mu.Lock()
	state.touch()
	state.mu.Unlock()

	s.evictStaleHandles()

	if got := s.OpenHandleCount(); got != 1 {
		t.Fatalf("expected 1 handle (active), got %d", got)
	}
	if s.getHandleState(activeFH) == nil {
		t.Fatal("active handle was evicted")
	}
	if s.getHandleState(staleFH) != nil {
		t.Fatal("stale handle was not evicted")
	}
}

func TestHandleEvictionFreesMemory(t *testing.T) {
	s := &StorageVNode{
		content:    NewContentCache(),
		handles:    make(map[FileHandle]*handleState),
		writes:     make(map[string]map[FileHandle]*handleState),
		nextFH:     1,
		warmupDirs: make(map[string]time.Time),
		stopWarmup: make(chan struct{}),
		asyncWriter: NewAsyncWriter(func(path string, off int64, data []byte) error {
			return nil
		}),
	}

	const numHandles = 200
	const bufSize = 512 * 1024
	for i := 0; i < numHandles; i++ {
		fh := s.allocHandle("/big/file.txt")
		state := s.getHandleState(fh)
		state.mu.Lock()
		state.writeBuf = make([]byte, bufSize)
		state.mu.Unlock()
	}

	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)

	s.mu.Lock()
	staleTime := time.Now().Add(-handleStaleTimeout - time.Minute)
	for _, state := range s.handles {
		state.mu.Lock()
		state.lastActivity = staleTime
		state.mu.Unlock()
	}
	s.mu.Unlock()

	s.evictStaleHandles()

	runtime.GC()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)

	if s.OpenHandleCount() != 0 {
		t.Fatalf("expected 0 handles, got %d", s.OpenHandleCount())
	}

	freedMB := int64(before.HeapAlloc-after.HeapAlloc) / (1024 * 1024)
	if freedMB < 30 {
		t.Logf("WARNING: only freed %dMB (before=%dMB, after=%dMB)",
			freedMB, before.HeapAlloc/1024/1024, after.HeapAlloc/1024/1024)
	}
}
