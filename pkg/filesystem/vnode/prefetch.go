package vnode

import (
	"sync"
	"time"
)

// prefetchChunkSize controls the size of sequential read-ahead.
const prefetchChunkSize = 256 * 1024
const writeBufferMax = 1024 * 1024 // Coalesce writes up to 1MB

// handleStaleTimeout is how long a handle can be idle before being evicted.
// Covers the case where FUSE Release is never received (process killed, 9P disconnect).
const handleStaleTimeout = 5 * time.Minute

// handleEvictionInterval is how often the stale-handle cleanup runs.
const handleEvictionInterval = 30 * time.Second

type handleState struct {
	path         string
	lastOff      int64
	lastSize     int
	prefetch     *prefetchState
	writeOff     int64
	writeBuf     []byte
	closed       bool
	lastActivity time.Time
	mu           sync.Mutex
}

func (h *handleState) touch() {
	h.lastActivity = time.Now()
}

type prefetchState struct {
	offset int64
	data   []byte
	mtime  int64
	err    error
	ready  chan struct{}
}
