package vnode

import "sync"

// prefetchChunkSize controls the size of sequential read-ahead.
const prefetchChunkSize = 256 * 1024
const writeBufferMax = 1024 * 1024 // Coalesce writes up to 1MB

type handleState struct {
	path     string
	lastOff  int64
	lastSize int
	prefetch *prefetchState
	writeOff int64
	writeBuf []byte
	closed   bool
	mu       sync.Mutex
}

type prefetchState struct {
	offset int64
	data   []byte
	mtime  int64
	err    error
	ready  chan struct{}
}
