package vnode

import (
	"sync"
	"time"

	"github.com/rs/zerolog/log"
)

const (
	// asyncDebounce is how long to wait after the last write before flushing.
	asyncDebounce = 500 * time.Millisecond
	// asyncMaxDelay caps how long writes can be buffered even with continuous updates.
	asyncMaxDelay = 2 * time.Second
	// asyncMaxRetries is how many times a failed async write is retried before
	// the data is dropped and an error is logged. Prevents infinite retry loops
	// when the backend is permanently unavailable.
	asyncMaxRetries = 5
)

// WriteFn uploads data to the backend (gRPC → S3). Called asynchronously.
type WriteFn func(path string, off int64, data []byte) error

// AsyncWriter buffers writes in memory and flushes to the backend
// asynchronously with debouncing. Multiple rapid writes to the same
// path are coalesced into a single upload.
type AsyncWriter struct {
	writeFn WriteFn
	pending map[string]*pendingWrite
	mu      sync.Mutex
}

type pendingWrite struct {
	off       int64
	data      []byte      // nil means nothing queued to upload
	timer     *time.Timer // debounce timer
	firstTime time.Time   // when first enqueued in this cycle (for max delay cap)
	flushDone chan error   // non-nil while an upload is in flight; receives result
	retries   int         // number of consecutive failed upload attempts
}

// NewAsyncWriter creates a new async writer that calls fn for uploads.
func NewAsyncWriter(fn WriteFn) *AsyncWriter {
	return &AsyncWriter{
		writeFn: fn,
		pending: make(map[string]*pendingWrite),
	}
}

// Enqueue buffers a write for async upload. Returns immediately.
// Repeated calls for the same path reset the debounce timer and replace
// the pending data, so only the latest version is uploaded.
func (aw *AsyncWriter) Enqueue(path string, off int64, data []byte) {
	dataCopy := make([]byte, len(data))
	copy(dataCopy, data)

	aw.mu.Lock()
	defer aw.mu.Unlock()

	pw := aw.pending[path]

	if pw != nil && pw.flushDone != nil {
		// Upload in progress — store new data; it will be picked up after
		// the current upload finishes (doFlush checks pw.data on completion).
		pw.data = dataCopy
		pw.off = off
		return
	}

	if pw == nil {
		pw = &pendingWrite{firstTime: time.Now()}
		aw.pending[path] = pw
	}

	pw.off = off
	pw.data = dataCopy

	if pw.timer != nil {
		pw.timer.Stop()
	}

	// If we've been buffering too long, flush immediately.
	if time.Since(pw.firstTime) >= asyncMaxDelay {
		go aw.doFlush(path)
		return
	}

	pw.timer = time.AfterFunc(asyncDebounce, func() {
		aw.doFlush(path)
	})
}

// EnqueueNoTimer stores data without starting the debounce timer.
// Used by Truncate(0) to mark the file as empty without racing against the
// subsequent Write that supplies the real content. The timer will be started
// by the next regular Enqueue (from flushWriteBuffer when real data arrives).
// If no Write follows, ForceFlush in Release/Fsync will still upload the data.
func (aw *AsyncWriter) EnqueueNoTimer(path string, off int64, data []byte) {
	dataCopy := make([]byte, len(data))
	copy(dataCopy, data)

	aw.mu.Lock()
	defer aw.mu.Unlock()

	pw := aw.pending[path]

	if pw != nil && pw.flushDone != nil {
		pw.data = dataCopy
		pw.off = off
		return
	}

	if pw == nil {
		pw = &pendingWrite{firstTime: time.Now()}
		aw.pending[path] = pw
	}

	pw.off = off
	pw.data = dataCopy

	// Stop any existing timer but do NOT start a new one.
	if pw.timer != nil {
		pw.timer.Stop()
		pw.timer = nil
	}
}

// DirtyFileInfo returns a FileInfo reflecting pending dirty data for a path.
// If fallbackMode is 0, defaults to S_IFREG|0644. Used by Getattr to report
// the correct size for files that have been written but not yet uploaded.
func (aw *AsyncWriter) DirtyFileInfo(path string, fallbackMode uint32) *FileInfo {
	aw.mu.Lock()
	pw := aw.pending[path]
	if pw == nil || pw.data == nil {
		aw.mu.Unlock()
		return nil
	}
	size := int64(len(pw.data))
	aw.mu.Unlock()

	if fallbackMode == 0 {
		fallbackMode = 0100644 // S_IFREG | 0644
	}
	uid, gid := GetOwner()
	now := time.Now()
	return &FileInfo{
		Ino: PathIno(path), Size: size,
		Mode: fallbackMode, Nlink: 1, Uid: uid, Gid: gid,
		Atime: now, Mtime: now, Ctime: now,
	}
}

// Get returns the pending (dirty) data for a path, if any.
// Used by Read to serve data that hasn't been uploaded yet.
func (aw *AsyncWriter) Get(path string) (data []byte, off int64, ok bool) {
	aw.mu.Lock()
	defer aw.mu.Unlock()

	pw := aw.pending[path]
	if pw != nil && pw.data != nil {
		return pw.data, pw.off, true
	}
	return nil, 0, false
}

// ForceFlush synchronously flushes any pending write for the given path.
// Blocks until all pending data has been uploaded. Used by Fsync and Release.
func (aw *AsyncWriter) ForceFlush(path string) error {
	for {
		aw.mu.Lock()
		pw := aw.pending[path]
		if pw == nil {
			aw.mu.Unlock()
			return nil
		}

		// If an upload is already in flight, wait for it to finish.
		if pw.flushDone != nil {
			ch := pw.flushDone
			aw.mu.Unlock()
			<-ch // wait for upload to complete
			continue // loop to check if new data arrived during upload
		}

		// Nothing to upload.
		if pw.data == nil {
			delete(aw.pending, path)
			aw.mu.Unlock()
			return nil
		}

		// Cancel debounce timer and upload synchronously.
		if pw.timer != nil {
			pw.timer.Stop()
			pw.timer = nil
		}

		data := pw.data
		off := pw.off
		pw.data = nil
		pw.flushDone = make(chan error, 1)
		aw.mu.Unlock()

		err := aw.writeFn(path, off, data)

		aw.mu.Lock()
		doneCh := pw.flushDone
		pw.flushDone = nil

		hasMore := pw.data != nil
		if !hasMore {
			delete(aw.pending, path)
		}
		aw.mu.Unlock()

		// Signal anyone waiting on the old done channel.
		doneCh <- err
		close(doneCh)

		if hasMore {
			continue // new data arrived during upload, flush it too
		}
		return err
	}
}

// doFlush is called by the debounce timer in a background goroutine.
func (aw *AsyncWriter) doFlush(path string) {
	aw.mu.Lock()
	pw := aw.pending[path]
	if pw == nil || pw.flushDone != nil || pw.data == nil {
		aw.mu.Unlock()
		return
	}

	data := pw.data
	off := pw.off
	pw.data = nil
	pw.flushDone = make(chan error, 1)
	if pw.timer != nil {
		pw.timer.Stop()
		pw.timer = nil
	}
	aw.mu.Unlock()

	err := aw.writeFn(path, off, data)

	aw.mu.Lock()
	doneCh := pw.flushDone
	pw.flushDone = nil

	if pw.data != nil {
		// New data was enqueued during upload — schedule another flush.
		// Reset retry counter since the data changed.
		pw.retries = 0
		pw.firstTime = time.Now()
		pw.timer = time.AfterFunc(asyncDebounce, func() {
			aw.doFlush(path)
		})
	} else if err != nil {
		pw.retries++
		if pw.retries >= asyncMaxRetries {
			// Give up after too many failures to avoid infinite retry loops.
			log.Error().Err(err).Str("path", path).Int("retries", pw.retries).
				Msg("async write failed permanently, data dropped")
			delete(aw.pending, path)
		} else {
			// Put the data back for retry.
			log.Warn().Err(err).Str("path", path).Int("retry", pw.retries).
				Msg("async write failed, will retry")
			pw.data = data
			pw.off = off
			pw.firstTime = time.Now()
			pw.timer = time.AfterFunc(asyncDebounce, func() {
				aw.doFlush(path)
			})
		}
	} else {
		delete(aw.pending, path)
	}
	aw.mu.Unlock()

	doneCh <- err
	close(doneCh)
}

// Cleanup flushes all pending writes synchronously. Called on unmount.
func (aw *AsyncWriter) Cleanup() {
	aw.mu.Lock()
	paths := make([]string, 0, len(aw.pending))
	for p := range aw.pending {
		paths = append(paths, p)
	}
	aw.mu.Unlock()

	for _, p := range paths {
		if err := aw.ForceFlush(p); err != nil {
			log.Warn().Err(err).Str("path", p).Msg("async writer cleanup flush failed")
		}
	}
}
