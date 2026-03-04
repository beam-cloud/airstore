package vnode

import (
	"fmt"
	"io/fs"
	"strings"

	pb "github.com/beam-cloud/airstore/proto"
)

// readResponseError maps a gateway Read error string to the appropriate
// filesystem error. Only "not found" is mapped to ErrNotExist; everything
// else becomes a generic error (which toErrno maps to EIO).
func readResponseError(msg string) error {
	if strings.Contains(msg, "not found") {
		return fs.ErrNotExist
	}
	return fmt.Errorf("read: %s", msg)
}

type cachedReadOps struct {
	content *ContentCache
	writer  *AsyncWriter

	getHandleState   func(FileHandle) *handleState
	peekHandleWrites func(string) (int64, []byte, bool)
	consumePrefetch  func(string, int64, *handleState) ([]byte, bool, error)
	maybeStatSmall   func(string) (*pb.FileInfo, bool)
	readRange        func(string, int64, int64) ([]byte, error)
	recordRead       func(*handleState, string, int64, int)
}

// readWithCachedFlow centralizes the shared read path used by writable
// context-like vnodes (ContextVNodeGRPC and StorageVNode).
func readWithCachedFlow(path string, buf []byte, off int64, fh FileHandle, ops cachedReadOps) (int, *ReadAttribution, error) {
	if isAppleDoublePath(path) {
		return 0, AttributionForCache(CacheSourceSynthetic), nil
	}

	// Peek at per-handle write buffers non-destructively. This avoids
	// draining the writer's in-progress buffer, which would split a
	// multi-pwrite sequence and cause the AsyncWriter to lose earlier
	// sections when later writes replace them.
	if dataOff, data, ok := ops.peekHandleWrites(path); ok {
		dataEnd := dataOff + int64(len(data))
		if off >= dataOff && off < dataEnd {
			n := copy(buf, data[off-dataOff:])
			return n, AttributionForCache(CacheSourceDirtyBuffer), nil
		}
		if off >= dataEnd {
			return 0, AttributionForCache(CacheSourceDirtyBuffer), nil
		}
	}

	if data, dataOff, ok := ops.writer.Get(path); ok {
		dataEnd := dataOff + int64(len(data))
		if off >= dataOff && off < dataEnd {
			n := copy(buf, data[off-dataOff:])
			return n, AttributionForCache(CacheSourceDirtyBuffer), nil
		}
		if off >= dataEnd {
			return 0, AttributionForCache(CacheSourceDirtyBuffer), nil
		}
		if err := ops.writer.ForceFlush(path); err != nil {
			return 0, nil, err
		}
	}

	state := ops.getHandleState(fh)
	if state != nil {
		if data, ok, err := ops.consumePrefetch(path, off, state); err != nil {
			return 0, nil, err
		} else if ok {
			n := copy(buf, data)
			ops.recordRead(state, path, off, n)
			return n, AttributionForCache(CacheSourcePrefetch), nil
		}
	}

	if info, ok := ops.maybeStatSmall(path); ok && info.Size <= smallFileMaxSize && info.Mtime != 0 {
		if data, ok := ops.content.Get(path, info.Mtime); ok {
			if off >= int64(len(data)) {
				return 0, AttributionForCache(CacheSourceContentCache), nil
			}
			n := copy(buf, data[off:])
			ops.recordRead(state, path, off, n)
			return n, AttributionForCache(CacheSourceContentCache), nil
		}

		data, err := ops.readRange(path, 0, info.Size)
		if err != nil {
			return 0, nil, err
		}
		_, data = compactNulls(0, data)
		ops.content.Set(path, data, info.Mtime)
		if off >= int64(len(data)) {
			return 0, AttributionForCache(CacheSourceBackendRPC), nil
		}
		n := copy(buf, data[off:])
		ops.recordRead(state, path, off, n)
		return n, AttributionForCache(CacheSourceBackendRPC), nil
	}

	data, err := ops.readRange(path, off, int64(len(buf)))
	if err != nil {
		return 0, nil, err
	}
	n := copy(buf, data)
	ops.recordRead(state, path, off, n)
	return n, AttributionForCache(CacheSourceBackendRPC), nil
}
