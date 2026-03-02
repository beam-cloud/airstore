package vnode

import (
	"context"
	"errors"
	"io/fs"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/beam-cloud/airstore/pkg/types"
	pb "github.com/beam-cloud/airstore/proto"
	"github.com/winfsp/cgofuse/fuse"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

const storageRPCTimeout = 30 * time.Second
const storageWarmupInterval = 15 * time.Second // Background warmup interval
const storageWarmupMaxAge = 5 * time.Minute    // Max age for tracking warm directories

// StorageVNode handles S3-backed storage for any path via gRPC.
// Used as a fallback for paths not matched by specific vnodes.
type StorageVNode struct {
	client      pb.ContextServiceClient
	token       string
	bearerToken string // precomputed auth header value
	cache       *MetadataCache
	content     *ContentCache
	asyncWriter *AsyncWriter
	handles     map[FileHandle]*handleState
	writeMu     sync.Mutex
	writes      map[string]map[FileHandle]*handleState
	nextFH      FileHandle
	mu          sync.Mutex

	// Background warmup for frequently accessed directories
	warmupDirs map[string]time.Time // path -> last access time
	warmupMu   sync.Mutex
	stopWarmup chan struct{}
}

// NewStorageVNode creates a new StorageVNode.
func NewStorageVNode(conn *grpc.ClientConn, token string) *StorageVNode {
	s := &StorageVNode{
		client:      pb.NewContextServiceClient(conn),
		token:       token,
		bearerToken: BearerToken(token),
		cache:       NewMetadataCache(),
		content:     NewContentCache(),
		handles:     make(map[FileHandle]*handleState),
		writes:      make(map[string]map[FileHandle]*handleState),
		nextFH:      1,
		warmupDirs:  make(map[string]time.Time),
		stopWarmup:  make(chan struct{}),
	}
	s.asyncWriter = NewAsyncWriter(s.writeRange)
	go s.backgroundWarmupLoop()
	return s
}

// Cleanup stops background goroutines and flushes pending writes. Called when filesystem is unmounted.
func (s *StorageVNode) Cleanup() {
	close(s.stopWarmup)
	s.asyncWriter.Cleanup()
}

func (s *StorageVNode) ctx() (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithTimeout(context.Background(), storageRPCTimeout)
	if s.bearerToken != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, "authorization", s.bearerToken)
	}
	return ctx, cancel
}

// Prefix returns empty - this is a fallback handler
func (s *StorageVNode) Prefix() string  { return "" }
func (s *StorageVNode) Type() VNodeType { return VNodeWritable }

// rel returns the storage path (leading slash stripped)
func (s *StorageVNode) rel(path string) string {
	return strings.TrimPrefix(path, "/")
}

func (s *StorageVNode) Getattr(path string) (*FileInfo, error) {
	if path == "/" {
		return newFileInfo(PathIno(path), 0, normalizeRuntimeWritableMode(syscall.S_IFDIR|0755), 2), nil
	}

	// Treat AppleDouble files as zero-length placeholders.
	if isAppleDoublePath(path) {
		return NewFileInfo(PathIno(path), 0, 0644), nil
	}

	// If we have dirty local data (buffered or async), report it immediately.
	// This keeps size/read behavior correct before async upload completes.
	if info := s.localDirtyFileInfo(path, s.cachedMode(path)); info != nil {
		return info, nil
	}

	if info := s.cache.GetInfo(path); info != nil {
		return info, nil
	}
	if s.cache.IsNegative(path) {
		return nil, fs.ErrNotExist
	}

	ctx, cancel := s.ctx()
	defer cancel()

	resp, err := s.client.Stat(ctx, &pb.ContextStatRequest{Path: s.rel(path)})
	if err != nil {
		return nil, err
	}
	if !resp.Ok {
		s.cache.SetNegative(path)
		return nil, fs.ErrNotExist
	}

	info := s.toFileInfo(path, resp.Info)
	s.cache.Set(path, info)
	return info, nil
}

func (s *StorageVNode) Readdir(path string) ([]DirEntry, error) {
	// Track this directory for background warmup
	s.trackWarmDir(path)

	if cached := s.cache.Get(path); cached != nil && cached.Children != nil {
		return cached.Children, nil
	}

	ctx, cancel := s.ctx()
	defer cancel()

	resp, err := s.client.ReadDir(ctx, &pb.ContextReadDirRequest{Path: s.rel(path)})
	if err != nil {
		return nil, err
	}
	if !resp.Ok {
		return nil, fs.ErrNotExist
	}

	entries := make([]DirEntry, 0, len(resp.Entries))
	childMeta := make(map[string]*FileInfo, len(resp.Entries))
	now := time.Now()

	for _, e := range resp.Entries {
		// Skip macOS AppleDouble files and reserved folders at root
		if strings.HasPrefix(e.Name, "._") {
			continue
		}
		if path == "/" && types.IsReservedFolder(e.Name) {
			continue
		}

		childPath := path + "/" + e.Name
		if path == "/" {
			childPath = "/" + e.Name
		}
		mode := normalizeRuntimeWritableMode(e.Mode)
		size := e.Size
		if dirty, ok := s.localDirtySize(childPath, mode); ok && dirty > size {
			size = dirty
		}
		ino := PathIno(childPath)
		entries = append(entries, DirEntry{Name: e.Name, Mode: mode, Ino: ino, Size: size})

		mtime := now
		if e.Mtime > 0 {
			mtime = time.Unix(e.Mtime, 0)
		}
		uid, gid := GetOwner()
		childMeta[e.Name] = &FileInfo{
			Ino: ino, Size: size, Mode: mode, Nlink: 1,
			Uid: uid, Gid: gid,
			Atime: now, Mtime: mtime, Ctime: mtime,
		}
	}

	s.cache.SetWithChildren(path, entries, childMeta)
	return entries, nil
}

func (s *StorageVNode) Open(path string, flags int) (FileHandle, error) {
	// Allow AppleDouble files to open without backend.
	if isAppleDoublePath(path) {
		return s.allocHandle("__appledouble__" + path), nil
	}

	if flags&fuse.O_CREAT != 0 {
		fh, err := s.openExisting(path)
		if err == nil {
			if flags&fuse.O_TRUNC != 0 {
				_ = s.Truncate(path, 0, fh)
			}
			return fh, nil
		}
		if errors.Is(err, fs.ErrNotExist) {
			return s.Create(path, flags, 0644)
		}
		return 0, err
	}

	fh, err := s.openExisting(path)
	if err != nil {
		return 0, err
	}
	if flags&fuse.O_TRUNC != 0 {
		_ = s.Truncate(path, 0, fh)
	}
	return fh, nil
}

func (s *StorageVNode) openExisting(path string) (FileHandle, error) {
	// Prefer positive cache so fresh metadata wins over stale negatives.
	if s.cache.GetInfo(path) != nil {
		return s.allocHandle(path), nil
	}
	if s.cache.IsNegative(path) {
		return 0, fs.ErrNotExist
	}

	ctx, cancel := s.ctx()
	defer cancel()

	resp, err := s.client.Stat(ctx, &pb.ContextStatRequest{Path: s.rel(path)})
	if err != nil {
		return 0, err
	}
	if !resp.Ok {
		s.cache.SetNegative(path)
		return 0, fs.ErrNotExist
	}

	s.cache.Set(path, s.toFileInfo(path, resp.Info))
	return s.allocHandle(path), nil
}

func (s *StorageVNode) Read(path string, buf []byte, off int64, fh FileHandle) (int, error) {
	n, _, err := s.ReadWithAttribution(path, buf, off, fh)
	return n, err
}

func (s *StorageVNode) cachedReadOps() cachedReadOps {
	return cachedReadOps{
		content:         s.content,
		writer:          s.asyncWriter,
		getHandleState:  s.getHandleState,
		enqueueWrites:   s.enqueueWritesForPath,
		consumePrefetch: s.consumePrefetch,
		maybeStatSmall:  s.maybeStatSmall,
		readRange:       s.readRange,
		recordRead:      s.recordRead,
	}
}

func (s *StorageVNode) ReadWithAttribution(path string, buf []byte, off int64, fh FileHandle) (int, *ReadAttribution, error) {
	return readWithCachedFlow(path, buf, off, fh, s.cachedReadOps())
}

func (s *StorageVNode) Create(path string, flags int, mode uint32) (FileHandle, error) {
	// Silently accept AppleDouble files (._*) without creating them on backend.
	// These are used by macOS to store extended attributes/resource forks.
	if isAppleDoublePath(path) {
		return s.allocHandle("__appledouble__" + path), nil
	}

	ctx, cancel := s.ctx()
	defer cancel()

	resp, err := s.client.Create(ctx, &pb.ContextCreateRequest{
		Path: s.rel(path),
		Mode: sanitizeNodeMode(mode, syscall.S_IFREG, 0644),
	})
	if err != nil {
		return 0, err
	}
	if !resp.Ok {
		return 0, fs.ErrInvalid
	}

	invalidatePathCaches(s.cache, s.content, path)
	invalidateParentCache(s.cache, path)
	return s.allocHandle(path), nil
}

func (s *StorageVNode) Write(path string, buf []byte, off int64, fh FileHandle) (int, error) {
	// Silently discard writes to AppleDouble files
	if isAppleDoublePath(path) {
		return len(buf), nil // Pretend write succeeded
	}

	// Clear cached small-file content eagerly so readers never observe stale bytes
	// when writes happen within the same mtime second.
	s.content.Invalidate(path)

	state := s.getHandleState(fh)
	if state == nil {
		// No file handle — write directly (synchronous).
		if err := s.writeRange(path, off, buf); err != nil {
			return 0, err
		}
		return len(buf), nil
	}

	state.mu.Lock()
	if state.closed {
		state.mu.Unlock()
		return 0, fs.ErrInvalid
	}

	// Empty buffer: start buffering or write directly if too large.
	if len(state.writeBuf) == 0 {
		if len(buf) >= writeBufferMax {
			state.mu.Unlock()
			if err := s.writeRange(path, off, buf); err != nil {
				return 0, err
			}
			return len(buf), nil
		}
		state.writeOff = off
		state.writeBuf = make([]byte, len(buf))
		copy(state.writeBuf, buf)
		state.mu.Unlock()
		return len(buf), nil
	}

	// Contiguous and fits: append to existing buffer.
	if off == state.writeOff+int64(len(state.writeBuf)) && len(state.writeBuf)+len(buf) <= writeBufferMax {
		state.writeBuf = append(state.writeBuf, buf...)
		state.mu.Unlock()
		return len(buf), nil
	}

	// Non-contiguous or buffer overflow. Try to merge both ranges in memory
	// so the file reaches S3 as a single PutObject. Flushing a partial write
	// at offset A followed by a gateway merge-write at offset B fills the gap
	// [A+len(first), B) with NULLs when B > A+len(first).
	merged := mergeWriteBuffer(state.writeOff, state.writeBuf, off, buf)
	if merged != nil && len(merged.data) <= writeBufferMax {
		state.writeOff = merged.off
		state.writeBuf = merged.data
		state.mu.Unlock()
		return len(buf), nil
	}

	// Combined range too large: flush old buffer synchronously then start fresh.
	data := append([]byte(nil), state.writeBuf...)
	writeOff := state.writeOff
	state.writeBuf = nil
	state.mu.Unlock()

	if err := s.writeRange(path, writeOff, data); err != nil {
		return 0, err
	}

	if len(buf) >= writeBufferMax {
		if err := s.writeRange(path, off, buf); err != nil {
			return 0, err
		}
		return len(buf), nil
	}

	state.mu.Lock()
	if state.closed {
		state.mu.Unlock()
		return 0, fs.ErrInvalid
	}
	state.writeOff = off
	state.writeBuf = make([]byte, len(buf))
	copy(state.writeBuf, buf)
	state.mu.Unlock()
	return len(buf), nil
}

func (s *StorageVNode) Truncate(path string, size int64, fh FileHandle) error {
	// AppleDouble files are discarded.
	if isAppleDoublePath(path) {
		return nil
	}

	if size == 0 {
		// Mark file as empty without starting a debounce timer. The real
		// content arrives via the next Write -> flushWriteBuffer -> Enqueue
		// (which starts the timer). This avoids the race where the timer
		// fires and uploads empty content before the Write data arrives.
		s.enqueueWritesForPath(path)
		s.asyncWriter.EnqueueNoTimer(path, 0, []byte{})
		invalidatePathCaches(s.cache, s.content, path)
		return nil
	}

	// Non-zero truncate is rare; keep it synchronous.
	if err := s.asyncWriter.ForceFlush(path); err != nil {
		return err
	}

	ctx, cancel := s.ctx()
	defer cancel()

	resp, err := s.client.Truncate(ctx, &pb.ContextTruncateRequest{Path: s.rel(path), Size: size})
	if err != nil {
		return err
	}
	if !resp.Ok {
		return fs.ErrInvalid
	}

	invalidatePathCaches(s.cache, s.content, path)
	return nil
}

func (s *StorageVNode) Mkdir(path string, mode uint32) error {
	ctx, cancel := s.ctx()
	defer cancel()

	resp, err := s.client.Mkdir(ctx, &pb.ContextMkdirRequest{
		Path: s.rel(path),
		Mode: sanitizeNodeMode(mode, syscall.S_IFDIR, 0755),
	})
	if err != nil {
		return err
	}
	if !resp.Ok {
		return fs.ErrInvalid
	}

	invalidatePathCaches(s.cache, s.content, path)
	invalidateParentCache(s.cache, path)
	return nil
}

func (s *StorageVNode) Rmdir(path string) error {
	ctx, cancel := s.ctx()
	defer cancel()

	resp, err := s.client.Delete(ctx, &pb.ContextDeleteRequest{Path: s.rel(path)})
	if err != nil {
		return err
	}
	if !resp.Ok {
		return fs.ErrInvalid
	}

	invalidatePathCaches(s.cache, s.content, path)
	invalidateParentCache(s.cache, path)
	return nil
}

func (s *StorageVNode) Unlink(path string) error {
	if isAppleDoublePath(path) {
		return nil
	}

	ctx, cancel := s.ctx()
	defer cancel()

	resp, err := s.client.Delete(ctx, &pb.ContextDeleteRequest{Path: s.rel(path)})
	if err != nil {
		return err
	}
	if !resp.Ok {
		return fs.ErrInvalid
	}

	invalidatePathCaches(s.cache, s.content, path)
	invalidateParentCache(s.cache, path)
	return nil
}

// Rename moves or renames a file or directory
func (s *StorageVNode) Rename(oldpath, newpath string) error {
	if isAppleDoublePath(oldpath) || isAppleDoublePath(newpath) {
		return nil
	}

	// Flush any dirty data for the old path before renaming.
	// The writes map is keyed by path; after rename, reads on the new path
	// would miss dirty data still keyed under the old path.
	s.enqueueWritesForPath(oldpath)
	_ = s.asyncWriter.ForceFlush(oldpath)

	ctx, cancel := s.ctx()
	defer cancel()

	resp, err := s.client.Rename(ctx, &pb.ContextRenameRequest{
		OldPath: s.rel(oldpath),
		NewPath: s.rel(newpath),
	})
	if err != nil {
		return err
	}
	if !resp.Ok {
		return fs.ErrInvalid
	}

	invalidatePathCaches(s.cache, s.content, oldpath)
	invalidatePathCaches(s.cache, s.content, newpath)
	invalidateParentCache(s.cache, oldpath)
	invalidateParentCache(s.cache, newpath)
	return nil
}

// Chmod updates mode metadata for files/directories.
func (s *StorageVNode) Chmod(path string, mode uint32) error {
	if path == "/" {
		return nil
	}
	if isAppleDoublePath(path) {
		return nil
	}

	info, err := s.Getattr(path)
	if err != nil {
		return err
	}

	ctx, cancel := s.ctx()
	defer cancel()

	if info.Mode&syscall.S_IFMT == syscall.S_IFDIR {
		resp, err := s.client.Mkdir(ctx, &pb.ContextMkdirRequest{
			Path: s.rel(path),
			Mode: sanitizeNodeMode(mode, syscall.S_IFDIR, 0755),
		})
		if err != nil {
			return err
		}
		if !resp.Ok {
			return fs.ErrInvalid
		}
	} else {
		fileType := info.Mode & syscall.S_IFMT
		if fileType == 0 {
			fileType = syscall.S_IFREG
		}
		resp, err := s.client.Create(ctx, &pb.ContextCreateRequest{
			Path: s.rel(path),
			Mode: sanitizeNodeMode(mode, fileType, 0644),
		})
		if err != nil {
			return err
		}
		if !resp.Ok {
			return fs.ErrInvalid
		}
	}

	invalidatePathCaches(s.cache, s.content, path)
	invalidateParentCache(s.cache, path)
	return nil
}

func (s *StorageVNode) Symlink(target, linkPath string) error {
	ctx, cancel := s.ctx()
	defer cancel()

	resp, err := s.client.Symlink(ctx, &pb.ContextSymlinkRequest{
		Target: target, LinkPath: s.rel(linkPath),
	})
	if err != nil {
		return err
	}
	if !resp.Ok {
		return fs.ErrInvalid
	}

	invalidatePathCaches(s.cache, s.content, linkPath)
	invalidateParentCache(s.cache, linkPath)
	return nil
}

func (s *StorageVNode) Readlink(path string) (string, error) {
	ctx, cancel := s.ctx()
	defer cancel()

	resp, err := s.client.Readlink(ctx, &pb.ContextReadlinkRequest{Path: s.rel(path)})
	if err != nil {
		return "", err
	}
	if !resp.Ok {
		return "", fs.ErrInvalid
	}
	return resp.Target, nil
}

func (s *StorageVNode) Release(path string, fh FileHandle) error {
	s.mu.Lock()
	state, ok := s.handles[fh]
	if !ok {
		s.mu.Unlock()
		return nil
	}
	s.mu.Unlock()

	// Drain handleState into asyncWriter, then force-upload to S3.
	// Release MUST be synchronous: after close() returns, data must be
	// on S3 because the FUSE-T SMB layer caches based on what was
	// persisted, and subsequent reads could bypass our dirty buffer.
	s.flushWriteBuffer(path, state)
	flushErr := s.asyncWriter.ForceFlush(path)

	// Always clean up the handle, even if flush failed.
	s.mu.Lock()
	state.mu.Lock()
	state.closed = true
	state.prefetch = nil
	state.mu.Unlock()
	delete(s.handles, fh)
	s.mu.Unlock()

	s.writeMu.Lock()
	if m, ok := s.writes[state.path]; ok {
		delete(m, fh)
		if len(m) == 0 {
			delete(s.writes, state.path)
		}
	}
	s.writeMu.Unlock()

	return flushErr
}

func (s *StorageVNode) Fsync(path string, fh FileHandle) error {
	if state := s.getHandleState(fh); state != nil {
		s.flushWriteBuffer(path, state)
	}
	return s.asyncWriter.ForceFlush(path)
}

// helpers

func (s *StorageVNode) allocHandle(path string) FileHandle {
	s.mu.Lock()
	defer s.mu.Unlock()
	fh := s.nextFH
	s.nextFH++
	state := &handleState{path: path}
	s.handles[fh] = state

	s.writeMu.Lock()
	if _, ok := s.writes[path]; !ok {
		s.writes[path] = make(map[FileHandle]*handleState)
	}
	s.writes[path][fh] = state
	s.writeMu.Unlock()

	return fh
}

func (s *StorageVNode) getHandleState(fh FileHandle) *handleState {
	s.mu.Lock()
	state := s.handles[fh]
	s.mu.Unlock()
	return state
}

func (s *StorageVNode) statInfo(path string) (*pb.FileInfo, error) {
	ctx, cancel := s.ctx()
	defer cancel()

	resp, err := s.client.Stat(ctx, &pb.ContextStatRequest{Path: s.rel(path)})
	if err != nil {
		return nil, err
	}
	if !resp.Ok {
		return nil, fs.ErrNotExist
	}
	return resp.Info, nil
}

// maybeStatSmall returns fresh stat info if the file might be small enough to cache.
func (s *StorageVNode) maybeStatSmall(path string) (*pb.FileInfo, bool) {
	if info := s.cache.GetInfo(path); info != nil && info.Size > smallFileMaxSize {
		return nil, false
	}
	info, err := s.statInfo(path)
	if err != nil {
		return nil, false
	}
	s.cache.Set(path, s.toFileInfo(path, info))
	return info, true
}

func (s *StorageVNode) readRange(path string, off int64, length int64) ([]byte, error) {
	ctx, cancel := s.ctx()
	defer cancel()

	resp, err := s.client.Read(ctx, &pb.ContextReadRequest{
		Path: s.rel(path), Offset: off, Length: length,
	})
	if err != nil {
		return nil, err
	}
	if !resp.Ok {
		return nil, readResponseError(resp.Error)
	}
	return resp.Data, nil
}

func (s *StorageVNode) writeRange(path string, off int64, data []byte) error {
	ctx, cancel := s.ctx()
	defer cancel()

	resp, err := s.client.Write(ctx, &pb.ContextWriteRequest{
		Path: s.rel(path), Data: data, Offset: off,
	})
	if err != nil {
		return err
	}
	if !resp.Ok {
		return fs.ErrInvalid
	}
	invalidatePathCaches(s.cache, s.content, path)
	return nil
}

func (s *StorageVNode) recordRead(state *handleState, path string, off int64, n int) {
	if state == nil || n <= 0 {
		return
	}

	state.mu.Lock()
	sequential := state.lastSize > 0 && state.lastOff+int64(state.lastSize) == off
	state.lastOff = off
	state.lastSize = n

	if !sequential || state.prefetch != nil || state.closed {
		state.mu.Unlock()
		return
	}

	nextOff := off + int64(n)
	pf := &prefetchState{offset: nextOff, ready: make(chan struct{})}
	state.prefetch = pf
	state.mu.Unlock()

	prefetchSize := int64(n)
	if prefetchSize < prefetchChunkSize {
		prefetchSize = prefetchChunkSize
	}
	go s.doPrefetch(path, pf, prefetchSize)
}

func (s *StorageVNode) doPrefetch(path string, pf *prefetchState, length int64) {
	info, err := s.statInfo(path)
	if err != nil {
		pf.err = err
		close(pf.ready)
		return
	}
	if info.Mtime == 0 {
		// Can't validate freshness without mtime; skip prefetch.
		close(pf.ready)
		return
	}
	pf.mtime = info.Mtime

	data, err := s.readRange(path, pf.offset, length)
	if err != nil {
		pf.err = err
		close(pf.ready)
		return
	}
	pf.data = data
	close(pf.ready)
}

func (s *StorageVNode) consumePrefetch(path string, off int64, state *handleState) ([]byte, bool, error) {
	state.mu.Lock()
	pf := state.prefetch
	if pf == nil || pf.offset != off || state.closed {
		state.mu.Unlock()
		return nil, false, nil
	}
	state.mu.Unlock()

	<-pf.ready

	state.mu.Lock()
	if state.prefetch != pf {
		state.mu.Unlock()
		return nil, false, nil
	}
	state.prefetch = nil
	state.mu.Unlock()

	if pf.err != nil || pf.mtime == 0 {
		return nil, false, pf.err
	}

	info, err := s.statInfo(path)
	if err != nil {
		return nil, false, err
	}
	if info.Mtime != pf.mtime {
		return nil, false, nil
	}
	return pf.data, true, nil
}

func (s *StorageVNode) flushWriteBuffer(path string, state *handleState) {
	state.mu.Lock()
	if state.closed || len(state.writeBuf) == 0 {
		state.mu.Unlock()
		return
	}
	data := append([]byte(nil), state.writeBuf...)
	writeOff := state.writeOff
	state.writeBuf = nil
	state.mu.Unlock()

	writeOff, data = compactNulls(writeOff, data)
	s.asyncWriter.Enqueue(path, writeOff, data)
}

// enqueueWritesForPath drains handleState write buffers into the asyncWriter (non-blocking).
func (s *StorageVNode) enqueueWritesForPath(path string) {
	s.writeMu.Lock()
	entries := s.writes[path]
	states := make([]*handleState, 0, len(entries))
	for _, state := range entries {
		states = append(states, state)
	}
	s.writeMu.Unlock()

	for _, state := range states {
		s.flushWriteBuffer(path, state)
	}
}

func (s *StorageVNode) localDirtyFileInfo(path string, fallbackMode uint32) *FileInfo {
	size, ok := s.localDirtySize(path, fallbackMode)
	if !ok {
		return nil
	}

	if fallbackMode == 0 {
		fallbackMode = syscall.S_IFREG | 0644
	}
	mode := normalizeRuntimeWritableMode(fallbackMode)
	uid, gid := GetOwner()
	now := time.Now()
	return &FileInfo{
		Ino: PathIno(path), Size: size, Mode: mode, Nlink: 1,
		Uid: uid, Gid: gid,
		Atime: now, Mtime: now, Ctime: now,
	}
}

func (s *StorageVNode) localDirtySize(path string, fallbackMode uint32) (int64, bool) {
	var (
		size     int64
		hasDirty bool
	)

	if info := s.cache.GetInfo(path); info != nil {
		size = info.Size
	}

	if info := s.asyncWriter.DirtyFileInfo(path, fallbackMode); info != nil {
		hasDirty = true
		if info.Size > size {
			size = info.Size
		}
	}

	if bufferedSize, ok := s.bufferedHandleSize(path); ok {
		hasDirty = true
		if bufferedSize > size {
			size = bufferedSize
		}
	}

	return size, hasDirty
}

func (s *StorageVNode) bufferedHandleSize(path string) (int64, bool) {
	s.writeMu.Lock()
	entries := s.writes[path]
	states := make([]*handleState, 0, len(entries))
	for _, state := range entries {
		states = append(states, state)
	}
	s.writeMu.Unlock()

	var (
		size int64
		ok   bool
	)
	for _, state := range states {
		state.mu.Lock()
		if !state.closed && len(state.writeBuf) > 0 {
			end := state.writeOff + int64(len(state.writeBuf))
			if !ok || end > size {
				size = end
			}
			ok = true
		}
		state.mu.Unlock()
	}
	return size, ok
}

// cachedMode returns the file mode from the metadata cache, or 0 if not cached.
func (s *StorageVNode) cachedMode(path string) uint32 {
	if info := s.cache.GetInfo(path); info != nil {
		return info.Mode
	}
	return 0
}

func (s *StorageVNode) toFileInfo(path string, info *pb.FileInfo) *FileInfo {
	now := time.Now()
	mtime := now
	if info.Mtime > 0 {
		mtime = time.Unix(info.Mtime, 0)
	}
	uid, gid := GetOwner()
	mode := info.Mode
	if info.IsDir {
		mode = withNodeFileType(mode, syscall.S_IFDIR)
	}
	mode = normalizeRuntimeWritableMode(mode)
	return &FileInfo{
		Ino: PathIno(path), Size: info.Size, Mode: mode, Nlink: 1,
		Uid: uid, Gid: gid,
		Atime: now, Mtime: mtime, Ctime: mtime,
	}
}

// Background warmup methods

// trackWarmDir records a directory as recently accessed for background warmup
func (s *StorageVNode) trackWarmDir(path string) {
	s.warmupMu.Lock()
	s.warmupDirs[path] = time.Now()
	s.warmupMu.Unlock()
}

// getWarmDirs returns directories accessed within the warmup max age
func (s *StorageVNode) getWarmDirs() []string {
	s.warmupMu.Lock()
	defer s.warmupMu.Unlock()

	cutoff := time.Now().Add(-storageWarmupMaxAge)
	dirs := make([]string, 0, len(s.warmupDirs))
	for path, accessed := range s.warmupDirs {
		if accessed.After(cutoff) {
			dirs = append(dirs, path)
		}
	}
	return dirs
}

// cleanupOldWarmDirs removes directories not accessed recently
func (s *StorageVNode) cleanupOldWarmDirs() {
	s.warmupMu.Lock()
	defer s.warmupMu.Unlock()

	cutoff := time.Now().Add(-storageWarmupMaxAge)
	for path, accessed := range s.warmupDirs {
		if accessed.Before(cutoff) {
			delete(s.warmupDirs, path)
		}
	}
}

// backgroundWarmupLoop periodically refreshes caches for frequently accessed directories
func (s *StorageVNode) backgroundWarmupLoop() {
	ticker := time.NewTicker(storageWarmupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-s.stopWarmup:
			return
		case <-ticker.C:
			s.doBackgroundWarmup()
		}
	}
}

// doBackgroundWarmup refreshes recently accessed directories
func (s *StorageVNode) doBackgroundWarmup() {
	dirs := s.getWarmDirs()

	for _, path := range dirs {
		ctx, cancel := s.ctx()
		resp, err := s.client.ReadDir(ctx, &pb.ContextReadDirRequest{Path: s.rel(path)})
		cancel()

		if err != nil || !resp.Ok {
			continue
		}

		// Update cache with fresh directory listing
		entries := make([]DirEntry, 0, len(resp.Entries))
		childMeta := make(map[string]*FileInfo, len(resp.Entries))
		now := time.Now()

		for _, e := range resp.Entries {
			if strings.HasPrefix(e.Name, "._") {
				continue
			}
			if path == "/" && types.IsReservedFolder(e.Name) {
				continue
			}

			childPath := path + "/" + e.Name
			if path == "/" {
				childPath = "/" + e.Name
			}
			mode := normalizeRuntimeWritableMode(e.Mode)
			size := e.Size
			if dirty, ok := s.localDirtySize(childPath, mode); ok && dirty > size {
				size = dirty
			}
			ino := PathIno(childPath)
			entries = append(entries, DirEntry{Name: e.Name, Mode: mode, Ino: ino, Size: size})

			mtime := now
			if e.Mtime > 0 {
				mtime = time.Unix(e.Mtime, 0)
			}
			uid, gid := GetOwner()
			childMeta[e.Name] = &FileInfo{
				Ino: ino, Size: size, Mode: mode, Nlink: 1,
				Uid: uid, Gid: gid,
				Atime: now, Mtime: mtime, Ctime: mtime,
			}
		}

		s.cache.SetWithChildren(path, entries, childMeta)
	}

	// Cleanup old tracking data
	s.cleanupOldWarmDirs()
}
