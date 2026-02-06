package vnode

import (
	"context"
	"errors"
	"io/fs"
	"strings"
	"sync"
	"time"

	"github.com/beam-cloud/airstore/pkg/types"
	pb "github.com/beam-cloud/airstore/proto"
	"github.com/winfsp/cgofuse/fuse"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

const rpcTimeout = 30 * time.Second // Per-RPC timeout for context operations

// ContextVNodeGRPC implements VirtualNode for S3-backed context storage.
// It supports all read and write operations.
type ContextVNodeGRPC struct {
	client      pb.ContextServiceClient
	token       string
	cache       *MetadataCache
	content     *ContentCache
	asyncWriter *AsyncWriter
	handles     map[FileHandle]*handleState
	writeMu     sync.Mutex
	writes      map[string]map[FileHandle]*handleState
	nextFH      FileHandle
	mu          sync.Mutex
}

// NewContextVNodeGRPC creates a new context virtual node.
func NewContextVNodeGRPC(conn *grpc.ClientConn, token string) *ContextVNodeGRPC {
	c := &ContextVNodeGRPC{
		client:  pb.NewContextServiceClient(conn),
		token:   token,
		cache:   NewMetadataCache(),
		content: NewContentCache(),
		handles: make(map[FileHandle]*handleState),
		writes:  make(map[string]map[FileHandle]*handleState),
		nextFH:  1,
	}
	c.asyncWriter = NewAsyncWriter(c.writeRange)
	return c
}

// ctx returns a context with auth token and timeout for RPC calls
func (c *ContextVNodeGRPC) ctx() (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
	if c.token != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+c.token)
	}
	return ctx, cancel
}

func (c *ContextVNodeGRPC) Prefix() string  { return SkillsPath }
func (c *ContextVNodeGRPC) Type() VNodeType { return VNodeWritable }

// rel returns the storage path (leading slash stripped, keeps skills prefix)
func (c *ContextVNodeGRPC) rel(path string) string {
	return strings.TrimPrefix(path, "/")
}

// Getattr returns file attributes with caching
func (c *ContextVNodeGRPC) Getattr(path string) (*FileInfo, error) {
	// Vnode root always exists (virtual directory, no RPC needed)
	if path == c.Prefix() {
		return NewDirInfo(PathIno(path)), nil
	}

	// AppleDouble files are zero-length placeholders
	if isAppleDoublePath(path) {
		return NewFileInfo(PathIno(path), 0, 0644), nil
	}

	// If we have dirty data in the async writer, report its size.
	// This ensures editors see the correct size after Truncate+Write
	// before the data is uploaded to S3.
	if data, _, ok := c.asyncWriter.Get(path); ok {
		uid, gid := GetOwner()
		return &FileInfo{
			Ino: PathIno(path), Size: int64(len(data)),
			Mode: 0100644, Nlink: 1, Uid: uid, Gid: gid,
			Atime: time.Now(), Mtime: time.Now(), Ctime: time.Now(),
		}, nil
	}

	// Check caches
	if c.cache.IsNegative(path) {
		return nil, fs.ErrNotExist
	}
	if info := c.cache.GetInfo(path); info != nil {
		return info, nil
	}

	// RPC to backend for actual files/subdirectories
	ctx, cancel := c.ctx()
	defer cancel()

	resp, err := c.client.Stat(ctx, &pb.ContextStatRequest{Path: c.rel(path)})
	if err != nil {
		return nil, err
	}
	if !resp.Ok {
		c.cache.SetNegative(path)
		return nil, fs.ErrNotExist
	}

	info := c.toFileInfo(path, resp.Info)
	c.cache.Set(path, info)
	return info, nil
}

// Readdir returns directory entries, caching child metadata from enriched response
func (c *ContextVNodeGRPC) Readdir(path string) ([]DirEntry, error) {
	if cached := c.cache.Get(path); cached != nil && cached.Children != nil {
		return cached.Children, nil
	}

	ctx, cancel := c.ctx()
	defer cancel()

	resp, err := c.client.ReadDir(ctx, &pb.ContextReadDirRequest{Path: c.rel(path)})
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
		// Skip macOS AppleDouble files and reserved folders at skills root
		if strings.HasPrefix(e.Name, "._") {
			continue
		}
		if path == SkillsPath && types.IsReservedFolder(e.Name) {
			continue
		}

		childPath := path + "/" + e.Name
		ino := PathIno(childPath)
		entries = append(entries, DirEntry{Name: e.Name, Mode: e.Mode, Ino: ino})

		mtime := now
		if e.Mtime > 0 {
			mtime = time.Unix(e.Mtime, 0)
		}
		uid, gid := GetOwner()
		childMeta[e.Name] = &FileInfo{
			Ino: ino, Size: e.Size, Mode: e.Mode, Nlink: 1,
			Uid: uid, Gid: gid,
			Atime: now, Mtime: mtime, Ctime: mtime,
		}
	}

	c.cache.SetWithChildren(path, entries, childMeta)
	return entries, nil
}

// Open opens a file, using cache to avoid redundant Stat calls.
// On macOS, some paths use Open with O_CREAT instead of Create, so we handle that here.
func (c *ContextVNodeGRPC) Open(path string, flags int) (FileHandle, error) {
	// Allow AppleDouble files to open without backend.
	if isAppleDoublePath(path) {
		return c.allocHandle("__appledouble__" + path), nil
	}

	if flags&fuse.O_CREAT != 0 {
		fh, err := c.openExisting(path)
		if err == nil {
			if flags&fuse.O_TRUNC != 0 {
				_ = c.Truncate(path, 0, fh)
			}
			return fh, nil
		}
		if errors.Is(err, fs.ErrNotExist) {
			// Default file mode when created via Open(O_CREAT)
			return c.Create(path, flags, 0644)
		}
		return 0, err
	}

	fh, err := c.openExisting(path)
	if err != nil {
		return 0, err
	}
	if flags&fuse.O_TRUNC != 0 {
		_ = c.Truncate(path, 0, fh)
	}
	return fh, nil
}

// openExisting opens an existing file and caches its metadata.
func (c *ContextVNodeGRPC) openExisting(path string) (FileHandle, error) {
	// Check cache first to avoid unnecessary RPC
	if c.cache.IsNegative(path) {
		return 0, fs.ErrNotExist
	}
	if c.cache.GetInfo(path) != nil {
		return c.allocHandle(path), nil
	}

	// Cache miss - verify file exists
	ctx, cancel := c.ctx()
	defer cancel()

	resp, err := c.client.Stat(ctx, &pb.ContextStatRequest{Path: c.rel(path)})
	if err != nil {
		return 0, err
	}
	if !resp.Ok {
		c.cache.SetNegative(path)
		return 0, fs.ErrNotExist
	}

	// Cache the result for future calls
	c.cache.Set(path, c.toFileInfo(path, resp.Info))
	return c.allocHandle(path), nil
}

// Read reads file data
func (c *ContextVNodeGRPC) Read(path string, buf []byte, off int64, fh FileHandle) (int, error) {
	// AppleDouble files are empty.
	if isAppleDoublePath(path) {
		return 0, nil
	}

	// Drain handleState buffers into asyncWriter (non-blocking).
	c.enqueueWritesForPath(path)

	// Serve from dirty buffer if the asyncWriter has pending data covering this range.
	if data, dataOff, ok := c.asyncWriter.Get(path); ok {
		dataEnd := dataOff + int64(len(data))
		if off >= dataOff && off < dataEnd {
			n := copy(buf, data[off-dataOff:])
			return n, nil
		}
		if off >= dataEnd {
			return 0, nil // EOF — file was truncated/rewritten
		}
		// Read is before the dirty range; force flush and fall through to S3.
		if err := c.asyncWriter.ForceFlush(path); err != nil {
			return 0, err
		}
	}

	state := c.getHandleState(fh)
	if state != nil {
		if data, ok, err := c.consumePrefetch(path, off, state); err != nil {
			return 0, err
		} else if ok {
			n := copy(buf, data)
			c.recordRead(state, path, off, n)
			return n, nil
		}
	}

	// Small file cache (mtime validated)
	if info, ok := c.maybeStatSmall(path); ok && info.Size <= smallFileMaxSize && info.Mtime != 0 {
		if data, ok := c.content.Get(path, info.Mtime); ok {
			if off >= int64(len(data)) {
				return 0, nil
			}
			n := copy(buf, data[off:])
			c.recordRead(state, path, off, n)
			return n, nil
		}

		data, err := c.readRange(path, 0, info.Size)
		if err != nil {
			return 0, err
		}
		c.content.Set(path, data, info.Mtime)
		if off >= int64(len(data)) {
			return 0, nil
		}
		n := copy(buf, data[off:])
		c.recordRead(state, path, off, n)
		return n, nil
	}

	// Regular read
	data, err := c.readRange(path, off, int64(len(buf)))
	if err != nil {
		return 0, err
	}
	n := copy(buf, data)
	c.recordRead(state, path, off, n)
	return n, nil
}

// Create creates a new file
func (c *ContextVNodeGRPC) Create(path string, flags int, mode uint32) (FileHandle, error) {
	// Silently accept AppleDouble files (._*) without creating them on backend.
	// These are used by macOS to store extended attributes/resource forks.
	// We pretend they're created successfully but discard all data.
	if isAppleDoublePath(path) {
		return c.allocHandle("__appledouble__" + path), nil
	}

	ctx, cancel := c.ctx()
	defer cancel()

	resp, err := c.client.Create(ctx, &pb.ContextCreateRequest{Path: c.rel(path), Mode: mode})
	if err != nil {
		return 0, err
	}
	if !resp.Ok {
		return 0, fs.ErrPermission
	}
	c.cache.Invalidate(path)
	return c.allocHandle(path), nil
}

// Write writes file data
func (c *ContextVNodeGRPC) Write(path string, buf []byte, off int64, fh FileHandle) (int, error) {
	// Silently discard writes to AppleDouble files
	if isAppleDoublePath(path) {
		return len(buf), nil // Pretend write succeeded
	}

	state := c.getHandleState(fh)
	if state == nil {
		// No file handle — write directly (synchronous).
		if err := c.writeRange(path, off, buf); err != nil {
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
			if err := c.writeRange(path, off, buf); err != nil {
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

	// Buffer overflow or non-contiguous: flush old buffer synchronously.
	// We use synchronous writeRange here (not Enqueue) because the asyncWriter
	// stores only ONE (offset, data) pair per path — sequential Enqueue calls
	// at different offsets would clobber each other.
	data := append([]byte(nil), state.writeBuf...)
	writeOff := state.writeOff
	state.writeBuf = nil
	state.mu.Unlock()

	if err := c.writeRange(path, writeOff, data); err != nil {
		return 0, err
	}

	if len(buf) >= writeBufferMax {
		if err := c.writeRange(path, off, buf); err != nil {
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

// Truncate changes file size
func (c *ContextVNodeGRPC) Truncate(path string, size int64, fh FileHandle) error {
	// AppleDouble files are discarded.
	if isAppleDoublePath(path) {
		return nil
	}

	if size == 0 {
		// Mark file as empty without starting a debounce timer. The real
		// content arrives via the next Write -> flushWriteBuffer -> Enqueue
		// (which starts the timer). This avoids the race where the timer
		// fires and uploads empty content before the Write data arrives.
		c.enqueueWritesForPath(path)
		c.asyncWriter.EnqueueNoTimer(path, 0, []byte{})
		c.cache.Invalidate(path)
		return nil
	}

	// Non-zero truncate is rare; keep it synchronous.
	if err := c.asyncWriter.ForceFlush(path); err != nil {
		return err
	}

	ctx, cancel := c.ctx()
	defer cancel()

	resp, err := c.client.Truncate(ctx, &pb.ContextTruncateRequest{Path: c.rel(path), Size: size})
	if err != nil {
		return err
	}
	if !resp.Ok {
		return fs.ErrPermission
	}
	c.cache.Invalidate(path)
	return nil
}

// Mkdir creates a directory
func (c *ContextVNodeGRPC) Mkdir(path string, mode uint32) error {
	ctx, cancel := c.ctx()
	defer cancel()

	resp, err := c.client.Mkdir(ctx, &pb.ContextMkdirRequest{Path: c.rel(path), Mode: mode})
	if err != nil {
		return err
	}
	if !resp.Ok {
		return fs.ErrPermission
	}
	c.cache.Invalidate(path)
	return nil
}

// Rmdir removes a directory
func (c *ContextVNodeGRPC) Rmdir(path string) error {
	ctx, cancel := c.ctx()
	defer cancel()

	resp, err := c.client.Delete(ctx, &pb.ContextDeleteRequest{Path: c.rel(path)})
	if err != nil {
		return err
	}
	if !resp.Ok {
		return fs.ErrPermission
	}
	c.cache.Invalidate(path)
	return nil
}

// Unlink removes a file
func (c *ContextVNodeGRPC) Unlink(path string) error {
	if isAppleDoublePath(path) {
		return nil
	}

	ctx, cancel := c.ctx()
	defer cancel()

	resp, err := c.client.Delete(ctx, &pb.ContextDeleteRequest{Path: c.rel(path)})
	if err != nil {
		return err
	}
	if !resp.Ok {
		return fs.ErrPermission
	}
	c.cache.Invalidate(path)
	return nil
}

// Rename moves or renames a file or directory
func (c *ContextVNodeGRPC) Rename(oldpath, newpath string) error {
	if isAppleDoublePath(oldpath) || isAppleDoublePath(newpath) {
		return nil
	}

	// Flush any dirty data for the old path before renaming.
	// The writes map is keyed by path; after rename, reads on the new path
	// would miss dirty data still keyed under the old path.
	c.enqueueWritesForPath(oldpath)
	_ = c.asyncWriter.ForceFlush(oldpath)

	ctx, cancel := c.ctx()
	defer cancel()

	resp, err := c.client.Rename(ctx, &pb.ContextRenameRequest{
		OldPath: c.rel(oldpath),
		NewPath: c.rel(newpath),
	})
	if err != nil {
		return err
	}
	if !resp.Ok {
		return fs.ErrPermission
	}

	c.cache.Invalidate(oldpath)
	c.cache.Invalidate(newpath)
	return nil
}

// Fsync forces all pending writes to be uploaded to S3.
func (c *ContextVNodeGRPC) Fsync(path string, fh FileHandle) error {
	if state := c.getHandleState(fh); state != nil {
		c.flushWriteBuffer(path, state)
	}
	return c.asyncWriter.ForceFlush(path)
}

// Symlink creates a symbolic link
func (c *ContextVNodeGRPC) Symlink(target, linkPath string) error {
	ctx, cancel := c.ctx()
	defer cancel()

	resp, err := c.client.Symlink(ctx, &pb.ContextSymlinkRequest{Target: target, LinkPath: c.rel(linkPath)})
	if err != nil {
		return err
	}
	if !resp.Ok {
		return fs.ErrPermission
	}
	c.cache.Invalidate(linkPath)
	return nil
}

// Readlink reads symlink target
func (c *ContextVNodeGRPC) Readlink(path string) (string, error) {
	ctx, cancel := c.ctx()
	defer cancel()

	resp, err := c.client.Readlink(ctx, &pb.ContextReadlinkRequest{Path: c.rel(path)})
	if err != nil {
		return "", err
	}
	if !resp.Ok {
		return "", fs.ErrNotExist
	}
	return resp.Target, nil
}

// Release closes a file handle
func (c *ContextVNodeGRPC) Release(path string, fh FileHandle) error {
	c.mu.Lock()
	state, ok := c.handles[fh]
	if !ok {
		c.mu.Unlock()
		return nil
	}
	c.mu.Unlock()

	// Drain handleState into asyncWriter, then force-upload to S3.
	// Release MUST be synchronous: after close() returns, data must be
	// on S3 because the FUSE-T SMB layer caches based on what was
	// persisted, and subsequent reads could bypass our dirty buffer.
	c.flushWriteBuffer(path, state)
	flushErr := c.asyncWriter.ForceFlush(path)

	// Always clean up the handle, even if flush failed.
	c.mu.Lock()
	state.mu.Lock()
	state.closed = true
	state.prefetch = nil
	state.mu.Unlock()
	delete(c.handles, fh)
	c.mu.Unlock()

	c.writeMu.Lock()
	if m, ok := c.writes[state.path]; ok {
		delete(m, fh)
		if len(m) == 0 {
			delete(c.writes, state.path)
		}
	}
	c.writeMu.Unlock()

	return flushErr
}

// helpers

func (c *ContextVNodeGRPC) allocHandle(path string) FileHandle {
	c.mu.Lock()
	defer c.mu.Unlock()
	fh := c.nextFH
	c.nextFH++
	state := &handleState{path: path}
	c.handles[fh] = state

	c.writeMu.Lock()
	if _, ok := c.writes[path]; !ok {
		c.writes[path] = make(map[FileHandle]*handleState)
	}
	c.writes[path][fh] = state
	c.writeMu.Unlock()

	return fh
}

func (c *ContextVNodeGRPC) getHandleState(fh FileHandle) *handleState {
	c.mu.Lock()
	state := c.handles[fh]
	c.mu.Unlock()
	return state
}

func (c *ContextVNodeGRPC) statInfo(path string) (*pb.FileInfo, error) {
	ctx, cancel := c.ctx()
	defer cancel()

	resp, err := c.client.Stat(ctx, &pb.ContextStatRequest{Path: c.rel(path)})
	if err != nil {
		return nil, err
	}
	if !resp.Ok {
		return nil, fs.ErrNotExist
	}
	return resp.Info, nil
}

// maybeStatSmall returns fresh stat info if the file might be small enough to cache.
func (c *ContextVNodeGRPC) maybeStatSmall(path string) (*pb.FileInfo, bool) {
	if info := c.cache.GetInfo(path); info != nil && info.Size > smallFileMaxSize {
		return nil, false
	}
	info, err := c.statInfo(path)
	if err != nil {
		return nil, false
	}
	c.cache.Set(path, c.toFileInfo(path, info))
	return info, true
}

func (c *ContextVNodeGRPC) readRange(path string, off int64, length int64) ([]byte, error) {
	ctx, cancel := c.ctx()
	defer cancel()

	resp, err := c.client.Read(ctx, &pb.ContextReadRequest{
		Path: c.rel(path), Offset: off, Length: length,
	})
	if err != nil {
		return nil, err
	}
	if !resp.Ok {
		return nil, fs.ErrNotExist
	}
	return resp.Data, nil
}

func (c *ContextVNodeGRPC) writeRange(path string, off int64, data []byte) error {
	ctx, cancel := c.ctx()
	defer cancel()

	resp, err := c.client.Write(ctx, &pb.ContextWriteRequest{
		Path: c.rel(path), Data: data, Offset: off,
	})
	if err != nil {
		return err
	}
	if !resp.Ok {
		return fs.ErrPermission
	}
	c.cache.Invalidate(path)
	return nil
}

func (c *ContextVNodeGRPC) recordRead(state *handleState, path string, off int64, n int) {
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
	go c.doPrefetch(path, pf, prefetchSize)
}

func (c *ContextVNodeGRPC) doPrefetch(path string, pf *prefetchState, length int64) {
	info, err := c.statInfo(path)
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

	data, err := c.readRange(path, pf.offset, length)
	if err != nil {
		pf.err = err
		close(pf.ready)
		return
	}
	pf.data = data
	close(pf.ready)
}

func (c *ContextVNodeGRPC) consumePrefetch(path string, off int64, state *handleState) ([]byte, bool, error) {
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

	info, err := c.statInfo(path)
	if err != nil {
		return nil, false, err
	}
	if info.Mtime != pf.mtime {
		return nil, false, nil
	}
	return pf.data, true, nil
}

func (c *ContextVNodeGRPC) flushWriteBuffer(path string, state *handleState) {
	state.mu.Lock()
	if state.closed || len(state.writeBuf) == 0 {
		state.mu.Unlock()
		return
	}
	data := append([]byte(nil), state.writeBuf...)
	writeOff := state.writeOff
	state.writeBuf = nil
	state.mu.Unlock()

	c.asyncWriter.Enqueue(path, writeOff, data)
}

// enqueueWritesForPath drains handleState write buffers into the asyncWriter (non-blocking).
func (c *ContextVNodeGRPC) enqueueWritesForPath(path string) {
	c.writeMu.Lock()
	entries := c.writes[path]
	states := make([]*handleState, 0, len(entries))
	for _, state := range entries {
		states = append(states, state)
	}
	c.writeMu.Unlock()

	for _, state := range states {
		c.flushWriteBuffer(path, state)
	}
}

// Cleanup flushes all pending async writes. Called when filesystem is unmounted.
func (c *ContextVNodeGRPC) Cleanup() {
	c.asyncWriter.Cleanup()
}

func (c *ContextVNodeGRPC) toFileInfo(path string, info *pb.FileInfo) *FileInfo {
	now := time.Now()
	mtime := now
	if info.Mtime > 0 {
		mtime = time.Unix(info.Mtime, 0)
	}
	uid, gid := GetOwner()
	return &FileInfo{
		Ino: PathIno(path), Size: info.Size, Mode: info.Mode, Nlink: 1,
		Uid: uid, Gid: gid,
		Atime: now, Mtime: mtime, Ctime: mtime,
	}
}
