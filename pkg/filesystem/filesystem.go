package filesystem

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/beam-cloud/airstore/pkg/filesystem/vnode"
	"github.com/rs/zerolog/log"
	"github.com/winfsp/cgofuse/fuse"
)

// Config configures the filesystem mount
type Config struct {
	MountPoint  string
	GatewayAddr string
	Token       string
	Verbose     bool
}

// Filesystem is a FUSE filesystem that connects to the gateway via gRPC.
type Filesystem struct {
	config   Config
	metadata LegacyMetadataEngine
	vnodes   *vnode.Registry
	rootID   string
	verbose  bool
	trace    *FuseTrace

	host      *fuse.FileSystemHost
	mounted   bool
	destroyed bool // Set when Destroy() is called by FUSE layer
	mu        sync.Mutex
}

// LegacyMetadataEngine provides filesystem metadata operations via gRPC.
// This interface is for backward compatibility with the old FUSE implementation.
// New code should use the path-based MetadataEngine interface.
type LegacyMetadataEngine interface {
	GetDirectoryContentMetadata(id string) (*DirectoryContentMetadata, error)
	GetDirectoryAccessMetadata(pid, name string) (*DirectoryAccessMetadata, error)
	GetFileMetadata(pid, name string) (*FileMetadata, error)
	SaveDirectoryContentMetadata(meta *DirectoryContentMetadata) error
	SaveDirectoryAccessMetadata(meta *DirectoryAccessMetadata) error
	SaveFileMetadata(meta *FileMetadata) error
	ListDirectory(path string) []DirEntry
	RenameDirectory(oldPID, oldName, newPID, newName string, version int) error
	DeleteDirectory(parentID, name string, version int) error
}

// NewFilesystem creates a new Filesystem that connects to the gateway via gRPC.
// All filesystem operations go through the gateway which handles Redis/S3.
func NewFilesystem(cfg Config) (*Filesystem, error) {
	if cfg.MountPoint == "" {
		cfg.MountPoint = "/tmp/airstore"
	}
	if cfg.GatewayAddr == "" {
		cfg.GatewayAddr = "localhost:1993"
	}

	metadata, err := NewGRPCMetadataEngine(GRPCConfig{
		GatewayAddr: cfg.GatewayAddr,
		Token:       cfg.Token,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create gRPC metadata engine: %w", err)
	}

	if cfg.Verbose {
		log.Debug().Str("gateway", cfg.GatewayAddr).Msg("connected to gateway")
	}

	fs := &Filesystem{
		config:   cfg,
		verbose:  cfg.Verbose,
		metadata: metadata,
		vnodes:   vnode.NewRegistry(),
		rootID:   GenerateDirectoryID("", "/", 0),
		trace:    newFuseTraceFromEnv(),
	}

	if err := fs.initRoot(); err != nil {
		return nil, err
	}

	return fs, nil
}

// RegisterVNode registers a virtual node handler for a path prefix
func (f *Filesystem) RegisterVNode(node vnode.VirtualNode) {
	f.vnodes.Register(node)
}

// SetStorageFallback sets the fallback vnode for unmatched storage paths
func (f *Filesystem) SetStorageFallback(node vnode.VirtualNode) {
	f.vnodes.SetFallback(node)
}

func (f *Filesystem) initRoot() error {
	if _, err := f.metadata.GetDirectoryAccessMetadata("", "/"); err != nil {
		meta := &DirectoryAccessMetadata{
			PID:        "",
			ID:         f.rootID,
			Permission: syscall.S_IFDIR | 0755,
		}
		if err := f.metadata.SaveDirectoryAccessMetadata(meta); err != nil {
			return err
		}
	}

	if _, err := f.metadata.GetDirectoryContentMetadata(f.rootID); err != nil {
		meta := &DirectoryContentMetadata{
			Id:         f.rootID,
			EntryList:  []string{},
			Timestamps: make(map[string]time.Time),
		}
		if err := f.metadata.SaveDirectoryContentMetadata(meta); err != nil {
			return err
		}
	}

	return nil
}

func (f *Filesystem) Mount() error {
	f.mu.Lock()
	if f.mounted {
		f.mu.Unlock()
		return fmt.Errorf("already mounted")
	}
	f.mu.Unlock()

	if err := os.MkdirAll(f.config.MountPoint, 0755); err != nil {
		return err
	}

	f.host = fuse.NewFileSystemHost(newAdapter(f))

	opts := f.mountOptions()
	if f.verbose {
		log.Debug().Str("path", f.config.MountPoint).Msg("mounting filesystem")
	}

	f.mu.Lock()
	f.mounted = true
	f.mu.Unlock()

	stopTrace := make(chan struct{})
	if f.trace != nil {
		log.Info().Str("mount", f.config.MountPoint).Msg("fuse trace enabled (AIRSTORE_FUSE_TRACE=1)")
		go f.trace.reportLoop(stopTrace, f.config.MountPoint)
	}

	ok := f.host.Mount(f.config.MountPoint, opts)

	if f.trace != nil {
		close(stopTrace)
	}

	f.mu.Lock()
	f.mounted = false
	f.mu.Unlock()

	if !ok {
		return fmt.Errorf("mount failed")
	}
	return nil
}

func (f *Filesystem) Unmount() error {
	f.mu.Lock()
	host := f.host
	destroyed := f.destroyed
	f.mu.Unlock()

	if destroyed || host == nil {
		return nil
	}

	// Note: host.Unmount may block depending on the FUSE backend.
	// Callers that need a hard timeout should enforce it at a higher level (e.g., CLI).
	_ = host.Unmount()
	return nil
}

func (f *Filesystem) IsMounted() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.mounted
}

func (f *Filesystem) IsDestroyed() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.destroyed
}

func (f *Filesystem) logDebug(msg string) {
	if f.verbose {
		log.Debug().Msg(msg)
	}
}

func (f *Filesystem) Init() error { return nil }
func (f *Filesystem) Destroy() {
	f.mu.Lock()
	f.destroyed = true
	f.mu.Unlock()

	for _, vn := range f.vnodes.List() {
		if c, ok := vn.(interface{ Cleanup() }); ok {
			c.Cleanup()
		}
	}
}

func (f *Filesystem) Statfs() (*StatInfo, error) {
	return &StatInfo{
		Bsize:   4096,
		Blocks:  1 << 30,
		Bfree:   1 << 29,
		Bavail:  1 << 29,
		Files:   1 << 20,
		Ffree:   1 << 19,
		Namemax: 255,
	}, nil
}

func (f *Filesystem) Getattr(path string) (*FileInfo, error) {
	if path == "/" {
		return f.rootInfo(), nil
	}

	// Fast path for macOS system files to avoid slow RPC lookups.
	name := filepath.Base(path)
	if isMacSystemName(name) {
		return nil, ErrNotFound
	}
	if isAppleDoubleName(name) || isMacResourceName(name) {
		return macPlaceholderInfo(path), nil
	}

	// Check for virtual node match
	if vn := f.vnodes.Match(path); vn != nil {
		info, err := vn.Getattr(path)
		if err != nil {
			return nil, err
		}
		return &FileInfo{
			Ino:   info.Ino,
			Size:  info.Size,
			Mode:  info.Mode,
			Nlink: info.Nlink,
			Uid:   info.Uid,
			Gid:   info.Gid,
			Atime: info.Atime,
			Mtime: info.Mtime,
			Ctime: info.Ctime,
		}, nil
	}

	parent, name := splitPath(path)
	parentID := f.resolveDir(parent)

	if meta, err := f.metadata.GetDirectoryAccessMetadata(parentID, name); err == nil {
		return &FileInfo{
			Ino:   hashToIno(meta.ID),
			Mode:  meta.Permission,
			Nlink: 2,
			Uid:   uint32(syscall.Getuid()),
			Gid:   uint32(syscall.Getgid()),
			Atime: time.Now(),
			Mtime: time.Now(),
			Ctime: time.Now(),
		}, nil
	}

	if meta, err := f.metadata.GetFileMetadata(parentID, name); err == nil {
		return &FileInfo{
			Ino:   hashToIno(meta.ID),
			Size:  int64(len(meta.FileData)),
			Mode:  syscall.S_IFREG | 0644,
			Nlink: 1,
			Uid:   uint32(syscall.Getuid()),
			Gid:   uint32(syscall.Getgid()),
			Atime: time.Now(),
			Mtime: time.Now(),
			Ctime: time.Now(),
		}, nil
	}

	// Try fallback storage for unmatched paths
	if fb := f.vnodes.Fallback(); fb != nil {
		if info, err := fb.Getattr(path); err == nil {
			return &FileInfo{
				Ino: info.Ino, Size: info.Size, Mode: info.Mode, Nlink: info.Nlink,
				Uid: info.Uid, Gid: info.Gid, Atime: info.Atime, Mtime: info.Mtime, Ctime: info.Ctime,
			}, nil
		}
	}

	return nil, ErrNotFound
}

func (f *Filesystem) Opendir(path string) (FileHandle, error) {
	// Virtual nodes are handled through Getattr
	if _, err := f.Getattr(path); err != nil {
		return 0, err
	}
	return 0, nil
}

func (f *Filesystem) Readdir(path string) ([]DirEntry, error) {
	// Check for virtual node match
	if vn := f.vnodes.Match(path); vn != nil {
		vnEntries, err := vn.Readdir(path)
		if err != nil {
			return nil, err
		}
		entries := make([]DirEntry, len(vnEntries))
		for i, e := range vnEntries {
			entries[i] = DirEntry{Name: e.Name, Mode: e.Mode, Ino: e.Ino, Size: e.Size, Mtime: e.Mtime}
		}
		return entries, nil
	}

	// For root, include virtual node directories
	if path == "/" {
		return f.readdirRoot(), nil
	}

	// Try storage fallback for unmatched paths
	if fb := f.vnodes.Fallback(); fb != nil {
		if vnEntries, err := fb.Readdir(path); err == nil {
			entries := make([]DirEntry, len(vnEntries))
			for i, e := range vnEntries {
				entries[i] = DirEntry{Name: e.Name, Mode: e.Mode, Ino: e.Ino, Size: e.Size, Mtime: e.Mtime}
			}
			return entries, nil
		}
	}

	// Fallback to local metadata
	content, err := f.metadata.GetDirectoryContentMetadata(f.resolveDir(path))
	if err != nil {
		return nil, ErrNotFound
	}

	entries := make([]DirEntry, 0, len(content.EntryList))
	for _, name := range content.EntryList {
		entryPath := path + "/" + name
		mode := uint32(syscall.S_IFREG | 0644)
		var ino uint64
		if info, err := f.Getattr(entryPath); err == nil {
			mode = info.Mode
			ino = info.Ino
		}
		entries = append(entries, DirEntry{Name: name, Mode: mode, Ino: ino})
	}

	return entries, nil
}

func (f *Filesystem) readdirRoot() []DirEntry {
	seen := make(map[string]bool)
	entries := make([]DirEntry, 0)

	// Add virtual node root directories (e.g., "tools", "sources", "skills", "tasks")
	for _, vn := range f.vnodes.List() {
		prefix := vn.Prefix()
		name := strings.TrimPrefix(prefix, "/")
		if name != "" && !strings.Contains(name, "/") && !seen[name] {
			if info, err := vn.Getattr(prefix); err == nil {
				entries = append(entries, DirEntry{Name: name, Mode: info.Mode, Ino: info.Ino})
				seen[name] = true
			}
		}
	}

	// Add user-created folders from storage fallback
	if fb := f.vnodes.Fallback(); fb != nil {
		if storageEntries, err := fb.Readdir("/"); err == nil {
			for _, e := range storageEntries {
				if !seen[e.Name] {
					entries = append(entries, DirEntry{Name: e.Name, Mode: e.Mode, Ino: e.Ino})
					seen[e.Name] = true
				}
			}
		}
	}

	// Add regular metadata entries
	content, err := f.metadata.GetDirectoryContentMetadata(f.rootID)
	if err == nil {
		for _, name := range content.EntryList {
			if seen[name] {
				continue
			}
			entryPath := "/" + name
			mode := uint32(syscall.S_IFREG | 0644)
			var ino uint64
			if info, err := f.Getattr(entryPath); err == nil {
				mode = info.Mode
				ino = info.Ino
			}
			entries = append(entries, DirEntry{Name: name, Mode: mode, Ino: ino})
		}
	}

	return entries
}

func (f *Filesystem) Releasedir(path string, fh FileHandle) error { return nil }

func (f *Filesystem) Open(path string, flags int) (FileHandle, error) {
	if vn := f.vnodes.MatchOrFallback(path); vn != nil {
		fh, err := vn.Open(path, flags)
		return FileHandle(fh), err
	}

	if _, err := f.Getattr(path); err != nil {
		return 0, err
	}
	return 0, nil
}

func (f *Filesystem) Read(path string, buf []byte, off int64, fh FileHandle) (int, error) {
	if vn := f.vnodes.MatchOrFallback(path); vn != nil {
		return vn.Read(path, buf, off, vnode.FileHandle(fh))
	}

	parent, name := splitPath(path)
	meta, err := f.metadata.GetFileMetadata(f.resolveDir(parent), name)
	if err != nil {
		return 0, ErrNotFound
	}

	if off >= int64(len(meta.FileData)) {
		return 0, nil
	}
	return copy(buf, meta.FileData[off:]), nil
}

func (f *Filesystem) Release(path string, fh FileHandle) error {
	if vn := f.vnodes.Match(path); vn != nil {
		return vn.Release(path, vnode.FileHandle(fh))
	}
	return nil
}

// Write operations - delegate to vnodes or fallback storage
func (f *Filesystem) Create(path string, flags int, mode uint32) (FileHandle, error) {
	if vn := f.vnodes.MatchOrFallback(path); vn != nil {
		fh, err := vn.Create(path, flags, mode)
		return FileHandle(fh), err
	}
	return 0, ErrReadOnly
}

func (f *Filesystem) Write(path string, buf []byte, off int64, fh FileHandle) (int, error) {
	if vn := f.vnodes.MatchOrFallback(path); vn != nil {
		return vn.Write(path, buf, off, vnode.FileHandle(fh))
	}
	return 0, ErrReadOnly
}

func (f *Filesystem) Truncate(path string, size int64, fh FileHandle) error {
	if vn := f.vnodes.MatchOrFallback(path); vn != nil {
		return vn.Truncate(path, size, vnode.FileHandle(fh))
	}
	return ErrReadOnly
}

func (f *Filesystem) Mkdir(path string, mode uint32) error {
	if vn := f.vnodes.MatchOrFallback(path); vn != nil {
		return vn.Mkdir(path, mode)
	}
	return ErrReadOnly
}

func (f *Filesystem) Rmdir(path string) error {
	if vn := f.vnodes.MatchOrFallback(path); vn != nil {
		return vn.Rmdir(path)
	}
	return ErrReadOnly
}

func (f *Filesystem) Unlink(path string) error {
	if vn := f.vnodes.MatchOrFallback(path); vn != nil {
		return vn.Unlink(path)
	}
	return ErrReadOnly
}

func (f *Filesystem) Rename(oldpath, newpath string) error {
	oldVN := f.vnodes.MatchOrFallback(oldpath)
	newVN := f.vnodes.MatchOrFallback(newpath)
	if oldVN == nil || newVN == nil {
		return ErrReadOnly
	}
	if oldVN != newVN {
		return ErrNotSupported
	}
	return oldVN.Rename(oldpath, newpath)
}

func (f *Filesystem) Chmod(path string, mode uint32) error {
	if vn := f.vnodes.MatchOrFallback(path); vn != nil {
		return nil // No-op for vnodes
	}
	return ErrReadOnly
}

func (f *Filesystem) Chown(path string, uid, gid uint32) error {
	if vn := f.vnodes.MatchOrFallback(path); vn != nil {
		return nil // No-op for vnodes
	}
	return ErrReadOnly
}

func (f *Filesystem) Utimens(path string, atime, mtime *int64) error {
	if vn := f.vnodes.MatchOrFallback(path); vn != nil {
		return nil // No-op for vnodes
	}
	return ErrReadOnly
}

// Symlink operations
func (f *Filesystem) Readlink(path string) (string, error) {
	if vn := f.vnodes.Match(path); vn != nil {
		return vn.Readlink(path)
	}
	return "", ErrNotSupported
}

func (f *Filesystem) Link(oldpath, newpath string) error { return ErrNotSupported }

func (f *Filesystem) Symlink(target, newpath string) error {
	if vn := f.vnodes.Match(newpath); vn != nil {
		return vn.Symlink(target, newpath)
	}
	return ErrNotSupported
}

// Getxattr returns empty data for all xattrs (we do not store them).
func (f *Filesystem) Getxattr(path, name string) ([]byte, error) { return []byte{}, nil }

// Setxattr silently accepts and discards extended attributes.
func (f *Filesystem) Setxattr(path, name string, value []byte, flags int) error {
	return nil
}

// Removexattr silently succeeds since we don't store xattrs.
func (f *Filesystem) Removexattr(path, name string) error     { return nil }
func (f *Filesystem) Listxattr(path string) ([]string, error) { return nil, nil }

// Flush and Fsync
func (f *Filesystem) Flush(path string, fh FileHandle) error { return nil }

func (f *Filesystem) Fsync(path string, datasync bool, fh FileHandle) error {
	if vn := f.vnodes.Match(path); vn != nil {
		return vn.Fsync(path, vnode.FileHandle(fh))
	}
	return nil
}

func (f *Filesystem) rootInfo() *FileInfo {
	return &FileInfo{
		Ino:   1,
		Mode:  syscall.S_IFDIR | 0755,
		Nlink: 2,
		Uid:   uint32(syscall.Getuid()),
		Gid:   uint32(syscall.Getgid()),
		Atime: time.Now(),
		Mtime: time.Now(),
		Ctime: time.Now(),
	}
}

func (f *Filesystem) resolveDir(path string) string {
	if path == "/" || path == "" {
		return f.rootID
	}

	parts := strings.Split(strings.Trim(path, "/"), "/")
	id := f.rootID

	for _, part := range parts {
		meta, err := f.metadata.GetDirectoryAccessMetadata(id, part)
		if err != nil {
			return ""
		}
		id = meta.ID
	}
	return id
}

func splitPath(path string) (parent, name string) {
	path = strings.TrimSuffix(path, "/")
	if path == "" || path == "/" {
		return "/", ""
	}

	i := strings.LastIndex(path, "/")
	if i == 0 {
		return "/", path[1:]
	}
	if i == -1 {
		return "/", path
	}
	return path[:i], path[i+1:]
}

func hashToIno(id string) uint64 {
	var ino uint64
	for i := 0; i < len(id) && i < 8; i++ {
		ino = ino<<8 | uint64(id[i])
	}
	return ino
}
