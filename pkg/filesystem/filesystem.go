package filesystem

import (
	"fmt"
	"os"
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

	ok := f.host.Mount(f.config.MountPoint, opts)

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
	if f.destroyed {
		f.mu.Unlock()
		return nil
	}
	f.mu.Unlock()

	if f.host != nil {
		// Try unmount with a timeout - FUSE-T SMB backend may hang
		done := make(chan struct{})
		go func() {
			f.host.Unmount()
			close(done)
		}()

		select {
		case <-done:
			// Unmount completed normally
		case <-time.After(2 * time.Second):
			// Unmount is hanging (common with FUSE-T SMB), force exit
			log.Info().Msg("unmounted")
			os.Exit(0)
		}
	}
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

	// With FUSE-T SMB backend, the Mount() call may not return after Destroy().
	// Force exit after cleanup to ensure the process terminates on Ctrl+C.
	log.Info().Msg("unmounted")
	os.Exit(0)
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
		entries := f.readdirRoot()
		return entries, nil
	}

	content, err := f.metadata.GetDirectoryContentMetadata(f.resolveDir(path))
	if err != nil {
		return nil, ErrIO
	}

	entries := make([]DirEntry, 0, len(content.EntryList))
	for _, name := range content.EntryList {
		entryPath := path + "/" + name
		if path == "/" {
			entryPath = "/" + name
		}

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
	entries := make([]DirEntry, 0)

	// Add virtual node root directories (e.g., "tools")
	for _, vn := range f.vnodes.List() {
		prefix := vn.Prefix()
		name := strings.TrimPrefix(prefix, "/")
		if name != "" && !strings.Contains(name, "/") {
			if info, err := vn.Getattr(prefix); err == nil {
				entries = append(entries, DirEntry{Name: name, Mode: info.Mode, Ino: info.Ino})
			}
		}
	}

	// Add regular metadata entries
	content, err := f.metadata.GetDirectoryContentMetadata(f.rootID)
	if err == nil {
		for _, name := range content.EntryList {
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
	// Check for virtual node match
	if vn := f.vnodes.Match(path); vn != nil {
		fh, err := vn.Open(path, flags)
		return FileHandle(fh), err
	}

	if _, err := f.Getattr(path); err != nil {
		return 0, err
	}
	return 0, nil
}

func (f *Filesystem) Read(path string, buf []byte, off int64, fh FileHandle) (int, error) {
	// Check for virtual node match
	if vn := f.vnodes.Match(path); vn != nil {
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

// Write operations - delegate to vnodes, otherwise read-only
func (f *Filesystem) Create(path string, flags int, mode uint32) (FileHandle, error) {
	if vn := f.vnodes.Match(path); vn != nil {
		fh, err := vn.Create(path, flags, mode)
		return FileHandle(fh), err
	}
	return 0, ErrReadOnly
}

func (f *Filesystem) Write(path string, buf []byte, off int64, fh FileHandle) (int, error) {
	if vn := f.vnodes.Match(path); vn != nil {
		return vn.Write(path, buf, off, vnode.FileHandle(fh))
	}
	return 0, ErrReadOnly
}

func (f *Filesystem) Truncate(path string, size int64, fh FileHandle) error {
	if vn := f.vnodes.Match(path); vn != nil {
		return vn.Truncate(path, size, vnode.FileHandle(fh))
	}
	return ErrReadOnly
}

func (f *Filesystem) Mkdir(path string, mode uint32) error {
	if vn := f.vnodes.Match(path); vn != nil {
		return vn.Mkdir(path, mode)
	}
	return ErrReadOnly
}

func (f *Filesystem) Rmdir(path string) error {
	if vn := f.vnodes.Match(path); vn != nil {
		return vn.Rmdir(path)
	}
	return ErrReadOnly
}

func (f *Filesystem) Unlink(path string) error {
	if vn := f.vnodes.Match(path); vn != nil {
		return vn.Unlink(path)
	}
	return ErrReadOnly
}

func (f *Filesystem) Rename(oldpath, newpath string) error { return ErrReadOnly }

func (f *Filesystem) Chmod(path string, mode uint32) error {
	if vn := f.vnodes.Match(path); vn != nil {
		return nil // No-op for vnodes
	}
	return ErrReadOnly
}

func (f *Filesystem) Chown(path string, uid, gid uint32) error {
	if vn := f.vnodes.Match(path); vn != nil {
		return nil // No-op for vnodes
	}
	return ErrReadOnly
}

func (f *Filesystem) Utimens(path string, atime, mtime *int64) error {
	if vn := f.vnodes.Match(path); vn != nil {
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

func (f *Filesystem) Getxattr(path, name string) ([]byte, error) { return nil, ErrNoAttr }

func (f *Filesystem) Setxattr(path, name string, value []byte, flags int) error {
	if vn := f.vnodes.Match(path); vn != nil {
		return nil // No-op for vnodes
	}
	return ErrNotSupported
}
func (f *Filesystem) Removexattr(path, name string) error     { return ErrNotSupported }
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
