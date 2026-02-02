package vnode

import (
	"strings"
	"syscall"
	"time"
)

// VNodeType defines the behavior category of a virtual node
type VNodeType int

const (
	// VNodeReadOnly is for read-only paths like /tools/, /.airstore/
	VNodeReadOnly VNodeType = iota
	// VNodeSmartQuery is for /sources/{integration}/ - mkdir/touch creates smart queries
	VNodeSmartQuery
	// VNodeWritable is for fully writable paths like /skills/
	VNodeWritable
)

var (
	ErrReadOnly     = syscall.EROFS
	ErrNotSupported = syscall.ENOTSUP
	ErrNotFound     = syscall.ENOENT
)

type FileHandle uint64

type FileInfo struct {
	Ino                 uint64
	Size                int64
	Mode, Nlink         uint32
	Uid, Gid            uint32
	Atime, Mtime, Ctime time.Time
}

type DirEntry struct {
	Name  string
	Mode  uint32
	Ino   uint64
	Size  int64 // File size (0 for directories)
	Mtime int64 // Unix timestamp (0 = use current time)
}

// VirtualNode handles a path prefix in the virtual filesystem.
//
// Write semantics by VNodeType:
//   - VNodeReadOnly: All writes return ErrReadOnly (e.g., /tools/, /.airstore/)
//   - VNodeSmartQuery: Mkdir/Create create smart queries, Write/Unlink/Rmdir not supported
//   - VNodeWritable: Full read/write access (e.g., /skills/)
type VirtualNode interface {
	Prefix() string
	Type() VNodeType

	// Read operations
	Getattr(path string) (*FileInfo, error)
	Readdir(path string) ([]DirEntry, error)
	Open(path string, flags int) (FileHandle, error)
	Read(path string, buf []byte, off int64, fh FileHandle) (int, error)
	Readlink(path string) (string, error)

	// Write operations (behavior depends on VNodeType)
	Create(path string, flags int, mode uint32) (FileHandle, error)
	Write(path string, buf []byte, off int64, fh FileHandle) (int, error)
	Truncate(path string, size int64, fh FileHandle) error
	Mkdir(path string, mode uint32) error
	Rmdir(path string) error
	Unlink(path string) error
	Rename(oldpath, newpath string) error
	Symlink(target, linkPath string) error

	// Lifecycle
	Release(path string, fh FileHandle) error
	Fsync(path string, fh FileHandle) error
}

// ReadOnlyBase returns ErrReadOnly for all write operations.
// Embed this in VNodes that don't support writes (e.g., /tools/).
type ReadOnlyBase struct{}

func (ReadOnlyBase) Type() VNodeType                                       { return VNodeReadOnly }
func (ReadOnlyBase) Create(string, int, uint32) (FileHandle, error)        { return 0, ErrReadOnly }
func (ReadOnlyBase) Write(string, []byte, int64, FileHandle) (int, error)  { return 0, ErrReadOnly }
func (ReadOnlyBase) Truncate(string, int64, FileHandle) error              { return ErrReadOnly }
func (ReadOnlyBase) Mkdir(string, uint32) error                            { return ErrReadOnly }
func (ReadOnlyBase) Rmdir(string) error                                    { return ErrReadOnly }
func (ReadOnlyBase) Unlink(string) error                                   { return ErrReadOnly }
func (ReadOnlyBase) Rename(string, string) error                           { return ErrReadOnly }
func (ReadOnlyBase) Symlink(string, string) error                          { return ErrReadOnly }
func (ReadOnlyBase) Readlink(string) (string, error)                       { return "", ErrNotSupported }
func (ReadOnlyBase) Release(string, FileHandle) error                      { return nil }
func (ReadOnlyBase) Fsync(string, FileHandle) error                        { return nil }

// SmartQueryBase provides default implementations for smart query VNodes.
// Embed this in VNodes that support smart queries (e.g., /sources/).
// Mkdir and Create should be overridden to create smart queries.
type SmartQueryBase struct{}

func (SmartQueryBase) Type() VNodeType                                       { return VNodeSmartQuery }
func (SmartQueryBase) Write(string, []byte, int64, FileHandle) (int, error)  { return 0, ErrReadOnly }
func (SmartQueryBase) Truncate(string, int64, FileHandle) error              { return ErrReadOnly }
func (SmartQueryBase) Rmdir(string) error                                    { return ErrNotSupported }
func (SmartQueryBase) Unlink(string) error                                   { return ErrNotSupported }
func (SmartQueryBase) Rename(string, string) error                           { return ErrNotSupported }
func (SmartQueryBase) Symlink(string, string) error                          { return ErrNotSupported }
func (SmartQueryBase) Readlink(string) (string, error)                       { return "", ErrNotSupported }
func (SmartQueryBase) Release(string, FileHandle) error                      { return nil }
func (SmartQueryBase) Fsync(string, FileHandle) error                        { return nil }

// Registry matches paths to virtual nodes.
type Registry struct {
	nodes    []VirtualNode
	fallback VirtualNode // handles paths not matched by any node
}

func NewRegistry() *Registry {
	return &Registry{}
}

func (r *Registry) Register(node VirtualNode) {
	r.nodes = append(r.nodes, node)
}

func (r *Registry) SetFallback(node VirtualNode) {
	r.fallback = node
}

func (r *Registry) Match(path string) VirtualNode {
	for _, n := range r.nodes {
		p := n.Prefix()
		if path == p || strings.HasPrefix(path, p+"/") {
			return n
		}
	}
	return nil
}

func (r *Registry) MatchOrFallback(path string) VirtualNode {
	if vn := r.Match(path); vn != nil {
		return vn
	}
	return r.fallback
}

func (r *Registry) Fallback() VirtualNode {
	return r.fallback
}

func (r *Registry) List() []VirtualNode {
	return r.nodes
}

// FileInfo constructors

func newFileInfo(ino uint64, size int64, mode uint32, nlink uint32) *FileInfo {
	now := time.Now()
	return &FileInfo{
		Ino: ino, Size: size, Mode: mode, Nlink: nlink,
		Uid: uint32(syscall.Getuid()), Gid: uint32(syscall.Getgid()),
		Atime: now, Mtime: now, Ctime: now,
	}
}

func NewDirInfo(ino uint64) *FileInfo {
	return newFileInfo(ino, 0, syscall.S_IFDIR|0755, 2)
}

func NewFileInfo(ino uint64, size int64, mode uint32) *FileInfo {
	return newFileInfo(ino, size, syscall.S_IFREG|mode, 1)
}

func NewExecFileInfo(ino uint64, size int64) *FileInfo {
	return newFileInfo(ino, size, syscall.S_IFREG|0755, 1)
}

func NewSymlinkInfo(ino uint64, targetLen int64) *FileInfo {
	return newFileInfo(ino, targetLen, syscall.S_IFLNK|0777, 1)
}

// PathIno generates a stable inode from a path (FNV-1a).
func PathIno(path string) uint64 {
	h := uint64(14695981039346656037)
	for i := 0; i < len(path); i++ {
		h ^= uint64(path[i])
		h *= 1099511628211
	}
	return h
}
