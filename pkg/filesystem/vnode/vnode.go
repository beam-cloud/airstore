package vnode

import (
	"strings"
	"syscall"
	"time"
)

var (
	ErrReadOnly     = syscall.EROFS
	ErrNotSupported = syscall.ENOTSUP
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
	Name string
	Mode uint32
	Ino  uint64
}

// VirtualNode handles a path prefix in the virtual filesystem.
// Read-only implementations embed ReadOnlyBase.
type VirtualNode interface {
	Prefix() string

	// Read
	Getattr(path string) (*FileInfo, error)
	Readdir(path string) ([]DirEntry, error)
	Open(path string, flags int) (FileHandle, error)
	Read(path string, buf []byte, off int64, fh FileHandle) (int, error)
	Readlink(path string) (string, error)

	// Write
	Create(path string, flags int, mode uint32) (FileHandle, error)
	Write(path string, buf []byte, off int64, fh FileHandle) (int, error)
	Truncate(path string, size int64, fh FileHandle) error
	Mkdir(path string, mode uint32) error
	Rmdir(path string) error
	Unlink(path string) error
	Symlink(target, linkPath string) error

	// Lifecycle
	Release(path string, fh FileHandle) error
	Fsync(path string, fh FileHandle) error
}

// ReadOnlyBase returns ErrReadOnly for all write operations.
type ReadOnlyBase struct{}

func (ReadOnlyBase) Create(string, int, uint32) (FileHandle, error)     { return 0, ErrReadOnly }
func (ReadOnlyBase) Write(string, []byte, int64, FileHandle) (int, error) { return 0, ErrReadOnly }
func (ReadOnlyBase) Truncate(string, int64, FileHandle) error            { return ErrReadOnly }
func (ReadOnlyBase) Mkdir(string, uint32) error                          { return ErrReadOnly }
func (ReadOnlyBase) Rmdir(string) error                                  { return ErrReadOnly }
func (ReadOnlyBase) Unlink(string) error                                 { return ErrReadOnly }
func (ReadOnlyBase) Symlink(string, string) error                        { return ErrReadOnly }
func (ReadOnlyBase) Readlink(string) (string, error)                     { return "", ErrNotSupported }
func (ReadOnlyBase) Release(string, FileHandle) error                    { return nil }
func (ReadOnlyBase) Fsync(string, FileHandle) error                      { return nil }

// Registry matches paths to virtual nodes.
type Registry struct {
	nodes []VirtualNode
}

func NewRegistry() *Registry {
	return &Registry{}
}

func (r *Registry) Register(node VirtualNode) {
	r.nodes = append(r.nodes, node)
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
