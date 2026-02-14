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
	// VNodeSourceView is for /sources/{integration}/ - mkdir/touch creates source views
	VNodeSourceView
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
//   - VNodeSourceView: Mkdir/Create create source views, Write/Unlink/Rmdir not supported
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

// SourceViewBase provides default implementations for source view VNodes.
// Embed this in VNodes that support source views (e.g., /sources/).
// Mkdir and Create should be overridden to create source views.
type SourceViewBase struct{}

func (SourceViewBase) Type() VNodeType                                       { return VNodeSourceView }
func (SourceViewBase) Write(string, []byte, int64, FileHandle) (int, error)  { return 0, ErrReadOnly }
func (SourceViewBase) Truncate(string, int64, FileHandle) error              { return ErrReadOnly }
func (SourceViewBase) Rmdir(string) error                                    { return ErrNotSupported }
func (SourceViewBase) Unlink(string) error                                   { return ErrNotSupported }
func (SourceViewBase) Rename(string, string) error                           { return ErrNotSupported }
func (SourceViewBase) Symlink(string, string) error                          { return ErrNotSupported }
func (SourceViewBase) Readlink(string) (string, error)                       { return "", ErrNotSupported }
func (SourceViewBase) Release(string, FileHandle) error                      { return nil }
func (SourceViewBase) Fsync(string, FileHandle) error                        { return nil }

// registeredNode pairs a VirtualNode with its precomputed prefix strings
// so Match() never allocates.
type registeredNode struct {
	node      VirtualNode
	prefix    string // e.g. "/sources"
	prefixDir string // e.g. "/sources/" — precomputed for HasPrefix check
}

// Registry matches paths to virtual nodes.
type Registry struct {
	nodes    []registeredNode
	fallback VirtualNode // handles paths not matched by any node
}

func NewRegistry() *Registry {
	return &Registry{}
}

func (r *Registry) Register(node VirtualNode) {
	p := node.Prefix()
	r.nodes = append(r.nodes, registeredNode{
		node:      node,
		prefix:    p,
		prefixDir: p + "/",
	})
}

func (r *Registry) SetFallback(node VirtualNode) {
	r.fallback = node
}

// Match returns the VirtualNode whose prefix matches path, or nil.
// Zero allocations — prefixes are precomputed at registration time.
func (r *Registry) Match(path string) VirtualNode {
	for i := range r.nodes {
		rn := &r.nodes[i]
		if path == rn.prefix || strings.HasPrefix(path, rn.prefixDir) {
			return rn.node
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
	out := make([]VirtualNode, len(r.nodes))
	for i := range r.nodes {
		out[i] = r.nodes[i].node
	}
	return out
}

// FileInfo constructors

func newFileInfo(ino uint64, size int64, mode uint32, nlink uint32) *FileInfo {
	uid, gid := GetOwner()
	now := time.Now()
	return &FileInfo{
		Ino: ino, Size: size, Mode: mode, Nlink: nlink,
		Uid: uid, Gid: gid,
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

// BearerToken returns a precomputed "Bearer <token>" string for gRPC auth metadata.
// If token is empty, returns empty (caller should skip attaching metadata).
func BearerToken(token string) string {
	if token == "" {
		return ""
	}
	return "Bearer " + token
}
