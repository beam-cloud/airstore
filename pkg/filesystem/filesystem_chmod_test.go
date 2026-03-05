package filesystem

import (
	"testing"

	"github.com/beam-cloud/airstore/pkg/filesystem/vnode"
)

type chmodFallbackNode struct {
	path string
	mode uint32
}

func (n *chmodFallbackNode) Prefix() string        { return "" }
func (n *chmodFallbackNode) Type() vnode.VNodeType { return vnode.VNodeWritable }
func (n *chmodFallbackNode) Getattr(string) (*vnode.FileInfo, error) {
	return vnode.NewFileInfo(1, 0, 0644), nil
}
func (n *chmodFallbackNode) Readdir(string) ([]vnode.DirEntry, error)   { return nil, nil }
func (n *chmodFallbackNode) Open(string, int) (vnode.FileHandle, error) { return 0, nil }
func (n *chmodFallbackNode) Read(string, []byte, int64, vnode.FileHandle) (int, error) {
	return 0, nil
}
func (n *chmodFallbackNode) Readlink(string) (string, error) { return "", nil }
func (n *chmodFallbackNode) Create(string, int, uint32) (vnode.FileHandle, error) {
	return 0, nil
}
func (n *chmodFallbackNode) Write(string, []byte, int64, vnode.FileHandle) (int, error) {
	return 0, nil
}
func (n *chmodFallbackNode) Truncate(string, int64, vnode.FileHandle) error { return nil }
func (n *chmodFallbackNode) Mkdir(string, uint32) error                     { return nil }
func (n *chmodFallbackNode) Rmdir(string) error                             { return nil }
func (n *chmodFallbackNode) Unlink(string) error                            { return nil }
func (n *chmodFallbackNode) Rename(string, string) error                    { return nil }
func (n *chmodFallbackNode) Symlink(string, string) error                   { return nil }
func (n *chmodFallbackNode) Release(string, vnode.FileHandle) error         { return nil }
func (n *chmodFallbackNode) Fsync(string, vnode.FileHandle) error           { return nil }
func (n *chmodFallbackNode) Chmod(path string, mode uint32) error {
	n.path = path
	n.mode = mode
	return nil
}

type chmodNoopFallbackNode struct{}

func (n *chmodNoopFallbackNode) Prefix() string        { return "" }
func (n *chmodNoopFallbackNode) Type() vnode.VNodeType { return vnode.VNodeWritable }
func (n *chmodNoopFallbackNode) Getattr(string) (*vnode.FileInfo, error) {
	return vnode.NewFileInfo(1, 0, 0644), nil
}
func (n *chmodNoopFallbackNode) Readdir(string) ([]vnode.DirEntry, error)   { return nil, nil }
func (n *chmodNoopFallbackNode) Open(string, int) (vnode.FileHandle, error) { return 0, nil }
func (n *chmodNoopFallbackNode) Read(string, []byte, int64, vnode.FileHandle) (int, error) {
	return 0, nil
}
func (n *chmodNoopFallbackNode) Readlink(string) (string, error) { return "", nil }
func (n *chmodNoopFallbackNode) Create(string, int, uint32) (vnode.FileHandle, error) {
	return 0, nil
}
func (n *chmodNoopFallbackNode) Write(string, []byte, int64, vnode.FileHandle) (int, error) {
	return 0, nil
}
func (n *chmodNoopFallbackNode) Truncate(string, int64, vnode.FileHandle) error { return nil }
func (n *chmodNoopFallbackNode) Mkdir(string, uint32) error                     { return nil }
func (n *chmodNoopFallbackNode) Rmdir(string) error                             { return nil }
func (n *chmodNoopFallbackNode) Unlink(string) error                            { return nil }
func (n *chmodNoopFallbackNode) Rename(string, string) error                    { return nil }
func (n *chmodNoopFallbackNode) Symlink(string, string) error                   { return nil }
func (n *chmodNoopFallbackNode) Release(string, vnode.FileHandle) error         { return nil }
func (n *chmodNoopFallbackNode) Fsync(string, vnode.FileHandle) error           { return nil }

func TestFilesystemChmodDelegatesToVNode(t *testing.T) {
	fs := &Filesystem{vnodes: vnode.NewRegistry()}
	node := &chmodFallbackNode{}
	fs.vnodes.SetFallback(node)

	if err := fs.Chmod("/example.sh", 0755); err != nil {
		t.Fatalf("chmod returned error: %v", err)
	}
	if node.path != "/example.sh" {
		t.Fatalf("expected path /example.sh, got %q", node.path)
	}
	if node.mode != 0755 {
		t.Fatalf("expected mode 0755, got %#o", node.mode)
	}
}

func TestFilesystemChmodNoopWhenVNodeDoesNotImplementChmod(t *testing.T) {
	fs := &Filesystem{vnodes: vnode.NewRegistry()}
	fs.vnodes.SetFallback(&chmodNoopFallbackNode{})

	if err := fs.Chmod("/example.sh", 0755); err != nil {
		t.Fatalf("expected nil error for vnode without chmod support, got %v", err)
	}
}
