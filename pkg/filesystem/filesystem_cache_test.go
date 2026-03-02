package filesystem

import (
	"testing"
	"time"

	"github.com/beam-cloud/airstore/pkg/filesystem/vnode"
	"github.com/hashicorp/golang-lru/v2/expirable"
)

type cacheAwareTestVNode struct {
	vnode.ReadOnlyBase
}

func (n *cacheAwareTestVNode) Prefix() string { return "/pdfs" }

func (n *cacheAwareTestVNode) Getattr(path string) (*vnode.FileInfo, error) {
	switch path {
	case "/pdfs":
		return vnode.NewDirInfo(vnode.PathIno(path)), nil
	case "/pdfs/test.pdf":
		return vnode.NewFileInfo(vnode.PathIno(path), 123, 0644), nil
	default:
		return nil, vnode.ErrNotFound
	}
}

func (n *cacheAwareTestVNode) Readdir(path string) ([]vnode.DirEntry, error) {
	if path != "/pdfs" {
		return nil, vnode.ErrNotFound
	}
	return []vnode.DirEntry{
		{Name: "test.pdf", Mode: 0100644, Ino: vnode.PathIno("/pdfs/test.pdf")},
	}, nil
}

func (n *cacheAwareTestVNode) Open(path string, flags int) (vnode.FileHandle, error) {
	if path != "/pdfs/test.pdf" {
		return 0, vnode.ErrNotFound
	}
	return 0, nil
}

func (n *cacheAwareTestVNode) Read(path string, buf []byte, off int64, fh vnode.FileHandle) (int, error) {
	return 0, nil
}

func TestFilesystemGetattr_ReaddirHitClearsStaleNegative(t *testing.T) {
	fs := &Filesystem{
		vnodes:        vnode.NewRegistry(),
		dirChildren:   expirable.NewLRU[string, map[string]struct{}](16, nil, time.Minute),
		negativeCache: expirable.NewLRU[string, struct{}](16, nil, time.Minute),
	}
	fs.vnodes.Register(&cacheAwareTestVNode{})

	targetPath := "/pdfs/test.pdf"
	fs.dirChildren.Add("/pdfs", map[string]struct{}{"test.pdf": {}})
	fs.negativeCache.Add(targetPath, struct{}{})

	info, err := fs.Getattr(targetPath)
	if err != nil {
		t.Fatalf("expected getattr success, got error: %v", err)
	}
	if info == nil {
		t.Fatal("expected file info, got nil")
	}
	if _, ok := fs.negativeCache.Get(targetPath); ok {
		t.Fatalf("expected stale negative cache entry for %s to be cleared", targetPath)
	}
}
