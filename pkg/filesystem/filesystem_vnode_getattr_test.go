package filesystem

import (
	"io/fs"
	"testing"

	"github.com/beam-cloud/airstore/pkg/filesystem/vnode"
	"github.com/hashicorp/golang-lru/v2/expirable"
)

type hiddenChildVNode struct {
	vnode.ReadOnlyBase
}

func (hiddenChildVNode) Prefix() string { return "/tools" }

func (hiddenChildVNode) Getattr(path string) (*vnode.FileInfo, error) {
	switch path {
	case "/tools":
		return vnode.NewDirInfo(vnode.PathIno("/tools")), nil
	case "/tools/.hidden-shim":
		return vnode.NewFileInfo(vnode.PathIno("/tools/.hidden-shim"), 4, 0444), nil
	default:
		return nil, fs.ErrNotExist
	}
}

func (hiddenChildVNode) Readdir(path string) ([]vnode.DirEntry, error) {
	if path != "/tools" {
		return nil, fs.ErrNotExist
	}
	// Intentionally omit .hidden-shim to exercise getattr behavior when
	// filesystem-level dirChildren cache does not include an internally served file.
	return []vnode.DirEntry{
		{Name: "wikipedia", Mode: 0755, Ino: vnode.PathIno("/tools/wikipedia")},
	}, nil
}

func (hiddenChildVNode) Open(path string, flags int) (vnode.FileHandle, error) {
	return 0, fs.ErrNotExist
}

func (hiddenChildVNode) Read(path string, buf []byte, off int64, fh vnode.FileHandle) (int, error) {
	return 0, fs.ErrNotExist
}

func TestGetattrVNodeBypassesNegativeCaches(t *testing.T) {
	reg := vnode.NewRegistry()
	reg.Register(hiddenChildVNode{})

	fs := &Filesystem{
		vnodes:         reg,
		dirChildren:    expirable.NewLRU[string, map[string]struct{}](dirChildrenSize, nil, dirChildrenTTL),
		negativeCache:  expirable.NewLRU[string, struct{}](negativeCacheSize, nil, negativeCacheTTL),
	}

	// Simulate a prior readdir cache that does not contain the hidden shim.
	fs.dirChildren.Add("/tools", map[string]struct{}{"wikipedia": {}})
	// Simulate a stale negative lookup result.
	fs.negativeCache.Add("/tools/.hidden-shim", struct{}{})

	info, err := fs.Getattr("/tools/.hidden-shim")
	if err != nil {
		t.Fatalf("expected vnode getattr to bypass generic negative caches, got error: %v", err)
	}
	if info == nil || info.Size != 4 {
		t.Fatalf("unexpected file info returned: %+v", info)
	}
}

