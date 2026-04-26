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

type cacheAwareTestMetadata struct{}

func (cacheAwareTestMetadata) GetDirectoryContentMetadata(id string) (*DirectoryContentMetadata, error) {
	return &DirectoryContentMetadata{Id: id, EntryList: []string{}}, nil
}

func (cacheAwareTestMetadata) GetDirectoryAccessMetadata(pid, name string) (*DirectoryAccessMetadata, error) {
	return nil, ErrNotFound
}

func (cacheAwareTestMetadata) GetFileMetadata(pid, name string) (*FileMetadata, error) {
	return nil, ErrNotFound
}

func (cacheAwareTestMetadata) SaveDirectoryContentMetadata(meta *DirectoryContentMetadata) error {
	return nil
}

func (cacheAwareTestMetadata) SaveDirectoryAccessMetadata(meta *DirectoryAccessMetadata) error {
	return nil
}

func (cacheAwareTestMetadata) SaveFileMetadata(meta *FileMetadata) error {
	return nil
}

func (cacheAwareTestMetadata) ListDirectory(path string) []DirEntry {
	return nil
}

func (cacheAwareTestMetadata) RenameDirectory(oldPID, oldName, newPID, newName string, version int) error {
	return nil
}

func (cacheAwareTestMetadata) DeleteDirectory(parentID, name string, version int) error {
	return nil
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

func TestFilesystemHiddenRoots(t *testing.T) {
	fs := &Filesystem{
		vnodes:        vnode.NewRegistry(),
		metadata:      cacheAwareTestMetadata{},
		dirChildren:   expirable.NewLRU[string, map[string]struct{}](16, nil, time.Minute),
		negativeCache: expirable.NewLRU[string, struct{}](16, nil, time.Minute),
		hiddenRoots:   normalizeHiddenRoots([]string{"/pdfs"}),
	}
	fs.vnodes.Register(&cacheAwareTestVNode{})

	entries, err := fs.Readdir("/")
	if err != nil {
		t.Fatalf("expected root readdir success, got error: %v", err)
	}
	for _, entry := range entries {
		if entry.Name == "pdfs" {
			t.Fatalf("expected hidden root to be omitted from root readdir")
		}
	}

	if _, err := fs.Getattr("/pdfs"); err == nil {
		t.Fatalf("expected hidden root getattr to fail")
	}
	if _, err := fs.Getattr("/pdfs/test.pdf"); err == nil {
		t.Fatalf("expected hidden child getattr to fail")
	}
	if _, err := fs.Readdir("/pdfs"); err == nil {
		t.Fatalf("expected hidden root readdir to fail")
	}
}
