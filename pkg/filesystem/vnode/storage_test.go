package vnode

import "testing"

func TestStorageVNodeWriteInvalidatesContentCache(t *testing.T) {
	path := "/workspace/demo.txt"
	s := &StorageVNode{
		content: NewContentCache(),
		handles: make(map[FileHandle]*handleState),
		writes:  make(map[string]map[FileHandle]*handleState),
		nextFH:  1,
	}

	s.content.Set(path, []byte("old"), 123)
	if _, ok := s.content.Get(path, 123); !ok {
		t.Fatal("expected cached content before write")
	}

	fh := s.allocHandle(path)
	if _, err := s.Write(path, []byte("new"), 0, fh); err != nil {
		t.Fatalf("Write returned error: %v", err)
	}

	if _, ok := s.content.Get(path, 123); ok {
		t.Fatal("expected content cache to be invalidated after write")
	}
}
