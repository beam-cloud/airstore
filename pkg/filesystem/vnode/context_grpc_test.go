package vnode

import (
	"syscall"
	"testing"
)

func TestContextVNodeLocalDirtySizeIncludesBufferedHandleWrites(t *testing.T) {
	path := "/skills/demo.txt"
	c := &ContextVNodeGRPC{
		cache:       NewMetadataCache(),
		asyncWriter: NewAsyncWriter(func(string, int64, []byte) error { return nil }),
		writes:      make(map[string]map[FileHandle]*handleState),
	}

	c.writes[path] = map[FileHandle]*handleState{
		1: {path: path, writeOff: 0, writeBuf: []byte("hello")},
	}

	size, ok := c.localDirtySize(path, syscall.S_IFREG|0644)
	if !ok {
		t.Fatal("expected dirty size to be detected")
	}
	if size != 5 {
		t.Fatalf("expected dirty size 5, got %d", size)
	}

	info := c.localDirtyFileInfo(path, syscall.S_IFREG|0644)
	if info == nil {
		t.Fatal("expected local dirty file info")
	}
	if info.Size != 5 {
		t.Fatalf("expected dirty file info size 5, got %d", info.Size)
	}
}
