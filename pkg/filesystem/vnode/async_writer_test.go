package vnode

import (
	"bytes"
	"sync"
	"testing"
)

func TestAsyncWriterDirtyFileInfoIncludesInflightData(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once

	aw := NewAsyncWriter(func(path string, off int64, data []byte) error {
		once.Do(func() { close(started) })
		<-release
		return nil
	})

	aw.Enqueue("/file.txt", 0, []byte("hello"))
	done := make(chan struct{})
	go func() {
		aw.doFlush("/file.txt")
		close(done)
	}()

	<-started

	info := aw.DirtyFileInfo("/file.txt", 0)
	if info == nil {
		t.Fatal("expected dirty file info while upload is inflight")
	}
	if info.Size != 5 {
		t.Fatalf("expected size 5, got %d", info.Size)
	}

	data, off, ok := aw.Get("/file.txt")
	if !ok {
		t.Fatal("expected dirty payload while upload is inflight")
	}
	if off != 0 {
		t.Fatalf("expected off 0, got %d", off)
	}
	if !bytes.Equal(data, []byte("hello")) {
		t.Fatalf("unexpected dirty payload: %q", string(data))
	}

	close(release)
	<-done
}

func TestAsyncWriterGetPrefersNewestQueuedDataDuringInflightUpload(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once

	aw := NewAsyncWriter(func(path string, off int64, data []byte) error {
		once.Do(func() { close(started) })
		<-release
		return nil
	})
	defer aw.Cleanup()

	aw.Enqueue("/file.txt", 0, []byte("old"))
	done := make(chan struct{})
	go func() {
		aw.doFlush("/file.txt")
		close(done)
	}()

	<-started
	aw.Enqueue("/file.txt", 0, []byte("newer-content"))

	info := aw.DirtyFileInfo("/file.txt", 0)
	if info == nil {
		t.Fatal("expected dirty file info while newer data is queued")
	}
	if want := int64(len("newer-content")); info.Size != want {
		t.Fatalf("expected size %d, got %d", want, info.Size)
	}

	data, off, ok := aw.Get("/file.txt")
	if !ok {
		t.Fatal("expected queued dirty payload")
	}
	if off != 0 {
		t.Fatalf("expected off 0, got %d", off)
	}
	if !bytes.Equal(data, []byte("newer-content")) {
		t.Fatalf("expected newest queued payload, got %q", string(data))
	}

	close(release)
	<-done
}
