package vnode

import (
	"bytes"
	"io/fs"
	"sync"
	"testing"

	pb "github.com/beam-cloud/airstore/proto"
)

func TestReadResponseError_NotFoundMapsToErrNotExist(t *testing.T) {
	err := readResponseError("not found")
	if err == nil {
		t.Fatal("expected error")
	}
	if err != fs.ErrNotExist {
		t.Fatalf("expected fs.ErrNotExist, got %v", err)
	}
}

func TestReadResponseError_OtherErrorDoesNotMapToErrNotExist(t *testing.T) {
	for _, msg := range []string{
		"timeout",
		"connection refused",
		"access denied",
		"internal server error",
	} {
		err := readResponseError(msg)
		if err == nil {
			t.Fatalf("expected error for %q", msg)
		}
		if err == fs.ErrNotExist {
			t.Fatalf("should NOT map %q to fs.ErrNotExist", msg)
		}
	}
}

// TestSnapshotTruncationRace_DestructiveDrain reproduces the bug where a
// concurrent read draining the write buffer mid-sequence causes truncation.
//
// Timeline:
//  1. Writer writes sections 1+2 → merged in handleState buffer
//  2. Concurrent reader drains buffer → AsyncWriter has sections 1+2
//  3. Writer continues with section 3 → new buffer at offset 131072
//  4. Writer closes → flushWriteBuffer enqueues section 3 → REPLACES 1+2
//  5. ForceFlush uploads only section 3 → file is truncated
func TestSnapshotTruncationRace_DestructiveDrain(t *testing.T) {
	section1 := []byte("# Snapshot file\nshopt -u autocd\n")
	section2 := []byte("# Functions\nalias ll='ls -la'\n")
	section3 := []byte("export PATH=/usr/bin\n")

	// Step 1: Writer merges sections 1+2 into handle buffer.
	merged12 := mergeWriteBuffer(0, section1, 65536, section2)
	if merged12 == nil {
		t.Fatal("merge failed")
	}

	var uploaded []byte
	aw := NewAsyncWriter(func(path string, off int64, data []byte) error {
		uploaded = make([]byte, len(data))
		copy(uploaded, data)
		return nil
	})

	// Step 2: Simulate destructive drain (old enqueueWritesForPath behavior).
	// This is what the read path used to do: copy buffer, clear it, enqueue.
	drainedOff, drainedData := compactNulls(merged12.off, merged12.data)
	aw.Enqueue("/snapshot.sh", drainedOff, drainedData)
	// Handle buffer is now empty (simulating state.writeBuf = nil).

	// Step 3: Writer continues with section 3 at offset 131072.
	// Since the buffer was cleared, this starts a fresh buffer.
	freshOff := int64(131072)
	freshData := make([]byte, len(section3))
	copy(freshData, section3)

	// Step 4: Writer closes → flushWriteBuffer enqueues section 3.
	// Enqueue REPLACES the pending sections 1+2.
	_, compactedFresh := compactNulls(freshOff, freshData)
	aw.Enqueue("/snapshot.sh", freshOff, compactedFresh)

	// Step 5: ForceFlush uploads whatever is pending.
	if err := aw.ForceFlush("/snapshot.sh"); err != nil {
		t.Fatalf("flush: %v", err)
	}

	// The uploaded data should contain ALL sections, but with the old
	// destructive drain, sections 1+2 were replaced by section 3.
	hasSection1 := bytes.Contains(uploaded, []byte("# Snapshot file"))
	hasSection2 := bytes.Contains(uploaded, []byte("# Functions"))
	hasSection3 := bytes.Contains(uploaded, []byte("export PATH"))

	if hasSection1 && hasSection2 && hasSection3 {
		t.Fatal("BUG NOT REPRODUCED: expected sections 1+2 to be lost, but all sections are present")
	}
	if !hasSection3 {
		t.Fatal("expected at least section 3 to be present")
	}
	// Confirm the bug: sections 1+2 are lost.
	if hasSection1 || hasSection2 {
		t.Fatal("expected sections 1+2 to be lost due to AsyncWriter replacement")
	}
	t.Logf("BUG CONFIRMED: uploaded data is only section 3 (%d bytes), sections 1+2 lost", len(uploaded))
}

// TestSnapshotTruncationRace_NonDestructivePeek verifies that the peek-based
// read path does NOT cause the truncation bug. The writer's buffer remains
// intact, so all sections are flushed together on Release.
func TestSnapshotTruncationRace_NonDestructivePeek(t *testing.T) {
	section1 := []byte("# Snapshot file\nshopt -u autocd\n")
	section2 := []byte("# Functions\nalias ll='ls -la'\n")
	section3 := []byte("export PATH=/usr/bin\n")

	// Writer merges all 3 sections into the handle buffer.
	merged12 := mergeWriteBuffer(0, section1, 65536, section2)
	if merged12 == nil {
		t.Fatal("merge failed")
	}
	mergedAll := mergeWriteBuffer(merged12.off, merged12.data, 131072, section3)
	if mergedAll == nil {
		t.Fatal("merge failed")
	}

	var uploaded []byte
	aw := NewAsyncWriter(func(path string, off int64, data []byte) error {
		uploaded = make([]byte, len(data))
		copy(uploaded, data)
		return nil
	})

	// Concurrent reader peeks (non-destructive) — buffer stays intact.
	// Writer continues writing section 3, which merges into existing buffer.
	// On Release, the FULL buffer is flushed.
	_, compacted := compactNulls(mergedAll.off, mergedAll.data)
	aw.Enqueue("/snapshot.sh", 0, compacted)

	if err := aw.ForceFlush("/snapshot.sh"); err != nil {
		t.Fatalf("flush: %v", err)
	}

	hasSection1 := bytes.Contains(uploaded, []byte("# Snapshot file"))
	hasSection2 := bytes.Contains(uploaded, []byte("# Functions"))
	hasSection3 := bytes.Contains(uploaded, []byte("export PATH"))

	if !hasSection1 || !hasSection2 || !hasSection3 {
		t.Fatalf("expected all sections present; has1=%v has2=%v has3=%v", hasSection1, hasSection2, hasSection3)
	}
	t.Logf("FIX VERIFIED: all 3 sections present in uploaded data (%d bytes)", len(uploaded))
}

// TestReadWithCachedFlow_PeekDoesNotClearWriteBuffer verifies that a
// concurrent read does not drain the writer's handle buffer. Before the fix,
// enqueueWritesForPath would clear the handle buffer, causing subsequent
// writes to start a new buffer at a later offset and lose earlier sections
// when the AsyncWriter replaced its pending data.
func TestReadWithCachedFlow_PeekDoesNotClearWriteBuffer(t *testing.T) {
	header := []byte("# Snapshot file\nif true; then\n")
	functions := []byte("  echo hello\nfi\n")

	merged := mergeWriteBuffer(0, header, 65536, functions)
	if merged == nil {
		t.Fatal("merge failed")
	}
	_, compacted := compactNulls(merged.off, merged.data)

	var handleBuf struct {
		mu  sync.Mutex
		off int64
		buf []byte
	}
	handleBuf.off = merged.off
	handleBuf.buf = make([]byte, len(merged.data))
	copy(handleBuf.buf, merged.data)

	peekCalled := false
	aw := NewAsyncWriter(func(path string, off int64, data []byte) error {
		return nil
	})

	ops := cachedReadOps{
		content: NewContentCache(),
		writer:  aw,
		getHandleState: func(fh FileHandle) *handleState {
			return nil
		},
		peekHandleWrites: func(path string) (int64, []byte, bool) {
			peekCalled = true
			handleBuf.mu.Lock()
			defer handleBuf.mu.Unlock()
			if len(handleBuf.buf) == 0 {
				return 0, nil, false
			}
			cp := make([]byte, len(handleBuf.buf))
			copy(cp, handleBuf.buf)
			compOff, compData := compactNulls(handleBuf.off, cp)
			return compOff, compData, true
		},
		consumePrefetch: func(path string, off int64, state *handleState) ([]byte, bool, error) {
			return nil, false, nil
		},
		maybeStatSmall: func(path string) (*pb.FileInfo, bool) {
			return nil, false
		},
		readRange: func(path string, off int64, length int64) ([]byte, error) {
			return nil, fs.ErrNotExist
		},
		recordRead: func(state *handleState, path string, off int64, n int) {},
	}

	readBuf := make([]byte, 4096)
	n, attr, err := readWithCachedFlow("/test/snapshot.sh", readBuf, 0, 0, ops)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if !peekCalled {
		t.Fatal("peekHandleWrites was not called")
	}
	if attr == nil || attr.CacheSource != CacheSourceDirtyBuffer {
		t.Fatalf("expected dirty buffer source, got %v", attr)
	}
	if !bytes.Equal(readBuf[:n], compacted) {
		t.Fatalf("read data mismatch:\n  got:  %q\n  want: %q", string(readBuf[:n]), string(compacted))
	}

	// The critical assertion: the handle buffer must NOT have been cleared.
	handleBuf.mu.Lock()
	bufStillIntact := len(handleBuf.buf) > 0
	handleBuf.mu.Unlock()
	if !bufStillIntact {
		t.Fatal("handle buffer was cleared by the read — this is the bug that causes truncated shell snapshots")
	}
}

// TestReadWithCachedFlow_FallsThroughToAsyncWriter verifies that when no
// handle buffer exists, the read path still serves from the AsyncWriter.
func TestReadWithCachedFlow_FallsThroughToAsyncWriter(t *testing.T) {
	aw := NewAsyncWriter(func(path string, off int64, data []byte) error {
		return nil
	})
	aw.EnqueueNoTimer("/test/file.txt", 0, []byte("dirty data"))

	ops := cachedReadOps{
		content: NewContentCache(),
		writer:  aw,
		getHandleState: func(fh FileHandle) *handleState {
			return nil
		},
		peekHandleWrites: func(path string) (int64, []byte, bool) {
			return 0, nil, false
		},
		consumePrefetch: func(path string, off int64, state *handleState) ([]byte, bool, error) {
			return nil, false, nil
		},
		maybeStatSmall: func(path string) (*pb.FileInfo, bool) {
			return nil, false
		},
		readRange: func(path string, off int64, length int64) ([]byte, error) {
			return nil, fs.ErrNotExist
		},
		recordRead: func(state *handleState, path string, off int64, n int) {},
	}

	readBuf := make([]byte, 4096)
	n, attr, err := readWithCachedFlow("/test/file.txt", readBuf, 0, 0, ops)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if attr == nil || attr.CacheSource != CacheSourceDirtyBuffer {
		t.Fatalf("expected dirty buffer source, got %v", attr)
	}
	if !bytes.Equal(readBuf[:n], []byte("dirty data")) {
		t.Fatalf("read data = %q, want %q", string(readBuf[:n]), "dirty data")
	}
}
