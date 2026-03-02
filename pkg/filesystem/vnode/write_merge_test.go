package vnode

import (
	"bytes"
	"testing"
)

func TestMergeWriteBuffer_Contiguous(t *testing.T) {
	m := mergeWriteBuffer(0, []byte("hello"), 5, []byte(" world"))
	if m == nil {
		t.Fatal("expected non-nil")
	}
	if m.off != 0 {
		t.Fatalf("off = %d, want 0", m.off)
	}
	if !bytes.Equal(m.data, []byte("hello world")) {
		t.Fatalf("data = %q", m.data)
	}
}

func TestMergeWriteBuffer_Gap_ZeroFilled(t *testing.T) {
	m := mergeWriteBuffer(0, []byte("AB"), 5, []byte("CD"))
	if m == nil {
		t.Fatal("expected non-nil")
	}
	if m.off != 0 {
		t.Fatalf("off = %d, want 0", m.off)
	}
	// Gap is zero-filled to preserve correct byte positions.
	want := []byte{'A', 'B', 0, 0, 0, 'C', 'D'}
	if !bytes.Equal(m.data, want) {
		t.Fatalf("data = %x, want %x", m.data, want)
	}
}

func TestMergeWriteBuffer_Overlap(t *testing.T) {
	m := mergeWriteBuffer(0, []byte("AAAA"), 2, []byte("BB"))
	if m == nil {
		t.Fatal("expected non-nil")
	}
	if m.off != 0 {
		t.Fatalf("off = %d, want 0", m.off)
	}
	if !bytes.Equal(m.data, []byte("AABB")) {
		t.Fatalf("data = %q", m.data)
	}
}

func TestMergeWriteBuffer_NewBeforeOld(t *testing.T) {
	m := mergeWriteBuffer(10, []byte("old"), 5, []byte("new"))
	if m == nil {
		t.Fatal("expected non-nil")
	}
	if m.off != 5 {
		t.Fatalf("off = %d, want 5", m.off)
	}
	// new: bytes 5-7, old: bytes 10-12 — gap at 8-9 is zero-filled.
	want := []byte{'n', 'e', 'w', 0, 0, 'o', 'l', 'd'}
	if !bytes.Equal(m.data, want) {
		t.Fatalf("data = %x, want %x", m.data, want)
	}
}

func TestMergeWriteBuffer_BothEmpty(t *testing.T) {
	m := mergeWriteBuffer(0, nil, 0, nil)
	if m != nil {
		t.Fatalf("expected nil for empty inputs, got %v", m)
	}
}

func TestCompactNulls_ShellScript(t *testing.T) {
	// Simulates what the page cache delivers: a single buffer at offset 0
	// with zero-filled gaps from sparse writes.
	data := make([]byte, 129)
	copy(data[0:], "# Snapshot file\n")
	copy(data[101:], "# Functions\nshopt -u autocd\n")
	// Bytes 16-100 are NULLs from the page cache.

	off, compacted := compactNulls(0, data)
	if off != 0 {
		t.Fatalf("off = %d, want 0", off)
	}
	if bytes.Contains(compacted, []byte{0}) {
		t.Fatal("compacted data still contains NULL bytes")
	}
	want := "# Snapshot file\n# Functions\nshopt -u autocd\n"
	if string(compacted) != want {
		t.Fatalf("compacted = %q, want %q", string(compacted), want)
	}
}

func TestCompactNulls_NonZeroOffset(t *testing.T) {
	data := []byte("# has\x00nulls")
	off, out := compactNulls(10, data)
	if off != 10 || !bytes.Equal(out, data) {
		t.Fatal("should not modify data at non-zero offset")
	}
}

func TestCompactNulls_BinaryFile(t *testing.T) {
	data := []byte{0x89, 0x50, 0x4E, 0x47, 0x00, 0x00}
	off, out := compactNulls(0, data)
	if off != 0 || !bytes.Equal(out, data) {
		t.Fatal("should not modify binary data (first byte is not #)")
	}
}

func TestCompactNulls_BinaryStartingWithHash(t *testing.T) {
	// Binary data that starts with '#' (0x23) but has no newline —
	// compactNulls must not strip the NULLs.
	data := []byte{'#', 0xFF, 0x00, 0xAB, 0x00, 0xCD, 0xEF}
	off, out := compactNulls(0, data)
	if off != 0 || !bytes.Equal(out, data) {
		t.Fatal("should not modify binary data starting with # but no newline")
	}
}

func TestCompactNulls_NoNulls(t *testing.T) {
	data := []byte("# clean shell script\necho hello\n")
	off, out := compactNulls(0, data)
	if off != 0 || !bytes.Equal(out, data) {
		t.Fatal("should not modify data without NULLs")
	}
}

func TestMergeWriteBuffer_SnapshotPattern(t *testing.T) {
	// Simulate the Claude Code shell snapshot write pattern:
	// 4 sections at fixed offsets with gaps between them.
	header := []byte("# Snapshot file\n")
	options := []byte("# Functions\n# Shell Options\nshopt -u autocd\n")
	aliases := []byte("# Aliases\n")
	env := []byte("export PATH=/usr/bin\n")

	// Merge header + options (gap at offsets 16-100)
	m := mergeWriteBuffer(0, header, 101, options)
	if m == nil {
		t.Fatal("expected non-nil after merge 1")
	}

	// Merge with aliases (gap at some offset)
	m = mergeWriteBuffer(m.off, m.data, 1330, aliases)
	if m == nil {
		t.Fatal("expected non-nil after merge 2")
	}

	// Merge with env
	m = mergeWriteBuffer(m.off, m.data, 1511, env)
	if m == nil {
		t.Fatal("expected non-nil after merge 3")
	}

	// After merge, the buffer has zero-filled gaps (correct offset preservation).
	if !bytes.Contains(m.data, []byte{0}) {
		t.Fatal("merged data should contain zero-filled gaps")
	}

	// compactNulls strips the NULLs for text files at flush time.
	_, compacted := compactNulls(m.off, m.data)
	if bytes.Contains(compacted, []byte{0}) {
		t.Fatal("compacted data still contains NULL bytes")
	}

	// Verify all sections are present in the compacted result.
	for _, section := range []string{"# Snapshot file", "# Functions", "# Aliases", "export PATH"} {
		if !bytes.Contains(compacted, []byte(section)) {
			t.Fatalf("missing section %q in compacted output", section)
		}
	}
}

func TestMergeWriteBuffer_BinaryPreservesOffsets(t *testing.T) {
	// Binary file with sparse writes — offsets must be preserved exactly.
	m := mergeWriteBuffer(0, []byte{0x89, 0x50, 0x4E, 0x47}, 100, []byte{0xFF, 0xD8, 0xFF})
	if m == nil {
		t.Fatal("expected non-nil")
	}
	if m.off != 0 {
		t.Fatalf("off = %d, want 0", m.off)
	}
	if len(m.data) != 103 {
		t.Fatalf("len = %d, want 103 (preserving gap)", len(m.data))
	}
	// First chunk at correct position.
	if m.data[0] != 0x89 || m.data[3] != 0x47 {
		t.Fatal("first chunk not at correct offset")
	}
	// Second chunk at correct position.
	if m.data[100] != 0xFF || m.data[102] != 0xFF {
		t.Fatal("second chunk not at correct offset")
	}
	// Gap is zero-filled.
	for i := 4; i < 100; i++ {
		if m.data[i] != 0 {
			t.Fatalf("expected zero at offset %d, got %x", i, m.data[i])
		}
	}
	// compactNulls should NOT strip NULLs from binary data.
	_, out := compactNulls(m.off, m.data)
	if !bytes.Equal(out, m.data) {
		t.Fatal("compactNulls should not modify binary data")
	}
}
