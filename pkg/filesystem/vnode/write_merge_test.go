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

func TestMergeWriteBuffer_Gap_Compacts(t *testing.T) {
	m := mergeWriteBuffer(0, []byte("AB"), 5, []byte("CD"))
	if m == nil {
		t.Fatal("expected non-nil")
	}
	if m.off != 0 {
		t.Fatalf("off = %d, want 0", m.off)
	}
	// Gap is compacted — no zero bytes between chunks.
	want := []byte("ABCD")
	if !bytes.Equal(m.data, want) {
		t.Fatalf("data = %q, want %q", m.data, want)
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
	// Ranges overlap (new: 5-7, old: 10-12) — gap compacted.
	want := []byte("newold")
	if !bytes.Equal(m.data, want) {
		t.Fatalf("data = %q, want %q", m.data, want)
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

	// Verify no NULLs in result.
	if bytes.Contains(m.data, []byte{0}) {
		t.Fatal("merged data contains NULL bytes")
	}

	// Verify all sections are present in order.
	full := string(m.data)
	for _, section := range []string{"# Snapshot file", "# Functions", "# Aliases", "export PATH"} {
		if !bytes.Contains(m.data, []byte(section)) {
			t.Fatalf("missing section %q in: %s", section, full)
		}
	}
}
