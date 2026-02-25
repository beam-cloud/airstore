package repository

import (
	"bytes"
	"testing"
)

func TestShouldIndexResultContent(t *testing.T) {
	text := []byte("hello world")
	if !shouldIndexResultContent("msg:abc", text) {
		t.Fatalf("expected message text content to be indexable")
	}

	if shouldIndexResultContent("att:abc:def", text) {
		t.Fatalf("expected attachment result IDs to be excluded from indexing")
	}

	large := bytes.Repeat([]byte("a"), maxIndexedContentSize+1)
	if shouldIndexResultContent("msg:abc", large) {
		t.Fatalf("expected oversized content to be excluded from indexing")
	}

	binary := []byte{0x00, 0x01, 0x02, 0x03}
	if shouldIndexResultContent("msg:abc", binary) {
		t.Fatalf("expected binary content to be excluded from indexing")
	}
}

func TestLooksBinaryContent(t *testing.T) {
	if looksBinaryContent([]byte("plain text\nwith newlines")) {
		t.Fatalf("expected plain text to be treated as non-binary")
	}

	if !looksBinaryContent([]byte{0xff, 0xfe, 0xfd, 0x00}) {
		t.Fatalf("expected binary bytes to be treated as binary")
	}
}
