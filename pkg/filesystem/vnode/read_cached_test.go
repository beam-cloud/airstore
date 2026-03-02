package vnode

import (
	"io/fs"
	"testing"
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
