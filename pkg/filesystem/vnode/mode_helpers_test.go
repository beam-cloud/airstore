package vnode

import (
	"syscall"
	"testing"
)

func TestNormalizeRuntimeWritableMode(t *testing.T) {
	tests := []struct {
		name string
		in   uint32
		want uint32
	}{
		{
			name: "directory becomes world writable",
			in:   syscall.S_IFDIR | 0755,
			want: syscall.S_IFDIR | 0777,
		},
		{
			name: "regular file becomes world writable",
			in:   syscall.S_IFREG | 0644,
			want: syscall.S_IFREG | 0666,
		},
		{
			name: "preserve execute bits on regular files",
			in:   syscall.S_IFREG | 0700,
			want: syscall.S_IFREG | 0766,
		},
		{
			name: "symlink remains symlink",
			in:   syscall.S_IFLNK | 0700,
			want: syscall.S_IFLNK | 0777,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := normalizeRuntimeWritableMode(tt.in)
			if got != tt.want {
				t.Fatalf("expected mode %#o, got %#o", tt.want, got)
			}
		})
	}
}

func TestSanitizeNodeModePreservesRequestedType(t *testing.T) {
	mode := sanitizeNodeMode(syscall.S_IFLNK|0777, syscall.S_IFREG, 0644)
	if mode&syscall.S_IFMT != syscall.S_IFLNK {
		t.Fatalf("expected symlink file type, got %#o", mode&syscall.S_IFMT)
	}
	if mode&07777 != 0777 {
		t.Fatalf("expected perms 0777, got %#o", mode&07777)
	}
}
