package services

import (
	"syscall"
	"testing"
)

func TestModeMetadataRoundTrip(t *testing.T) {
	mode := uint32(syscall.S_IFREG | 0755)
	encoded := encodeModeMetadata(mode)
	decoded, ok := decodeModeMetadata(encoded)
	if !ok {
		t.Fatal("expected mode metadata to decode")
	}
	if decoded != mode {
		t.Fatalf("expected %#o, got %#o", mode, decoded)
	}
}

func TestModeFromMetadataUsesFallbackType(t *testing.T) {
	metadata := map[string]string{storageModeMetadataKey: "755"}
	mode := modeFromMetadata(metadata, defaultStorageFileMode)
	if mode&syscall.S_IFMT != syscall.S_IFREG {
		t.Fatalf("expected regular file type, got %#o", mode&syscall.S_IFMT)
	}
	if mode&07777 != 0755 {
		t.Fatalf("expected perms 0755, got %#o", mode&07777)
	}
}

func TestWithModeMetadataPreservesExistingKeys(t *testing.T) {
	metadata := map[string]string{
		"symlink-target": "/tmp/target",
		"custom":         "value",
	}

	result := withModeMetadata(metadata, syscall.S_IFLNK|0777)
	if result["symlink-target"] != "/tmp/target" {
		t.Fatalf("expected symlink-target to be preserved, got %q", result["symlink-target"])
	}
	if result["custom"] != "value" {
		t.Fatalf("expected custom metadata to be preserved, got %q", result["custom"])
	}
	if result[storageModeMetadataKey] == "" {
		t.Fatal("expected mode metadata to be set")
	}
	if _, exists := metadata[storageModeMetadataKey]; exists {
		t.Fatal("expected original metadata map to remain unchanged")
	}
}

func TestSanitizeModePreservesRequestedType(t *testing.T) {
	mode := sanitizeMode(syscall.S_IFLNK|0777, syscall.S_IFREG, 0644)
	if mode&syscall.S_IFMT != syscall.S_IFLNK {
		t.Fatalf("expected symlink type, got %#o", mode&syscall.S_IFMT)
	}
	if mode&07777 != 0777 {
		t.Fatalf("expected perms 0777, got %#o", mode&07777)
	}
}
