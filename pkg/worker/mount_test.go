package worker

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCheckFilesystemMountReady_AllRootsPresent(t *testing.T) {
	mountPath := t.TempDir()
	for _, root := range requiredFilesystemRoots {
		if err := os.MkdirAll(filepath.Join(mountPath, root), 0o755); err != nil {
			t.Fatalf("mkdir %q: %v", root, err)
		}
	}

	ready, missing, err := checkFilesystemMountReady(mountPath)
	if err != nil {
		t.Fatalf("checkFilesystemMountReady error: %v", err)
	}
	if !ready {
		t.Fatalf("expected ready mount, got missing=%v", missing)
	}
	if len(missing) != 0 {
		t.Fatalf("expected no missing roots, got %v", missing)
	}
}

func TestCheckFilesystemMountReady_CaseInsensitiveRoots(t *testing.T) {
	mountPath := t.TempDir()
	for _, root := range requiredFilesystemRoots {
		if err := os.MkdirAll(filepath.Join(mountPath, strings.ToUpper(root)), 0o755); err != nil {
			t.Fatalf("mkdir %q: %v", root, err)
		}
	}

	ready, missing, err := checkFilesystemMountReady(mountPath)
	if err != nil {
		t.Fatalf("checkFilesystemMountReady error: %v", err)
	}
	if !ready {
		t.Fatalf("expected ready mount, got missing=%v", missing)
	}
}

func TestCheckFilesystemMountReady_MissingRoots(t *testing.T) {
	mountPath := t.TempDir()
	if err := os.MkdirAll(filepath.Join(mountPath, "memory"), 0o755); err != nil {
		t.Fatalf("mkdir memory: %v", err)
	}

	ready, missing, err := checkFilesystemMountReady(mountPath)
	if err != nil {
		t.Fatalf("checkFilesystemMountReady error: %v", err)
	}
	if ready {
		t.Fatalf("expected mount to be not ready")
	}
	if len(missing) == 0 {
		t.Fatalf("expected at least one missing root")
	}
}
