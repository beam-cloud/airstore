package worker

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/beam-cloud/airstore/pkg/types"
)

func TestImageIDFromRef(t *testing.T) {
	arch := runtime.GOARCH
	tests := []struct {
		input    string
		expected string
	}{
		{"ubuntu:22.04", "ubuntu_22_04_" + arch},
		{"docker.io/library/ubuntu:22.04", "docker_io_library_ubuntu_22_04_" + arch},
		{"registry.localhost:5000/myimage:latest", "registry_localhost_5000_myimage_latest_" + arch},
		{"ghcr.io/owner/repo@sha256:abc123", "ghcr_io_owner_repo_sha256_abc123_" + arch},
	}

	for _, tc := range tests {
		t.Run(tc.input, func(t *testing.T) {
			result := imageIDFromRef(tc.input)
			if result != tc.expected {
				t.Errorf("imageIDFromRef(%q) = %q, want %q", tc.input, result, tc.expected)
			}
		})
	}
}

func TestNormalizeImageRef(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"ubuntu:22.04", "docker.io/library/ubuntu:22.04"},
		{"library/ubuntu:22.04", "docker.io/library/ubuntu:22.04"},
		{"docker.io/library/ubuntu:22.04", "docker.io/library/ubuntu:22.04"},
		{"registry.localhost:5000/myimage:latest", "registry.localhost:5000/myimage:latest"},
		{"ghcr.io/owner/repo:tag", "ghcr.io/owner/repo:tag"},
		{"ubuntu", "docker.io/library/ubuntu:latest"},
		{"user/repo", "docker.io/user/repo:latest"},
	}

	for _, tc := range tests {
		t.Run(tc.input, func(t *testing.T) {
			result := normalizeImageRef(tc.input)
			if result != tc.expected {
				t.Errorf("normalizeImageRef(%q) = %q, want %q", tc.input, result, tc.expected)
			}
		})
	}
}

// TestImageManagerIntegration is an integration test that requires network access.
// Run with: INTEGRATION_TEST=true go test -v -run TestImageManagerIntegration
func TestImageManagerIntegration(t *testing.T) {
	if os.Getenv("INTEGRATION_TEST") != "true" {
		t.Skip("Skipping integration test; set INTEGRATION_TEST=true to run")
	}

	tmpDir, err := os.MkdirTemp("", "clip-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	cfg := types.ImageConfig{
		CachePath: filepath.Join(tmpDir, "cache"),
		WorkPath:  filepath.Join(tmpDir, "work"),
		MountPath: filepath.Join(tmpDir, "mnt"),
	}

	mgr, err := NewImageManager(cfg)
	if err != nil {
		t.Fatalf("Failed to create image manager: %v", err)
	}
	defer mgr.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	rootfs, cleanup, err := mgr.PrepareRootfs(ctx, "alpine:3.18")
	if err != nil {
		t.Fatalf("Failed to prepare rootfs: %v", err)
	}
	defer cleanup()

	// Verify rootfs structure
	expectedPaths := []string{"bin", "etc", "usr"}
	for _, p := range expectedPaths {
		fullPath := filepath.Join(rootfs, p)
		if _, err := os.Stat(fullPath); err != nil {
			t.Errorf("Expected path %s not found: %v", fullPath, err)
		}
	}

	t.Logf("Successfully prepared rootfs at: %s", rootfs)
}

// TestImageManagerRefCounting tests reference counting for mount reuse.
func TestImageManagerRefCounting(t *testing.T) {
	if os.Getenv("INTEGRATION_TEST") != "true" {
		t.Skip("Skipping integration test; set INTEGRATION_TEST=true to run")
	}

	tmpDir, err := os.MkdirTemp("", "clip-refcount-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	cfg := types.ImageConfig{
		CachePath: filepath.Join(tmpDir, "cache"),
		WorkPath:  filepath.Join(tmpDir, "work"),
		MountPath: filepath.Join(tmpDir, "mnt"),
	}

	mgr, err := NewImageManager(cfg)
	if err != nil {
		t.Fatalf("Failed to create image manager: %v", err)
	}
	defer mgr.Close()

	ctx := context.Background()
	imageRef := "alpine:3.18"

	// First mount
	rootfs1, cleanup1, err := mgr.PrepareRootfs(ctx, imageRef)
	if err != nil {
		t.Fatalf("First PrepareRootfs failed: %v", err)
	}

	// Second mount should reuse existing
	rootfs2, cleanup2, err := mgr.PrepareRootfs(ctx, imageRef)
	if err != nil {
		t.Fatalf("Second PrepareRootfs failed: %v", err)
	}

	if rootfs1 != rootfs2 {
		t.Errorf("Expected same rootfs path, got %s and %s", rootfs1, rootfs2)
	}

	// First cleanup - mount should still exist
	cleanup1()
	if _, err := os.Stat(rootfs1); err != nil {
		t.Errorf("Rootfs should still exist after first cleanup: %v", err)
	}

	// Second cleanup - mount should be removed
	cleanup2()
	time.Sleep(500 * time.Millisecond)
}
