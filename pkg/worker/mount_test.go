package worker

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
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

func TestWatchMountStopsOnTaskContextDone(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	mount := &taskMount{
		mountPath: t.TempDir(),
		exited:    make(chan error, 1),
		taskCtx:   ctx,
		stopWatch: make(chan struct{}),
	}
	mgr := &MountManager{
		config: DefaultMountConfig(),
		mounts: map[string]*taskMount{"test-task": mount},
	}

	done := make(chan struct{})
	go func() {
		mgr.watchMount("test-task", mount)
		close(done)
	}()

	// Cancel the task context — watcher should exit when it checks
	cancel()
	mount.exited <- nil

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("watchMount did not exit after task context cancelled")
	}
}

func TestWatchMountStopsOnStopSignal(t *testing.T) {
	mount := &taskMount{
		mountPath: t.TempDir(),
		exited:    make(chan error, 1),
		taskCtx:   context.Background(),
		stopWatch: make(chan struct{}),
	}
	mgr := &MountManager{
		config: DefaultMountConfig(),
		mounts: map[string]*taskMount{"test-task": mount},
	}

	done := make(chan struct{})
	go func() {
		mgr.watchMount("test-task", mount)
		close(done)
	}()

	close(mount.stopWatch)
	mount.exited <- nil

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("watchMount did not exit after stop signal")
	}
}
