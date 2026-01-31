package worker

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/rs/zerolog/log"
)

// MountConfig configures the mount manager
type MountConfig struct {
	// MountDir is the base directory for per-task mounts
	MountDir string

	// CLIBinary is the path to the CLI binary
	CLIBinary string

	// GatewayAddr is the gateway gRPC address
	GatewayAddr string

	// MountReadyTimeout is how long to wait for a mount to be ready
	MountReadyTimeout time.Duration
}

// DefaultMountConfig returns sensible defaults
func DefaultMountConfig() MountConfig {
	return MountConfig{
		MountDir:          "/tmp/airstore-mounts",
		CLIBinary:         "/usr/local/bin/cli",
		MountReadyTimeout: 5 * time.Second,
	}
}

// MountManager manages per-task FUSE mounts
type MountManager struct {
	config MountConfig
	mounts map[string]*taskMount
	mu     sync.RWMutex
}

// taskMount represents an active FUSE mount for a task
type taskMount struct {
	taskID    string
	token     string
	mountPath string
	cmd       *exec.Cmd
	ctx       context.Context
	cancel    context.CancelFunc
	ready     bool
	err       error
}

// NewMountManager creates a new mount manager
func NewMountManager(config MountConfig) (*MountManager, error) {
	// Ensure mount directory exists
	if err := os.MkdirAll(config.MountDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create mount directory %s: %w", config.MountDir, err)
	}

	// Verify CLI binary exists
	if _, err := os.Stat(config.CLIBinary); err != nil {
		return nil, fmt.Errorf("CLI binary not found at %s: %w", config.CLIBinary, err)
	}

	return &MountManager{
		config: config,
		mounts: make(map[string]*taskMount),
	}, nil
}

// Mount creates a FUSE mount for a task using the given token
// Returns the mount path that can be bind-mounted into containers
func (m *MountManager) Mount(ctx context.Context, taskID, token string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Check if mount already exists
	if mount, exists := m.mounts[taskID]; exists {
		if mount.ready {
			return mount.mountPath, nil
		}
		return "", fmt.Errorf("mount for task %s is still initializing", taskID)
	}

	// Create mount directory for this task
	mountPath := filepath.Join(m.config.MountDir, taskID)
	if err := os.MkdirAll(mountPath, 0755); err != nil {
		return "", fmt.Errorf("failed to create task mount directory: %w", err)
	}

	// Create cancellable context for the mount process
	mountCtx, cancel := context.WithCancel(ctx)

	// Start FUSE mount process
	cmd := exec.CommandContext(mountCtx, m.config.CLIBinary, "mount",
		mountPath,
		"--gateway", m.config.GatewayAddr,
		"--token", token,
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Start(); err != nil {
		cancel()
		os.RemoveAll(mountPath)
		return "", fmt.Errorf("failed to start mount process: %w", err)
	}

	mount := &taskMount{
		taskID:    taskID,
		token:     token,
		mountPath: mountPath,
		cmd:       cmd,
		ctx:       mountCtx,
		cancel:    cancel,
		ready:     false,
	}
	m.mounts[taskID] = mount

	// Wait for mount to be ready (check for files in the mount)
	go m.waitForReady(mount)

	// Block until ready or timeout
	readyCtx, readyCancel := context.WithTimeout(ctx, m.config.MountReadyTimeout)
	defer readyCancel()

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-readyCtx.Done():
			// Timeout - check if mount has any files
			entries, err := os.ReadDir(mountPath)
			if err == nil && len(entries) > 0 {
				mount.ready = true
				log.Info().
					Str("task_id", taskID).
					Str("mount_path", mountPath).
					Int("files", len(entries)).
					Msg("task mount ready")
				return mountPath, nil
			}
			log.Warn().
				Str("task_id", taskID).
				Str("mount_path", mountPath).
				Msg("mount ready timeout, proceeding anyway")
			mount.ready = true
			return mountPath, nil
		case <-ticker.C:
			if mount.ready {
				return mountPath, nil
			}
			if mount.err != nil {
				return "", mount.err
			}
		}
	}
}

// waitForReady waits for the mount to have files
func (m *MountManager) waitForReady(mount *taskMount) {
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-mount.ctx.Done():
			return
		case <-ticker.C:
			entries, err := os.ReadDir(mount.mountPath)
			if err == nil && len(entries) > 0 {
				m.mu.Lock()
				mount.ready = true
				m.mu.Unlock()

				log.Debug().
					Str("task_id", mount.taskID).
					Int("files", len(entries)).
					Msg("mount became ready")
				return
			}
		}
	}
}

// Unmount stops the FUSE mount for a task and cleans up
func (m *MountManager) Unmount(taskID string) error {
	m.mu.Lock()
	mount, exists := m.mounts[taskID]
	if !exists {
		m.mu.Unlock()
		return nil // Already unmounted
	}
	delete(m.mounts, taskID)
	m.mu.Unlock()

	log.Info().Str("task_id", taskID).Msg("unmounting task filesystem")

	// Stop the mount process
	if mount.cmd != nil && mount.cmd.Process != nil {
		mount.cancel()
		mount.cmd.Process.Signal(syscall.SIGTERM)

		// Wait with timeout
		done := make(chan error, 1)
		go func() { done <- mount.cmd.Wait() }()

		select {
		case <-done:
		case <-time.After(5 * time.Second):
			mount.cmd.Process.Kill()
		}
	}

	// Try fusermount to ensure clean unmount
	exec.Command("fusermount", "-u", mount.mountPath).Run()

	// Remove mount directory
	if err := os.RemoveAll(mount.mountPath); err != nil {
		log.Warn().Err(err).Str("path", mount.mountPath).Msg("failed to remove mount directory")
	}

	return nil
}

// GetMountPath returns the mount path for a task, or empty if not mounted
func (m *MountManager) GetMountPath(taskID string) string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if mount, exists := m.mounts[taskID]; exists && mount.ready {
		return mount.mountPath
	}
	return ""
}

// CleanupAll unmounts all active mounts (call on shutdown)
func (m *MountManager) CleanupAll() {
	m.mu.Lock()
	taskIDs := make([]string, 0, len(m.mounts))
	for id := range m.mounts {
		taskIDs = append(taskIDs, id)
	}
	m.mu.Unlock()

	for _, id := range taskIDs {
		if err := m.Unmount(id); err != nil {
			log.Warn().Err(err).Str("task_id", id).Msg("failed to unmount during cleanup")
		}
	}
}

// CleanupStale removes mounts for tasks that are no longer running
func (m *MountManager) CleanupStale(activeTasks map[string]bool) int {
	m.mu.Lock()
	stale := make([]string, 0)
	for id := range m.mounts {
		if !activeTasks[id] {
			stale = append(stale, id)
		}
	}
	m.mu.Unlock()

	for _, id := range stale {
		log.Info().Str("task_id", id).Msg("cleaning up stale mount")
		m.Unmount(id)
	}

	return len(stale)
}

// ActiveMounts returns the number of active mounts
func (m *MountManager) ActiveMounts() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.mounts)
}
