package worker

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/beam-cloud/airstore/pkg/types"
	"github.com/rs/zerolog/log"
)

const (
	defaultMountReadyTimeout = 5 * time.Second
	mountReadyPollInterval   = 100 * time.Millisecond
	mountDirPerm             = 0o755
	mountSubcommand          = "mount"
	fusermountBinary         = "fusermount"
	fusermountUnmountFlag    = "-u"
	termWaitTimeout          = 5 * time.Second
	killWaitTimeout          = 2 * time.Second
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
		MountDir:          types.DefaultMountDir,
		CLIBinary:         types.DefaultCLIBinary,
		MountReadyTimeout: defaultMountReadyTimeout,
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
	mountPath string
	cmd       *exec.Cmd
	cancel    context.CancelFunc
	ready     bool
	exited    chan error
}

var (
	requiredFilesystemRoots   = buildRequiredFilesystemRoots()
	requiredFilesystemRootSet = buildRequiredFilesystemRootSet(requiredFilesystemRoots)
)

func buildRequiredFilesystemRoots() []string {
	roots := make([]string, 0, len(types.SystemPaths()))
	for _, path := range types.SystemPaths() {
		root := strings.TrimPrefix(strings.ToLower(path), "/")
		if root != "" {
			roots = append(roots, root)
		}
	}
	sort.Strings(roots)
	return roots
}

func buildRequiredFilesystemRootSet(roots []string) map[string]struct{} {
	out := make(map[string]struct{}, len(roots))
	for _, root := range roots {
		out[root] = struct{}{}
	}
	return out
}

// checkFilesystemMountReady validates that the mount path contains all expected
// system root directories (/skills, /sources, /tasks, /tools).
func checkFilesystemMountReady(mountPath string) (bool, []string, error) {
	entries, err := os.ReadDir(mountPath)
	if err != nil {
		return false, nil, err
	}

	present := make(map[string]struct{}, len(entries))
	for _, entry := range entries {
		name := strings.ToLower(strings.TrimSpace(entry.Name()))
		if name == "" {
			continue
		}

		if _, tracked := requiredFilesystemRootSet[name]; !tracked {
			continue
		}
		if entry.IsDir() {
			present[name] = struct{}{}
		}
	}

	missing := make([]string, 0)
	for _, root := range requiredFilesystemRoots {
		if _, ok := present[root]; !ok {
			missing = append(missing, root)
		}
	}
	return len(missing) == 0, missing, nil
}

// NewMountManager creates a new mount manager
func NewMountManager(config MountConfig) (*MountManager, error) {
	// Ensure mount directory exists
	if err := os.MkdirAll(config.MountDir, mountDirPerm); err != nil {
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

func (m *MountManager) getOrCreateMount(taskID string) (*taskMount, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if mount, exists := m.mounts[taskID]; exists {
		return mount, false
	}

	mount := &taskMount{
		mountPath: filepath.Join(m.config.MountDir, taskID),
		exited:    make(chan error, 1),
	}
	m.mounts[taskID] = mount
	return mount, true
}

func (m *MountManager) markMountReady(mount *taskMount) {
	m.mu.Lock()
	mount.ready = true
	m.mu.Unlock()
}

// Mount creates a FUSE mount for a task using the given token
// Returns the mount path that can be bind-mounted into containers
func (m *MountManager) Mount(ctx context.Context, taskID, token string) (string, error) {
	mount, created := m.getOrCreateMount(taskID)
	if !created {
		if mount.ready {
			return mount.mountPath, nil
		}

		select {
		case err, ok := <-mount.exited:
			if ok && err != nil {
				return "", fmt.Errorf("mount process for task %s exited during initialization: %w", taskID, err)
			}
			return "", fmt.Errorf("mount process for task %s exited before becoming ready", taskID)
		default:
			return "", fmt.Errorf("mount for task %s is still initializing at %s", taskID, mount.mountPath)
		}
	}

	cleanupOnError := true
	defer func() {
		if cleanupOnError {
			if err := m.Unmount(taskID); err != nil {
				log.Warn().Err(err).Str("task_id", taskID).Msg("failed to cleanup mount after error")
			}
		}
	}()

	// Create mount directory for this task
	mountPath := mount.mountPath
	if err := os.MkdirAll(mountPath, mountDirPerm); err != nil {
		return "", fmt.Errorf("failed to create task mount directory: %w", err)
	}

	// Create cancellable context for the mount process
	mountCtx, cancel := context.WithCancel(ctx)

	// Start FUSE mount process with sandbox user ownership
	cmd := exec.CommandContext(mountCtx, m.config.CLIBinary, mountSubcommand,
		mountPath,
		"--gateway", m.config.GatewayAddr,
		"--token", token,
		"--daemon",
		"--uid", fmt.Sprintf("%d", types.SandboxUserUID),
		"--gid", fmt.Sprintf("%d", types.SandboxUserGID),
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Start(); err != nil {
		cancel()
		return "", fmt.Errorf("failed to start mount process: %w", err)
	}

	m.mu.Lock()
	mount.cmd = cmd
	mount.cancel = cancel
	m.mu.Unlock()

	go func() {
		mount.exited <- cmd.Wait()
		close(mount.exited)
	}()

	timeout := time.NewTimer(m.config.MountReadyTimeout)
	defer timeout.Stop()

	ticker := time.NewTicker(mountReadyPollInterval)
	defer ticker.Stop()

	var lastMissing []string
	var lastReadErr error
	for {
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-timeout.C:
			ready, missing, err := checkFilesystemMountReady(mountPath)
			if ready {
				m.markMountReady(mount)
				cleanupOnError = false
				log.Info().
					Str("task_id", taskID).
					Str("mount_path", mountPath).
					Msg("task mount ready")
				return mountPath, nil
			}
			if err != nil {
				if lastReadErr != nil {
					err = lastReadErr
				}
				return "", fmt.Errorf("task mount %s not ready: %w", mountPath, err)
			}
			if len(missing) == 0 {
				missing = lastMissing
			}
			return "", fmt.Errorf("task mount %s missing required roots: %s", mountPath, strings.Join(missing, ", "))
		case err, ok := <-mount.exited:
			if ok && err != nil {
				return "", fmt.Errorf("mount process for task %s exited before ready: %w", taskID, err)
			}
			return "", fmt.Errorf("mount process for task %s exited before becoming ready", taskID)
		case <-ticker.C:
			ready, missing, err := checkFilesystemMountReady(mountPath)
			if err != nil {
				lastReadErr = err
				continue
			}
			lastMissing = missing
			if !ready {
				continue
			}

			m.markMountReady(mount)
			cleanupOnError = false
			log.Info().
				Str("task_id", taskID).
				Str("mount_path", mountPath).
				Msg("task mount ready")
			return mountPath, nil
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
		_ = mount.cmd.Process.Signal(syscall.SIGTERM)

		if mount.exited != nil {
			select {
			case <-mount.exited:
			case <-time.After(termWaitTimeout):
				_ = mount.cmd.Process.Kill()
				select {
				case <-mount.exited:
				case <-time.After(killWaitTimeout):
				}
			}
		}
	}

	// Try fusermount to ensure clean unmount
	exec.Command(fusermountBinary, fusermountUnmountFlag, mount.mountPath).Run()

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
