package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"time"

	"github.com/beam-cloud/airstore/pkg/common"
	"github.com/beam-cloud/airstore/pkg/runtime"
	"github.com/beam-cloud/airstore/pkg/types"
	"github.com/opencontainers/runtime-spec/specs-go"
	"github.com/rs/zerolog/log"
)

const (
	// Default paths for sandbox bundles and state
	defaultBundleDir = "/var/lib/airstore/bundles"
	defaultStateDir  = "/mnt/overlay" // Mounted as tmpfs volume - overlay-on-overlay fails

	// Path where FUSE is mounted on the worker (host)
	workerFilesystemMount = "/var/lib/airstore/fs"

	// Path where the filesystem is bind-mounted inside sandboxes
	sandboxFilesystemMount = "/workspace/fs"

	// Path to the CLI binary on the host (provides `mount` subcommand)
	filesystemBinaryPath = "/usr/local/bin/cli"
)

// SandboxManager manages the lifecycle of sandboxes on a worker
type SandboxManager struct {
	runtime          runtime.Runtime
	bundleDir        string
	stateDir         string
	sandboxes        map[string]*ManagedSandbox
	mu               sync.RWMutex
	ctx              context.Context
	cancel           context.CancelFunc
	workerID         string
	gatewayGRPCAddr  string // gRPC address for gateway
	authToken        string
	filesystemBinary string
	enableFilesystem bool
	filesystemCmd    *exec.Cmd // cli mount process running on worker
	imageManager     ImageManager
}

// ManagedSandbox represents a sandbox being managed
type ManagedSandbox struct {
	Config        types.SandboxConfig
	State         types.SandboxState
	BundlePath    string
	Cancel        context.CancelFunc
	RootfsCleanup func()                   // Cleanup function for the CLIP rootfs mount
	Overlay       *common.ContainerOverlay // Overlay filesystem for writable layer
	Output        *SandboxOutput           // Captured stdout/stderr
}

// SandboxManagerConfig configures the SandboxManager
type SandboxManagerConfig struct {
	RuntimeType      string // "runc" or "gvisor"
	BundleDir        string
	StateDir         string
	WorkerID         string
	GatewayGRPCAddr  string            // gRPC address for gateway (e.g., "airstore-gateway:1993")
	AuthToken        string            // Token for authenticating with gateway
	FilesystemBinary string            // Path to filesystem binary on host
	EnableFilesystem bool              // Whether to mount the airstore filesystem
	ImageConfig      types.ImageConfig // Image management configuration (CLIP + S3)
	RuntimeConfig    runtime.Config
}

// NewSandboxManager creates a new SandboxManager
func NewSandboxManager(ctx context.Context, cfg SandboxManagerConfig) (*SandboxManager, error) {
	// Set defaults
	if cfg.BundleDir == "" {
		cfg.BundleDir = defaultBundleDir
	}
	if cfg.StateDir == "" {
		cfg.StateDir = defaultStateDir
	}
	if cfg.RuntimeType == "" {
		cfg.RuntimeType = types.ContainerRuntimeGvisor.String()
	}

	// Create directories
	if err := os.MkdirAll(cfg.BundleDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create bundle dir: %w", err)
	}
	if err := os.MkdirAll(cfg.StateDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create state dir: %w", err)
	}

	// Create runtime
	runtimeCfg := cfg.RuntimeConfig
	if runtimeCfg.Type == "" {
		runtimeCfg.Type = cfg.RuntimeType
	}

	rt, err := runtime.New(runtimeCfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create runtime: %w", err)
	}

	managerCtx, cancel := context.WithCancel(ctx)

	filesystemBinary := cfg.FilesystemBinary
	if filesystemBinary == "" {
		filesystemBinary = filesystemBinaryPath
	}

	// Create CLIP image manager (required for container image handling)
	imageManager, err := NewImageManager(cfg.ImageConfig)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to create image manager: %w", err)
	}

	sm := &SandboxManager{
		runtime:          rt,
		bundleDir:        cfg.BundleDir,
		stateDir:         cfg.StateDir,
		sandboxes:        make(map[string]*ManagedSandbox),
		ctx:              managerCtx,
		cancel:           cancel,
		workerID:         cfg.WorkerID,
		gatewayGRPCAddr:  cfg.GatewayGRPCAddr,
		authToken:        cfg.AuthToken,
		filesystemBinary: filesystemBinary,
		enableFilesystem: cfg.EnableFilesystem,
		imageManager:     imageManager,
	}

	// Start airstore-fs on the worker if enabled
	if cfg.EnableFilesystem {
		if err := sm.startFilesystem(); err != nil {
			cancel()
			return nil, fmt.Errorf("failed to start filesystem: %w", err)
		}
	}

	return sm, nil
}

// startFilesystem starts the filesystem FUSE mount on the worker
func (m *SandboxManager) startFilesystem() error {
	// Check if binary exists
	if _, err := os.Stat(m.filesystemBinary); os.IsNotExist(err) {
		return fmt.Errorf("filesystem binary not found at %s", m.filesystemBinary)
	}

	// Create mount directory
	if err := os.MkdirAll(workerFilesystemMount, 0755); err != nil {
		return fmt.Errorf("failed to create filesystem mount dir: %w", err)
	}

	// Build command: cli mount <path> --gateway <addr> --token <token>
	args := []string{"mount", workerFilesystemMount, "--gateway", m.gatewayGRPCAddr}
	if m.authToken != "" {
		args = append(args, "--token", m.authToken)
	}
	cmd := exec.CommandContext(m.ctx, m.filesystemBinary, args...)

	// Capture stdout/stderr for debugging
	stdout, _ := cmd.StdoutPipe()
	stderr, _ := cmd.StderrPipe()

	// Start the process
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("failed to start filesystem: %w", err)
	}
	m.filesystemCmd = cmd

	log.Info().
		Str("mount", workerFilesystemMount).
		Str("gateway", m.gatewayGRPCAddr).
		Int("pid", cmd.Process.Pid).
		Msg("started cli mount on worker")

	// Stream stdout/stderr to logs in background
	go func() {
		buf := make([]byte, 4096)
		for {
			n, err := stdout.Read(buf)
			if n > 0 {
				log.Info().Str("source", "cli-mount").Msg(string(buf[:n]))
			}
			if err != nil {
				return
			}
		}
	}()

	go func() {
		buf := make([]byte, 4096)
		for {
			n, err := stderr.Read(buf)
			if n > 0 {
				log.Debug().Str("source", "cli-mount").Msg(string(buf[:n]))
			}
			if err != nil {
				return
			}
		}
	}()

	// Wait for mount to be ready (check for files or process exit)
	timeout := time.After(10 * time.Second)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	// Also wait for the process in background to detect early exit
	exitChan := make(chan error, 1)
	go func() {
		exitChan <- cmd.Wait()
	}()

	for {
		select {
		case <-timeout:
			log.Warn().Msg("timeout waiting for filesystem mount, continuing anyway")
			return nil
		case err := <-exitChan:
			if err != nil {
				return fmt.Errorf("cli mount exited unexpectedly: %w", err)
			}
			return fmt.Errorf("cli mount exited unexpectedly with code 0")
		case <-ticker.C:
			// Check if mount has files
			entries, err := os.ReadDir(workerFilesystemMount)
			if err == nil && len(entries) > 0 {
				log.Info().
					Int("files", len(entries)).
					Msg("filesystem mount ready")
				return nil
			}
		}
	}
}

// Create creates a new sandbox from the given config
func (m *SandboxManager) Create(cfg types.SandboxConfig) (*types.SandboxState, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.sandboxes[cfg.ID]; exists {
		return nil, fmt.Errorf("sandbox %s already exists", cfg.ID)
	}

	log.Info().
		Str("sandbox_id", cfg.ID).
		Str("workspace_id", cfg.WorkspaceID).
		Str("image", cfg.Image).
		Str("runtime", string(cfg.Runtime)).
		Msg("creating sandbox")

	// Prepare rootfs from image using CLIP (lazy-loading FUSE mount)
	rootfsPath, cleanupRootfs, err := m.imageManager.PrepareRootfs(m.ctx, cfg.Image)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare rootfs from image %s: %w", cfg.Image, err)
	}

	// Create bundle directory
	bundlePath := filepath.Join(m.bundleDir, cfg.ID)
	if err := os.MkdirAll(bundlePath, 0755); err != nil {
		cleanupRootfs()
		return nil, fmt.Errorf("failed to create bundle dir: %w", err)
	}

	// Create overlay filesystem on top of CLIP FUSE mount
	// This provides a writable layer while keeping the base image read-only
	overlay := common.NewContainerOverlay(cfg.ID, rootfsPath, m.stateDir)
	if err := overlay.Setup(); err != nil {
		cleanupRootfs()
		os.RemoveAll(bundlePath)
		return nil, fmt.Errorf("failed to setup overlay: %w", err)
	}

	// Use overlay's merged path as the container rootfs
	overlayRootfs := overlay.TopLayerPath()

	// Generate OCI spec using the overlay rootfs
	spec, err := m.generateSpec(cfg, overlayRootfs)
	if err != nil {
		overlay.Cleanup()
		cleanupRootfs()
		os.RemoveAll(bundlePath)
		return nil, fmt.Errorf("failed to generate spec: %w", err)
	}

	// Let runtime prepare the spec (e.g., gVisor removes seccomp)
	if err := m.runtime.Prepare(m.ctx, spec); err != nil {
		overlay.Cleanup()
		cleanupRootfs()
		os.RemoveAll(bundlePath)
		return nil, fmt.Errorf("failed to prepare spec: %w", err)
	}

	// Write config.json
	configPath := filepath.Join(bundlePath, "config.json")
	configData, err := json.MarshalIndent(spec, "", "  ")
	if err != nil {
		overlay.Cleanup()
		cleanupRootfs()
		os.RemoveAll(bundlePath)
		return nil, fmt.Errorf("failed to marshal spec: %w", err)
	}
	if err := os.WriteFile(configPath, configData, 0644); err != nil {
		overlay.Cleanup()
		cleanupRootfs()
		os.RemoveAll(bundlePath)
		return nil, fmt.Errorf("failed to write config.json: %w", err)
	}

	// Create sandbox state
	state := types.SandboxState{
		ID:        cfg.ID,
		Status:    types.SandboxStatusCreating,
		PID:       0,
		ExitCode:  -1,
		CreatedAt: time.Now(),
	}

	// Store managed sandbox with cleanup functions and output capture
	// Max output size: 1MB to prevent memory issues
	m.sandboxes[cfg.ID] = &ManagedSandbox{
		Config:        cfg,
		State:         state,
		BundlePath:    bundlePath,
		RootfsCleanup: cleanupRootfs,
		Overlay:       overlay,
		Output:        NewSandboxOutput(cfg.ID, 1<<20),
	}

	return &state, nil
}

// Start starts a created sandbox
func (m *SandboxManager) Start(sandboxID string) error {
	m.mu.Lock()
	sandbox, exists := m.sandboxes[sandboxID]
	if !exists {
		m.mu.Unlock()
		return fmt.Errorf("sandbox %s not found", sandboxID)
	}

	if sandbox.State.Status == types.SandboxStatusRunning {
		m.mu.Unlock()
		return fmt.Errorf("sandbox %s is already running", sandboxID)
	}

	// Create a cancellable context for this sandbox
	sandboxCtx, cancel := context.WithCancel(m.ctx)
	sandbox.Cancel = cancel
	m.mu.Unlock()

	log.Info().
		Str("sandbox_id", sandboxID).
		Str("bundle_path", sandbox.BundlePath).
		Msg("starting sandbox")

	// Capture the output writer for the goroutine
	output := sandbox.Output

	// Start the container in a goroutine
	go func() {
		started := make(chan int, 1)
		opts := &runtime.RunOpts{
			Started:      started,
			OutputWriter: output, // Capture stdout/stderr
		}

		// Run the container (blocks until exit)
		exitCode, err := m.runtime.Run(sandboxCtx, sandboxID, sandbox.BundlePath, opts)

		// Wait for PID notification
		select {
		case pid := <-started:
			m.mu.Lock()
			if s, ok := m.sandboxes[sandboxID]; ok {
				s.State.PID = pid
				s.State.Status = types.SandboxStatusRunning
				s.State.StartedAt = time.Now()
			}
			m.mu.Unlock()
		default:
		}

		// Update state on exit
		m.mu.Lock()
		if s, ok := m.sandboxes[sandboxID]; ok {
			s.State.Status = types.SandboxStatusStopped
			s.State.ExitCode = exitCode
			s.State.FinishedAt = time.Now()
			if err != nil {
				s.State.Error = err.Error()
				s.State.Status = types.SandboxStatusFailed
			}
		}
		m.mu.Unlock()

		// Log sandbox output (shows what the command printed)
		if output != nil && output.Len() > 0 {
			output.Log("sandbox output")
		}

		log.Info().
			Str("sandbox_id", sandboxID).
			Int("exit_code", exitCode).
			Err(err).
			Msg("sandbox exited")
	}()

	// Update status to running
	m.mu.Lock()
	sandbox.State.Status = types.SandboxStatusRunning
	m.mu.Unlock()

	return nil
}

// Stop stops a running sandbox
func (m *SandboxManager) Stop(sandboxID string, force bool) error {
	m.mu.RLock()
	sandbox, exists := m.sandboxes[sandboxID]
	m.mu.RUnlock()

	if !exists {
		return fmt.Errorf("sandbox %s not found", sandboxID)
	}

	log.Info().
		Str("sandbox_id", sandboxID).
		Bool("force", force).
		Msg("stopping sandbox")

	// Cancel the sandbox context
	if sandbox.Cancel != nil {
		sandbox.Cancel()
	}

	// Kill the container
	opts := &runtime.KillOpts{All: true}
	if err := m.runtime.Kill(m.ctx, sandboxID, 15, opts); err != nil { // SIGTERM
		if !force {
			return fmt.Errorf("failed to kill sandbox: %w", err)
		}
		// Force kill with SIGKILL
		if err := m.runtime.Kill(m.ctx, sandboxID, 9, opts); err != nil {
			log.Warn().Err(err).Str("sandbox_id", sandboxID).Msg("force kill failed")
		}
	}

	return nil
}

// Delete removes a sandbox and cleans up resources
func (m *SandboxManager) Delete(sandboxID string, force bool) error {
	m.mu.Lock()
	sandbox, exists := m.sandboxes[sandboxID]
	if !exists {
		m.mu.Unlock()
		return fmt.Errorf("sandbox %s not found", sandboxID)
	}
	m.mu.Unlock()

	log.Info().
		Str("sandbox_id", sandboxID).
		Bool("force", force).
		Msg("deleting sandbox")

	// Stop if running
	if sandbox.State.Status == types.SandboxStatusRunning {
		if err := m.Stop(sandboxID, force); err != nil && !force {
			return fmt.Errorf("failed to stop sandbox: %w", err)
		}
	}

	// Delete from runtime
	opts := &runtime.DeleteOpts{Force: force}
	if err := m.runtime.Delete(m.ctx, sandboxID, opts); err != nil {
		log.Warn().Err(err).Str("sandbox_id", sandboxID).Msg("runtime delete failed")
	}

	// Clean up bundle directory
	if sandbox.BundlePath != "" {
		if err := os.RemoveAll(sandbox.BundlePath); err != nil {
			log.Warn().Err(err).Str("path", sandbox.BundlePath).Msg("failed to remove bundle")
		}
	}

	// Clean up overlay filesystem (must be done before CLIP rootfs cleanup)
	if sandbox.Overlay != nil {
		if err := sandbox.Overlay.Cleanup(); err != nil {
			log.Warn().Err(err).Str("sandbox_id", sandboxID).Msg("failed to cleanup overlay")
		}
	}

	// Clean up CLIP rootfs mount
	if sandbox.RootfsCleanup != nil {
		sandbox.RootfsCleanup()
	}

	// Remove from managed sandboxes
	m.mu.Lock()
	delete(m.sandboxes, sandboxID)
	m.mu.Unlock()

	return nil
}

// Get returns the state of a sandbox
func (m *SandboxManager) Get(sandboxID string) (*types.SandboxState, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	sandbox, exists := m.sandboxes[sandboxID]
	if !exists {
		return nil, fmt.Errorf("sandbox %s not found", sandboxID)
	}

	// Get fresh state from runtime if running
	if sandbox.State.Status == types.SandboxStatusRunning {
		state, err := m.runtime.State(m.ctx, sandboxID)
		if err == nil {
			sandbox.State.PID = state.Pid
		}
	}

	return &sandbox.State, nil
}

// List returns all managed sandboxes
func (m *SandboxManager) List() []types.SandboxState {
	m.mu.RLock()
	defer m.mu.RUnlock()

	states := make([]types.SandboxState, 0, len(m.sandboxes))
	for _, sandbox := range m.sandboxes {
		states = append(states, sandbox.State)
	}

	return states
}

// Close shuts down the sandbox manager and all sandboxes
func (m *SandboxManager) Close() error {
	log.Info().Msg("shutting down sandbox manager")

	m.cancel()

	// Stop all sandboxes
	m.mu.RLock()
	sandboxIDs := make([]string, 0, len(m.sandboxes))
	for id := range m.sandboxes {
		sandboxIDs = append(sandboxIDs, id)
	}
	m.mu.RUnlock()

	for _, id := range sandboxIDs {
		if err := m.Delete(id, true); err != nil {
			log.Warn().Err(err).Str("sandbox_id", id).Msg("failed to delete sandbox during shutdown")
		}
	}

	// Close image manager
	if m.imageManager != nil {
		if err := m.imageManager.Close(); err != nil {
			log.Warn().Err(err).Msg("failed to close image manager")
		}
	}

	return m.runtime.Close()
}

// generateSpec generates an OCI spec for a sandbox
func (m *SandboxManager) generateSpec(cfg types.SandboxConfig, rootfsPath string) (*specs.Spec, error) {
	// Load base config
	baseConfig := runtime.GetBaseConfig(string(cfg.Runtime))

	var spec specs.Spec
	if err := json.Unmarshal([]byte(baseConfig), &spec); err != nil {
		return nil, fmt.Errorf("failed to unmarshal base config: %w", err)
	}

	// Set the rootfs path for the container
	spec.Root = &specs.Root{
		Path:     rootfsPath,
		Readonly: false,
	}

	// Set entrypoint
	if len(cfg.Entrypoint) > 0 {
		spec.Process.Args = cfg.Entrypoint
	}

	// Set working directory
	if cfg.WorkingDir != "" {
		spec.Process.Cwd = cfg.WorkingDir
	}

	// Add environment variables
	for key, value := range cfg.Env {
		spec.Process.Env = append(spec.Process.Env, fmt.Sprintf("%s=%s", key, value))
	}

	// Add gateway connection info to environment
	log.Debug().
		Str("gateway_addr", m.gatewayGRPCAddr).
		Str("workspace_id", cfg.WorkspaceID).
		Msg("setting sandbox environment for filesystem")
	spec.Process.Env = append(spec.Process.Env,
		fmt.Sprintf("GATEWAY_ADDR=%s", m.gatewayGRPCAddr),
		fmt.Sprintf("WORKSPACE_ID=%s", cfg.WorkspaceID),
	)

	// Add auth token if available (for filesystem to authenticate with gateway)
	if m.authToken != "" {
		spec.Process.Env = append(spec.Process.Env,
			fmt.Sprintf("AIRSTORE_TOKEN=%s", m.authToken),
		)
	}

	// Add filesystem mount if enabled (bind mount from worker's FUSE mount)
	if m.enableFilesystem {
		if err := m.addFilesystemMount(&spec); err != nil {
			log.Warn().Err(err).Msg("failed to add filesystem mount, continuing without it")
		}
	}

	// Set resource limits
	if spec.Linux == nil {
		spec.Linux = &specs.Linux{}
	}
	if spec.Linux.Resources == nil {
		spec.Linux.Resources = &specs.LinuxResources{}
	}

	if cfg.Resources.CPU > 0 {
		period := uint64(100000)
		quota := int64(cfg.Resources.CPU) * int64(period) / 1000
		spec.Linux.Resources.CPU = &specs.LinuxCPU{
			Quota:  &quota,
			Period: &period,
		}
	}

	if cfg.Resources.Memory > 0 {
		spec.Linux.Resources.Memory = &specs.LinuxMemory{
			Limit: &cfg.Resources.Memory,
		}
	}

	// Add custom mounts
	for _, mount := range cfg.Mounts {
		options := []string{"rbind"}
		if mount.ReadOnly {
			options = append(options, "ro")
		} else {
			options = append(options, "rw")
		}

		spec.Mounts = append(spec.Mounts, specs.Mount{
			Destination: mount.Destination,
			Type:        "bind",
			Source:      mount.Source,
			Options:     options,
		})
	}

	// Set hostname to sandbox ID
	spec.Hostname = cfg.ID

	return &spec, nil
}

// addFilesystemMount bind-mounts the worker's FUSE mount into the sandbox.
// The FUSE filesystem runs on the worker (started in NewSandboxManager), not inside the sandbox.
func (m *SandboxManager) addFilesystemMount(spec *specs.Spec) error {
	// Verify the worker filesystem mount exists and has files
	entries, err := os.ReadDir(workerFilesystemMount)
	if err != nil {
		return fmt.Errorf("worker filesystem mount not ready at %s: %w", workerFilesystemMount, err)
	}
	if len(entries) == 0 {
		log.Warn().Str("mount", workerFilesystemMount).Msg("worker filesystem mount is empty")
	}

	// Bind mount the worker's FUSE mount into the sandbox
	spec.Mounts = append(spec.Mounts, specs.Mount{
		Destination: sandboxFilesystemMount, // /workspace/fs inside sandbox
		Type:        "bind",
		Source:      workerFilesystemMount, // /var/lib/airstore/fs on worker
		Options:     []string{"rbind", "ro"},
	})

	log.Debug().
		Str("source", workerFilesystemMount).
		Str("dest", sandboxFilesystemMount).
		Int("files", len(entries)).
		Msg("added filesystem bind mount to sandbox")

	return nil
}

// ptrInt64 returns a pointer to an int64
func ptrInt64(v int64) *int64 {
	return &v
}

// RunTask creates and runs a sandbox for a task, returning when complete
func (m *SandboxManager) RunTask(ctx context.Context, task types.Task) (*types.TaskResult, error) {
	sandboxID := fmt.Sprintf("task-%s", task.ExternalId)

	// Create sandbox config from task with defaults
	cfg := types.SandboxConfig{
		ID:          sandboxID,
		WorkspaceID: fmt.Sprintf("%d", task.WorkspaceId),
		Image:       task.Image,
		Runtime:     types.ContainerRuntimeGvisor, // Default to gVisor
		Entrypoint:  task.Entrypoint,
		Env:         task.Env,
		WorkingDir:  "/", // Default working directory
		Resources: types.SandboxResources{
			CPU:    1000,      // Default 1 CPU
			Memory: 512 << 20, // Default 512MB
			GPU:    0,
		},
	}

	// Create the sandbox
	state, err := m.Create(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create sandbox: %w", err)
	}

	// Ensure cleanup
	defer m.Delete(sandboxID, true)

	// Start the sandbox
	if err := m.Start(sandboxID); err != nil {
		return nil, fmt.Errorf("failed to start sandbox: %w", err)
	}

	startTime := time.Now()

	// Wait for completion (use parent context - no per-task timeout for now)
	waitCtx := ctx

	// Poll for completion
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-waitCtx.Done():
			// Timeout or cancellation
			m.Stop(sandboxID, true)
			return &types.TaskResult{
				ID:       task.ExternalId,
				ExitCode: -1,
				Error:    "task timeout or cancelled",
				Duration: time.Since(startTime),
			}, nil

		case <-ticker.C:
			state, err = m.Get(sandboxID)
			if err != nil {
				return nil, fmt.Errorf("failed to get sandbox state: %w", err)
			}

			if state.Status == types.SandboxStatusStopped || state.Status == types.SandboxStatusFailed {
				return &types.TaskResult{
					ID:       task.ExternalId,
					ExitCode: state.ExitCode,
					Error:    state.Error,
					Duration: time.Since(startTime),
				}, nil
			}
		}
	}
}
