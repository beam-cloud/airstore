package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"time"

	"github.com/beam-cloud/airstore/pkg/common"
	"github.com/beam-cloud/airstore/pkg/gateway"
	"github.com/beam-cloud/airstore/pkg/runtime"
	"github.com/beam-cloud/airstore/pkg/types"
	"github.com/opencontainers/runtime-spec/specs-go"
	"github.com/rs/zerolog/log"
)

// SandboxManager manages the lifecycle of sandboxes on a worker.
type SandboxManager struct {
	// Configuration
	paths           types.WorkerPaths
	workerID        string
	gatewayAddr     string
	authToken       string
	anthropicAPIKey string
	enableFS        bool

	// Components
	runtime      runtime.Runtime
	imageManager ImageManager
	mountManager *MountManager
	network      *NetworkManager
	s2           *common.S2Client

	// State
	sandboxes map[string]*Sandbox
	mu        sync.RWMutex
	ctx       context.Context
	cancel    context.CancelFunc
	fsCmd     *exec.Cmd
}

// Sandbox represents a running sandbox with its resources.
type Sandbox struct {
	Config  types.SandboxConfig
	State   types.SandboxState
	Bundle  string
	Cancel  context.CancelFunc
	Overlay *common.ContainerOverlay
	Rootfs  func() // cleanup function
	Output  io.Writer
	Flush   func()
}

// Config for creating a SandboxManager.
type Config struct {
	// Paths
	BundleDir   string
	StateDir    string
	MountDir    string
	WorkerMount string
	CLIBinary   string

	// Identity
	WorkerID string

	// Gateway
	GatewayAddr   string
	AuthToken     string
	GatewayClient *gateway.GatewayClient

	// Features
	EnableFilesystem bool
	EnableNetwork    bool

	// Runtime
	RuntimeType   string
	RuntimeConfig runtime.Config
	ImageConfig   types.ImageConfig

	// Streaming
	S2Token string
	S2Basin string

	// API keys
	AnthropicAPIKey string
}

func NewSandboxManager(ctx context.Context, cfg Config) (*SandboxManager, error) {
	defaults := types.DefaultWorkerPaths()
	paths := types.WorkerPaths{
		BundleDir:   coalesce(cfg.BundleDir, defaults.BundleDir),
		StateDir:    coalesce(cfg.StateDir, defaults.StateDir),
		MountDir:    coalesce(cfg.MountDir, defaults.MountDir),
		WorkerMount: coalesce(cfg.WorkerMount, defaults.WorkerMount),
		CLIBinary:   coalesce(cfg.CLIBinary, defaults.CLIBinary),
	}

	// Ensure directories exist
	for _, dir := range []string{paths.BundleDir, paths.StateDir, paths.MountDir} {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return nil, fmt.Errorf("mkdir %s: %w", dir, err)
		}
	}

	// Runtime
	runtimeType := coalesce(cfg.RuntimeType, types.ContainerRuntimeGvisor.String())
	runtimeCfg := cfg.RuntimeConfig
	if runtimeCfg.Type == "" {
		runtimeCfg.Type = runtimeType
	}
	rt, err := runtime.New(runtimeCfg)
	if err != nil {
		return nil, fmt.Errorf("runtime: %w", err)
	}

	managerCtx, cancel := context.WithCancel(ctx)

	// Image manager
	imgMgr, err := NewImageManager(cfg.ImageConfig)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("image manager: %w", err)
	}

	// Mount manager (optional)
	var mountMgr *MountManager
	if cfg.EnableFilesystem {
		mountMgr, err = NewMountManager(MountConfig{
			MountDir:          paths.MountDir,
			CLIBinary:         paths.CLIBinary,
			GatewayAddr:       cfg.GatewayAddr,
			MountReadyTimeout: 5 * time.Second,
		})
		if err != nil {
			cancel()
			return nil, fmt.Errorf("mount manager: %w", err)
		}
	}

	// Network manager (optional)
	var netMgr *NetworkManager
	if cfg.EnableNetwork && cfg.GatewayClient != nil {
		netMgr, err = NewNetworkManager(managerCtx, cfg.WorkerID, cfg.GatewayClient)
		if err != nil {
			cancel()
			return nil, fmt.Errorf("network manager: %w", err)
		}
	}

	// S2 client (optional)
	var s2 *common.S2Client
	if cfg.S2Token != "" && cfg.S2Basin != "" {
		s2 = common.NewS2Client(common.S2Config{Token: cfg.S2Token, Basin: cfg.S2Basin})
		log.Info().Str("basin", cfg.S2Basin).Msg("S2 streaming enabled")
	}

	return &SandboxManager{
		paths:           paths,
		workerID:        cfg.WorkerID,
		gatewayAddr:     cfg.GatewayAddr,
		authToken:       cfg.AuthToken,
		anthropicAPIKey: cfg.AnthropicAPIKey,
		enableFS:        cfg.EnableFilesystem,
		runtime:         rt,
		imageManager:    imgMgr,
		mountManager:    mountMgr,
		network:         netMgr,
		s2:              s2,
		sandboxes:       make(map[string]*Sandbox),
		ctx:             managerCtx,
		cancel:          cancel,
	}, nil
}

func coalesce(a, b string) string {
	if a != "" {
		return a
	}
	return b
}

func (m *SandboxManager) publishStatus(ctx context.Context, taskID string, status types.TaskStatus, exitCode *int, errMsg string) {
	if m.s2 != nil && m.s2.Enabled() {
		m.s2.AppendStatus(ctx, taskID, string(status), exitCode, errMsg)
	}
}

// startFilesystem starts the filesystem FUSE mount on the worker
func (m *SandboxManager) startFilesystem() error {
	// Check if binary exists
	if _, err := os.Stat(m.paths.CLIBinary); os.IsNotExist(err) {
		return fmt.Errorf("filesystem binary not found at %s", m.paths.CLIBinary)
	}

	// Create mount directory
	if err := os.MkdirAll(m.paths.WorkerMount, 0755); err != nil {
		return fmt.Errorf("failed to create filesystem mount dir: %w", err)
	}

	// Build command: cli mount <path> --gateway <addr> --token <token>
	args := []string{"mount", m.paths.WorkerMount, "--gateway", m.gatewayAddr}
	if m.authToken != "" {
		args = append(args, "--token", m.authToken)
	}
	cmd := exec.CommandContext(m.ctx, m.paths.CLIBinary, args...)

	// Capture stdout/stderr for debugging
	stdout, _ := cmd.StdoutPipe()
	stderr, _ := cmd.StderrPipe()

	// Start the process
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("failed to start filesystem: %w", err)
	}
	m.fsCmd = cmd

	log.Info().
		Str("mount", m.paths.WorkerMount).
		Str("gateway", m.gatewayAddr).
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
			entries, err := os.ReadDir(m.paths.WorkerMount)
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
	bundlePath := filepath.Join(m.paths.BundleDir, cfg.ID)
	if err := os.MkdirAll(bundlePath, 0755); err != nil {
		cleanupRootfs()
		return nil, fmt.Errorf("failed to create bundle dir: %w", err)
	}

	// Create overlay filesystem on top of CLIP FUSE mount
	// This provides a writable layer while keeping the base image read-only
	overlay := common.NewContainerOverlay(cfg.ID, rootfsPath, m.paths.StateDir)
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

	// Set up container networking (NAT for internet access)
	var containerIP string
	if m.network != nil {
		ip, err := m.network.Setup(cfg.ID, spec)
		if err != nil {
			overlay.Cleanup()
			cleanupRootfs()
			os.RemoveAll(bundlePath)
			return nil, fmt.Errorf("failed to setup network: %w", err)
		}
		containerIP = ip
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
		ID:          cfg.ID,
		Status:      types.SandboxStatusCreating,
		PID:         0,
		ExitCode:    -1,
		ContainerIP: containerIP,
		CreatedAt:   time.Now(),
	}

	// Store managed sandbox with cleanup functions
	m.sandboxes[cfg.ID] = &Sandbox{
		Config:  cfg,
		State:   state,
		Bundle:  bundlePath,
		Rootfs:  cleanupRootfs,
		Overlay: overlay,
	}

	return &state, nil
}

// SetOutput configures the output writer and flusher for a sandbox.
// Must be called before Start.
func (m *SandboxManager) SetOutput(sandboxID string, writer io.Writer, flusher func()) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	sandbox, exists := m.sandboxes[sandboxID]
	if !exists {
		return fmt.Errorf("sandbox %s not found", sandboxID)
	}

	sandbox.Output = writer
	sandbox.Flush = flusher
	return nil
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
		Str("bundle_path", sandbox.Bundle).
		Msg("starting sandbox")

	// Use configured output writer, or discard if none set
	outputWriter := sandbox.Output
	if outputWriter == nil {
		outputWriter = io.Discard
	}

	// Start the container in a goroutine
	go func() {
		started := make(chan int, 1)
		opts := &runtime.RunOpts{
			Started:      started,
			OutputWriter: outputWriter, // Capture stdout/stderr
		}

		// Run the container (blocks until exit)
		exitCode, err := m.runtime.Run(sandboxCtx, sandboxID, sandbox.Bundle, opts)

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
	if sandbox.Bundle != "" {
		if err := os.RemoveAll(sandbox.Bundle); err != nil {
			log.Warn().Err(err).Str("path", sandbox.Bundle).Msg("failed to remove bundle")
		}
	}

	// Clean up overlay filesystem (must be done before CLIP rootfs cleanup)
	if sandbox.Overlay != nil {
		if err := sandbox.Overlay.Cleanup(); err != nil {
			log.Warn().Err(err).Str("sandbox_id", sandboxID).Msg("failed to cleanup overlay")
		}
	}

	// Clean up CLIP rootfs mount
	if sandbox.Rootfs != nil {
		sandbox.Rootfs()
	}

	// Tear down container networking
	if m.network != nil {
		if err := m.network.TearDown(sandboxID); err != nil {
			log.Warn().Err(err).Str("sandbox_id", sandboxID).Msg("failed to teardown network")
		}
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

	// Set user identity from constants (single source of truth)
	spec.Process.User.UID = types.SandboxUserUID
	spec.Process.User.GID = types.SandboxUserGID

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
		Str("gateway_addr", m.gatewayAddr).
		Str("workspace_id", cfg.WorkspaceID).
		Msg("setting sandbox environment for filesystem")
	spec.Process.Env = append(spec.Process.Env,
		fmt.Sprintf("GATEWAY_ADDR=%s", m.gatewayAddr),
		fmt.Sprintf("WORKSPACE_ID=%s", cfg.WorkspaceID),
	)

	// Add auth token if available (for filesystem to authenticate with gateway)
	// Only add worker's auth token if task doesn't have its own member token
	if m.authToken != "" && cfg.Env["AIRSTORE_TOKEN"] == "" {
		spec.Process.Env = append(spec.Process.Env,
			fmt.Sprintf("AIRSTORE_TOKEN=%s", m.authToken),
		)
	}

	// Add filesystem mount (bind mount from FUSE mount)
	if cfg.FilesystemMount != "" {
		if err := m.addFilesystemMount(&spec, cfg.FilesystemMount); err != nil {
			log.Warn().Err(err).Str("source", cfg.FilesystemMount).Msg("failed to add filesystem mount")
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

	// Add DNS configuration (resolv.conf is created in Dockerfile.worker with nameserver 8.8.8.8)
	spec.Mounts = append(spec.Mounts, specs.Mount{
		Destination: "/etc/resolv.conf",
		Type:        "none",
		Source:      "/workspace/etc/resolv.conf",
		Options:     []string{"ro", "rbind", "rprivate", "nosuid", "noexec", "nodev"},
	})

	return &spec, nil
}

// addFilesystemMount bind-mounts a FUSE filesystem into the sandbox at /workspace.
func (m *SandboxManager) addFilesystemMount(spec *specs.Spec, source string) error {
	// Verify the mount exists and has files
	entries, err := os.ReadDir(source)
	if err != nil {
		return fmt.Errorf("filesystem mount not ready at %s: %w", source, err)
	}
	if len(entries) == 0 {
		log.Warn().Str("mount", source).Msg("filesystem mount is empty")
	}

	// Bind mount at container working directory
	spec.Mounts = append(spec.Mounts, specs.Mount{
		Destination: types.ContainerWorkDir,
		Type:        "bind",
		Source:      source,
		Options:     []string{"rbind", "rw"},
	})

	log.Debug().
		Str("source", source).
		Str("dest", types.ContainerWorkDir).
		Int("files", len(entries)).
		Msg("added filesystem bind mount to sandbox")

	return nil
}

// ptrInt64 returns a pointer to an int64
func ptrInt64(v int64) *int64 {
	return &v
}

// buildEntrypoint constructs the entrypoint for a task.
// For Claude Code tasks, wraps the prompt in the CLI invocation.
func (m *SandboxManager) buildEntrypoint(task types.Task, env map[string]string) []string {
	if !task.IsClaudeCodeTask() {
		return task.Entrypoint
	}

	// Inject Anthropic API key for Claude Code
	if m.anthropicAPIKey != "" {
		env["ANTHROPIC_API_KEY"] = m.anthropicAPIKey
	}

	log.Info().
		Str("task_id", task.ExternalId).
		Str("prompt", task.Prompt[:min(50, len(task.Prompt))]).
		Msg("running claude code task")

	// Claude Code CLI: non-interactive, streaming JSON output
	return []string{
		"claude",
		"--print",
		"--verbose",
		"--output-format", "stream-json",
		"--dangerously-skip-permissions",
		"-p", task.Prompt,
	}
}

// mountFilesystem sets up the filesystem mount for a task.
// Prefers task-specific mount with member token, falls back to worker global mount.
func (m *SandboxManager) mountFilesystem(ctx context.Context, task types.Task) string {
	// Try task-specific mount with member token
	if task.MemberToken != "" && m.mountManager != nil {
		mountPath, err := m.mountManager.Mount(ctx, task.ExternalId, task.MemberToken)
		if err != nil {
			log.Warn().Err(err).Str("task_id", task.ExternalId).Msg("failed to create task mount")
		} else {
			log.Debug().Str("task_id", task.ExternalId).Msg("mounted filesystem with task token")
			return mountPath
		}
	}

	// Fall back to worker's global mount
	if m.enableFS {
		log.Debug().Str("task_id", task.ExternalId).Msg("using worker global mount")
		return m.paths.WorkerMount
	}

	return ""
}

// cleanupMount removes the task-specific mount if one was created.
func (m *SandboxManager) cleanupMount(taskID string) {
	if m.mountManager != nil {
		m.mountManager.Unmount(taskID)
	}
}

// RunTask creates and runs a sandbox for a task, returning when complete
func (m *SandboxManager) RunTask(ctx context.Context, task types.Task) (*types.TaskResult, error) {
	sandboxID := fmt.Sprintf("task-%s", task.ExternalId)

	// Initialize env map
	env := task.Env
	if env == nil {
		env = make(map[string]string)
	}

	// Build entrypoint
	entrypoint := m.buildEntrypoint(task, env)

	// Mount filesystem for task
	taskMountSource := m.mountFilesystem(ctx, task)
	defer m.cleanupMount(task.ExternalId)

	// Build sandbox config
	cfg := types.SandboxConfig{
		ID:              sandboxID,
		WorkspaceID:     fmt.Sprintf("%d", task.WorkspaceId),
		Image:           task.Image,
		Runtime:         types.ContainerRuntimeGvisor,
		Entrypoint:      entrypoint,
		Env:             env,
		WorkingDir:      types.ContainerWorkDir,
		FilesystemMount: taskMountSource,
		Resources:       task.GetResources(),
	}

	// Create the sandbox
	state, err := m.Create(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create sandbox: %w", err)
	}

	// Ensure cleanup
	defer m.Delete(sandboxID, true)

	// Set up task output: S2 streams + worker console
	taskOutput := NewTaskOutput(task.ExternalId, "stdout",
		NewS2Writer(ctx, m.s2, task.ExternalId, "stdout"),
		NewConsoleWriter(task.ExternalId, "stdout"),
	)
	if err := m.SetOutput(sandboxID, taskOutput, taskOutput.Flush); err != nil {
		log.Warn().Err(err).Str("task_id", task.ExternalId).Msg("failed to set output")
	}

	// Publish starting status
	m.publishStatus(ctx, task.ExternalId, types.TaskStatusRunning, nil, "")

	// Start the sandbox
	if err := m.Start(sandboxID); err != nil {
		m.publishStatus(ctx, task.ExternalId, types.TaskStatusFailed, nil, err.Error())
		return nil, fmt.Errorf("failed to start sandbox: %w", err)
	}

	startTime := time.Now()

	// Poll for completion
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			// Timeout or cancellation
			m.Stop(sandboxID, true)
			exitCode := -1
			errMsg := "task timeout or cancelled"
			m.publishStatus(ctx, task.ExternalId, types.TaskStatusCancelled, &exitCode, errMsg)
			return &types.TaskResult{
				ID:       task.ExternalId,
				ExitCode: exitCode,
				Error:    errMsg,
				Duration: time.Since(startTime),
			}, nil

		case <-ticker.C:
			state, err = m.Get(sandboxID)
			if err != nil {
				return nil, fmt.Errorf("failed to get sandbox state: %w", err)
			}

			if state.Status == types.SandboxStatusStopped || state.Status == types.SandboxStatusFailed {
				// Flush any remaining output
				taskOutput.Flush()

				// Publish completion status
				status := types.TaskStatusComplete
				if state.ExitCode != 0 || state.Error != "" {
					status = types.TaskStatusFailed
				}
				m.publishStatus(ctx, task.ExternalId, status, &state.ExitCode, state.Error)

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
