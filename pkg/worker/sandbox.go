package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/beam-cloud/airstore/pkg/common"
	gatewayclient "github.com/beam-cloud/airstore/pkg/gateway/client"
	"github.com/beam-cloud/airstore/pkg/runtime"
	"github.com/beam-cloud/airstore/pkg/types"
	pb "github.com/beam-cloud/airstore/proto"
	"github.com/opencontainers/runtime-spec/specs-go"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// SandboxManager manages the lifecycle of sandboxes on a worker.
type SandboxManager struct {
	// Configuration
	paths             types.WorkerPaths
	workerID          string
	gatewayAddr       string
	authToken         string
	enableFS          bool
	useHostResolvConf bool

	// Components
	runtime       runtime.Runtime
	imageManager  ImageManager
	mountManager  *MountManager
	network       *NetworkManager
	s2            *common.S2Client
	gatewayClient *gatewayclient.GatewayClient

	// State
	sandboxes map[string]*Sandbox
	mu        sync.RWMutex
	ctx       context.Context
	cancel    context.CancelFunc
	fsCmd     *exec.Cmd

	// Prompt runners
	promptRunners       map[string]AgentExecutionRunner
	defaultPromptRunner AgentExecutionRunner
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

	// done is closed when the Run() goroutine exits. Delete waits on this
	// so it doesn't race with the runtime supervisor process.
	done chan struct{}
}

const (
	sandboxKillTimeout       = 2 * time.Second
	sandboxDeleteTimeout     = 2 * time.Second
	sandboxRunDrainTimeout   = 5 * time.Second
	sandboxForceDrainTimeout = 2 * time.Second
)

type sandboxContextClient interface {
	Mkdir(ctx context.Context, in *pb.ContextMkdirRequest, opts ...grpc.CallOption) (*pb.ContextMkdirResponse, error)
	Stat(ctx context.Context, in *pb.ContextStatRequest, opts ...grpc.CallOption) (*pb.ContextStatResponse, error)
}

var newSandboxContextClient = func(addr, token string) (sandboxContextClient, func() error, error) {
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(common.TransportCredentials(addr)),
	}
	if token != "" {
		opts = append(opts, grpc.WithUnaryInterceptor(sandboxAuthInterceptor(token)))
		opts = append(opts, grpc.WithStreamInterceptor(sandboxStreamAuthInterceptor(token)))
	}

	conn, err := grpc.NewClient(addr, opts...)
	if err != nil {
		return nil, nil, err
	}
	return pb.NewContextServiceClient(conn), conn.Close, nil
}

func sandboxAuthInterceptor(token string) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}

func sandboxStreamAuthInterceptor(token string) grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)
		return streamer(ctx, desc, cc, method, opts...)
	}
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
	GatewayClient *gatewayclient.GatewayClient

	// Features
	EnableFilesystem  bool
	EnableNetwork     bool
	UseHostResolvConf bool

	// Runtime
	RuntimeType   string
	RuntimeConfig runtime.Config
	ImageConfig   types.ImageConfig

	// Streaming
	S2Token string
	S2Basin string

	// API keys
	AnthropicAPIKey string
	KernelAPIKey    string
	CerebrasAPIKey  string
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
			MountReadyTimeout: 20 * time.Second,
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

	manager := &SandboxManager{
		paths:             paths,
		workerID:          cfg.WorkerID,
		gatewayAddr:       cfg.GatewayAddr,
		authToken:         cfg.AuthToken,
		enableFS:          cfg.EnableFilesystem,
		useHostResolvConf: cfg.UseHostResolvConf,
		runtime:           rt,
		imageManager:      imgMgr,
		mountManager:      mountMgr,
		network:           netMgr,
		s2:                s2,
		gatewayClient:     cfg.GatewayClient,
		sandboxes:         make(map[string]*Sandbox),
		ctx:               managerCtx,
		cancel:            cancel,
	}
	claudeRunner := NewClaudeCodeRunner(ClaudeCodeRunnerOptions{
		AnthropicAPIKey: cfg.AnthropicAPIKey,
		KernelAPIKey:    cfg.KernelAPIKey,
	})
	airRunner := NewAirRunner(AirRunnerOptions{
		AnthropicAPIKey: cfg.AnthropicAPIKey,
		CerebrasAPIKey:  cfg.CerebrasAPIKey,
		KernelAPIKey:    cfg.KernelAPIKey,
		S2Key:           cfg.S2Token,
		S2Basin:         cfg.S2Basin,
	})
	manager.defaultPromptRunner = claudeRunner
	manager.promptRunners = map[string]AgentExecutionRunner{
		"claude": claudeRunner,
		"air":    airRunner,
	}

	// Bring up the global worker mount during initialization so the first task
	// doesn't race filesystem startup on cold workers.
	if manager.enableFS {
		if err := manager.startFilesystem(); err != nil {
			cancel()
			return nil, fmt.Errorf("start global filesystem mount: %w", err)
		}
	}

	return manager, nil
}

func coalesce(a, b string) string {
	if a != "" {
		return a
	}
	return b
}

func (m *SandboxManager) publishStatus(ctx context.Context, taskID string, status types.RunExecutionStatus, exitCode *int, errMsg string) {
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

	// Build command: cli mount <path> --gateway <addr> --token <token> --uid <uid> --gid <gid>
	args := []string{"mount", m.paths.WorkerMount, "--gateway", m.gatewayAddr,
		"--daemon",
		"--uid", fmt.Sprintf("%d", types.SandboxUserUID),
		"--gid", fmt.Sprintf("%d", types.SandboxUserGID),
	}
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

	// Wait for mount to be ready (required system roots visible) or process exit.
	timeout := time.After(20 * time.Second)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	// Also wait for the process in background to detect early exit
	exitChan := make(chan error, 1)
	go func() {
		exitChan <- cmd.Wait()
	}()

	var lastMissing []string
	var lastReadErr error
	for {
		select {
		case <-timeout:
			ready, missing, err := checkFilesystemMountReady(m.paths.WorkerMount)
			if ready {
				log.Info().
					Str("mount", m.paths.WorkerMount).
					Msg("filesystem mount became ready at timeout boundary")
				return nil
			}
			if err != nil {
				if lastReadErr != nil {
					err = lastReadErr
				}
				return fmt.Errorf("timed out waiting for filesystem mount readiness: %w", err)
			}
			if len(missing) == 0 {
				missing = lastMissing
			}
			return fmt.Errorf(
				"timed out waiting for filesystem mount readiness; missing required roots: %s",
				strings.Join(missing, ", "),
			)
		case err := <-exitChan:
			if err != nil {
				return fmt.Errorf("cli mount exited unexpectedly: %w", err)
			}
			return fmt.Errorf("cli mount exited unexpectedly with code 0")
		case <-ticker.C:
			ready, missing, err := checkFilesystemMountReady(m.paths.WorkerMount)
			if err != nil {
				lastReadErr = err
				continue
			}
			lastMissing = missing
			if ready {
				entries, readErr := os.ReadDir(m.paths.WorkerMount)
				entryCount := 0
				if readErr == nil {
					entryCount = len(entries)
				}
				log.Info().
					Str("mount", m.paths.WorkerMount).
					Int("entries", entryCount).
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

	taskID := ""
	if strings.HasPrefix(cfg.ID, "task-") {
		taskID = strings.TrimPrefix(cfg.ID, "task-")
	}
	createEvent := addTaskExecutionContextFromEnv(
		log.Info().
			Str("sandbox_id", cfg.ID).
			Str("workspace_id", cfg.WorkspaceID).
			Str("image", cfg.Image).
			Str("runtime", string(cfg.Runtime)),
		taskID,
		cfg.Env,
	)
	createEvent.Msg("creating sandbox")

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

	// Verify the overlay rootfs is actually readable before proceeding.
	// A broken FUSE mount or corrupt overlay will cause gVisor to crash
	// with an opaque "cannot read client sync file" error.
	if err := verifyRootfs(overlayRootfs); err != nil {
		overlay.Cleanup()
		cleanupRootfs()
		os.RemoveAll(bundlePath)
		return nil, fmt.Errorf("rootfs verification failed at %s: %w", overlayRootfs, err)
	}

	if err := m.ensureSandboxWorkingDirOnMount(cfg); err != nil {
		overlay.Cleanup()
		cleanupRootfs()
		os.RemoveAll(bundlePath)
		return nil, fmt.Errorf("prepare working directory: %w", err)
	}

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

	// Set up container networking (NAT for internet access) unless disabled.
	var containerIP string
	if m.network != nil && cfg.Network.Mode != "none" {
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
	sandbox.done = make(chan struct{})
	sandbox.State.Status = types.SandboxStatusCreating
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
	started := make(chan int, 1)
	doneCh := sandbox.done
	runDone := make(chan struct {
		exitCode int
		err      error
	}, 1)
	go func() {
		defer close(doneCh)

		opts := &runtime.RunOpts{
			Started:      started,
			OutputWriter: outputWriter,
		}

		exitCode, err := m.runtime.Run(sandboxCtx, sandboxID, sandbox.Bundle, opts)

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

		expectedTeardownExit := isSandboxTeardownError(err)

		m.mu.Lock()
		if s, ok := m.sandboxes[sandboxID]; ok {
			s.State.Status = types.SandboxStatusStopped
			s.State.ExitCode = exitCode
			s.State.FinishedAt = time.Now()
			if err != nil && !expectedTeardownExit {
				s.State.Error = err.Error()
				s.State.Status = types.SandboxStatusFailed
			} else {
				s.State.Error = ""
			}
		}
		m.mu.Unlock()

		if err != nil && expectedTeardownExit {
			log.Debug().
				Str("sandbox_id", sandboxID).
				Int("exit_code", exitCode).
				Err(err).
				Msg("sandbox exited during teardown")
		} else {
			log.Info().
				Str("sandbox_id", sandboxID).
				Int("exit_code", exitCode).
				Err(err).
				Msg("sandbox exited")
		}
		runDone <- struct {
			exitCode int
			err      error
		}{
			exitCode: exitCode,
			err:      err,
		}
	}()

	// Most sandboxes are one-shot command runs and don't need a runtime-level
	// readiness probe. Interactive sessions run a long-lived "sleep infinity"
	// entrypoint and then exec per-turn commands; those need startup gating.
	if !isInteractiveBootstrapEntrypoint(sandbox.Config.Entrypoint) {
		m.mu.Lock()
		if s, ok := m.sandboxes[sandboxID]; ok {
			s.State.Status = types.SandboxStatusRunning
			s.State.StartedAt = time.Now()
		}
		m.mu.Unlock()
		return nil
	}

	// Block startup until the runtime reports this sandbox as running.
	// This avoids Exec races where runsc is still "loading sandbox".
	startupTimeout := time.NewTimer(10 * time.Second)
	defer startupTimeout.Stop()
	probeTicker := time.NewTicker(50 * time.Millisecond)
	defer probeTicker.Stop()

	pid := 0
	lastProbeStatus := ""
	var lastProbeErr error

	for {
		select {
		case startedPID := <-started:
			if startedPID > 0 {
				pid = startedPID
			}

		case result := <-runDone:
			m.mu.RLock()
			s, ok := m.sandboxes[sandboxID]
			m.mu.RUnlock()

			errMsg := ""
			if ok {
				errMsg = strings.TrimSpace(s.State.Error)
			}
			if errMsg == "" && result.err != nil {
				errMsg = result.err.Error()
			}
			if errMsg == "" {
				errMsg = fmt.Sprintf("sandbox exited before startup (exit_code=%d)", result.exitCode)
			}
			return fmt.Errorf("failed to start sandbox %s: %s", sandboxID, errMsg)

		case <-probeTicker.C:
			runtimeState, err := m.runtime.State(m.ctx, sandboxID)
			if err != nil {
				lastProbeErr = err
				continue
			}

			lastProbeErr = nil
			lastProbeStatus = strings.TrimSpace(runtimeState.Status)
			if !strings.EqualFold(lastProbeStatus, string(types.SandboxStatusRunning)) {
				continue
			}

			if pid == 0 && runtimeState.Pid > 0 {
				pid = runtimeState.Pid
			}

			m.mu.Lock()
			if s, ok := m.sandboxes[sandboxID]; ok {
				s.State.PID = pid
				s.State.Status = types.SandboxStatusRunning
				s.State.StartedAt = time.Now()
				s.State.Error = ""
			}
			m.mu.Unlock()
			return nil

		case <-startupTimeout.C:
			reason := "runtime did not report running"
			if lastProbeErr != nil {
				reason = lastProbeErr.Error()
			} else if lastProbeStatus != "" {
				reason = fmt.Sprintf("runtime status %q", lastProbeStatus)
			}
			return fmt.Errorf("timed out waiting for sandbox %s startup: %s", sandboxID, reason)
		}
	}
}

func isInteractiveBootstrapEntrypoint(entrypoint []string) bool {
	if len(entrypoint) != 2 {
		return false
	}

	return strings.TrimSpace(entrypoint[0]) == "sleep" &&
		strings.TrimSpace(entrypoint[1]) == "infinity"
}

// Stop stops a running sandbox by signalling container processes.
// Non-force sends SIGTERM; force sends SIGKILL. Neither cancels the
// sandbox context — that is handled by Delete after waiting for the
// Run() goroutine to drain.
func (m *SandboxManager) Stop(sandboxID string, force bool) error {
	m.mu.RLock()
	_, exists := m.sandboxes[sandboxID]
	m.mu.RUnlock()

	if !exists {
		return fmt.Errorf("sandbox %s not found", sandboxID)
	}

	log.Info().
		Str("sandbox_id", sandboxID).
		Bool("force", force).
		Msg("stopping sandbox")

	opts := &runtime.KillOpts{All: true}
	killSignal := syscall.SIGTERM
	if force {
		killSignal = syscall.SIGKILL
	}
	killCtx, killCancel := context.WithTimeout(m.ctx, sandboxKillTimeout)
	defer killCancel()
	if err := m.runtime.Kill(killCtx, sandboxID, killSignal, opts); err != nil {
		if isContainerAlreadyStopped(err) {
			log.Debug().Str("sandbox_id", sandboxID).Msg("container already stopped, skipping kill")
		} else if !force {
			return fmt.Errorf("failed to kill sandbox: %w", err)
		} else {
			log.Warn().Err(err).Str("sandbox_id", sandboxID).Msg("force kill failed")
		}
	}

	return nil
}

// isContainerAlreadyStopped returns true when a runtime kill error
// indicates the container has already exited. Both runsc and runc
// return exit code 128 in this case.
func isContainerAlreadyStopped(err error) bool {
	if err == nil {
		return false
	}
	var exitErr *exec.ExitError
	if ok := errors.As(err, &exitErr); ok {
		return exitErr.ExitCode() == 128
	}
	msg := err.Error()
	return strings.Contains(msg, "container not running") ||
		strings.Contains(msg, "container is not running") ||
		strings.Contains(msg, "not found") ||
		strings.Contains(msg, "does not exist")
}

func isSandboxTeardownError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		if exitErr.ExitCode() == 137 {
			return true
		}
		if status, ok := exitErr.Sys().(syscall.WaitStatus); ok {
			return status.Signaled() && status.Signal() == syscall.SIGKILL
		}
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "signal: killed") ||
		strings.Contains(msg, "context canceled") ||
		strings.Contains(msg, "context deadline exceeded")
}

// Delete removes a sandbox and cleans up resources.
//
// The teardown sequence avoids racing with the runtime supervisor:
//  1. Stop container processes (SIGTERM or SIGKILL depending on force)
//  2. Wait for the Run() goroutine to finish so runsc exits cleanly
//  3. Only cancel the sandbox context as a fallback if Run() doesn't drain
//  4. runtime.Delete to clean up any residual state
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

	// Step 1: Stop container processes if still running.
	if sandbox.State.Status == types.SandboxStatusRunning {
		if err := m.Stop(sandboxID, force); err != nil && !force {
			return fmt.Errorf("failed to stop sandbox: %w", err)
		}
	}

	// Step 2: Wait for the Run() goroutine to exit so the runtime supervisor
	// (runsc) can finish its own cleanup before we try to delete.
	if sandbox.done != nil {
		drainTimeout := sandboxRunDrainTimeout
		if force {
			drainTimeout = sandboxForceDrainTimeout
		}
		select {
		case <-sandbox.done:
		case <-time.After(drainTimeout):
			log.Warn().
				Str("sandbox_id", sandboxID).
				Dur("timeout", drainTimeout).
				Msg("timed out waiting for sandbox run goroutine; forcing context cancel")
			if sandbox.Cancel != nil {
				sandbox.Cancel()
			}
			select {
			case <-sandbox.done:
			case <-time.After(sandboxForceDrainTimeout):
			}
		}
	} else if sandbox.Cancel != nil {
		sandbox.Cancel()
	}

	// Step 3: Ensure context is cancelled for any remaining goroutines.
	if sandbox.Cancel != nil {
		sandbox.Cancel()
	}

	// Step 4: Ask the runtime to clean up residual container state.
	// The Run() goroutine's deferred delete should have handled this already,
	// but we do it explicitly to be safe.
	opts := &runtime.DeleteOpts{Force: force}
	deleteCtx, deleteCancel := context.WithTimeout(m.ctx, sandboxDeleteTimeout)
	defer deleteCancel()
	if err := m.runtime.Delete(deleteCtx, sandboxID, opts); err != nil {
		if isContainerAlreadyStopped(err) || isSandboxTeardownError(err) {
			log.Debug().Err(err).Str("sandbox_id", sandboxID).Msg("runtime delete skipped; container already cleaned up")
		} else {
			log.Warn().Err(err).Str("sandbox_id", sandboxID).Msg("runtime delete failed")
		}
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

	if m.mountManager != nil {
		m.mountManager.CleanupAll()
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

	// Inject auth token: prefer the task's workspace-scoped member token
	// (which resolves to the correct workspace/member for integrations like
	// GitHub), falling back to the worker's cluster-level token.
	if token := strings.TrimSpace(cfg.Env["AIRSTORE_TOKEN"]); token != "" {
		spec.Process.Env = append(spec.Process.Env, fmt.Sprintf("AIRSTORE_TOKEN=%s", token))
	} else if m.authToken != "" {
		spec.Process.Env = append(spec.Process.Env, fmt.Sprintf("AIRSTORE_TOKEN=%s", m.authToken))
	}

	// Add filesystem mount (bind mount from FUSE mount)
	if cfg.FilesystemMount != "" {
		if err := m.addFilesystemMount(&spec, cfg.FilesystemMount, cfg.FilesystemReadOnly); err != nil {
			return nil, fmt.Errorf("failed to add filesystem mount: %w", err)
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

	// Prefer the worker pod's resolv.conf so sandbox DNS matches cluster DNS behavior.
	resolvConfSource := resolveSandboxResolvConfSource(m.useHostResolvConf)
	spec.Mounts = append(spec.Mounts, specs.Mount{
		Destination: "/etc/resolv.conf",
		Type:        "none",
		Source:      resolvConfSource,
		Options:     []string{"ro", "rbind", "rprivate", "nosuid", "noexec", "nodev"},
	})

	return &spec, nil
}

func resolveSandboxResolvConfSource(useHostResolvConf bool) string {
	if useHostResolvConf {
		if _, err := os.Stat("/etc/resolv.conf"); err == nil {
			return "/etc/resolv.conf"
		}
	}

	if _, err := os.Stat("/workspace/etc/resolv.conf"); err == nil {
		return "/workspace/etc/resolv.conf"
	}

	// Final fallback for environments where /workspace/etc may not exist.
	return "/etc/resolv.conf"
}

func (m *SandboxManager) ensureSandboxWorkingDirOnMount(cfg types.SandboxConfig) error {
	if strings.TrimSpace(cfg.FilesystemMount) == "" {
		return nil
	}

	workDir := path.Clean(strings.TrimSpace(cfg.WorkingDir))
	switch workDir {
	case "", ".", "/", types.ContainerWorkDir:
		return nil
	}

	if workDir != types.ContainerWorkDir && !strings.HasPrefix(workDir, types.ContainerWorkDir+"/") {
		return nil
	}

	storagePath := strings.TrimPrefix(workDir, types.ContainerWorkDir)
	if storagePath == "" {
		return nil
	}
	if !strings.HasPrefix(storagePath, "/") {
		storagePath = "/" + storagePath
	}

	hostPath, err := vfsHostPathWithinMount(cfg.FilesystemMount, workDir)
	if err != nil {
		return fmt.Errorf("resolve working directory %q within mount %q: %w", workDir, cfg.FilesystemMount, err)
	}

	if cfg.FilesystemReadOnly {
		if err := m.ensureContextDirectoryVisible(cfg, storagePath, hostPath, false); err != nil {
			return fmt.Errorf("working directory %q does not exist on read-only mount %q: %w", workDir, cfg.FilesystemMount, err)
		}
		return nil
	}

	log.Debug().
		Str("container_workdir", workDir).
		Str("storage_path", storagePath).
		Str("host_path", hostPath).
		Msg("preparing sandbox working directory on mounted workspace")
	if err := m.ensureContextDirectoryVisible(cfg, storagePath, hostPath, true); err != nil {
		return fmt.Errorf("ensure working directory %q via gateway context service: %w", workDir, err)
	}
	return nil
}

func (m *SandboxManager) ensureContextDirectoryVisible(
	cfg types.SandboxConfig,
	storagePath string,
	hostPath string,
	create bool,
) error {
	token := strings.TrimSpace(cfg.Env["AIRSTORE_TOKEN"])
	if token == "" {
		token = strings.TrimSpace(m.authToken)
	}
	if token == "" {
		return fmt.Errorf("no auth token available for workspace directory preparation")
	}
	if strings.TrimSpace(m.gatewayAddr) == "" {
		return fmt.Errorf("gateway address unavailable for workspace directory preparation")
	}

	client, closeClient, err := newSandboxContextClient(m.gatewayAddr, token)
	if err != nil {
		return fmt.Errorf("connect context client: %w", err)
	}
	defer closeClient()

	ctx, cancel := context.WithTimeout(m.ctx, 15*time.Second)
	defer cancel()

	if create {
		if err := ensureContextPath(ctx, client, storagePath); err != nil {
			return err
		}
	} else {
		if err := statContextPath(ctx, client, storagePath); err != nil {
			return err
		}
	}

	if err := waitForWorkspacePath(hostPath, 2*time.Second); err != nil {
		return fmt.Errorf("path not visible on mount after context update: %w", err)
	}
	return nil
}

func ensureContextPath(ctx context.Context, client sandboxContextClient, storagePath string) error {
	current := ""
	for _, segment := range strings.Split(strings.TrimPrefix(storagePath, "/"), "/") {
		segment = strings.TrimSpace(segment)
		if segment == "" {
			continue
		}
		current += "/" + segment
		resp, err := client.Mkdir(ctx, &pb.ContextMkdirRequest{
			Path: current,
			Mode: uint32(syscall.S_IFDIR | 0o755),
		})
		if err != nil {
			return fmt.Errorf("mkdir %q: %w", current, err)
		}
		if !resp.Ok {
			return fmt.Errorf("mkdir %q rejected: %s", current, strings.TrimSpace(resp.Error))
		}
	}
	return nil
}

func statContextPath(ctx context.Context, client sandboxContextClient, storagePath string) error {
	resp, err := client.Stat(ctx, &pb.ContextStatRequest{Path: storagePath})
	if err != nil {
		return fmt.Errorf("stat %q: %w", storagePath, err)
	}
	if !resp.Ok {
		return fmt.Errorf("stat %q rejected: %s", storagePath, strings.TrimSpace(resp.Error))
	}
	if resp.Info == nil || !resp.Info.IsDir {
		return fmt.Errorf("path %q is not a directory", storagePath)
	}
	return nil
}

func waitForWorkspacePath(hostPath string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	var lastErr error
	for {
		info, err := os.Stat(hostPath)
		switch {
		case err == nil:
			if !info.IsDir() {
				return fmt.Errorf("%q is not a directory", hostPath)
			}
			return nil
		case !errors.Is(err, os.ErrNotExist):
			lastErr = err
		default:
			lastErr = err
		}

		if time.Now().After(deadline) {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if lastErr == nil {
		lastErr = os.ErrNotExist
	}
	return fmt.Errorf("stat %q: %w", hostPath, lastErr)
}

// addFilesystemMount bind-mounts a FUSE filesystem into the sandbox at /workspace.
func (m *SandboxManager) addFilesystemMount(spec *specs.Spec, source string, readOnly bool) error {
	// Verify the mount exists and includes required system roots.
	ready, missing, err := checkFilesystemMountReady(source)
	if err != nil {
		return fmt.Errorf("filesystem mount not ready at %s: %w", source, err)
	}
	if !ready {
		return fmt.Errorf("filesystem mount missing required roots at %s: %s", source, strings.Join(missing, ", "))
	}

	entries, _ := os.ReadDir(source)

	// Bind mount at container working directory.
	// rslave propagation lets mount restarts on the host propagate into the
	// sandbox, which is necessary for recovery after a FUSE mount process crash.
	options := []string{"rbind", "rslave"}
	if readOnly {
		options = append(options, "ro")
	} else {
		options = append(options, "rw")
	}

	spec.Mounts = append(spec.Mounts, specs.Mount{
		Destination: types.ContainerWorkDir,
		Type:        "bind",
		Source:      source,
		Options:     options,
	})

	log.Debug().
		Str("source", source).
		Str("dest", types.ContainerWorkDir).
		Int("files", len(entries)).
		Msg("added filesystem bind mount to sandbox")

	return nil
}

// verifyRootfs checks that the overlay-merged rootfs directory is readable and
// contains the minimum structure expected by gVisor's gofer. A broken FUSE
// mount or incomplete overlay can cause the gofer to crash silently.
func verifyRootfs(rootfsPath string) error {
	entries, err := os.ReadDir(rootfsPath)
	if err != nil {
		return fmt.Errorf("cannot read rootfs dir: %w", err)
	}
	if len(entries) == 0 {
		return fmt.Errorf("rootfs is empty")
	}

	essential := []string{"bin", "etc", "usr"}
	found := make(map[string]bool, len(entries))
	for _, e := range entries {
		found[e.Name()] = true
	}
	var missing []string
	for _, dir := range essential {
		if !found[dir] {
			missing = append(missing, dir)
		}
	}
	if len(missing) > 0 {
		log.Warn().
			Str("rootfs", rootfsPath).
			Strs("missing", missing).
			Int("entry_count", len(entries)).
			Msg("rootfs missing expected directories")
		return fmt.Errorf("rootfs missing: %s", strings.Join(missing, ", "))
	}

	// Spot-check readability of /bin/sh — if the FUSE layer is dead, this
	// will return an I/O error before gVisor has a chance to crash.
	shPath := filepath.Join(rootfsPath, "bin", "sh")
	if _, err := os.Stat(shPath); err != nil {
		return fmt.Errorf("cannot stat %s: %w", shPath, err)
	}

	return nil
}

type promptTaskPlan struct {
	runner   AgentExecutionRunner
	analyzer OutputAnalyzer
	bamlEnv  map[string]string
}

// buildEntrypoint constructs the entrypoint for a task.
// Prompt tasks are resolved through prompt runner entrypoints; all other tasks
// use their explicit task entrypoint.
func (m *SandboxManager) buildEntrypoint(task types.RunExecution, env map[string]string, promptPlan promptTaskPlan) []string {
	if task.Prompt == "" {
		return task.Entrypoint
	}

	return m.buildPromptTaskEntrypoint(task, env, promptPlan.runner)
}

func (m *SandboxManager) buildPromptTaskEntrypoint(task types.RunExecution, env map[string]string, runner AgentExecutionRunner) []string {
	if runner == nil {
		runner = m.resolvePromptRunner(task, env)
	}
	return runner.BuildEntrypoint(task, env)
}

func (m *SandboxManager) resolvePromptTaskPlan(task types.RunExecution, env map[string]string) promptTaskPlan {
	return promptTaskPlanForRunner(m.resolvePromptRunner(task, env))
}

func promptTaskPlanForRunner(runner AgentExecutionRunner) promptTaskPlan {
	return promptTaskPlan{
		runner:   runner,
		analyzer: analyzerForRunner(runner),
		bamlEnv:  bamlEnvForRunner(runner),
	}
}

func (m *SandboxManager) resolvePromptRunner(task types.RunExecution, env map[string]string) AgentExecutionRunner {
	defaultRunner := m.defaultPromptRunner
	if defaultRunner == nil {
		defaultRunner = NewClaudeCodeRunner(ClaudeCodeRunnerOptions{})
	}

	provider := promptTaskProvider(env)
	if provider == "" {
		return defaultRunner
	}

	runner, ok := m.promptRunners[provider]
	if !ok || runner == nil {
		addTaskExecutionContext(
			log.Warn().
				Str("provider", provider).
				Str("default_runner", defaultRunner.Name()),
			task,
		).Msg("unsupported agent provider for prompt task, falling back to default runner")
		return defaultRunner
	}
	return runner
}

func promptTaskProvider(env map[string]string) string {
	if provider := runnerProviderFromEnv(env); provider != "" {
		return provider
	}
	return inferProviderFromModel(env)
}

func analyzerForRunner(runner AgentExecutionRunner) OutputAnalyzer {
	if provider, ok := runner.(AnalyzerProvider); ok {
		return provider.OutputAnalyzer()
	}
	return nil
}

func bamlEnvForRunner(runner AgentExecutionRunner) map[string]string {
	if provider, ok := runner.(ClassifierEnvProvider); ok {
		return cloneMap(provider.ClassifierEnv())
	}
	if runner == nil {
		return nil
	}
	return map[string]string{}
}

func (m *SandboxManager) BamlEnvForRunner(runner AgentExecutionRunner) map[string]string {
	env := bamlEnvForRunner(runner)
	if len(env) > 0 || runner == m.defaultPromptRunner {
		return env
	}
	return bamlEnvForRunner(m.defaultPromptRunner)
}

// taskOutputPipeline builds the standard set of writers for capturing task
// stdout: S2 streaming, console logging, structured output capture, and
// (when applicable) BAML output analysis.
type outputWaiter interface {
	Wait()
}

type taskOutputPipeline struct {
	writers []io.Writer
	waiters []outputWaiter
	tracker *taskOutputTracker
}

func (p taskOutputPipeline) Wait() {
	for _, waiter := range p.waiters {
		if waiter != nil {
			waiter.Wait()
		}
	}
}

func (m *SandboxManager) taskOutputPipeline(ctx context.Context, task types.RunExecution, promptPlan promptTaskPlan) taskOutputPipeline {
	tracker := &taskOutputTracker{}
	outputWriter := newOutputWriter(ctx, m.gatewayClient, task, tracker)

	pipeline := taskOutputPipeline{
		writers: []io.Writer{
			NewS2Writer(ctx, m.s2, task.ExternalId, "stdout"),
			NewConsoleWriter(task.ExternalId, "stdout"),
			outputWriter,
		},
		waiters: []outputWaiter{outputWriter},
		tracker: tracker,
	}
	if analyzer := promptPlan.analyzer; analyzer != nil {
		analyzerWriter := newAnalyzerWriter(ctx, analyzer, m.gatewayClient, task, promptPlan.bamlEnv, tracker)
		pipeline.writers = append(pipeline.writers, analyzerWriter)
		pipeline.waiters = append(pipeline.waiters, analyzerWriter)
	}
	return pipeline
}

func (m *SandboxManager) copyTaskEnv(task types.RunExecution) map[string]string {
	return cloneMap(task.Env)
}

func (m *SandboxManager) buildTaskSandboxConfig(task types.RunExecution, entrypoint []string, env map[string]string, mountSource string) types.SandboxConfig {
	if task.MemberToken != "" {
		env["AIRSTORE_TOKEN"] = task.MemberToken
	}

	runtimeType := types.ContainerRuntimeGvisor
	if task.RuntimeType != nil && *task.RuntimeType == types.ContainerRuntimeRunc.String() {
		runtimeType = types.ContainerRuntimeRunc
	}

	workspaceAccess := "rw"
	if task.WorkspaceAccess != nil && *task.WorkspaceAccess != "" {
		workspaceAccess = *task.WorkspaceAccess
	}
	if workspaceAccess == "none" {
		mountSource = ""
	}

	// Point git at the VFS-hosted config (credential helper + user identity).
	// Actual files are written by setupGitFiles; resolveGitIdentity
	// populates the [user] section after the sandbox starts.
	for k, v := range gitEnvVars() {
		env[k] = v
	}

	networkMode := "bridge"
	if task.NetworkEnabled != nil && !*task.NetworkEnabled {
		networkMode = "none"
	}

	workDir := types.ContainerWorkDir
	if wd := strings.TrimSpace(env["AIRSTORE_AGENT_WORKSPACE_DIR"]); wd != "" {
		workDir = wd
	}

	return types.SandboxConfig{
		ID:                 fmt.Sprintf("task-%s", task.ExternalId),
		WorkspaceID:        fmt.Sprintf("%d", task.WorkspaceId),
		Image:              task.Image,
		Runtime:            runtimeType,
		Entrypoint:         entrypoint,
		Env:                env,
		WorkingDir:         workDir,
		FilesystemMount:    mountSource,
		FilesystemReadOnly: workspaceAccess == "ro",
		Resources:          task.GetResources(),
		Network: types.SandboxNetwork{
			Mode: networkMode,
		},
	}
}

// mountFilesystem sets up the filesystem mount for a task.
// Prefers task-specific mount with member token, falls back to worker global mount.
func (m *SandboxManager) mountFilesystem(ctx context.Context, task types.RunExecution) string {
	if task.WorkspaceAccess != nil && *task.WorkspaceAccess == "none" {
		return ""
	}

	// Try task-specific mount with member token
	if task.MemberToken != "" && m.mountManager != nil {
		mountPath, err := m.mountManager.Mount(ctx, task.ExternalId, task.MemberToken)
		if err != nil {
			addTaskExecutionContext(log.Warn().Err(err), task).Msg("failed to create task mount")
		} else {
			addTaskExecutionContext(log.Debug().Str("mount_path", mountPath), task).Msg("mounted filesystem with task token")
			return mountPath
		}
	}

	// Fall back to worker's global mount
	if m.enableFS {
		addTaskExecutionContext(log.Debug().Str("mount_path", m.paths.WorkerMount), task).Msg("using worker global mount")
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

// ptySetupScript is the shell script executed inside the sandbox for interactive
// terminal sessions.  It starts an interactive shell under a PTY wrapper,
// falling back through script → python pty → bash -i → sh -i depending on
// what's available in the image.
//
// Each PTY path sets the terminal size to a reasonable default (COLUMNS x LINES
// from the environment, defaulting to 180x40) since there is no dynamic resize.
const ptySetupScript = `
C=${COLUMNS:-180}; L=${LINES:-40}
S="stty cols $C rows $L 2>/dev/null; exec /bin/bash"
if command -v script >/dev/null 2>&1; then exec script -qc "$S" /dev/null
elif command -v python3 >/dev/null 2>&1; then exec python3 -c "import pty; pty.spawn([\"/bin/bash\",\"-c\",\"$S\"])"
elif command -v python  >/dev/null 2>&1; then exec python  -c "import pty; pty.spawn([\"/bin/bash\",\"-c\",\"$S\"])"
elif [ -x /bin/bash ];                  then exec /bin/bash -i
else                                          exec /bin/sh -i
fi
`

const (
	ptyDefaultPATH = "PATH=/workspace/tools:/home/sandbox/airstore-runners/.venv/bin:/home/sandbox/.npm-global/bin:/home/sandbox/.local/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
	ptyDefaultCols = "180"
	ptyDefaultRows = "40"
)

func buildPTYExecEnv(envMap map[string]string) []string {
	env := []string{
		ptyDefaultPATH,
		"TERM=xterm-256color",
		"COLUMNS=" + ptyDefaultCols,
		"LINES=" + ptyDefaultRows,
	}
	for k, v := range envMap {
		env = append(env, fmt.Sprintf("%s=%s", k, v))
	}
	return env
}

// AttachPTY starts a long-lived PTY-backed process in a running sandbox.
// Claude interactive tasks run their task entrypoint directly; other interactive
// tasks keep the existing shell PTY behavior.
func (m *SandboxManager) AttachPTY(
	ctx context.Context,
	sandboxID string,
	stdin io.Reader,
	stdout io.Writer,
) error {
	envMap, err := m.sandboxEnv(sandboxID)
	if err != nil {
		return err
	}

	proc := specs.Process{
		Args: []string{"/bin/sh", "-lc", ptySetupScript},
		Cwd:  types.ContainerWorkDir,
		Env:  buildPTYExecEnv(envMap),
		User: specs.User{UID: types.SandboxUserUID, GID: types.SandboxUserGID},
	}

	return m.runtime.Exec(ctx, sandboxID, proc, &runtime.ExecOpts{
		OutputWriter: stdout,
		StdinReader:  stdin,
	})
}

// ExecPTY runs a command inside an existing sandbox with PTY-compatible env.
// Unlike AttachPTY, it takes explicit args and does not attach stdin — the
// prompt is passed via command-line args. This is used by the turn-based
// session loop where each turn is a separate Exec call.
//
// The command is wrapped in a login shell so that the user's profile (and
// full PATH) is available — tools like `claude` are often installed outside
// the minimal system PATH.
func (m *SandboxManager) ExecPTY(
	ctx context.Context,
	sandboxID string,
	args []string,
	env map[string]string,
	stdout io.Writer,
) error {
	sandboxEnv, err := m.sandboxEnv(sandboxID)
	if err != nil {
		return err
	}
	for k, v := range env {
		sandboxEnv[k] = v
	}

	proc := specs.Process{
		Args: []string{"/bin/bash", "-lc", shellJoinArgs(args)},
		Cwd:  types.ContainerWorkDir,
		Env:  buildPTYExecEnv(sandboxEnv),
		User: specs.User{UID: types.SandboxUserUID, GID: types.SandboxUserGID},
	}

	return m.runtime.Exec(ctx, sandboxID, proc, &runtime.ExecOpts{
		OutputWriter: stdout,
	})
}

// ExecCheck runs a short-lived command inside a sandbox and returns nil if
// the command exits 0. This is useful for probing container state (e.g.
// checking whether specific processes are still running via pgrep).
func (m *SandboxManager) ExecCheck(ctx context.Context, sandboxID string, args []string) error {
	proc := specs.Process{
		Args: args,
		Cwd:  types.ContainerWorkDir,
		Env:  []string{ptyDefaultPATH},
		User: specs.User{UID: types.SandboxUserUID, GID: types.SandboxUserGID},
	}
	return m.runtime.Exec(ctx, sandboxID, proc, &runtime.ExecOpts{
		OutputWriter: io.Discard,
	})
}

func shellJoinArgs(args []string) string {
	parts := make([]string, len(args))
	for i, arg := range args {
		parts[i] = "'" + strings.ReplaceAll(arg, "'", `'"'"'`) + "'"
	}
	return strings.Join(parts, " ")
}

// sandboxEnv returns a copy of the sandbox's configured environment.
func (m *SandboxManager) sandboxEnv(sandboxID string) (map[string]string, error) {
	m.mu.RLock()
	sandbox, exists := m.sandboxes[sandboxID]
	m.mu.RUnlock()
	if !exists {
		return nil, fmt.Errorf("sandbox %s not found", sandboxID)
	}
	if sandbox.State.Status != types.SandboxStatusRunning {
		return nil, fmt.Errorf("sandbox %s is not running", sandboxID)
	}
	envMap := make(map[string]string, len(sandbox.Config.Env))
	for k, v := range sandbox.Config.Env {
		envMap[k] = v
	}
	return envMap, nil
}

// ResolveRunner returns the AgentExecutionRunner for the given task.
func (m *SandboxManager) ResolveRunner(task types.RunExecution, env map[string]string) AgentExecutionRunner {
	return m.resolvePromptTaskPlan(task, env).runner
}

// RunTask creates and runs a sandbox for a task, returning when complete
func (m *SandboxManager) RunTask(ctx context.Context, task types.RunExecution) (*types.RunExecutionResult, error) {
	sandboxID := fmt.Sprintf("task-%s", task.ExternalId)

	env := m.copyTaskEnv(task)
	promptPlan := m.resolvePromptTaskPlan(task, env)

	// Build entrypoint
	entrypoint := m.buildEntrypoint(task, env, promptPlan)

	// Mount filesystem for task
	taskMountSource := m.mountFilesystem(ctx, task)
	defer m.cleanupMount(task.ExternalId)

	cfg := m.buildTaskSandboxConfig(task, entrypoint, env, taskMountSource)

	// Create the sandbox
	state, err := m.Create(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create sandbox: %w", err)
	}

	// Ensure cleanup
	defer m.Delete(sandboxID, true)

	pipeline := m.taskOutputPipeline(ctx, task, promptPlan)
	taskOutput := NewTaskStreamOutput(task.ExternalId, "stdout", pipeline.writers...)
	defer pipeline.Wait()
	defer taskOutput.Flush()
	outputWriter := io.Writer(taskOutput)
	if err := m.SetOutput(sandboxID, outputWriter, taskOutput.Flush); err != nil {
		addTaskExecutionContext(log.Warn().Err(err), task).Msg("failed to set output")
	}

	// Publish starting status
	m.publishStatus(ctx, task.ExternalId, types.RunExecutionStatusRunning, nil, "")

	// Start the sandbox
	if err := m.Start(sandboxID); err != nil {
		m.publishStatus(ctx, task.ExternalId, types.RunExecutionStatusFailed, nil, err.Error())
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
			m.publishStatus(ctx, task.ExternalId, types.RunExecutionStatusCancelled, &exitCode, errMsg)
			return &types.RunExecutionResult{
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
				pipeline.Wait()

				// Publish completion status
				status := types.RunExecutionStatusComplete
				if state.ExitCode != 0 || state.Error != "" {
					status = types.RunExecutionStatusFailed
				}
				m.publishStatus(ctx, task.ExternalId, status, &state.ExitCode, state.Error)

				return &types.RunExecutionResult{
					ID:       task.ExternalId,
					ExitCode: state.ExitCode,
					Error:    state.Error,
					Duration: time.Since(startTime),
				}, nil
			}
		}
	}
}
