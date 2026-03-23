package worker

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	runtimepkg "github.com/beam-cloud/airstore/pkg/runtime"
	"github.com/beam-cloud/airstore/pkg/types"
	pb "github.com/beam-cloud/airstore/proto"
	"github.com/opencontainers/runtime-spec/specs-go"
	"google.golang.org/grpc"
)

type startProbeRuntime struct {
	readyAfter time.Duration
	running    atomic.Bool
	lastKill   atomic.Int32
}

func (r *startProbeRuntime) Name() string { return "start-probe" }

func (r *startProbeRuntime) Capabilities() runtimepkg.Capabilities {
	return runtimepkg.Capabilities{}
}

func (r *startProbeRuntime) Prepare(_ context.Context, _ *specs.Spec) error { return nil }

func (r *startProbeRuntime) Run(
	ctx context.Context,
	_ string,
	_ string,
	opts *runtimepkg.RunOpts,
) (int, error) {
	if opts != nil && opts.Started != nil {
		opts.Started <- 4321
	}

	timer := time.NewTimer(r.readyAfter)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return 0, nil
	case <-timer.C:
		r.running.Store(true)
	}

	<-ctx.Done()
	return 0, nil
}

func (r *startProbeRuntime) Exec(
	_ context.Context,
	_ string,
	_ specs.Process,
	_ *runtimepkg.ExecOpts,
) error {
	return nil
}

func (r *startProbeRuntime) Kill(
	_ context.Context,
	_ string,
	sig syscall.Signal,
	_ *runtimepkg.KillOpts,
) error {
	r.lastKill.Store(int32(sig))
	return nil
}

func (r *startProbeRuntime) Delete(_ context.Context, _ string, _ *runtimepkg.DeleteOpts) error {
	return nil
}

func (r *startProbeRuntime) State(_ context.Context, containerID string) (runtimepkg.State, error) {
	if !r.running.Load() {
		return runtimepkg.State{}, fmt.Errorf("sandbox %s not found", containerID)
	}
	return runtimepkg.State{
		ID:     containerID,
		Pid:    4321,
		Status: string(types.SandboxStatusRunning),
	}, nil
}

func (r *startProbeRuntime) Events(_ context.Context, _ string) (<-chan runtimepkg.Event, error) {
	ch := make(chan runtimepkg.Event)
	close(ch)
	return ch, nil
}

func (r *startProbeRuntime) Checkpoint(_ context.Context, _ string, _ *runtimepkg.CheckpointOpts) error {
	return nil
}

func (r *startProbeRuntime) Restore(_ context.Context, _ string, _ *runtimepkg.RestoreOpts) (int, error) {
	return 0, nil
}

func (r *startProbeRuntime) Close() error { return nil }

func (r *startProbeRuntime) lastKillSignal() syscall.Signal {
	return syscall.Signal(r.lastKill.Load())
}

type stubSandboxContextClient struct {
	mkdir func(ctx context.Context, req *pb.ContextMkdirRequest) (*pb.ContextMkdirResponse, error)
	stat  func(ctx context.Context, req *pb.ContextStatRequest) (*pb.ContextStatResponse, error)
}

func (s *stubSandboxContextClient) Mkdir(ctx context.Context, req *pb.ContextMkdirRequest, _ ...grpc.CallOption) (*pb.ContextMkdirResponse, error) {
	return s.mkdir(ctx, req)
}

func (s *stubSandboxContextClient) Stat(ctx context.Context, req *pb.ContextStatRequest, _ ...grpc.CallOption) (*pb.ContextStatResponse, error) {
	return s.stat(ctx, req)
}

type stubPromptRunner struct{}

func (stubPromptRunner) Name() string { return "stub" }

func (stubPromptRunner) BuildEntrypoint(_ types.RunExecution, _ map[string]string) []string {
	return []string{"stub"}
}

func TestSandboxStartWaitsForRuntimeReady(t *testing.T) {
	runtime := &startProbeRuntime{readyAfter: 120 * time.Millisecond}
	manager := &SandboxManager{
		runtime: runtime,
		sandboxes: map[string]*Sandbox{
			"task-startup": {
				Config: types.SandboxConfig{
					ID:         "task-startup",
					Entrypoint: []string{"sleep", "infinity"},
				},
				State: types.SandboxState{
					ID:       "task-startup",
					Status:   types.SandboxStatusCreating,
					ExitCode: -1,
				},
				Bundle: "/tmp/task-startup",
			},
		},
		ctx: context.Background(),
	}

	startedAt := time.Now()
	if err := manager.Start("task-startup"); err != nil {
		t.Fatalf("start failed: %v", err)
	}

	if elapsed := time.Since(startedAt); elapsed < 100*time.Millisecond {
		t.Fatalf("sandbox start returned before runtime readiness: elapsed=%s", elapsed)
	}

	state, err := manager.Get("task-startup")
	if err != nil {
		t.Fatalf("get state failed: %v", err)
	}
	if state.Status != types.SandboxStatusRunning {
		t.Fatalf("expected running status, got %s", state.Status)
	}
	if state.PID != 4321 {
		t.Fatalf("expected pid 4321, got %d", state.PID)
	}

	if err := manager.Stop("task-startup", true); err != nil {
		t.Fatalf("stop failed: %v", err)
	}
	if got := runtime.lastKillSignal(); got != syscall.SIGKILL {
		t.Fatalf("expected stop(force=true) to send SIGKILL, got %d", got)
	}
}

func TestResolveSandboxResolvConfSourcePrefersHostConfig(t *testing.T) {
	if _, err := os.Stat("/etc/resolv.conf"); err != nil {
		t.Skip("/etc/resolv.conf is not available in this environment")
	}

	source := resolveSandboxResolvConfSource(true)
	if source != "/etc/resolv.conf" {
		t.Fatalf("expected host resolv.conf, got %s", source)
	}
}

func TestResolveSandboxResolvConfSourceReturnsKnownFallbackPath(t *testing.T) {
	source := resolveSandboxResolvConfSource(false)
	if source != "/workspace/etc/resolv.conf" && source != "/etc/resolv.conf" {
		t.Fatalf("unexpected resolv.conf source: %s", source)
	}
}

func TestEnsureSandboxWorkingDirOnMountCreatesMissingDir(t *testing.T) {
	mountSource := t.TempDir()
	prev := newSandboxContextClient
	newSandboxContextClient = func(addr, token string) (sandboxContextClient, func() error, error) {
		client := &stubSandboxContextClient{
			mkdir: func(_ context.Context, req *pb.ContextMkdirRequest) (*pb.ContextMkdirResponse, error) {
				if err := os.MkdirAll(filepath.Join(mountSource, strings.TrimPrefix(req.Path, "/")), 0o755); err != nil {
					return nil, err
				}
				return &pb.ContextMkdirResponse{Ok: true}, nil
			},
			stat: func(_ context.Context, req *pb.ContextStatRequest) (*pb.ContextStatResponse, error) {
				info, err := os.Stat(filepath.Join(mountSource, strings.TrimPrefix(req.Path, "/")))
				if err != nil {
					return &pb.ContextStatResponse{Ok: false, Error: err.Error()}, nil
				}
				return &pb.ContextStatResponse{Ok: true, Info: &pb.FileInfo{IsDir: info.IsDir()}}, nil
			},
		}
		return client, func() error { return nil }, nil
	}
	t.Cleanup(func() { newSandboxContextClient = prev })

	manager := &SandboxManager{
		ctx:         context.Background(),
		gatewayAddr: "gateway.test.internal:1993",
		authToken:   "token",
	}
	cfg := types.SandboxConfig{
		FilesystemMount: mountSource,
		WorkingDir:      "/workspace/agents/email-outreach",
		Env:             map[string]string{"AIRSTORE_TOKEN": "task-token"},
	}

	if err := manager.ensureSandboxWorkingDirOnMount(cfg); err != nil {
		t.Fatalf("ensureSandboxWorkingDirOnMount returned error: %v", err)
	}

	hostPath := filepath.Join(mountSource, "agents", "email-outreach")
	info, err := os.Stat(hostPath)
	if err != nil {
		t.Fatalf("expected workdir to exist: %v", err)
	}
	if !info.IsDir() {
		t.Fatalf("expected %s to be a directory", hostPath)
	}
}

func TestEnsureSandboxWorkingDirOnMountFailsForMissingDirOnReadOnlyMount(t *testing.T) {
	prev := newSandboxContextClient
	newSandboxContextClient = func(addr, token string) (sandboxContextClient, func() error, error) {
		client := &stubSandboxContextClient{
			mkdir: func(_ context.Context, req *pb.ContextMkdirRequest) (*pb.ContextMkdirResponse, error) {
				return &pb.ContextMkdirResponse{Ok: false, Error: "unexpected mkdir"}, nil
			},
			stat: func(_ context.Context, req *pb.ContextStatRequest) (*pb.ContextStatResponse, error) {
				return &pb.ContextStatResponse{Ok: false, Error: "not found"}, nil
			},
		}
		return client, func() error { return nil }, nil
	}
	t.Cleanup(func() { newSandboxContextClient = prev })

	manager := &SandboxManager{
		ctx:         context.Background(),
		gatewayAddr: "gateway.test.internal:1993",
		authToken:   "token",
	}
	cfg := types.SandboxConfig{
		FilesystemMount:    t.TempDir(),
		FilesystemReadOnly: true,
		WorkingDir:         "/workspace/agents/email-outreach",
		Env:                map[string]string{"AIRSTORE_TOKEN": "task-token"},
	}

	err := manager.ensureSandboxWorkingDirOnMount(cfg)
	if err == nil {
		t.Fatal("expected missing read-only workdir to fail")
	}
	if !strings.Contains(err.Error(), "read-only mount") {
		t.Fatalf("expected read-only mount error, got %v", err)
	}
}

func TestResolvePromptTaskPlanUsesSelectedRunnerCapabilities(t *testing.T) {
	claudeRunner := NewClaudeCodeRunner(ClaudeCodeRunnerOptions{AnthropicAPIKey: "claude-key"})
	airRunner := NewAirRunner(AirRunnerOptions{AnthropicAPIKey: "air-key"})
	manager := &SandboxManager{
		defaultPromptRunner: claudeRunner,
		promptRunners: map[string]AgentExecutionRunner{
			"claude": claudeRunner,
			"air":    airRunner,
		},
	}

	plan := manager.resolvePromptTaskPlan(types.RunExecution{Prompt: "hi"}, map[string]string{
		agentProviderEnvKey: "air",
	})

	if _, ok := plan.runner.(*AirRunner); !ok {
		t.Fatalf("expected air runner, got %T", plan.runner)
	}
	if _, ok := plan.analyzer.(*AirAnalyzer); !ok {
		t.Fatalf("expected air analyzer, got %T", plan.analyzer)
	}
	if got := plan.bamlEnv["ANTHROPIC_API_KEY"]; got != "air-key" {
		t.Fatalf("expected air classifier env, got %q", got)
	}
}

func TestResolvePromptTaskPlanFallsBackToDefaultRunner(t *testing.T) {
	claudeRunner := NewClaudeCodeRunner(ClaudeCodeRunnerOptions{AnthropicAPIKey: "claude-key"})
	manager := &SandboxManager{
		defaultPromptRunner: claudeRunner,
		promptRunners: map[string]AgentExecutionRunner{
			"claude": claudeRunner,
		},
	}

	plan := manager.resolvePromptTaskPlan(types.RunExecution{Prompt: "hi"}, map[string]string{
		agentProviderEnvKey: "unknown",
	})

	if _, ok := plan.runner.(*ClaudeCodeRunner); !ok {
		t.Fatalf("expected claude fallback runner, got %T", plan.runner)
	}
	if _, ok := plan.analyzer.(*ClaudeCodeAnalyzer); !ok {
		t.Fatalf("expected claude analyzer, got %T", plan.analyzer)
	}
	if got := plan.bamlEnv["ANTHROPIC_API_KEY"]; got != "claude-key" {
		t.Fatalf("expected default classifier env, got %q", got)
	}
}

func TestBamlEnvForRunnerFallsBackToDefaultRunnerEnv(t *testing.T) {
	defaultRunner := NewClaudeCodeRunner(ClaudeCodeRunnerOptions{AnthropicAPIKey: "claude-key"})
	manager := &SandboxManager{defaultPromptRunner: defaultRunner}

	env := manager.BamlEnvForRunner(stubPromptRunner{})
	if got := env["ANTHROPIC_API_KEY"]; got != "claude-key" {
		t.Fatalf("expected default classifier env fallback, got %q", got)
	}
}

func TestIsContainerAlreadyStopped(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil error", nil, false},
		{"generic error", fmt.Errorf("something bad"), false},
		{"exit code 128", &exec.ExitError{ProcessState: exitCodeState(t, 128)}, true},
		{"exit code 1", &exec.ExitError{ProcessState: exitCodeState(t, 1)}, false},
		{"container not running message", fmt.Errorf("container not running"), true},
		{"does not exist message", fmt.Errorf("container does not exist"), true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isContainerAlreadyStopped(tt.err)
			if got != tt.want {
				t.Errorf("isContainerAlreadyStopped(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}

// exitCodeState runs a subprocess that exits with the given code to
// obtain a real *os.ProcessState, since the field is unexported.
func exitCodeState(t *testing.T, code int) *os.ProcessState {
	t.Helper()
	cmd := exec.Command("sh", "-c", fmt.Sprintf("exit %d", code))
	err := cmd.Run()
	if err == nil {
		t.Fatal("expected non-zero exit")
	}
	ee, ok := err.(*exec.ExitError)
	if !ok {
		t.Fatalf("expected ExitError, got %T", err)
	}
	return ee.ProcessState
}
