package worker

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	runtimepkg "github.com/beam-cloud/airstore/pkg/runtime"
	"github.com/beam-cloud/airstore/pkg/types"
	"github.com/opencontainers/runtime-spec/specs-go"
)

type startProbeRuntime struct {
	readyAfter time.Duration
	running    atomic.Bool
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
	_ syscall.Signal,
	_ *runtimepkg.KillOpts,
) error {
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
