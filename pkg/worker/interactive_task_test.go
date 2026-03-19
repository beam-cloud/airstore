package worker

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	runtimepkg "github.com/beam-cloud/airstore/pkg/runtime"
	"github.com/beam-cloud/airstore/pkg/types"
	"github.com/opencontainers/runtime-spec/specs-go"
)

// subagentProbeTestRuntime is a minimal Runtime whose Exec behaviour is
// controlled by a caller-supplied function.
type subagentProbeTestRuntime struct {
	execFunc func(ctx context.Context, proc specs.Process) error
}

func (r *subagentProbeTestRuntime) Name() string { return "test" }
func (r *subagentProbeTestRuntime) Capabilities() runtimepkg.Capabilities {
	return runtimepkg.Capabilities{}
}
func (r *subagentProbeTestRuntime) Prepare(_ context.Context, _ *specs.Spec) error { return nil }
func (r *subagentProbeTestRuntime) Run(_ context.Context, _ string, _ string, _ *runtimepkg.RunOpts) (int, error) {
	return 0, nil
}
func (r *subagentProbeTestRuntime) Exec(ctx context.Context, _ string, proc specs.Process, _ *runtimepkg.ExecOpts) error {
	if r.execFunc != nil {
		return r.execFunc(ctx, proc)
	}
	return fmt.Errorf("no exec func")
}
func (r *subagentProbeTestRuntime) Kill(_ context.Context, _ string, _ syscall.Signal, _ *runtimepkg.KillOpts) error {
	return nil
}
func (r *subagentProbeTestRuntime) Delete(_ context.Context, _ string, _ *runtimepkg.DeleteOpts) error {
	return nil
}
func (r *subagentProbeTestRuntime) State(_ context.Context, _ string) (runtimepkg.State, error) {
	return runtimepkg.State{}, nil
}
func (r *subagentProbeTestRuntime) Events(_ context.Context, _ string) (<-chan runtimepkg.Event, error) {
	ch := make(chan runtimepkg.Event)
	close(ch)
	return ch, nil
}
func (r *subagentProbeTestRuntime) Checkpoint(_ context.Context, _ string, _ *runtimepkg.CheckpointOpts) error {
	return nil
}
func (r *subagentProbeTestRuntime) Restore(_ context.Context, _ string, _ *runtimepkg.RestoreOpts) (int, error) {
	return 0, nil
}
func (r *subagentProbeTestRuntime) Close() error { return nil }

func newTestWorkerForSubagents(rt runtimepkg.Runtime) *Worker {
	return &Worker{
		sandboxManager: &SandboxManager{runtime: rt},
	}
}

func TestWaitForSubagents_NoneDetected(t *testing.T) {
	rt := &subagentProbeTestRuntime{
		execFunc: func(_ context.Context, _ specs.Process) error {
			return fmt.Errorf("exit status 1")
		},
	}
	w := newTestWorkerForSubagents(rt)
	task := types.RunExecution{ExternalId: "test-none"}

	outcome := w.waitForSubagentsWithTiming(
		context.Background(), task, "sandbox-1", nil,
		10*time.Millisecond, 100*time.Millisecond, 50*time.Millisecond,
	)
	if outcome != subagentNoneDetected {
		t.Fatalf("expected subagentNoneDetected, got %s", outcome)
	}
}

func TestWaitForSubagents_Finished(t *testing.T) {
	var calls atomic.Int32
	rt := &subagentProbeTestRuntime{
		execFunc: func(_ context.Context, _ specs.Process) error {
			if calls.Add(1) == 1 {
				return nil
			}
			return fmt.Errorf("exit status 1")
		},
	}
	w := newTestWorkerForSubagents(rt)
	task := types.RunExecution{ExternalId: "test-finished"}

	outcome := w.waitForSubagentsWithTiming(
		context.Background(), task, "sandbox-1", nil,
		10*time.Millisecond, 5*time.Second, 50*time.Millisecond,
	)
	if outcome != subagentFinished {
		t.Fatalf("expected subagentFinished, got %s", outcome)
	}
}

func TestWaitForSubagents_SessionCancelled(t *testing.T) {
	rt := &subagentProbeTestRuntime{
		execFunc: func(_ context.Context, _ specs.Process) error {
			return nil
		},
	}
	w := newTestWorkerForSubagents(rt)
	task := types.RunExecution{ExternalId: "test-cancelled"}

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(30 * time.Millisecond)
		cancel()
	}()

	outcome := w.waitForSubagentsWithTiming(
		ctx, task, "sandbox-1", nil,
		10*time.Millisecond, 5*time.Second, 50*time.Millisecond,
	)
	if outcome != subagentSessionCancelled {
		t.Fatalf("expected subagentSessionCancelled, got %s", outcome)
	}
}

func TestWaitForSubagents_MaxWaitReached(t *testing.T) {
	rt := &subagentProbeTestRuntime{
		execFunc: func(_ context.Context, _ specs.Process) error {
			return nil
		},
	}
	w := newTestWorkerForSubagents(rt)
	task := types.RunExecution{ExternalId: "test-maxwait"}

	outcome := w.waitForSubagentsWithTiming(
		context.Background(), task, "sandbox-1", nil,
		10*time.Millisecond, 50*time.Millisecond, 50*time.Millisecond,
	)
	if outcome != subagentMaxWaitReached {
		t.Fatalf("expected subagentMaxWaitReached, got %s", outcome)
	}
}

func TestWaitForSubagents_ProbeTimeoutRetries(t *testing.T) {
	var calls atomic.Int32
	rt := &subagentProbeTestRuntime{
		execFunc: func(ctx context.Context, _ specs.Process) error {
			n := calls.Add(1)
			if n == 1 {
				return nil // initial probe: found
			}
			if n == 2 {
				<-ctx.Done()
				return ctx.Err()
			}
			return fmt.Errorf("exit status 1") // third probe: done
		},
	}
	w := newTestWorkerForSubagents(rt)
	task := types.RunExecution{ExternalId: "test-probe-timeout"}

	outcome := w.waitForSubagentsWithTiming(
		context.Background(), task, "sandbox-1", nil,
		10*time.Millisecond, 5*time.Second, 30*time.Millisecond,
	)
	if outcome != subagentFinished {
		t.Fatalf("expected subagentFinished after probe timeout retry, got %s", outcome)
	}
	if got := calls.Load(); got < 3 {
		t.Fatalf("expected at least 3 exec calls (initial + timed-out + success), got %d", got)
	}
}

func TestWaitForSubagents_FollowUpOnlyOnFinished(t *testing.T) {
	// When the max wait is reached the outcome should NOT be subagentFinished,
	// so the caller will not issue a follow-up turn prompt.
	rt := &subagentProbeTestRuntime{
		execFunc: func(_ context.Context, _ specs.Process) error {
			return nil // always report processes
		},
	}
	w := newTestWorkerForSubagents(rt)
	task := types.RunExecution{ExternalId: "test-no-followup"}

	outcome := w.waitForSubagentsWithTiming(
		context.Background(), task, "sandbox-1", nil,
		10*time.Millisecond, 40*time.Millisecond, 50*time.Millisecond,
	)
	if outcome == subagentFinished {
		t.Fatalf("expected non-finished outcome on max wait, got %s", outcome)
	}
}

func TestWaitForSubagents_SignalsActivityWhilePolling(t *testing.T) {
	var calls atomic.Int32
	rt := &subagentProbeTestRuntime{
		execFunc: func(_ context.Context, _ specs.Process) error {
			if calls.Add(1) < 3 {
				return nil
			}
			return fmt.Errorf("exit status 1")
		},
	}
	w := newTestWorkerForSubagents(rt)
	task := types.RunExecution{ExternalId: "test-activity"}
	activityCh := make(chan struct{}, 8)

	outcome := w.waitForSubagentsWithTiming(
		context.Background(), task, "sandbox-1", activityCh,
		10*time.Millisecond, 5*time.Second, 50*time.Millisecond,
	)
	if outcome != subagentFinished {
		t.Fatalf("expected subagentFinished, got %s", outcome)
	}
	if len(activityCh) == 0 {
		t.Fatal("expected subagent polling to signal activity")
	}
}

func TestSubagentProbeArgsExcludeHelperHooks(t *testing.T) {
	if len(subagentProbeArgs) < 3 {
		t.Fatalf("expected shell command args, got %v", subagentProbeArgs)
	}
	script := subagentProbeArgs[2]
	if !strings.Contains(script, ".claude/") {
		t.Fatalf("probe script should filter .claude/ helper hooks, got: %s", script)
	}
	if !strings.Contains(script, "pgrep") {
		t.Fatalf("probe script should use pgrep, got: %s", script)
	}
	if subagentProbeArgs[0] != "/bin/sh" {
		t.Fatalf("probe should run via /bin/sh, got: %s", subagentProbeArgs[0])
	}
}

func TestSubagentWaitOutcomeStringer(t *testing.T) {
	tests := []struct {
		o    subagentWaitOutcome
		want string
	}{
		{subagentNoneDetected, "none_detected"},
		{subagentFinished, "finished"},
		{subagentMaxWaitReached, "max_wait_reached"},
		{subagentSessionCancelled, "session_cancelled"},
		{subagentWaitOutcome(99), "unknown"},
	}
	for _, tt := range tests {
		if got := tt.o.String(); got != tt.want {
			t.Errorf("subagentWaitOutcome(%d).String() = %q, want %q", tt.o, got, tt.want)
		}
	}
}

func TestBuildWakePlannerContextReadsActiveSkillAndHandoffFiles(t *testing.T) {
	mountSource := t.TempDir()
	skillDir := filepath.Join(mountSource, "skills", "prospect-followup")
	if err := os.MkdirAll(skillDir, 0o755); err != nil {
		t.Fatalf("mkdir skill dir: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(mountSource, "notes"), 0o755); err != nil {
		t.Fatalf("mkdir handoff dir: %v", err)
	}

	skillBody := strings.Join([]string{
		"# Prospect follow-up",
		"",
		"Always check replies before sending another email.",
		"Use `notes/next-actions.json` to track what should happen on the next wake.",
	}, "\n")
	if err := os.WriteFile(filepath.Join(skillDir, "SKILL.md"), []byte(skillBody), 0o644); err != nil {
		t.Fatalf("write skill file: %v", err)
	}

	handoffBody := `{"next":"check replies, then draft the second follow-up if nobody responded"}`
	if err := os.WriteFile(filepath.Join(mountSource, "notes", "next-actions.json"), []byte(handoffBody), 0o644); err != nil {
		t.Fatalf("write handoff file: %v", err)
	}

	skillContext, handoffContext := buildWakePlannerContext(mountSource, map[string]string{
		"AIRSTORE_AGENT_SYSTEM_PROMPT": strings.Join([]string{
			"## MANDATORY - Active Skills",
			"1. cat /workspace/skills/prospect-followup/SKILL.md",
		}, "\n"),
	})

	if !strings.Contains(skillContext, "Always check replies before sending another email.") {
		t.Fatalf("expected skill context to include skill file contents, got:\n%s", skillContext)
	}
	if !strings.Contains(handoffContext, handoffBody) {
		t.Fatalf("expected handoff context to include referenced file contents, got:\n%s", handoffContext)
	}
}

