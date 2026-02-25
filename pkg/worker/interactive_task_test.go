package worker

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/beam-cloud/airstore/pkg/types"
)

type testTerminalIO struct {
	inputCh chan []byte
}

func (tio *testTerminalIO) PublishInput(_ context.Context, _ string, _ []byte) error {
	return nil
}

func (tio *testTerminalIO) SubscribeInput(_ context.Context, _ string) (<-chan []byte, func(), error) {
	if tio.inputCh == nil {
		tio.inputCh = make(chan []byte)
	}
	return tio.inputCh, func() {}, nil
}

func (tio *testTerminalIO) PublishOutput(_ context.Context, _ string, _ []byte) error {
	return nil
}

func (tio *testTerminalIO) SubscribeOutput(_ context.Context, _ string) (<-chan []byte, func(), error) {
	ch := make(chan []byte)
	close(ch)
	return ch, func() {}, nil
}

func (tio *testTerminalIO) PublishCancel(_ context.Context, _ string) error {
	return nil
}

func (tio *testTerminalIO) SubscribeCancel(_ context.Context, _ string) (<-chan struct{}, func(), error) {
	ch := make(chan struct{})
	close(ch)
	return ch, func() {}, nil
}

func TestInteractiveResult(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		exitCode, errMsg, status := interactiveResult(nil, false)
		if exitCode != 0 || errMsg != "" || status != types.RunExecutionStatusComplete {
			t.Fatalf("unexpected result: exit=%d err=%q status=%s", exitCode, errMsg, status)
		}
	})

	t.Run("cancelled", func(t *testing.T) {
		exitCode, errMsg, status := interactiveResult(context.Canceled, false)
		if exitCode != -1 || errMsg == "" || status != types.RunExecutionStatusCancelled {
			t.Fatalf("unexpected cancelled result: exit=%d err=%q status=%s", exitCode, errMsg, status)
		}
	})

	t.Run("idle timeout completes task", func(t *testing.T) {
		exitCode, errMsg, status := interactiveResult(context.Canceled, true)
		if exitCode != 0 || errMsg != "" || status != types.RunExecutionStatusComplete {
			t.Fatalf("unexpected idle-timeout result: exit=%d err=%q status=%s", exitCode, errMsg, status)
		}
	})

	t.Run("failed", func(t *testing.T) {
		exitCode, errMsg, status := interactiveResult(errors.New("boom"), false)
		if exitCode != -1 || errMsg != "boom" || status != types.RunExecutionStatusFailed {
			t.Fatalf("unexpected failed result: exit=%d err=%q status=%s", exitCode, errMsg, status)
		}
	})
}

func TestMonitorInteractiveSessionIdleTimeout(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var idleTimedOut atomic.Bool
	activityCh := make(chan struct{}, 1)
	done := make(chan struct{})

	go func() {
		monitorInteractiveSessionIdle(ctx, "task-1", taskExecutionContext{}, cancel, 20*time.Millisecond, activityCh, &idleTimedOut)
		close(done)
	}()

	select {
	case <-ctx.Done():
	case <-time.After(200 * time.Millisecond):
		t.Fatal("expected context cancellation from idle timeout")
	}

	if !idleTimedOut.Load() {
		t.Fatal("expected idle timeout to be recorded")
	}

	select {
	case <-done:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("idle monitor goroutine did not exit")
	}
}

func TestMonitorInteractiveSessionIdleResetOnActivity(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var idleTimedOut atomic.Bool
	activityCh := make(chan struct{}, 1)
	done := make(chan struct{})

	go func() {
		monitorInteractiveSessionIdle(ctx, "task-2", taskExecutionContext{}, cancel, 60*time.Millisecond, activityCh, &idleTimedOut)
		close(done)
	}()

	time.Sleep(30 * time.Millisecond)
	signalActivity(activityCh)

	select {
	case <-ctx.Done():
		t.Fatal("session cancelled before reset timeout elapsed")
	case <-time.After(40 * time.Millisecond):
	}

	select {
	case <-ctx.Done():
	case <-time.After(200 * time.Millisecond):
		t.Fatal("expected cancellation after post-activity idle period")
	}

	if !idleTimedOut.Load() {
		t.Fatal("expected idle timeout to be recorded after reset period")
	}

	select {
	case <-done:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("idle monitor goroutine did not exit")
	}
}

func TestWaitForFollowupInputTimesOut(t *testing.T) {
	terminalIO := &testTerminalIO{inputCh: make(chan []byte)}
	worker := &Worker{terminalIO: terminalIO}

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	started := time.Now()
	prompt := worker.waitForFollowupInput(ctx, "task-1", 25*time.Millisecond)
	elapsed := time.Since(started)

	if prompt != "" {
		t.Fatalf("expected empty prompt on timeout, got %q", prompt)
	}
	if elapsed > 200*time.Millisecond {
		t.Fatalf("expected between-turn timeout to end quickly, elapsed=%s", elapsed)
	}
}

func TestWaitForFollowupInputReturnsPrompt(t *testing.T) {
	terminalIO := &testTerminalIO{inputCh: make(chan []byte, 1)}
	worker := &Worker{terminalIO: terminalIO}

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	go func() {
		time.Sleep(10 * time.Millisecond)
		terminalIO.inputCh <- []byte("  follow up  ")
	}()

	prompt := worker.waitForFollowupInput(ctx, "task-2", 200*time.Millisecond)
	if prompt != "follow up" {
		t.Fatalf("expected trimmed prompt, got %q", prompt)
	}
}

func TestShouldContinueFromFirstTurn(t *testing.T) {
	tests := []struct {
		name string
		env  map[string]string
		want bool
	}{
		{
			name: "missing env",
			env:  map[string]string{},
			want: false,
		},
		{
			name: "explicit false",
			env: map[string]string{
				agentResumeSessionEnvKey: "false",
			},
			want: false,
		},
		{
			name: "explicit true",
			env: map[string]string{
				agentResumeSessionEnvKey: "true",
			},
			want: true,
		},
		{
			name: "numeric true",
			env: map[string]string{
				agentResumeSessionEnvKey: "1",
			},
			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := shouldContinueFromFirstTurn(tt.env)
			if got != tt.want {
				t.Fatalf("unexpected continue flag: got=%t want=%t", got, tt.want)
			}
		})
	}
}
