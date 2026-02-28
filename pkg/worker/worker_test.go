package worker

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/beam-cloud/airstore/pkg/types"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestSetTaskResultWithRetry_SucceedsAfterTransientFailures(t *testing.T) {
	task := types.RunExecution{ExternalId: "task-123"}
	result := &types.RunExecutionResult{ID: "task-123", ExitCode: 0}

	var attempts int
	err := setTaskResultWithRetry(context.Background(), task, result,
		func(ctx context.Context, _ string, _ int, _ string, _ string) error {
			attempts++
			if _, ok := ctx.Deadline(); !ok {
				t.Fatal("expected context with deadline")
			}
			if attempts < setTaskResultMaxAttempts {
				return errors.New("transient")
			}
			return nil
		},
		func(context.Context, time.Duration) {},
	)
	if err != nil {
		t.Fatalf("expected nil error after retries, got: %v", err)
	}
	if attempts != setTaskResultMaxAttempts {
		t.Fatalf("expected %d attempts, got %d", setTaskResultMaxAttempts, attempts)
	}
}

func TestSetTaskResultWithRetry_ReturnsLastErrorWhenExhausted(t *testing.T) {
	task := types.RunExecution{ExternalId: "task-err"}
	result := &types.RunExecutionResult{ID: "task-err", ExitCode: -1, Error: "boom"}

	permanent := errors.New("gateway down")
	var attempts int
	err := setTaskResultWithRetry(context.Background(), task, result,
		func(context.Context, string, int, string, string) error { attempts++; return permanent },
		func(context.Context, time.Duration) {},
	)
	if !errors.Is(err, permanent) {
		t.Fatalf("expected permanent error, got: %v", err)
	}
	if attempts != setTaskResultMaxAttempts {
		t.Fatalf("expected %d attempts, got %d", setTaskResultMaxAttempts, attempts)
	}
}

func TestSetTaskResultWithRetry_BackoffSchedule(t *testing.T) {
	task := types.RunExecution{ExternalId: "task-bo"}
	result := &types.RunExecutionResult{ID: "task-bo", ExitCode: 0}

	var sleeps []time.Duration
	_ = setTaskResultWithRetry(context.Background(), task, result,
		func(context.Context, string, int, string, string) error { return errors.New("fail") },
		func(_ context.Context, d time.Duration) { sleeps = append(sleeps, d) },
	)
	want := []time.Duration{1 * time.Second, 2 * time.Second}
	if len(sleeps) != len(want) {
		t.Fatalf("expected %d sleeps, got %d", len(want), len(sleeps))
	}
	for i, d := range want {
		if sleeps[i] != d {
			t.Fatalf("sleep[%d]: want %v, got %v", i, d, sleeps[i])
		}
	}
}

func TestSetTaskResultWithRetry_StopsOnContextCancel(t *testing.T) {
	task := types.RunExecution{ExternalId: "task-cancel"}
	result := &types.RunExecutionResult{ID: "task-cancel", ExitCode: 0}

	ctx, cancel := context.WithCancel(context.Background())

	var attempts int
	transient := errors.New("transient")
	err := setTaskResultWithRetry(ctx, task, result,
		func(context.Context, string, int, string, string) error {
			attempts++
			return transient
		},
		func(_ context.Context, _ time.Duration) {
			cancel() // simulate shutdown during backoff
		},
	)
	if attempts != 1 {
		t.Fatalf("expected 1 attempt before cancellation, got %d", attempts)
	}
	if !errors.Is(err, transient) {
		t.Fatalf("expected last transient error, got: %v", err)
	}
}

func TestSetTaskResultWithRetry_DoesNotRetryOnNotFound(t *testing.T) {
	task := types.RunExecution{ExternalId: "task-not-found"}
	result := &types.RunExecutionResult{ID: "task-not-found", ExitCode: -1, Error: "boom"}

	notFoundErr := fmt.Errorf("set task result failed: %w", status.Error(codes.NotFound, "task not found: task-not-found"))
	var attempts int
	err := setTaskResultWithRetry(
		context.Background(),
		task,
		result,
		func(context.Context, string, int, string, string) error {
			attempts++
			return notFoundErr
		},
		func(context.Context, time.Duration) {},
	)
	if !errors.Is(err, notFoundErr) {
		t.Fatalf("expected not found error, got: %v", err)
	}
	if attempts != 1 {
		t.Fatalf("expected 1 attempt for non-retriable error, got %d", attempts)
	}
}

func TestSubscribeTaskCancellationCancelsNonInteractiveTaskContext(t *testing.T) {
	terminalIO := &testTerminalIO{cancelCh: make(chan struct{}, 1)}
	worker := &Worker{terminalIO: terminalIO}

	taskCtx, taskCancel := context.WithCancel(context.Background())
	defer taskCancel()

	task := types.RunExecution{ExternalId: "task-cancel"}
	cleanup := worker.subscribeTaskCancellation(taskCtx, task, taskCancel)
	defer cleanup()

	terminalIO.cancelCh <- struct{}{}

	select {
	case <-taskCtx.Done():
	case <-time.After(250 * time.Millisecond):
		t.Fatal("expected task context to be cancelled after cancel signal")
	}
}

func TestSubscribeTaskCancellationSkipsInteractiveTasks(t *testing.T) {
	terminalIO := &testTerminalIO{cancelCh: make(chan struct{}, 1)}
	worker := &Worker{terminalIO: terminalIO}

	taskCtx, taskCancel := context.WithCancel(context.Background())
	defer taskCancel()

	task := types.RunExecution{
		ExternalId: "task-interactive",
		Type:       types.RunExecutionTypeInteractive,
	}
	cleanup := worker.subscribeTaskCancellation(taskCtx, task, taskCancel)
	defer cleanup()

	terminalIO.cancelCh <- struct{}{}

	select {
	case <-taskCtx.Done():
		t.Fatal("did not expect interactive task context to be cancelled by non-interactive subscription path")
	case <-time.After(75 * time.Millisecond):
	}
}
