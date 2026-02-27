package worker

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/beam-cloud/airstore/pkg/types"
)

func TestReportTaskResultWithRetry_RetriesThenSucceeds(t *testing.T) {
	task := types.RunExecution{ExternalId: "task-123"}
	result := &types.RunExecutionResult{ID: "task-123", ExitCode: 0}

	var attempts int
	var sleeps []time.Duration

	err := reportTaskResultWithRetry(
		task,
		result,
		func(ctx context.Context, taskID string, exitCode int, errorMsg string) error {
			attempts++
			if taskID != task.ExternalId {
				t.Fatalf("unexpected task id: got %q want %q", taskID, task.ExternalId)
			}
			if exitCode != result.ExitCode {
				t.Fatalf("unexpected exit code: got %d want %d", exitCode, result.ExitCode)
			}
			select {
			case <-ctx.Done():
				t.Fatal("report context should not be cancelled before request")
			default:
			}
			if _, ok := ctx.Deadline(); !ok {
				t.Fatal("expected report context to have timeout")
			}

			if attempts < 3 {
				return errors.New("transient gateway error")
			}
			return nil
		},
		func(d time.Duration) {
			sleeps = append(sleeps, d)
		},
	)
	if err != nil {
		t.Fatalf("expected success after retries, got error: %v", err)
	}

	if attempts != 3 {
		t.Fatalf("expected 3 attempts, got %d", attempts)
	}
	if len(sleeps) != 2 {
		t.Fatalf("expected 2 backoff sleeps, got %d", len(sleeps))
	}
	if sleeps[0] != time.Second || sleeps[1] != 2*time.Second {
		t.Fatalf("unexpected backoff schedule: %v", sleeps)
	}
}

func TestReportTaskResultWithRetry_ExhaustsRetries(t *testing.T) {
	task := types.RunExecution{ExternalId: "task-err"}
	result := &types.RunExecutionResult{ID: "task-err", ExitCode: -1, Error: "boom"}

	var attempts int
	var sleeps []time.Duration
	terminalErr := errors.New("gateway unavailable")

	err := reportTaskResultWithRetry(
		task,
		result,
		func(ctx context.Context, taskID string, exitCode int, errorMsg string) error {
			attempts++
			if taskID != task.ExternalId {
				t.Fatalf("unexpected task id: got %q want %q", taskID, task.ExternalId)
			}
			return terminalErr
		},
		func(d time.Duration) {
			sleeps = append(sleeps, d)
		},
	)
	if !errors.Is(err, terminalErr) {
		t.Fatalf("expected terminal error, got %v", err)
	}

	if attempts != finishTaskResultMaxAttempts {
		t.Fatalf("expected %d attempts, got %d", finishTaskResultMaxAttempts, attempts)
	}
	if len(sleeps) != finishTaskResultMaxAttempts-1 {
		t.Fatalf("expected %d backoff sleeps, got %d", finishTaskResultMaxAttempts-1, len(sleeps))
	}
	if sleeps[0] != time.Second || sleeps[1] != 2*time.Second {
		t.Fatalf("unexpected backoff schedule: %v", sleeps)
	}
}
