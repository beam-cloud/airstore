package worker

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/beam-cloud/airstore/pkg/types"
)

func TestSetTaskResultWithRetry_SucceedsAfterTransientFailures(t *testing.T) {
	task := types.RunExecution{ExternalId: "task-123"}
	result := &types.RunExecutionResult{ID: "task-123", ExitCode: 0}

	var attempts int
	err := setTaskResultWithRetry(task, result,
		func(ctx context.Context, _ string, _ int, _ string) error {
			attempts++
			if _, ok := ctx.Deadline(); !ok {
				t.Fatal("expected context with deadline")
			}
			if attempts < setTaskResultMaxAttempts {
				return errors.New("transient")
			}
			return nil
		},
		func(time.Duration) {},
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
	err := setTaskResultWithRetry(task, result,
		func(context.Context, string, int, string) error { attempts++; return permanent },
		func(time.Duration) {},
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
	_ = setTaskResultWithRetry(task, result,
		func(context.Context, string, int, string) error { return errors.New("fail") },
		func(d time.Duration) { sleeps = append(sleeps, d) },
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
