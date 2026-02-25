package worker

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/beam-cloud/airstore/pkg/types"
)

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
