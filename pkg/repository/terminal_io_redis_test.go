package repository

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/beam-cloud/airstore/pkg/types"
)

func TestRedisTerminalIORepository_PublishSubscribeOutput(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	if err != nil {
		t.Fatalf("new redis test client: %v", err)
	}

	repo := NewRedisTerminalIORepository(rdb)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const taskID = "task-output-123"

	outputCh, outputCleanup, err := repo.SubscribeOutput(ctx, taskID)
	if err != nil {
		t.Fatalf("subscribe output: %v", err)
	}
	defer outputCleanup()

	wantOutput := []byte("hello from shell")
	if err := repo.PublishOutput(ctx, taskID, wantOutput); err != nil {
		t.Fatalf("publish output: %v", err)
	}

	gotOutput, err := waitFor[[]byte](outputCh)
	if err != nil {
		t.Fatalf("wait output: %v", err)
	}
	if string(gotOutput) != string(wantOutput) {
		t.Fatalf("output mismatch: got %q want %q", gotOutput, wantOutput)
	}
}

func TestInputWakeRoundTrip(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	if err != nil {
		t.Fatalf("new redis test client: %v", err)
	}

	repo := NewRedisTerminalIORepository(rdb)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const taskID = "task-wake-123"

	wakeCh, cleanup, err := repo.SubscribeInputWake(ctx, taskID)
	if err != nil {
		t.Fatalf("subscribe input wake: %v", err)
	}
	defer cleanup()

	if err := repo.PublishInputWake(ctx, taskID); err != nil {
		t.Fatalf("publish input wake: %v", err)
	}

	if _, err := waitFor[struct{}](wakeCh); err != nil {
		t.Fatalf("wait wake: %v", err)
	}
}

func TestSessionCheckpointRoundTrip(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	if err != nil {
		t.Fatalf("new redis test client: %v", err)
	}

	repo := NewRedisTerminalIORepository(rdb)
	ctx := context.Background()

	const workspaceID uint = 7
	const sessionID = "session-checkpoint-test"
	want := &types.SessionCheckpoint{
		RunID:       "run-123",
		ExecutionID: "exec-123",
		UpdatedAt:   time.Now().UnixMilli(),
	}

	if err := repo.SetSessionCheckpoint(ctx, workspaceID, sessionID, want, time.Minute); err != nil {
		t.Fatalf("set checkpoint: %v", err)
	}

	got, err := repo.GetSessionCheckpoint(ctx, workspaceID, sessionID)
	if err != nil {
		t.Fatalf("get checkpoint: %v", err)
	}
	if got == nil {
		t.Fatal("expected checkpoint to round trip")
	}
	if got.RunID != want.RunID || got.ExecutionID != want.ExecutionID || got.UpdatedAt != want.UpdatedAt {
		t.Fatalf("checkpoint mismatch: got %#v want %#v", got, want)
	}
}

func waitFor[T any](ch <-chan T) (T, error) {
	var zero T
	select {
	case v, ok := <-ch:
		if !ok {
			return zero, errors.New("channel closed")
		}
		return v, nil
	case <-time.After(2 * time.Second):
		return zero, errors.New("timeout")
	}
}

func TestSessionLeaseAcquireReleaseRoundTrip(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	if err != nil {
		t.Fatalf("new redis test client: %v", err)
	}

	repo := NewRedisTerminalIORepository(rdb)
	ctx := context.Background()

	const workspaceID uint = 42
	const sessionID = "session-lease-test"
	const ownerA = "task-aaa"
	const ownerB = "task-bbb"

	acquired, err := repo.AcquireSessionLease(ctx, workspaceID, sessionID, ownerA, 10*time.Second)
	if err != nil {
		t.Fatalf("acquire A: %v", err)
	}
	if !acquired {
		t.Fatal("expected acquire A to succeed")
	}

	acquired, err = repo.AcquireSessionLease(ctx, workspaceID, sessionID, ownerA, 10*time.Second)
	if err != nil {
		t.Fatalf("re-acquire A: %v", err)
	}
	if !acquired {
		t.Fatal("expected idempotent re-acquire A to succeed")
	}

	acquired, err = repo.AcquireSessionLease(ctx, workspaceID, sessionID, ownerB, 10*time.Second)
	if err != nil {
		t.Fatalf("acquire B: %v", err)
	}
	if acquired {
		t.Fatal("expected B acquire to fail while A holds lease")
	}

	owner, err := repo.GetSessionLeaseOwner(ctx, workspaceID, sessionID)
	if err != nil {
		t.Fatalf("get owner: %v", err)
	}
	if owner != ownerA {
		t.Fatalf("expected owner %q, got %q", ownerA, owner)
	}

	renewed, err := repo.RenewSessionLease(ctx, workspaceID, sessionID, ownerA, 10*time.Second)
	if err != nil {
		t.Fatalf("renew A: %v", err)
	}
	if !renewed {
		t.Fatal("expected renew by A to succeed")
	}

	renewed, err = repo.RenewSessionLease(ctx, workspaceID, sessionID, ownerB, 10*time.Second)
	if err != nil {
		t.Fatalf("renew B: %v", err)
	}
	if renewed {
		t.Fatal("expected renew by B to fail")
	}

	if err := repo.ReleaseSessionLease(ctx, workspaceID, sessionID, ownerB); err != nil {
		t.Fatalf("release B: %v", err)
	}
	owner, _ = repo.GetSessionLeaseOwner(ctx, workspaceID, sessionID)
	if owner != ownerA {
		t.Fatalf("expected A still owner after B release, got %q", owner)
	}

	if err := repo.ReleaseSessionLease(ctx, workspaceID, sessionID, ownerA); err != nil {
		t.Fatalf("release A: %v", err)
	}
	owner, _ = repo.GetSessionLeaseOwner(ctx, workspaceID, sessionID)
	if owner != "" {
		t.Fatalf("expected no owner after A release, got %q", owner)
	}

	acquired, err = repo.AcquireSessionLease(ctx, workspaceID, sessionID, ownerB, 10*time.Second)
	if err != nil {
		t.Fatalf("acquire B after release: %v", err)
	}
	if !acquired {
		t.Fatal("expected B acquire to succeed after A release")
	}

	_ = repo.ReleaseSessionLease(ctx, workspaceID, sessionID, ownerB)
}

func TestSessionLeaseRenewFailsAfterRelease(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	if err != nil {
		t.Fatalf("new redis test client: %v", err)
	}

	repo := NewRedisTerminalIORepository(rdb)
	ctx := context.Background()

	const workspaceID uint = 42
	const sessionID = "session-lease-renew-test"
	const ownerA = "task-ccc"

	acquired, err := repo.AcquireSessionLease(ctx, workspaceID, sessionID, ownerA, 10*time.Second)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if !acquired {
		t.Fatal("expected acquire to succeed")
	}

	if err := repo.ReleaseSessionLease(ctx, workspaceID, sessionID, ownerA); err != nil {
		t.Fatalf("release: %v", err)
	}

	renewed, err := repo.RenewSessionLease(ctx, workspaceID, sessionID, ownerA, 10*time.Second)
	if err != nil {
		t.Fatalf("renew after release: %v", err)
	}
	if renewed {
		t.Fatal("expected renew to fail after release")
	}

	owner, _ := repo.GetSessionLeaseOwner(ctx, workspaceID, sessionID)
	if owner != "" {
		t.Fatalf("expected no owner after release, got %q", owner)
	}
}

func TestRunInteractionSetGetRoundTrip(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	if err != nil {
		t.Fatalf("new redis test client: %v", err)
	}

	repo := NewRedisTerminalIORepository(rdb)
	ctx := context.Background()

	const workspaceID uint = 42
	const runID = "run-interaction-test"
	const executionID = "exec-interaction-test"

	if err := repo.SetRunInteraction(
		ctx,
		workspaceID,
		runID,
		types.RunInteraction{
			State:             types.RunInteractionStateWaitingForInput,
			ActiveExecutionID: executionID,
		},
		time.Minute,
	); err != nil {
		t.Fatalf("set run interaction: %v", err)
	}

	interaction, err := repo.GetRunInteraction(ctx, workspaceID, runID)
	if err != nil {
		t.Fatalf("get run interaction: %v", err)
	}
	if interaction == nil {
		t.Fatal("expected interaction payload, got nil")
	}
	if interaction.State != types.RunInteractionStateWaitingForInput {
		t.Fatalf("unexpected interaction state: got %q", interaction.State)
	}
	if interaction.ActiveExecutionID != executionID {
		t.Fatalf("unexpected active execution id: got %q want %q", interaction.ActiveExecutionID, executionID)
	}
	if interaction.UpdatedAt <= 0 {
		t.Fatalf("expected updated_at to be set, got %d", interaction.UpdatedAt)
	}
}
