package repository

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/beam-cloud/airstore/pkg/types"
)

func TestRedisTerminalIORepository_PublishSubscribe(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	if err != nil {
		t.Fatalf("new redis test client: %v", err)
	}

	repo := NewRedisTerminalIORepository(rdb)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const taskID = "task-123"

	inputCh, inputCleanup, err := repo.SubscribeInput(ctx, taskID)
	if err != nil {
		t.Fatalf("subscribe input: %v", err)
	}
	defer inputCleanup()

	outputCh, outputCleanup, err := repo.SubscribeOutput(ctx, taskID)
	if err != nil {
		t.Fatalf("subscribe output: %v", err)
	}
	defer outputCleanup()

	wantInput := []byte("echo hi\n")
	if err := repo.PublishInput(ctx, taskID, wantInput); err != nil {
		t.Fatalf("publish input: %v", err)
	}

	gotInput, err := waitFor[[]byte](inputCh)
	if err != nil {
		t.Fatalf("wait input: %v", err)
	}
	if string(gotInput) != string(wantInput) {
		t.Fatalf("input mismatch: got %q want %q", gotInput, wantInput)
	}

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

func TestRedisTerminalIORepository_InputPublishedBeforeSubscribeIsBuffered(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	if err != nil {
		t.Fatalf("new redis test client: %v", err)
	}

	repo := NewRedisTerminalIORepository(rdb)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const taskID = "task-buffered-123"
	want := []byte("buffered follow-up\n")
	if err := repo.PublishInput(ctx, taskID, want); err != nil {
		t.Fatalf("publish input: %v", err)
	}

	inputCh, inputCleanup, err := repo.SubscribeInput(ctx, taskID)
	if err != nil {
		t.Fatalf("subscribe input: %v", err)
	}
	defer inputCleanup()

	got, err := waitFor[[]byte](inputCh)
	if err != nil {
		t.Fatalf("wait input: %v", err)
	}
	if string(got) != string(want) {
		t.Fatalf("input mismatch: got %q want %q", got, want)
	}
}

func TestRedisTerminalIORepository_MultipleBufferedInputsAreDrainedInOrder(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	if err != nil {
		t.Fatalf("new redis test client: %v", err)
	}

	repo := NewRedisTerminalIORepository(rdb)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const taskID = "task-buffered-multi-123"
	first := []byte("first queued input\n")
	second := []byte("second queued input\n")
	if err := repo.PublishInput(ctx, taskID, first); err != nil {
		t.Fatalf("publish first input: %v", err)
	}
	if err := repo.PublishInput(ctx, taskID, second); err != nil {
		t.Fatalf("publish second input: %v", err)
	}

	inputCh, cleanup, err := repo.SubscribeInput(ctx, taskID)
	if err != nil {
		t.Fatalf("subscribe input: %v", err)
	}
	defer cleanup()

	gotFirst, err := waitFor[[]byte](inputCh)
	if err != nil {
		t.Fatalf("wait first input: %v", err)
	}
	if string(gotFirst) != string(first) {
		t.Fatalf("first input mismatch: got %q want %q", gotFirst, first)
	}

	gotSecond, err := waitFor[[]byte](inputCh)
	if err != nil {
		t.Fatalf("wait second input: %v", err)
	}
	if string(gotSecond) != string(second) {
		t.Fatalf("second input mismatch: got %q want %q", gotSecond, second)
	}
}

func TestRedisTerminalIORepository_ListPendingInputs(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	if err != nil {
		t.Fatalf("new redis test client: %v", err)
	}

	repo := NewRedisTerminalIORepository(rdb)
	ctx := context.Background()

	const taskID = "task-pending-list-123"

	pending, err := repo.ListPendingInputs(ctx, taskID)
	if err != nil {
		t.Fatalf("list empty pending: %v", err)
	}
	if len(pending) != 0 {
		t.Fatalf("expected 0 pending, got %d", len(pending))
	}

	if err := repo.PublishInput(ctx, taskID, []byte("first\n")); err != nil {
		t.Fatalf("publish first: %v", err)
	}
	if err := repo.PublishInput(ctx, taskID, []byte("second\n")); err != nil {
		t.Fatalf("publish second: %v", err)
	}

	pending, err = repo.ListPendingInputs(ctx, taskID)
	if err != nil {
		t.Fatalf("list pending: %v", err)
	}
	if len(pending) != 2 {
		t.Fatalf("expected 2 pending, got %d", len(pending))
	}
	if pending[0].Message != "first\n" {
		t.Fatalf("pending[0].Message = %q, want %q", pending[0].Message, "first\n")
	}
	if pending[1].Message != "second\n" {
		t.Fatalf("pending[1].Message = %q, want %q", pending[1].Message, "second\n")
	}
	if pending[0].ID == "" || pending[1].ID == "" {
		t.Fatal("pending entries should have non-empty IDs")
	}
	if pending[0].ID == pending[1].ID {
		t.Fatal("pending entries should have distinct IDs")
	}

	// After consuming via SubscribeInput, ListPendingInputs should shrink.
	inputCh, cleanup, err := repo.SubscribeInput(ctx, taskID)
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	if _, err := waitFor[[]byte](inputCh); err != nil {
		t.Fatalf("consume first: %v", err)
	}
	cleanup()

	pending, err = repo.ListPendingInputs(ctx, taskID)
	if err != nil {
		t.Fatalf("list after consume: %v", err)
	}
	if len(pending) != 0 {
		t.Fatalf("expected buffered inputs to be fully drained after subscribe, got %d", len(pending))
	}
}

func TestExtractMessage_StructuredAndLegacy(t *testing.T) {
	structured := []byte(`{"id":"abc","message":"hello\n","created_at":1234}`)
	got := extractMessage(structured)
	if string(got) != "hello\n" {
		t.Fatalf("structured: got %q, want %q", got, "hello\n")
	}

	legacy := []byte("raw input\n")
	got = extractMessage(legacy)
	if string(got) != "raw input\n" {
		t.Fatalf("legacy: got %q, want %q", got, "raw input\n")
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

	// Acquire by A.
	acquired, err := repo.AcquireSessionLease(ctx, workspaceID, sessionID, ownerA, 10*time.Second)
	if err != nil {
		t.Fatalf("acquire A: %v", err)
	}
	if !acquired {
		t.Fatal("expected acquire A to succeed")
	}

	// A can re-acquire (idempotent).
	acquired, err = repo.AcquireSessionLease(ctx, workspaceID, sessionID, ownerA, 10*time.Second)
	if err != nil {
		t.Fatalf("re-acquire A: %v", err)
	}
	if !acquired {
		t.Fatal("expected idempotent re-acquire A to succeed")
	}

	// B cannot acquire while A holds the lease.
	acquired, err = repo.AcquireSessionLease(ctx, workspaceID, sessionID, ownerB, 10*time.Second)
	if err != nil {
		t.Fatalf("acquire B: %v", err)
	}
	if acquired {
		t.Fatal("expected B acquire to fail while A holds lease")
	}

	// Check owner.
	owner, err := repo.GetSessionLeaseOwner(ctx, workspaceID, sessionID)
	if err != nil {
		t.Fatalf("get owner: %v", err)
	}
	if owner != ownerA {
		t.Fatalf("expected owner %q, got %q", ownerA, owner)
	}

	// Renew by A succeeds.
	renewed, err := repo.RenewSessionLease(ctx, workspaceID, sessionID, ownerA, 10*time.Second)
	if err != nil {
		t.Fatalf("renew A: %v", err)
	}
	if !renewed {
		t.Fatal("expected renew by A to succeed")
	}

	// Renew by B fails.
	renewed, err = repo.RenewSessionLease(ctx, workspaceID, sessionID, ownerB, 10*time.Second)
	if err != nil {
		t.Fatalf("renew B: %v", err)
	}
	if renewed {
		t.Fatal("expected renew by B to fail")
	}

	// Release by B is a no-op.
	if err := repo.ReleaseSessionLease(ctx, workspaceID, sessionID, ownerB); err != nil {
		t.Fatalf("release B: %v", err)
	}
	owner, _ = repo.GetSessionLeaseOwner(ctx, workspaceID, sessionID)
	if owner != ownerA {
		t.Fatalf("expected A still owner after B release, got %q", owner)
	}

	// Release by A.
	if err := repo.ReleaseSessionLease(ctx, workspaceID, sessionID, ownerA); err != nil {
		t.Fatalf("release A: %v", err)
	}
	owner, _ = repo.GetSessionLeaseOwner(ctx, workspaceID, sessionID)
	if owner != "" {
		t.Fatalf("expected no owner after A release, got %q", owner)
	}

	// Now B can acquire.
	acquired, err = repo.AcquireSessionLease(ctx, workspaceID, sessionID, ownerB, 10*time.Second)
	if err != nil {
		t.Fatalf("acquire B after release: %v", err)
	}
	if !acquired {
		t.Fatal("expected B acquire to succeed after A release")
	}

	// Cleanup.
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

func TestRunInteractionSetGetClearRoundTrip(t *testing.T) {
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
		types.RunInteractionStateWaitingForInput,
		executionID,
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

	if err := repo.ClearRunInteraction(ctx, workspaceID, runID); err != nil {
		t.Fatalf("clear run interaction: %v", err)
	}
	interaction, err = repo.GetRunInteraction(ctx, workspaceID, runID)
	if err != nil {
		t.Fatalf("get run interaction after clear: %v", err)
	}
	if interaction != nil {
		t.Fatalf("expected no interaction after clear, got %#v", interaction)
	}
}
