package orchestration

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/beam-cloud/airstore/pkg/repository"
	"github.com/beam-cloud/airstore/pkg/types"
)

type attemptProvisionBackend struct {
	repository.BackendRepository
	attemptResultStatus types.AgentAttemptStatus
	attemptResultError  string
	attemptResultCalled bool
	snapshots           []*types.AgentRunSnapshot
}

func (b *attemptProvisionBackend) ListAgentRunAttempts(_ context.Context, _ string) ([]*types.AgentRunAttempt, error) {
	return nil, nil
}

func (b *attemptProvisionBackend) CreateAgentRunAttempt(_ context.Context, attempt *types.AgentRunAttempt) error {
	attempt.ID = "attempt-1"
	return nil
}

func (b *attemptProvisionBackend) EnsureWorkspaceServiceToken(_ context.Context, _ uint) (*types.Token, string, error) {
	return nil, "member-token", nil
}

func (b *attemptProvisionBackend) CreateRunExecution(_ context.Context, task *types.RunExecution) error {
	task.ExternalId = "run-1"
	return nil
}

func (b *attemptProvisionBackend) BindAttemptExecutionTask(_ context.Context, _, _ string) error {
	return nil
}

func (b *attemptProvisionBackend) ListChannelBindings(_ context.Context, _ uint, _ *string) ([]*types.ChannelBinding, error) {
	return nil, nil
}

func (b *attemptProvisionBackend) UpdateAgentRunAttemptResult(
	_ context.Context,
	_ string,
	status types.AgentAttemptStatus,
	_ *int,
	_ time.Time,
	errorMsg *string,
) error {
	b.attemptResultCalled = true
	b.attemptResultStatus = status
	if errorMsg != nil {
		b.attemptResultError = *errorMsg
	}
	return nil
}

func (b *attemptProvisionBackend) IncrementAgentRunSnapshotSeq(_ context.Context, _ string) (int64, error) {
	return int64(len(b.snapshots) + 1), nil
}

func (b *attemptProvisionBackend) AppendAgentRunSnapshot(_ context.Context, snap *types.AgentRunSnapshot) error {
	copied := *snap
	copied.PayloadJSON = cloneAnyMap(snap.PayloadJSON)
	b.snapshots = append(b.snapshots, &copied)
	return nil
}

type failingPushQueue struct {
	repository.TaskQueue
	pushErr error
}

func (q *failingPushQueue) Push(_ context.Context, _ *types.RunExecution) error {
	return q.pushErr
}

type blockingResumeTerminalIO struct {
	repository.TerminalIORepository
	blockLeaseOwner bool
	blockCheckpoint bool
	checkpointRunID string
}

func (t *blockingResumeTerminalIO) GetSessionLeaseOwner(ctx context.Context, _ uint, _ string) (string, error) {
	if !t.blockLeaseOwner {
		return "", nil
	}
	<-ctx.Done()
	return "", ctx.Err()
}

func (t *blockingResumeTerminalIO) GetSessionCheckpoint(ctx context.Context, _ uint, _ string) (*types.SessionCheckpoint, error) {
	if t.blockCheckpoint {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	if t.checkpointRunID == "" {
		return nil, nil
	}
	return &types.SessionCheckpoint{RunID: t.checkpointRunID}, nil
}

func TestCreateAttemptExecutionTaskMarksProvisioningFailuresAsErrored(t *testing.T) {
	backend := &attemptProvisionBackend{}
	queue := &failingPushQueue{pushErr: errors.New("failed to push task: temporary queue outage")}
	factory := NewRunFactory(RunFactoryConfig{
		Backend:      backend,
		TaskQueue:    queue,
		DefaultImage: "sandbox:latest",
	})
	run := &types.AgentRun{
		ID:              "run-1",
		WorkspaceID:     7,
		OriginTaskID:    "task-1",
		ExecHost:        "local",
		ExecSecurity:    "workspace_write",
		ExecAsk:         string(ExecAskOff),
		RuntimeType:     "sandbox",
		WorkspaceAccess: "write",
		Interactive:     true,
		TimeoutMs:       30_000,
	}

	_, err := factory.CreateAttemptExecutionTask(
		context.Background(),
		run,
		RunExecutionPolicy{},
		"do the thing",
		map[string]any{},
		map[string]any{},
	)
	if err == nil {
		t.Fatal("expected CreateAttemptExecutionTask to return the push error")
	}
	if !backend.attemptResultCalled {
		t.Fatal("expected failed provisioning to mark the attempt result")
	}
	if backend.attemptResultStatus != types.AgentAttemptStatusError {
		t.Fatalf("attempt result status = %q, want %q", backend.attemptResultStatus, types.AgentAttemptStatusError)
	}
	if backend.attemptResultError == "" {
		t.Fatal("expected provisioning failure error text to be recorded")
	}
	if len(backend.snapshots) != 1 {
		t.Fatalf("snapshot count = %d, want 1", len(backend.snapshots))
	}
	if backend.snapshots[0].Status != types.AgentRunStatusError {
		t.Fatalf("snapshot status = %q, want %q", backend.snapshots[0].Status, types.AgentRunStatusError)
	}
}

func TestResumeBarrierTimesOutBlockingLeaseProbe(t *testing.T) {
	prevTimeout := sessionStateCallTimeout
	sessionStateCallTimeout = 20 * time.Millisecond
	defer func() { sessionStateCallTimeout = prevTimeout }()

	barrier := NewResumeBarrier(nil, &blockingResumeTerminalIO{blockLeaseOwner: true})
	start := time.Now()
	err := barrier.WaitForResume(context.Background(), 7, "session-1", "run-1")
	if err == nil {
		t.Fatal("expected resume barrier to fail when lease probe blocks")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected deadline exceeded, got %v", err)
	}
	if elapsed := time.Since(start); elapsed > 250*time.Millisecond {
		t.Fatalf("lease probe timeout took too long: %s", elapsed)
	}
}

func TestResumeBarrierTimesOutBlockingCheckpointProbe(t *testing.T) {
	prevTimeout := sessionStateCallTimeout
	sessionStateCallTimeout = 20 * time.Millisecond
	defer func() { sessionStateCallTimeout = prevTimeout }()

	barrier := NewResumeBarrier(nil, &blockingResumeTerminalIO{
		blockCheckpoint: true,
	})
	start := time.Now()
	err := barrier.WaitForResume(context.Background(), 7, "session-1", "run-1")
	if err == nil {
		t.Fatal("expected resume barrier to fail when checkpoint probe blocks")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected deadline exceeded, got %v", err)
	}
	if elapsed := time.Since(start); elapsed > 250*time.Millisecond {
		t.Fatalf("checkpoint probe timeout took too long: %s", elapsed)
	}
}
