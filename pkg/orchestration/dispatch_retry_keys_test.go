package orchestration

import (
	"context"
	"testing"
	"time"

	"github.com/beam-cloud/airstore/pkg/repository"
	"github.com/beam-cloud/airstore/pkg/types"
)

type retryGuardBackend struct {
	repository.BackendRepository
	guards           map[string]struct{}
	taskStateUpdates []types.TaskStateUpdate
	outboxEvents     []*types.OrchestrationOutboxEvent
}

func (b *retryGuardBackend) AcquireOrchestrationRetryGuard(_ context.Context, guardKey string) (bool, error) {
	if b.guards == nil {
		b.guards = make(map[string]struct{})
	}
	if _, exists := b.guards[guardKey]; exists {
		return false, nil
	}
	b.guards[guardKey] = struct{}{}
	return true, nil
}

func (b *retryGuardBackend) UpdateTaskState(_ context.Context, update types.TaskStateUpdate) error {
	b.taskStateUpdates = append(b.taskStateUpdates, update)
	return nil
}

func (b *retryGuardBackend) EnqueueOrchestrationOutboxEvent(_ context.Context, event *types.OrchestrationOutboxEvent) error {
	copied := *event
	copied.PayloadJSON = cloneAnyMap(event.PayloadJSON)
	b.outboxEvents = append(b.outboxEvents, &copied)
	return nil
}

func (b *retryGuardBackend) GetRunExecution(context.Context, string) (*types.RunExecution, error) {
	return nil, nil
}

func TestRuntimeScheduleRetryScopesGuardPerDispatchCycle(t *testing.T) {
	backend := &retryGuardBackend{}
	loops := &RuntimeLoops{
		backend:   backend,
		lifecycle: NewTaskLifecycle(backend, nil, nil),
	}

	firstDispatch := time.Unix(1_710_000_000, 0).UTC()
	secondDispatch := firstDispatch.Add(2 * time.Minute)
	task := &types.AgentTask{
		ID:           "task-1",
		WorkspaceID:  347,
		QueuedAt:     &firstDispatch,
		DispatchedAt: &firstDispatch,
	}

	if err := loops.scheduleRetry(context.Background(), task, 0, "dispatch_capacity", 500*time.Millisecond, nil); err != nil {
		t.Fatalf("first scheduleRetry returned error: %v", err)
	}

	task.QueuedAt = &secondDispatch
	task.DispatchedAt = &secondDispatch
	if err := loops.scheduleRetry(context.Background(), task, 0, "dispatch_capacity", 500*time.Millisecond, nil); err != nil {
		t.Fatalf("second scheduleRetry returned error: %v", err)
	}

	if len(backend.taskStateUpdates) != 2 {
		t.Fatalf("task state updates = %d, want 2", len(backend.taskStateUpdates))
	}
	if len(backend.outboxEvents) != 2 {
		t.Fatalf("outbox events = %d, want 2", len(backend.outboxEvents))
	}
	if backend.outboxEvents[0].DedupeKey == backend.outboxEvents[1].DedupeKey {
		t.Fatalf("dedupe keys should differ across dispatch cycles: %q", backend.outboxEvents[0].DedupeKey)
	}
}

func TestTaskLifecycleScheduleRetryScopesGuardPerRun(t *testing.T) {
	backend := &retryGuardBackend{}
	lifecycle := NewTaskLifecycle(backend, nil, nil)

	retryableErr := "session session-1 still held by run-0 after drain timeout"
	task := &types.AgentTask{
		ID:          "task-1",
		WorkspaceID: 347,
		State:       types.AgentTaskStateRunning,
	}

	runOne := &types.AgentRun{
		ID:           "run-1",
		WorkspaceID:  task.WorkspaceID,
		OriginTaskID: task.ID,
		Status:       types.AgentRunStatusError,
		Error:        &retryableErr,
		DeliveryJSON: map[string]any{
			types.OrchestrationOutboxPayloadDispatchAttempt: 0,
		},
	}
	retried, err := lifecycle.scheduleRetry(context.Background(), task, runOne)
	if err != nil {
		t.Fatalf("first lifecycle retry returned error: %v", err)
	}
	if !retried {
		t.Fatal("expected first lifecycle retry to requeue task")
	}

	runTwo := &types.AgentRun{
		ID:           "run-2",
		WorkspaceID:  task.WorkspaceID,
		OriginTaskID: task.ID,
		Status:       types.AgentRunStatusError,
		Error:        &retryableErr,
		DeliveryJSON: map[string]any{
			types.OrchestrationOutboxPayloadDispatchAttempt: 0,
		},
	}
	retried, err = lifecycle.scheduleRetry(context.Background(), task, runTwo)
	if err != nil {
		t.Fatalf("second lifecycle retry returned error: %v", err)
	}
	if !retried {
		t.Fatal("expected second lifecycle retry to requeue task")
	}

	if len(backend.taskStateUpdates) != 2 {
		t.Fatalf("task state updates = %d, want 2", len(backend.taskStateUpdates))
	}
	if len(backend.outboxEvents) != 2 {
		t.Fatalf("outbox events = %d, want 2", len(backend.outboxEvents))
	}
	if backend.outboxEvents[0].DedupeKey == backend.outboxEvents[1].DedupeKey {
		t.Fatalf("dedupe keys should differ across runs: %q", backend.outboxEvents[0].DedupeKey)
	}
}
