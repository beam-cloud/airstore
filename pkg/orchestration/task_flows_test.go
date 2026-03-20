package orchestration

import (
	"context"
	"testing"
	"time"

	"github.com/beam-cloud/airstore/pkg/repository"
	"github.com/beam-cloud/airstore/pkg/types"
)

type acceptAgentCommandBackend struct {
	repository.BackendRepository
	profile      *types.AgentProfile
	createdTask  *types.AgentTask
	createdEvent *types.OrchestrationOutboxEvent
}

func (b *acceptAgentCommandBackend) GetAgentProfile(_ context.Context, _ uint, _ string) (*types.AgentProfile, error) {
	return b.profile, nil
}

func (b *acceptAgentCommandBackend) GetTaskByIdempotency(_ context.Context, _ uint, _ *string, idempotencyKey string) (*types.AgentTask, error) {
	return nil, &types.ErrAgentTaskNotFound{ID: idempotencyKey}
}

func (b *acceptAgentCommandBackend) ListAgentRunsFiltered(_ context.Context, _ uint, _ types.AgentRunListFilter) ([]*types.AgentRun, error) {
	return nil, nil
}

func (b *acceptAgentCommandBackend) CreateTaskWithOutbox(_ context.Context, task *types.AgentTask, event *types.OrchestrationOutboxEvent) error {
	now := time.Now()
	task.ID = "task-1"
	task.AcceptedAt = now
	task.CreatedAt = now
	task.UpdatedAt = now

	copiedTask := *task
	copiedTask.PayloadJSON = cloneAnyMap(task.PayloadJSON)
	copiedTask.RoutingJSON = cloneAnyMap(task.RoutingJSON)
	b.createdTask = &copiedTask

	if event != nil {
		copiedEvent := *event
		copiedEvent.PayloadJSON = cloneAnyMap(event.PayloadJSON)
		b.createdEvent = &copiedEvent
	}
	return nil
}

func TestAcceptAgentCommandCreatesSleepingTaskForDelayedDispatch(t *testing.T) {
	backend := &acceptAgentCommandBackend{
		profile: &types.AgentProfile{
			ID:         "agent-1",
			AgentKey:   "outreach",
			ConfigJSON: map[string]any{agentConfigKeyRunner: AgentRunnerClaudeCode},
		},
	}
	flows := NewTaskFlows(backend, nil, nil, nil, nil, nil, nil)

	agentID := "agent-1"
	label := "luke@beam.cloud"
	spawnedBy := types.AgentTaskSpawnedByFanOut
	delay := 2 * time.Hour
	before := time.Now()

	task, deduped, err := flows.AcceptAgentCommand(context.Background(), 7, AgentCommandParams{
		Message:        "Check the thread for replies.",
		AgentID:        &agentID,
		SessionID:      "session-1",
		IdempotencyKey: "idem-1",
		Label:          &label,
		SpawnedBy:      &spawnedBy,
		DispatchDelay:  delay,
	})
	if err != nil {
		t.Fatalf("AcceptAgentCommand returned error: %v", err)
	}
	if deduped {
		t.Fatal("expected a new task, got deduped=true")
	}
	if task.State != types.AgentTaskStateSleeping {
		t.Fatalf("task state = %q, want sleeping", task.State)
	}
	if task.WakeAt == nil {
		t.Fatal("expected wake_at to be populated")
	}
	if task.WakeReason == nil || *task.WakeReason != "Follow up with luke@beam.cloud" {
		t.Fatalf("wake_reason = %#v, want %q", task.WakeReason, "Follow up with luke@beam.cloud")
	}
	if got := stringFromPayload(task.PayloadJSON, "spawned_by"); got != types.AgentTaskSpawnedByFanOut {
		t.Fatalf("spawned_by = %q, want %q", got, types.AgentTaskSpawnedByFanOut)
	}
	if backend.createdTask == nil {
		t.Fatal("expected CreateTaskWithOutbox to receive a task")
	}
	if backend.createdTask.State != types.AgentTaskStateSleeping {
		t.Fatalf("persisted task state = %q, want sleeping", backend.createdTask.State)
	}
	if backend.createdTask.WakeAt == nil {
		t.Fatal("expected persisted wake_at to be populated")
	}
	if backend.createdEvent == nil {
		t.Fatal("expected delayed dispatch outbox event")
	}
	if !backend.createdEvent.AvailableAt.Equal(*backend.createdTask.WakeAt) {
		t.Fatalf("dispatch available_at = %v, want %v", backend.createdEvent.AvailableAt, *backend.createdTask.WakeAt)
	}
	if got := task.WakeAt.Sub(before); got < delay || got > delay+2*time.Second {
		t.Fatalf("wake delay = %s, want approximately %s", got, delay)
	}
}
