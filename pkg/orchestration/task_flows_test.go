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

type deferredToolExecutorStub struct {
	requested types.DeferredToolExecutionRequest
	stdout    string
	stderr    string
	exitCode  int
	err       error
}

func (s *deferredToolExecutorStub) ExecuteDeferred(_ context.Context, req types.DeferredToolExecutionRequest) (string, string, int, error) {
	s.requested = req
	return s.stdout, s.stderr, s.exitCode, s.err
}

func (s *deferredToolExecutorStub) RecordToolRejection(context.Context, string, string, string) error {
	return nil
}

func (s *deferredToolExecutorStub) GrantWritePreapproval(context.Context, string) error {
	return nil
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
	flows := NewTaskFlows(backend, nil, nil, nil, nil, nil)

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

type resumeRequeueBackend struct {
	repository.BackendRepository
	run                  *types.AgentRun
	pending              []*types.TaskInput
	requeueResult        bool
	requeueCalled        bool
	requeueExpectedRunID string
	requeuedTask         *types.AgentTask
	consumeCalls         int
}

func (b *resumeRequeueBackend) GetAgentRun(_ context.Context, _ uint, _ string) (*types.AgentRun, error) {
	return b.run, nil
}

func (b *resumeRequeueBackend) ListPendingTaskInputs(_ context.Context, _ string, _ int) ([]*types.TaskInput, error) {
	return b.pending, nil
}

func (b *resumeRequeueBackend) RequeueTaskWithOutboxIfCurrentRun(
	_ context.Context,
	task *types.AgentTask,
	expectedRunID string,
	_ *types.OrchestrationOutboxEvent,
) (bool, error) {
	b.requeueCalled = true
	b.requeueExpectedRunID = expectedRunID
	copied := *task
	copied.PayloadJSON = cloneAnyMap(task.PayloadJSON)
	b.requeuedTask = &copied
	return b.requeueResult, nil
}

func (b *resumeRequeueBackend) ConsumeOldestPendingInput(_ context.Context, _ string) (string, error) {
	b.consumeCalls++
	if len(b.pending) == 0 || b.pending[0] == nil {
		return "", nil
	}
	message := b.pending[0].Message
	b.pending = b.pending[1:]
	return message, nil
}

type leaseTerminalIO struct {
	repository.TerminalIORepository
	owner     string
	wakeCalls []string
}

func (t *leaseTerminalIO) GetSessionLeaseOwner(_ context.Context, _ uint, _ string) (string, error) {
	return t.owner, nil
}

func (t *leaseTerminalIO) PublishInputWake(_ context.Context, taskID string) error {
	t.wakeCalls = append(t.wakeCalls, taskID)
	return nil
}

func TestRequeueTaskForResumeDoesNotConsumeInputBeforeCAS(t *testing.T) {
	timeoutMs := 30_000
	run := &types.AgentRun{
		ID:          "run-1",
		WorkspaceID: 7,
		SessionID:   "session-1",
		TimeoutMs:   timeoutMs,
	}
	backend := &resumeRequeueBackend{
		run:           run,
		requeueResult: false,
		pending: []*types.TaskInput{{
			Message: "new follow-up",
		}},
	}
	flows := NewTaskFlows(backend, nil, nil, nil, nil, nil)
	task := &types.AgentTask{
		ID:          "task-1",
		WorkspaceID: 7,
		PayloadJSON: map[string]any{
			"message": "original prompt",
			"prompt":  "original prompt",
		},
	}

	if err := flows.requeueTaskForResume(context.Background(), task, run); err != nil {
		t.Fatalf("unexpected error on CAS miss: %v", err)
	}
	if backend.consumeCalls != 0 {
		t.Fatalf("consume oldest pending input calls = %d, want 0 (CAS miss should not consume)", backend.consumeCalls)
	}
	if got := stringFromPayload(task.PayloadJSON, "message"); got != "original prompt" {
		t.Fatalf("task payload message = %q, want original prompt", got)
	}
}

func TestDeliverTaskInputRequeuesWhenSessionLeaseIsGone(t *testing.T) {
	timeoutMs := 30_000
	runID := "run-1"
	run := &types.AgentRun{
		ID:          runID,
		WorkspaceID: 7,
		Status:      types.AgentRunStatusRunning,
		SessionID:   "session-1",
		TimeoutMs:   timeoutMs,
	}
	backend := &resumeRequeueBackend{
		run:           run,
		requeueResult: true,
		pending: []*types.TaskInput{{
			Message: "please try again tomorrow",
		}},
	}
	terminalIO := &leaseTerminalIO{}
	flows := NewTaskFlows(
		backend,
		terminalIO,
		nil,
		nil,
		nil,
		func(context.Context, *types.AgentRun) (*types.RunInteraction, error) {
			return &types.RunInteraction{
				State:             types.RunInteractionStateWorking,
				ActiveExecutionID: "exec-1",
			}, nil
		},
	)
	task := &types.AgentTask{
		ID:          "task-1",
		WorkspaceID: 7,
		TargetRunID: &runID,
		PayloadJSON: map[string]any{
			"message": "original prompt",
			"prompt":  "original prompt",
		},
	}

	if err := flows.deliverTaskInput(context.Background(), task); err != nil {
		t.Fatalf("deliverTaskInput returned error: %v", err)
	}
	if !backend.requeueCalled {
		t.Fatal("expected follow-up input to be requeued for resume")
	}
	if backend.requeueExpectedRunID != run.ID {
		t.Fatalf("expected run id = %q, want %q", backend.requeueExpectedRunID, run.ID)
	}
	if backend.consumeCalls != 1 {
		t.Fatalf("consume oldest pending input calls = %d, want 1", backend.consumeCalls)
	}
	if len(terminalIO.wakeCalls) != 0 {
		t.Fatalf("expected no wake to be published, got %d wake(s)", len(terminalIO.wakeCalls))
	}
	if backend.requeuedTask == nil {
		t.Fatal("expected requeue payload to be captured")
	}
	spec := parseTaskCommandPayload(backend.requeuedTask.PayloadJSON)
	if !spec.Resume.Enabled {
		t.Fatal("expected requeued payload to enable resume mode")
	}
	if spec.Resume.CheckpointRunID != run.ID {
		t.Fatalf("checkpoint run id = %q, want %q", spec.Resume.CheckpointRunID, run.ID)
	}
	if spec.Message != "please try again tomorrow" {
		t.Fatalf("resume message = %q, want %q", spec.Message, "please try again tomorrow")
	}
}

func TestMaybeExecuteDeferredToolCallPassesTaskContextToExecutor(t *testing.T) {
	executor := &deferredToolExecutorStub{
		stdout:   `{"thread_id":"thread-123","message_id":"msg-123"}`,
		exitCode: 0,
	}
	flows := NewTaskFlows(nil, nil, nil, nil, nil, nil)
	flows.SetToolExecutor(executor)

	runID := "run-1"
	agentID := "agent-1"
	blockerID := "blocker-1"
	task := &types.AgentTask{
		ID:               "task-1",
		WorkspaceID:      7,
		AgentID:          &agentID,
		TargetRunID:      &runID,
		CurrentBlockerID: &blockerID,
		CurrentBlocker: &types.TaskBlocker{
			PayloadJSON: map[string]any{
				"tool_call": map[string]any{
					"tool":         "gmail",
					"args":         []string{"send-email", "luke@example.com", "Hello", "Body"},
					"workspace_id": 7,
					"member_id":    42,
					"summary":      "send the email",
				},
			},
		},
	}
	approve := types.TaskInputActionApprove

	result := flows.maybeExecuteDeferredToolCall(context.Background(), task, &approve, "")

	if executor.requested.Task == nil {
		t.Fatal("expected task context to be forwarded to deferred executor")
	}
	if got := executor.requested.Task.ID; got != "task-1" {
		t.Fatalf("forwarded task id = %q, want task-1", got)
	}
	if got := executor.requested.MemberID; got != 42 {
		t.Fatalf("forwarded member id = %d, want 42", got)
	}
	if got := executor.requested.ToolName; got != "gmail" {
		t.Fatalf("forwarded tool name = %q, want gmail", got)
	}
	if got := executor.requested.Args[0]; got != "send-email" {
		t.Fatalf("forwarded command = %q, want send-email", got)
	}
	if result == "" {
		t.Fatal("expected approval result message")
	}
}
