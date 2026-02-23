package services

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/beam-cloud/airstore/pkg/orchestration"
	"github.com/beam-cloud/airstore/pkg/repository"
	"github.com/beam-cloud/airstore/pkg/types"
	pb "github.com/beam-cloud/airstore/proto"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"
)

type retryTestBackend struct {
	repository.BackendRepository

	runs          map[string]*types.AgentRun
	tasks         map[string]*types.AgentTask
	runExecutions map[string]*types.RunExecution
	attemptByID   map[string]*types.AgentRunAttempt
	attemptsByRun map[string][]*types.AgentRunAttempt
	snapshotSeq   map[string]int64

	nextRunID       int
	nextAttemptID   int
	nextExecutionID int
}

func newRetryTestBackend() *retryTestBackend {
	return &retryTestBackend{
		runs:          map[string]*types.AgentRun{},
		tasks:         map[string]*types.AgentTask{},
		runExecutions: map[string]*types.RunExecution{},
		attemptByID:   map[string]*types.AgentRunAttempt{},
		attemptsByRun: map[string][]*types.AgentRunAttempt{},
		snapshotSeq:   map[string]int64{},
	}
}

func (b *retryTestBackend) GetAgentRunByID(_ context.Context, runID string) (*types.AgentRun, error) {
	run, ok := b.runs[runID]
	if !ok {
		return nil, &types.ErrAgentRunNotFound{ID: runID}
	}
	return run, nil
}

func (b *retryTestBackend) GetTaskByID(_ context.Context, taskID string) (*types.AgentTask, error) {
	task, ok := b.tasks[taskID]
	if !ok {
		return nil, &types.ErrAgentTaskNotFound{ID: taskID}
	}
	return task, nil
}

func (b *retryTestBackend) GetRunExecution(_ context.Context, externalID string) (*types.RunExecution, error) {
	exec, ok := b.runExecutions[externalID]
	if !ok {
		return nil, &types.ErrRunExecutionNotFound{ExternalId: externalID}
	}
	return exec, nil
}

func (b *retryTestBackend) CreateAgentRun(_ context.Context, run *types.AgentRun) error {
	copyRun := *run
	if copyRun.ID == "" {
		copyRun.ID = b.nextRunExternalID()
	}
	b.runs[copyRun.ID] = &copyRun
	run.ID = copyRun.ID
	return nil
}

func (b *retryTestBackend) UpdateTaskState(_ context.Context, taskID string, state types.AgentTaskState, droppedReason *string, targetRunID *string) error {
	task, ok := b.tasks[taskID]
	if !ok {
		return &types.ErrAgentTaskNotFound{ID: taskID}
	}
	task.State = state
	task.DroppedReason = droppedReason
	task.TargetRunID = targetRunID
	return nil
}

func (b *retryTestBackend) CreateAgentRunAttempt(_ context.Context, attempt *types.AgentRunAttempt) error {
	copyAttempt := *attempt
	if copyAttempt.ID == "" {
		copyAttempt.ID = b.nextAttemptExternalID()
	}
	b.attemptByID[copyAttempt.ID] = &copyAttempt
	b.attemptsByRun[copyAttempt.RunID] = append(b.attemptsByRun[copyAttempt.RunID], &copyAttempt)
	attempt.ID = copyAttempt.ID
	return nil
}

func (b *retryTestBackend) EnsureWorkspaceServiceToken(_ context.Context, _ uint) (*types.Token, string, error) {
	return &types.Token{}, "svc-token", nil
}

func (b *retryTestBackend) CreateRunExecution(_ context.Context, task *types.RunExecution) error {
	copyTask := *task
	if copyTask.ExternalId == "" {
		copyTask.ExternalId = b.nextExecutionExternalID()
	}
	b.runExecutions[copyTask.ExternalId] = &copyTask
	task.ExternalId = copyTask.ExternalId
	return nil
}

func (b *retryTestBackend) BindAttemptExecutionTask(_ context.Context, attemptID, taskExternalID string) error {
	attempt, ok := b.attemptByID[attemptID]
	if !ok {
		return fmt.Errorf("attempt not found: %s", attemptID)
	}
	attempt.ExecutionID = &taskExternalID
	return nil
}

func (b *retryTestBackend) IncrementAgentRunSnapshotSeq(_ context.Context, runID string) (int64, error) {
	b.snapshotSeq[runID]++
	return b.snapshotSeq[runID], nil
}

func (b *retryTestBackend) AppendAgentRunSnapshot(_ context.Context, _ *types.AgentRunSnapshot) error {
	return nil
}

func (b *retryTestBackend) nextRunExternalID() string {
	for {
		b.nextRunID++
		id := fmt.Sprintf("run-retry-%d", b.nextRunID)
		if _, exists := b.runs[id]; !exists {
			return id
		}
	}
}

func (b *retryTestBackend) nextAttemptExternalID() string {
	b.nextAttemptID++
	return fmt.Sprintf("attempt-%d", b.nextAttemptID)
}

func (b *retryTestBackend) nextExecutionExternalID() string {
	b.nextExecutionID++
	return fmt.Sprintf("exec-retry-%d", b.nextExecutionID)
}

type capturingTaskQueue struct {
	repository.TaskQueue
	pushed []*types.RunExecution
}

func (q *capturingTaskQueue) Push(_ context.Context, task *types.RunExecution) error {
	copyTask := *task
	q.pushed = append(q.pushed, &copyTask)
	return nil
}

func TestScheduleRetryRunCreatesNewRunAndRebindsTask(t *testing.T) {
	backend := newRetryTestBackend()
	queue := &capturingTaskQueue{}
	svc := &WorkerService{backend: backend, taskQueue: queue}

	agentID := "agent-1"
	originRunID := "run-1"
	originTaskID := "task-1"
	originExecutionID := "exec-1"

	backend.runs[originRunID] = &types.AgentRun{
		ID:           originRunID,
		WorkspaceID:  42,
		AgentID:      &agentID,
		OriginTaskID: originTaskID,
		ExecHost:     string(orchestration.ExecHostSandbox),
		ExecSecurity: string(orchestration.ExecSecurityFull),
		ExecAsk:      string(orchestration.ExecAskOff),
		RuntimeType:  orchestration.RuntimeTypeGvisor,
		TimeoutMs:    60_000,
		DeliveryJSON: map[string]any{
			types.AgentExecutionMetaKeyRetryMaxAttempts: 3,
			types.AgentExecutionMetaKeyRetryDelayMs:     0,
		},
	}
	backend.tasks[originTaskID] = &types.AgentTask{
		ID:          originTaskID,
		WorkspaceID: 42,
		State:       types.AgentTaskStateDispatched,
		TargetRunID: &originRunID,
	}
	backend.runExecutions[originExecutionID] = &types.RunExecution{
		ExternalId:  originExecutionID,
		WorkspaceId: 42,
		Status:      types.RunExecutionStatusFailed,
		Type:        types.RunExecutionTypeBackground,
		Prompt:      "retry me",
		Image:       "ghcr.io/beam/sandbox:latest",
		Entrypoint:  []string{"runner"},
		Env:         map[string]string{"A": "B"},
	}

	attempt := &types.AgentRunAttempt{
		ID:        "attempt-1",
		RunID:     originRunID,
		AttemptNo: 1,
		Status:    types.AgentAttemptStatusError,
	}
	backend.attemptByID[attempt.ID] = attempt
	backend.attemptsByRun[originRunID] = []*types.AgentRunAttempt{attempt}

	result, err := svc.scheduleRetryRun(context.Background(), attempt, originExecutionID)
	require.NoError(t, err)
	require.True(t, result.scheduled)
	require.NotEmpty(t, result.nextRunID)
	require.NotEqual(t, originRunID, result.nextRunID)
	require.Equal(t, 2, result.nextAttemptNo)

	originTask := backend.tasks[originTaskID]
	require.NotNil(t, originTask.TargetRunID)
	require.Equal(t, result.nextRunID, *originTask.TargetRunID)

	retryRun := backend.runs[result.nextRunID]
	require.NotNil(t, retryRun)
	require.Equal(t, originTaskID, retryRun.OriginTaskID)

	retryAttempts := backend.attemptsByRun[result.nextRunID]
	require.Len(t, retryAttempts, 1)
	require.Equal(t, 2, retryAttempts[0].AttemptNo)
	require.Equal(t, types.AgentAttemptStrategyRetry, retryAttempts[0].Strategy)

	require.Len(t, queue.pushed, 1)
	queuedExecution := queue.pushed[0]
	require.NotEqual(t, originExecutionID, queuedExecution.ExternalId)
	require.NotNil(t, queuedExecution.RunAttemptID)
	require.Equal(t, retryAttempts[0].ID, *queuedExecution.RunAttemptID)
}

func TestAgentCommandParamsFromProtoMatchesHTTPShape(t *testing.T) {
	policy := map[string]any{
		"host":             "sandbox",
		"security":         "full",
		"ask":              "off",
		"runtime_type":     "gvisor",
		"workspace_access": "rw",
		"network_enabled":  true,
		"interactive":      false,
		"resources": map[string]any{
			"cpu":    "1",
			"memory": "1Gi",
		},
		"retry": map[string]any{
			"max_attempts": 4,
			"delay_ms":     250,
		},
	}
	inputProvenance := map[string]any{
		"source":         "chat",
		"message_id":     "msg-123",
		"channel":        "web",
		"tool_call_id":   "tool-abc",
		"correlation_id": "corr-xyz",
	}
	routing := map[string]any{
		"to":            "user-1",
		"reply_to":      "assistant-1",
		"channel":       "chat",
		"reply_channel": "thread",
		"account_id":    "acct-1",
		"thread_id":     "thread-1",
		"group_id":      "group-1",
	}
	attachment := map[string]any{
		"type": "text",
		"path": "/workspace/memory/input.txt",
	}

	deliver := true
	timeoutMs := int32(45_000)
	protoReq := &pb.CreateTaskRequest{
		Message:           "run this task",
		AgentId:           "agent-1",
		SessionId:         "session-1",
		SessionKey:        "session-key",
		IdempotencyKey:    "idem-1",
		Lane:              "lane-1",
		ExtraSystemPrompt: "extra prompt",
		Deliver:           &deliver,
		TimeoutMs:         &timeoutMs,
		Policy:            mustStruct(t, policy),
		InputProvenance:   mustStruct(t, inputProvenance),
		Routing:           mustStruct(t, routing),
		Attachments:       []*structpb.Struct{mustStruct(t, attachment)},
		Label:             "label-1",
		SpawnedBy:         "sdk-test",
	}

	grpcParams, err := agentCommandParamsFromProto(protoReq)
	require.NoError(t, err)

	httpBody := map[string]any{
		"message":             protoReq.Message,
		"agent_id":            protoReq.AgentId,
		"session_id":          protoReq.SessionId,
		"session_key":         protoReq.SessionKey,
		"idempotency_key":     protoReq.IdempotencyKey,
		"lane":                protoReq.Lane,
		"extra_system_prompt": protoReq.ExtraSystemPrompt,
		"deliver":             deliver,
		"timeout_ms":          int(timeoutMs),
		"policy":              policy,
		"input_provenance":    inputProvenance,
		"routing":             routing,
		"attachments":         []map[string]any{attachment},
		"label":               protoReq.Label,
		"spawned_by":          protoReq.SpawnedBy,
	}
	httpJSON, err := json.Marshal(httpBody)
	require.NoError(t, err)

	var httpParams orchestration.AgentCommandParams
	require.NoError(t, json.Unmarshal(httpJSON, &httpParams))

	grpcJSON, err := json.Marshal(grpcParams)
	require.NoError(t, err)
	httpParamsJSON, err := json.Marshal(httpParams)
	require.NoError(t, err)

	require.JSONEq(t, string(httpParamsJSON), string(grpcJSON))
}

func mustStruct(t *testing.T, values map[string]any) *structpb.Struct {
	t.Helper()
	st, err := structpb.NewStruct(values)
	require.NoError(t, err)
	return st
}
