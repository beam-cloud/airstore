package orchestration

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/beam-cloud/airstore/pkg/common"
	"github.com/beam-cloud/airstore/pkg/repository"
	"github.com/beam-cloud/airstore/pkg/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

type fakeBackend struct {
	repository.BackendRepository
	mu             sync.Mutex
	agentTasks     map[string]*types.AgentTask
	profiles       map[string]*types.AgentProfile
	runs           map[string]*types.AgentRun
	attempts       map[string][]*types.AgentRunAttempt
	runExecutions  map[string]*types.RunExecution
	idempotency    map[string]string
	runSnapshotSeq map[string]int64
	droppedCount   int
}

func newFakeBackend() *fakeBackend {
	return &fakeBackend{
		agentTasks:     map[string]*types.AgentTask{},
		profiles:       map[string]*types.AgentProfile{},
		runs:           map[string]*types.AgentRun{},
		attempts:       map[string][]*types.AgentRunAttempt{},
		runExecutions:  map[string]*types.RunExecution{},
		idempotency:    map[string]string{},
		runSnapshotSeq: map[string]int64{},
	}
}

func idempotencyKey(workspaceID uint, agentID *string, key string) string {
	agent := "_"
	if agentID != nil {
		agent = *agentID
	}
	return fmt.Sprintf("%d:%s:%s", workspaceID, agent, key)
}

func (f *fakeBackend) CreateTask(_ context.Context, task *types.AgentTask) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	task.ID = uuid.NewString()
	f.agentTasks[task.ID] = task
	f.idempotency[idempotencyKey(task.WorkspaceID, task.AgentID, task.IdempotencyKey)] = task.ID
	return nil
}

func (f *fakeBackend) GetTaskByIdempotency(_ context.Context, workspaceID uint, agentID *string, key string) (*types.AgentTask, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	id, ok := f.idempotency[idempotencyKey(workspaceID, agentID, key)]
	if !ok {
		return nil, &types.ErrAgentTaskNotFound{ID: key}
	}
	return f.agentTasks[id], nil
}

func (f *fakeBackend) GetTaskByID(_ context.Context, taskID string) (*types.AgentTask, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	env, ok := f.agentTasks[taskID]
	if !ok {
		return nil, &types.ErrAgentTaskNotFound{ID: taskID}
	}
	return env, nil
}

func (f *fakeBackend) GetTask(_ context.Context, workspaceID uint, taskID string) (*types.AgentTask, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	task, ok := f.agentTasks[taskID]
	if !ok || task.WorkspaceID != workspaceID {
		return nil, &types.ErrAgentTaskNotFound{ID: taskID}
	}
	return task, nil
}

func (f *fakeBackend) UpdateTaskState(_ context.Context, taskID string, state types.AgentTaskState, droppedReason *string, targetRunID *string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	env, ok := f.agentTasks[taskID]
	if !ok {
		return &types.ErrAgentTaskNotFound{ID: taskID}
	}
	env.State = state
	env.TargetRunID = targetRunID
	if state == types.AgentTaskStateDropped {
		env.DroppedReason = droppedReason
		f.droppedCount++
	}
	return nil
}

func (f *fakeBackend) GetAgentProfile(_ context.Context, workspaceID uint, agentID string) (*types.AgentProfile, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	profile, ok := f.profiles[agentID]
	if !ok || profile.WorkspaceID != workspaceID {
		return nil, &types.ErrAgentProfileNotFound{ID: agentID}
	}
	return profile, nil
}

func (f *fakeBackend) CreateAgentProfile(_ context.Context, profile *types.AgentProfile) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if profile.ID == "" {
		profile.ID = uuid.NewString()
	}
	f.profiles[profile.ID] = profile
	return nil
}

func (f *fakeBackend) GetAgentRun(_ context.Context, workspaceID uint, runID string) (*types.AgentRun, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	run, ok := f.runs[runID]
	if !ok || run.WorkspaceID != workspaceID {
		return nil, &types.ErrAgentRunNotFound{ID: runID}
	}
	return run, nil
}

func (f *fakeBackend) CreateAgentRun(_ context.Context, run *types.AgentRun) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if run.ID == "" {
		run.ID = uuid.NewString()
	}
	f.runs[run.ID] = run
	return nil
}

func (f *fakeBackend) IncrementAgentRunSnapshotSeq(_ context.Context, runID string) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.runSnapshotSeq[runID] = f.runSnapshotSeq[runID] + 1
	return f.runSnapshotSeq[runID], nil
}

func (f *fakeBackend) AppendAgentRunSnapshot(_ context.Context, _ *types.AgentRunSnapshot) error {
	return nil
}

func (f *fakeBackend) ListAgentRunAttempts(_ context.Context, runID string) ([]*types.AgentRunAttempt, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	attempts := f.attempts[runID]
	out := make([]*types.AgentRunAttempt, 0, len(attempts))
	out = append(out, attempts...)
	return out, nil
}

func (f *fakeBackend) GetRunExecution(_ context.Context, taskID string) (*types.RunExecution, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	task, ok := f.runExecutions[taskID]
	if !ok {
		return nil, &types.ErrRunExecutionNotFound{ExternalId: taskID}
	}
	return task, nil
}

func (f *fakeBackend) EnsureWorkspaceServiceToken(_ context.Context, _ uint) (*types.Token, string, error) {
	return &types.Token{}, "svc-token", nil
}

func (f *fakeBackend) CreateAgentRunAttempt(_ context.Context, attempt *types.AgentRunAttempt) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if attempt.ID == "" {
		attempt.ID = uuid.NewString()
	}
	copyAttempt := *attempt
	f.attempts[attempt.RunID] = append(f.attempts[attempt.RunID], &copyAttempt)
	return nil
}

func (f *fakeBackend) CreateRunExecution(_ context.Context, exec *types.RunExecution) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if exec.ExternalId == "" {
		exec.ExternalId = uuid.NewString()
	}
	copyExec := *exec
	f.runExecutions[exec.ExternalId] = &copyExec
	return nil
}

func (f *fakeBackend) BindAttemptExecutionTask(_ context.Context, attemptID, taskExternalID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, attempts := range f.attempts {
		for _, attempt := range attempts {
			if attempt == nil || attempt.ID != attemptID {
				continue
			}
			executionID := taskExternalID
			attempt.ExecutionID = &executionID
			return nil
		}
	}
	return &types.ErrAgentRunAttemptNotFound{ID: attemptID}
}

type fakeTerminalIO struct {
	mu     sync.Mutex
	inputs map[string][][]byte
}

func newFakeTerminalIO() *fakeTerminalIO {
	return &fakeTerminalIO{inputs: map[string][][]byte{}}
}

func (f *fakeTerminalIO) PublishInput(_ context.Context, taskID string, data []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.inputs[taskID] = append(f.inputs[taskID], append([]byte(nil), data...))
	return nil
}

func (f *fakeTerminalIO) SubscribeInput(_ context.Context, _ string) (<-chan []byte, func(), error) {
	ch := make(chan []byte)
	close(ch)
	return ch, func() {}, nil
}

func (f *fakeTerminalIO) PublishOutput(_ context.Context, _ string, _ []byte) error { return nil }

func (f *fakeTerminalIO) SubscribeOutput(_ context.Context, _ string) (<-chan []byte, func(), error) {
	ch := make(chan []byte)
	close(ch)
	return ch, func() {}, nil
}

func (f *fakeTerminalIO) PublishCancel(_ context.Context, _ string) error { return nil }

func (f *fakeTerminalIO) SubscribeCancel(_ context.Context, _ string) (<-chan struct{}, func(), error) {
	ch := make(chan struct{})
	close(ch)
	return ch, func() {}, nil
}

func newTestRedis(t *testing.T) (*common.RedisClient, func()) {
	t.Helper()
	mr, err := miniredis.Run()
	require.NoError(t, err)

	client, err := common.NewRedisClient(types.RedisConfig{
		Mode:  types.RedisModeSingle,
		Addrs: []string{mr.Addr()},
	})
	require.NoError(t, err)

	cleanup := func() {
		_ = client.Close()
		mr.Close()
	}
	return client, cleanup
}

func TestAcceptAgentCommandAcceptedFirstAndIdempotent(t *testing.T) {
	redisClient, cleanup := newTestRedis(t)
	defer cleanup()

	backend := newFakeBackend()
	agentID := uuid.NewString()
	backend.profiles[agentID] = &types.AgentProfile{
		ID:          agentID,
		WorkspaceID: 42,
		AgentKey:    "agent-key",
		Name:        "Agent",
		ConfigJSON: map[string]any{
			agentConfigKeyRunner: AgentRunnerClaudeCode,
		},
		Active: true,
	}
	svc := NewAgentService(context.Background(), backend, nil, redisClient, nil, "ghcr.io/beam/sandbox:latest")

	params := AgentCommandParams{
		Message:        "hello world",
		AgentID:        &agentID,
		SessionID:      "session-1",
		IdempotencyKey: "idem-1",
	}

	task, deduped, err := svc.AcceptAgentCommand(context.Background(), 42, params)
	require.NoError(t, err)
	require.False(t, deduped)
	require.NotEmpty(t, task.ID)
	require.Equal(t, types.AgentTaskStateQueued, task.State)
	require.Nil(t, task.TargetRunID, "run should not exist at acceptance time")

	queueLen, err := redisClient.LLen(context.Background(), common.Keys.TaskQueue()).Result()
	require.NoError(t, err)
	require.EqualValues(t, 1, queueLen)

	again, deduped, err := svc.AcceptAgentCommand(context.Background(), 42, params)
	require.NoError(t, err)
	require.True(t, deduped)
	require.Equal(t, task.ID, again.ID)

	queueLen, err = redisClient.LLen(context.Background(), common.Keys.TaskQueue()).Result()
	require.NoError(t, err)
	require.EqualValues(t, 1, queueLen, "idempotent replay must not enqueue duplicate work")
}

func TestQueueReshapingDropsOlderFollowupTask(t *testing.T) {
	redisClient, cleanup := newTestRedis(t)
	defer cleanup()

	backend := newFakeBackend()
	store := repository.NewOrchestrationStore(backend, redisClient)
	router := NewTaskQueueRouter(store)
	ctx := context.Background()

	instanceKey := "execclass_test"
	first := &types.AgentTask{
		ID:             uuid.NewString(),
		WorkspaceID:    1,
		Kind:           types.AgentTaskKindRunInput,
		QueueMode:      types.AgentQueueModeFollowup,
		State:          types.AgentTaskStateQueued,
		IdempotencyKey: "f1",
	}
	second := &types.AgentTask{
		ID:             uuid.NewString(),
		WorkspaceID:    1,
		Kind:           types.AgentTaskKindRunInput,
		QueueMode:      types.AgentQueueModeFollowup,
		State:          types.AgentTaskStateQueued,
		IdempotencyKey: "f2",
	}

	backend.agentTasks[first.ID] = first
	backend.agentTasks[second.ID] = second

	require.NoError(t, router.Enqueue(ctx, first, instanceKey))
	require.NoError(t, router.Enqueue(ctx, second, instanceKey))

	require.Equal(t, types.AgentTaskStateDropped, backend.agentTasks[first.ID].State)
	require.Equal(t, types.AgentTaskStateQueued, backend.agentTasks[second.ID].State)

	token, err := router.Pop(ctx, 0)
	require.NoError(t, err)
	require.NotEmpty(t, token)

	taskID, err := router.ResolveTaskID(ctx, token)
	require.NoError(t, err)
	require.Equal(t, second.ID, taskID)
}

func TestAcceptAgentCommandAppliesAgentConfigModelAndProvider(t *testing.T) {
	redisClient, cleanup := newTestRedis(t)
	defer cleanup()

	backend := newFakeBackend()
	agentID := uuid.NewString()
	backend.profiles[agentID] = &types.AgentProfile{
		ID:          agentID,
		WorkspaceID: 42,
		AgentKey:    "agent-key",
		Name:        "Agent",
		ConfigJSON: map[string]any{
			agentConfigKeyProvider: AgentProviderClaude,
			agentConfigKeyModel:    "claude-sonnet-4",
			"purpose":              "test",
		},
		Active: true,
	}

	svc := NewAgentService(context.Background(), backend, nil, redisClient, nil, "ghcr.io/beam/sandbox:latest")
	params := AgentCommandParams{
		Message:        "hello world",
		AgentID:        &agentID,
		SessionID:      "session-1",
		IdempotencyKey: "idem-with-model",
	}

	task, deduped, err := svc.AcceptAgentCommand(context.Background(), 42, params)
	require.NoError(t, err)
	require.False(t, deduped)
	require.Equal(t, AgentProviderClaude, task.PayloadJSON[agentConfigKeyProvider])
	require.Equal(t, "claude-sonnet-4", task.PayloadJSON[agentConfigKeyModel])
	require.Equal(t, map[string]any{
		agentConfigKeyProvider: AgentProviderClaude,
		agentConfigKeyModel:    "claude-sonnet-4",
		"purpose":              "test",
	}, task.PayloadJSON[agentPayloadKeyAgentConfig])
}

func TestAcceptAgentCommandDerivesProviderFromRunner(t *testing.T) {
	redisClient, cleanup := newTestRedis(t)
	defer cleanup()

	backend := newFakeBackend()
	agentID := uuid.NewString()
	backend.profiles[agentID] = &types.AgentProfile{
		ID:          agentID,
		WorkspaceID: 42,
		AgentKey:    "agent-key",
		Name:        "Agent",
		ConfigJSON: map[string]any{
			agentConfigKeyRunner: AgentRunnerClaudeCode,
			agentConfigKeyModel:  "claude-sonnet-4-6",
		},
		Active: true,
	}

	svc := NewAgentService(context.Background(), backend, nil, redisClient, nil, "ghcr.io/beam/sandbox:latest")
	task, deduped, err := svc.AcceptAgentCommand(context.Background(), 42, AgentCommandParams{
		Message: "hello world",
		AgentID: &agentID,
	})
	require.NoError(t, err)
	require.False(t, deduped)
	require.Equal(t, AgentProviderClaude, task.PayloadJSON[agentConfigKeyProvider])
	require.Equal(t, "claude-sonnet-4-6", task.PayloadJSON[agentConfigKeyModel])
}

func TestMaterializeRunForcesInteractiveForClaudeAgentCommand(t *testing.T) {
	backend := newFakeBackend()
	taskID := uuid.NewString()
	task := &types.AgentTask{
		ID:          taskID,
		WorkspaceID: 42,
		Kind:        types.AgentTaskKindAgentCommand,
		State:       types.AgentTaskStateQueued,
		PayloadJSON: map[string]any{
			"message":    "hello world",
			"session_id": "session-1",
			"policy": map[string]any{
				"interactive": false,
			},
			agentPayloadKeyAgentConfig: map[string]any{
				agentConfigKeyRunner: AgentRunnerClaudeCode,
			},
		},
	}
	backend.agentTasks[taskID] = task

	svc := NewAgentService(context.Background(), backend, nil, nil, nil, "ghcr.io/beam/sandbox:latest")
	run, runPolicy, prompt, err := svc.materializeRun(context.Background(), task)
	require.NoError(t, err)
	require.Equal(t, "hello world", prompt)
	require.True(t, runPolicy.Interactive)
	require.True(t, run.Interactive)
}

func TestMaterializeRunRejectsAgentCommandWithoutProvider(t *testing.T) {
	backend := newFakeBackend()
	taskID := uuid.NewString()
	task := &types.AgentTask{
		ID:          taskID,
		WorkspaceID: 42,
		Kind:        types.AgentTaskKindAgentCommand,
		State:       types.AgentTaskStateQueued,
		PayloadJSON: map[string]any{
			"message":    "hello world",
			"session_id": "session-1",
			"policy": map[string]any{
				"interactive": false,
			},
		},
	}
	backend.agentTasks[taskID] = task

	svc := NewAgentService(context.Background(), backend, nil, nil, nil, "ghcr.io/beam/sandbox:latest")
	_, _, _, err := svc.materializeRun(context.Background(), task)
	require.Error(t, err)
	require.Contains(t, err.Error(), "agent provider is required")
}

func TestAcceptAgentCommandGeneratesIDsWhenMissing(t *testing.T) {
	redisClient, cleanup := newTestRedis(t)
	defer cleanup()

	backend := newFakeBackend()
	agentID := uuid.NewString()
	backend.profiles[agentID] = &types.AgentProfile{
		ID:          agentID,
		WorkspaceID: 42,
		AgentKey:    "agent-key",
		Name:        "Agent",
		ConfigJSON: map[string]any{
			agentConfigKeyRunner: AgentRunnerClaudeCode,
		},
		Active: true,
	}
	svc := NewAgentService(context.Background(), backend, nil, redisClient, nil, "ghcr.io/beam/sandbox:latest")

	task, deduped, err := svc.AcceptAgentCommand(context.Background(), 42, AgentCommandParams{
		Message: "hello world",
		AgentID: &agentID,
	})
	require.NoError(t, err)
	require.False(t, deduped)
	require.NotEmpty(t, task.IdempotencyKey)
	require.NotEmpty(t, task.PayloadJSON["session_id"])
}

func TestAcceptAgentCommandRejectsProfileWithoutRunnerOrProvider(t *testing.T) {
	redisClient, cleanup := newTestRedis(t)
	defer cleanup()

	backend := newFakeBackend()
	agentID := uuid.NewString()
	backend.profiles[agentID] = &types.AgentProfile{
		ID:          agentID,
		WorkspaceID: 42,
		AgentKey:    "agent-key",
		Name:        "Agent",
		ConfigJSON:  map[string]any{},
		Active:      true,
	}
	svc := NewAgentService(context.Background(), backend, nil, redisClient, nil, "ghcr.io/beam/sandbox:latest")

	_, _, err := svc.AcceptAgentCommand(context.Background(), 42, AgentCommandParams{
		Message: "hello world",
		AgentID: &agentID,
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "agent provider is required")
}

func TestAcceptAgentCommandRejectsMissingAgentID(t *testing.T) {
	redisClient, cleanup := newTestRedis(t)
	defer cleanup()

	backend := newFakeBackend()
	svc := NewAgentService(context.Background(), backend, nil, redisClient, nil, "ghcr.io/beam/sandbox:latest")

	_, _, err := svc.AcceptAgentCommand(context.Background(), 42, AgentCommandParams{
		Message: "hello world",
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "agent_id is required")
}

func TestAcceptRunInputGeneratesIdempotencyKeyWhenMissing(t *testing.T) {
	redisClient, cleanup := newTestRedis(t)
	defer cleanup()

	backend := newFakeBackend()
	runID := uuid.NewString()
	backend.runs[runID] = &types.AgentRun{
		ID:          runID,
		WorkspaceID: 42,
		Status:      types.AgentRunStatusAccepted,
		SessionID:   "session-1",
		TimeoutMs:   60000,
	}

	svc := NewAgentService(context.Background(), backend, nil, redisClient, nil, "ghcr.io/beam/sandbox:latest")
	task, deduped, err := svc.AcceptRunInput(context.Background(), 42, runID, types.AgentQueueModeFollowup, "follow up", "")
	require.NoError(t, err)
	require.False(t, deduped)
	require.NotEmpty(t, task.IdempotencyKey)
}

func TestAcceptRunInputRestartsTerminalRunOnSameTask(t *testing.T) {
	redisClient, cleanup := newTestRedis(t)
	defer cleanup()

	backend := newFakeBackend()
	agentID := uuid.NewString()
	originTaskID := uuid.NewString()
	runID := uuid.NewString()
	sessionKey := "session-key"
	model := "claude-sonnet-4-6"
	taskQueue := repository.NewRedisTaskQueue(redisClient, "default")

	backend.agentTasks[originTaskID] = &types.AgentTask{
		ID:          originTaskID,
		WorkspaceID: 42,
		AgentID:     &agentID,
		Kind:        types.AgentTaskKindAgentCommand,
		QueueMode:   types.AgentQueueModeQueue,
		State:       types.AgentTaskStateDone,
		PayloadJSON: map[string]any{
			"message":              "original prompt",
			"session_id":           "session-1",
			"session_key":          sessionKey,
			"timeout_ms":           60000,
			agentConfigKeyProvider: AgentProviderClaude,
			agentConfigKeyModel:    model,
			agentPayloadKeyAgentConfig: map[string]any{
				agentConfigKeyProvider: AgentProviderClaude,
				agentConfigKeyModel:    model,
			},
		},
	}
	backend.runs[runID] = &types.AgentRun{
		ID:           runID,
		WorkspaceID:  42,
		AgentID:      &agentID,
		OriginTaskID: originTaskID,
		Status:       types.AgentRunStatusOK,
		SessionID:    "session-1",
		SessionKey:   &sessionKey,
		Provider:     strPtr(AgentProviderClaude),
		Model:        &model,
		TimeoutMs:    60000,
	}

	svc := NewAgentService(context.Background(), backend, taskQueue, redisClient, nil, "ghcr.io/beam/sandbox:latest")
	task, deduped, err := svc.AcceptRunInput(
		context.Background(),
		42,
		runID,
		types.AgentQueueModeFollowup,
		"follow up",
		"",
	)
	require.NoError(t, err)
	require.False(t, deduped)
	require.NotNil(t, task)
	require.Equal(t, originTaskID, task.ID)
	require.Equal(t, types.AgentTaskStateRunning, task.State)
	require.NotNil(t, task.TargetRunID)
	require.NotEqual(t, runID, *task.TargetRunID)

	newRun, ok := backend.runs[*task.TargetRunID]
	require.True(t, ok)
	require.Equal(t, originTaskID, newRun.OriginTaskID)
	require.Equal(t, "session-1", newRun.SessionID)
	require.NotNil(t, newRun.SessionKey)
	require.Equal(t, sessionKey, *newRun.SessionKey)
	require.NotNil(t, newRun.Provider)
	require.Equal(t, AgentProviderClaude, *newRun.Provider)
	require.NotNil(t, newRun.Model)
	require.Equal(t, model, *newRun.Model)

	require.NotEmpty(t, backend.attempts[newRun.ID])
	require.NotEmpty(t, backend.runExecutions)
	require.Len(t, backend.runExecutions, 1)
	for _, exec := range backend.runExecutions {
		require.NotNil(t, exec)
		require.Equal(t, "true", exec.Env["AIRSTORE_AGENT_RESUME_SESSION"])
	}
}

func TestAcceptRunInputDeliversDirectlyForActiveInteractiveRun(t *testing.T) {
	redisClient, cleanup := newTestRedis(t)
	defer cleanup()

	backend := newFakeBackend()
	agentID := uuid.NewString()
	runID := uuid.NewString()
	executionID := uuid.NewString()
	originTaskID := uuid.NewString()

	backend.agentTasks[originTaskID] = &types.AgentTask{
		ID:          originTaskID,
		WorkspaceID: 42,
		AgentID:     &agentID,
		Kind:        types.AgentTaskKindAgentCommand,
		State:       types.AgentTaskStateRunning,
		PayloadJSON: map[string]any{"message": "original"},
		TargetRunID: &runID,
	}
	backend.runs[runID] = &types.AgentRun{
		ID:           runID,
		WorkspaceID:  42,
		AgentID:      &agentID,
		OriginTaskID: originTaskID,
		Status:       types.AgentRunStatusRunning,
		Interactive:  true,
		SessionID:    "session-1",
	}
	backend.runExecutions[executionID] = &types.RunExecution{
		ExternalId: executionID,
		Type:       types.RunExecutionTypeInteractive,
		Status:     types.RunExecutionStatusRunning,
	}
	backend.attempts[runID] = []*types.AgentRunAttempt{
		{
			ID:          uuid.NewString(),
			RunID:       runID,
			AttemptNo:   1,
			Status:      types.AgentAttemptStatusRunning,
			ExecutionID: &executionID,
		},
	}

	terminalIO := newFakeTerminalIO()
	svc := NewAgentService(context.Background(), backend, nil, redisClient, nil, "ghcr.io/beam/sandbox:latest")
	svc.terminalIO = terminalIO

	task, deduped, err := svc.AcceptRunInput(
		context.Background(), 42, runID,
		types.AgentQueueModeFollowup, "follow up", "",
	)
	require.NoError(t, err)
	require.False(t, deduped)
	require.NotNil(t, task)
	require.Equal(t, originTaskID, task.ID, "should return the origin task, not create a new one")

	writes := terminalIO.inputs[executionID]
	require.Len(t, writes, 1)
	require.Equal(t, "follow up\n", string(writes[0]))

	taskCountBefore := len(backend.agentTasks)
	require.Equal(t, 1, taskCountBefore, "no new RunInput task should be created")
}

func TestTrySteerRunInputTaskInjectsInteractiveInput(t *testing.T) {
	backend := newFakeBackend()
	runID := uuid.NewString()
	executionID := uuid.NewString()
	backend.runs[runID] = &types.AgentRun{
		ID:          runID,
		WorkspaceID: 42,
		Status:      types.AgentRunStatusRunning,
		SessionID:   "session-1",
	}
	backend.runExecutions[executionID] = &types.RunExecution{
		ExternalId: executionID,
		Type:       types.RunExecutionTypeInteractive,
		Status:     types.RunExecutionStatusRunning,
	}
	backend.attempts[runID] = []*types.AgentRunAttempt{
		{
			ID:          uuid.NewString(),
			RunID:       runID,
			AttemptNo:   1,
			Status:      types.AgentAttemptStatusRunning,
			ExecutionID: &executionID,
		},
	}

	taskID := uuid.NewString()
	task := &types.AgentTask{
		ID:          taskID,
		WorkspaceID: 42,
		Kind:        types.AgentTaskKindRunInput,
		QueueMode:   types.AgentQueueModeSteer,
		State:       types.AgentTaskStateQueued,
		PayloadJSON: map[string]any{"message": "please stop"},
		TargetRunID: &runID,
	}
	backend.agentTasks[taskID] = task

	svc := NewAgentService(context.Background(), backend, nil, nil, nil, "ghcr.io/beam/sandbox:latest")
	terminalIO := newFakeTerminalIO()
	svc.terminalIO = terminalIO

	steered, err := svc.trySteerRunInputTask(context.Background(), task)
	require.NoError(t, err)
	require.True(t, steered)
	require.Equal(t, types.AgentTaskStateDone, backend.agentTasks[taskID].State)

	writes := terminalIO.inputs[executionID]
	require.Len(t, writes, 1)
	require.Equal(t, "please stop\n", string(writes[0]))
}

func TestHandleRunInputTaskFollowupSteersWhenRunInteractive(t *testing.T) {
	backend := newFakeBackend()
	runID := uuid.NewString()
	executionID := uuid.NewString()
	backend.runs[runID] = &types.AgentRun{
		ID:          runID,
		WorkspaceID: 42,
		Status:      types.AgentRunStatusRunning,
		SessionID:   "session-1",
	}
	backend.runExecutions[executionID] = &types.RunExecution{
		ExternalId: executionID,
		Type:       types.RunExecutionTypeInteractive,
		Status:     types.RunExecutionStatusRunning,
	}
	backend.attempts[runID] = []*types.AgentRunAttempt{
		{
			ID:          uuid.NewString(),
			RunID:       runID,
			AttemptNo:   1,
			Status:      types.AgentAttemptStatusRunning,
			ExecutionID: &executionID,
		},
	}

	taskID := uuid.NewString()
	task := &types.AgentTask{
		ID:          taskID,
		WorkspaceID: 42,
		Kind:        types.AgentTaskKindRunInput,
		QueueMode:   types.AgentQueueModeFollowup,
		State:       types.AgentTaskStateQueued,
		PayloadJSON: map[string]any{"message": "continue"},
		TargetRunID: &runID,
	}
	backend.agentTasks[taskID] = task

	svc := NewAgentService(context.Background(), backend, nil, nil, nil, "ghcr.io/beam/sandbox:latest")
	terminalIO := newFakeTerminalIO()
	svc.terminalIO = terminalIO

	err := svc.handleRunInputTask(context.Background(), task)
	require.NoError(t, err)
	require.Equal(t, types.AgentTaskStateDone, backend.agentTasks[taskID].State)
	writes := terminalIO.inputs[executionID]
	require.Len(t, writes, 1)
	require.Equal(t, "continue\n", string(writes[0]))
}

func TestTrySteerRunInputTaskFallsBackWhenTaskNotInteractive(t *testing.T) {
	backend := newFakeBackend()
	runID := uuid.NewString()
	executionID := uuid.NewString()
	backend.runs[runID] = &types.AgentRun{
		ID:          runID,
		WorkspaceID: 42,
		Status:      types.AgentRunStatusRunning,
		SessionID:   "session-1",
	}
	backend.runExecutions[executionID] = &types.RunExecution{
		ExternalId: executionID,
		Type:       types.RunExecutionTypeBackground,
		Status:     types.RunExecutionStatusRunning,
	}
	backend.attempts[runID] = []*types.AgentRunAttempt{
		{
			ID:          uuid.NewString(),
			RunID:       runID,
			AttemptNo:   1,
			Status:      types.AgentAttemptStatusRunning,
			ExecutionID: &executionID,
		},
	}

	taskID := uuid.NewString()
	task := &types.AgentTask{
		ID:          taskID,
		WorkspaceID: 42,
		Kind:        types.AgentTaskKindRunInput,
		QueueMode:   types.AgentQueueModeSteer,
		State:       types.AgentTaskStateQueued,
		PayloadJSON: map[string]any{"message": "fallback"},
		TargetRunID: &runID,
	}
	backend.agentTasks[taskID] = task

	svc := NewAgentService(context.Background(), backend, nil, nil, nil, "ghcr.io/beam/sandbox:latest")
	terminalIO := newFakeTerminalIO()
	svc.terminalIO = terminalIO

	steered, err := svc.trySteerRunInputTask(context.Background(), task)
	require.NoError(t, err)
	require.False(t, steered)
	require.Equal(t, types.AgentTaskStateQueued, backend.agentTasks[taskID].State)
	require.Empty(t, terminalIO.inputs[executionID])
}

func TestHandleRunInputTaskDropsWhenTargetRunTerminal(t *testing.T) {
	backend := newFakeBackend()
	runID := uuid.NewString()
	backend.runs[runID] = &types.AgentRun{
		ID:          runID,
		WorkspaceID: 42,
		Status:      types.AgentRunStatusCancelled,
		SessionID:   "session-1",
	}

	taskID := uuid.NewString()
	task := &types.AgentTask{
		ID:          taskID,
		WorkspaceID: 42,
		Kind:        types.AgentTaskKindRunInput,
		QueueMode:   types.AgentQueueModeSteer,
		State:       types.AgentTaskStateQueued,
		PayloadJSON: map[string]any{"message": "fallback"},
		TargetRunID: &runID,
	}
	backend.agentTasks[taskID] = task

	svc := NewAgentService(context.Background(), backend, nil, nil, nil, "ghcr.io/beam/sandbox:latest")
	err := svc.handleRunInputTask(context.Background(), task)
	require.NoError(t, err)
	require.Equal(t, types.AgentTaskStateDropped, backend.agentTasks[taskID].State)
	require.NotNil(t, backend.agentTasks[taskID].DroppedReason)
	require.Equal(t, types.AgentTaskDropReasonRunInputTerminalTarget, *backend.agentTasks[taskID].DroppedReason)
}
