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
	mu           sync.Mutex
	envelopes    map[string]*types.AgentTask
	profiles     map[string]*types.AgentProfile
	runs         map[string]*types.AgentRun
	attempts     map[string][]*types.AgentRunAttempt
	tasks        map[string]*types.RunExecution
	idempotency  map[string]string
	droppedCount int
}

func newFakeBackend() *fakeBackend {
	return &fakeBackend{
		envelopes:   map[string]*types.AgentTask{},
		profiles:    map[string]*types.AgentProfile{},
		runs:        map[string]*types.AgentRun{},
		attempts:    map[string][]*types.AgentRunAttempt{},
		tasks:       map[string]*types.RunExecution{},
		idempotency: map[string]string{},
	}
}

func idempotencyKey(workspaceID uint, agentID *string, key string) string {
	agent := "_"
	if agentID != nil {
		agent = *agentID
	}
	return fmt.Sprintf("%d:%s:%s", workspaceID, agent, key)
}

func (f *fakeBackend) CreateTask(_ context.Context, envelope *types.AgentTask) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	envelope.ID = uuid.NewString()
	f.envelopes[envelope.ID] = envelope
	f.idempotency[idempotencyKey(envelope.WorkspaceID, envelope.AgentID, envelope.IdempotencyKey)] = envelope.ID
	return nil
}

func (f *fakeBackend) GetTaskByIdempotency(_ context.Context, workspaceID uint, agentID *string, key string) (*types.AgentTask, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	id, ok := f.idempotency[idempotencyKey(workspaceID, agentID, key)]
	if !ok {
		return nil, &types.ErrAgentTaskNotFound{ID: key}
	}
	return f.envelopes[id], nil
}

func (f *fakeBackend) GetTaskByID(_ context.Context, envelopeID string) (*types.AgentTask, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	env, ok := f.envelopes[envelopeID]
	if !ok {
		return nil, &types.ErrAgentTaskNotFound{ID: envelopeID}
	}
	return env, nil
}

func (f *fakeBackend) UpdateTaskState(_ context.Context, envelopeID string, state types.AgentTaskState, droppedReason *string, targetRunID *string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	env, ok := f.envelopes[envelopeID]
	if !ok {
		return &types.ErrAgentTaskNotFound{ID: envelopeID}
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

func (f *fakeBackend) GetAgentRun(_ context.Context, workspaceID uint, runID string) (*types.AgentRun, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	run, ok := f.runs[runID]
	if !ok || run.WorkspaceID != workspaceID {
		return nil, &types.ErrAgentRunNotFound{ID: runID}
	}
	return run, nil
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
	task, ok := f.tasks[taskID]
	if !ok {
		return nil, &types.ErrRunExecutionNotFound{ExternalId: taskID}
	}
	return task, nil
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
		ConfigJSON:  map[string]any{},
		Active:      true,
	}
	svc := NewAgentService(context.Background(), backend, nil, redisClient, nil, "ghcr.io/beam/sandbox:latest")

	params := AgentCommandParams{
		Message:        "hello world",
		AgentID:        &agentID,
		SessionID:      "session-1",
		IdempotencyKey: "idem-1",
	}

	envelope, deduped, err := svc.AcceptAgentCommand(context.Background(), 42, params)
	require.NoError(t, err)
	require.False(t, deduped)
	require.NotEmpty(t, envelope.ID)
	require.Equal(t, types.AgentTaskStateQueued, envelope.State)
	require.Nil(t, envelope.TargetRunID, "run should not exist at acceptance time")

	queueLen, err := redisClient.LLen(context.Background(), common.Keys.TaskQueue()).Result()
	require.NoError(t, err)
	require.EqualValues(t, 1, queueLen)

	again, deduped, err := svc.AcceptAgentCommand(context.Background(), 42, params)
	require.NoError(t, err)
	require.True(t, deduped)
	require.Equal(t, envelope.ID, again.ID)

	queueLen, err = redisClient.LLen(context.Background(), common.Keys.TaskQueue()).Result()
	require.NoError(t, err)
	require.EqualValues(t, 1, queueLen, "idempotent replay must not enqueue duplicate work")
}

func TestQueueReshapingDropsOlderFollowupEnvelope(t *testing.T) {
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
		State:          types.AgentTaskStateAccepted,
		IdempotencyKey: "f1",
	}
	second := &types.AgentTask{
		ID:             uuid.NewString(),
		WorkspaceID:    1,
		Kind:           types.AgentTaskKindRunInput,
		QueueMode:      types.AgentQueueModeFollowup,
		State:          types.AgentTaskStateAccepted,
		IdempotencyKey: "f2",
	}

	backend.envelopes[first.ID] = first
	backend.envelopes[second.ID] = second

	require.NoError(t, router.Enqueue(ctx, first, instanceKey))
	require.NoError(t, router.Enqueue(ctx, second, instanceKey))

	require.Equal(t, types.AgentTaskStateDropped, backend.envelopes[first.ID].State)
	require.Equal(t, types.AgentTaskStateQueued, backend.envelopes[second.ID].State)

	token, err := router.Pop(ctx, 0)
	require.NoError(t, err)
	require.NotEmpty(t, token)

	envelopeID, err := router.ResolveTaskID(ctx, token)
	require.NoError(t, err)
	require.Equal(t, second.ID, envelopeID)
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
			"provider": "claude",
			"model":    "claude-sonnet-4",
			"purpose":  "test",
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

	envelope, deduped, err := svc.AcceptAgentCommand(context.Background(), 42, params)
	require.NoError(t, err)
	require.False(t, deduped)
	require.Equal(t, "claude", envelope.PayloadJSON["provider"])
	require.Equal(t, "claude-sonnet-4", envelope.PayloadJSON["model"])
	require.Equal(t, map[string]any{
		"provider": "claude",
		"model":    "claude-sonnet-4",
		"purpose":  "test",
	}, envelope.PayloadJSON["agent_config"])
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
		ConfigJSON:  map[string]any{},
		Active:      true,
	}
	svc := NewAgentService(context.Background(), backend, nil, redisClient, nil, "ghcr.io/beam/sandbox:latest")

	envelope, deduped, err := svc.AcceptAgentCommand(context.Background(), 42, AgentCommandParams{
		Message: "hello world",
		AgentID: &agentID,
	})
	require.NoError(t, err)
	require.False(t, deduped)
	require.NotEmpty(t, envelope.IdempotencyKey)
	require.NotEmpty(t, envelope.PayloadJSON["session_id"])
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
	envelope, deduped, err := svc.AcceptRunInput(context.Background(), 42, runID, types.AgentQueueModeFollowup, "follow up", "")
	require.NoError(t, err)
	require.False(t, deduped)
	require.NotEmpty(t, envelope.IdempotencyKey)
}

func TestTrySteerRunInputEnvelopeInjectsInteractiveInput(t *testing.T) {
	backend := newFakeBackend()
	runID := uuid.NewString()
	taskID := uuid.NewString()
	backend.runs[runID] = &types.AgentRun{
		ID:          runID,
		WorkspaceID: 42,
		Status:      types.AgentRunStatusRunning,
		SessionID:   "session-1",
	}
	backend.tasks[taskID] = &types.RunExecution{
		ExternalId: taskID,
		Type:       types.RunExecutionTypeInteractive,
		Status:     types.RunExecutionStatusRunning,
	}
	backend.attempts[runID] = []*types.AgentRunAttempt{
		{
			ID:                      uuid.NewString(),
			RunID:                   runID,
			AttemptNo:               1,
			Status:                  types.AgentAttemptStatusRunning,
			ExecutionID:             &taskID,
		},
	}

	envelopeID := uuid.NewString()
	envelope := &types.AgentTask{
		ID:          envelopeID,
		WorkspaceID: 42,
		Kind:        types.AgentTaskKindRunInput,
		QueueMode:   types.AgentQueueModeSteer,
		State:       types.AgentTaskStateQueued,
		PayloadJSON: map[string]any{"message": "please stop"},
		TargetRunID: &runID,
	}
	backend.envelopes[envelopeID] = envelope

	svc := NewAgentService(context.Background(), backend, nil, nil, nil, "ghcr.io/beam/sandbox:latest")
	terminalIO := newFakeTerminalIO()
	svc.terminalIO = terminalIO

	steered, err := svc.trySteerRunInputEnvelope(context.Background(), envelope)
	require.NoError(t, err)
	require.True(t, steered)
	require.Equal(t, types.AgentTaskStateDispatched, backend.envelopes[envelopeID].State)

	writes := terminalIO.inputs[taskID]
	require.Len(t, writes, 1)
	require.Equal(t, "please stop\n", string(writes[0]))
}

func TestTrySteerRunInputEnvelopeFallsBackWhenTaskNotInteractive(t *testing.T) {
	backend := newFakeBackend()
	runID := uuid.NewString()
	taskID := uuid.NewString()
	backend.runs[runID] = &types.AgentRun{
		ID:          runID,
		WorkspaceID: 42,
		Status:      types.AgentRunStatusRunning,
		SessionID:   "session-1",
	}
	backend.tasks[taskID] = &types.RunExecution{
		ExternalId: taskID,
		Type:       types.RunExecutionTypeBackground,
		Status:     types.RunExecutionStatusRunning,
	}
	backend.attempts[runID] = []*types.AgentRunAttempt{
		{
			ID:                      uuid.NewString(),
			RunID:                   runID,
			AttemptNo:               1,
			Status:                  types.AgentAttemptStatusRunning,
			ExecutionID:             &taskID,
		},
	}

	envelopeID := uuid.NewString()
	envelope := &types.AgentTask{
		ID:          envelopeID,
		WorkspaceID: 42,
		Kind:        types.AgentTaskKindRunInput,
		QueueMode:   types.AgentQueueModeSteer,
		State:       types.AgentTaskStateQueued,
		PayloadJSON: map[string]any{"message": "fallback"},
		TargetRunID: &runID,
	}
	backend.envelopes[envelopeID] = envelope

	svc := NewAgentService(context.Background(), backend, nil, nil, nil, "ghcr.io/beam/sandbox:latest")
	terminalIO := newFakeTerminalIO()
	svc.terminalIO = terminalIO

	steered, err := svc.trySteerRunInputEnvelope(context.Background(), envelope)
	require.NoError(t, err)
	require.False(t, steered)
	require.Equal(t, types.AgentTaskStateQueued, backend.envelopes[envelopeID].State)
	require.Empty(t, terminalIO.inputs[taskID])
}

func TestHandleRunInputEnvelopeDropsWhenTargetRunTerminal(t *testing.T) {
	backend := newFakeBackend()
	runID := uuid.NewString()
	backend.runs[runID] = &types.AgentRun{
		ID:          runID,
		WorkspaceID: 42,
		Status:      types.AgentRunStatusCancelled,
		SessionID:   "session-1",
	}

	envelopeID := uuid.NewString()
	envelope := &types.AgentTask{
		ID:          envelopeID,
		WorkspaceID: 42,
		Kind:        types.AgentTaskKindRunInput,
		QueueMode:   types.AgentQueueModeSteer,
		State:       types.AgentTaskStateQueued,
		PayloadJSON: map[string]any{"message": "fallback"},
		TargetRunID: &runID,
	}
	backend.envelopes[envelopeID] = envelope

	svc := NewAgentService(context.Background(), backend, nil, nil, nil, "ghcr.io/beam/sandbox:latest")
	err := svc.handleRunInputEnvelope(context.Background(), envelope)
	require.NoError(t, err)
	require.Equal(t, types.AgentTaskStateDropped, backend.envelopes[envelopeID].State)
	require.NotNil(t, backend.envelopes[envelopeID].DroppedReason)
	require.Equal(t, types.AgentTaskDropReasonRunInputTerminalTarget, *backend.envelopes[envelopeID].DroppedReason)
}
