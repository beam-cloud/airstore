package orchestration

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

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

func (f *fakeBackend) ArchiveTask(_ context.Context, taskID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	task, ok := f.agentTasks[taskID]
	if !ok {
		return &types.ErrAgentTaskNotFound{ID: taskID}
	}
	now := time.Now()
	task.ArchivedAt = &now
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

func (f *fakeBackend) DeleteAgentProfile(_ context.Context, workspaceID uint, agentID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	profile, ok := f.profiles[agentID]
	if !ok || profile.WorkspaceID != workspaceID {
		return &types.ErrAgentProfileNotFound{ID: agentID}
	}
	delete(f.profiles, agentID)
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

func (f *fakeBackend) ListAgentRuns(ctx context.Context, workspaceID uint, limit int) ([]*types.AgentRun, error) {
	return f.ListAgentRunsFiltered(ctx, workspaceID, types.AgentRunListFilter{Limit: limit})
}

func (f *fakeBackend) ListAgentRunsFiltered(_ context.Context, workspaceID uint, filter types.AgentRunListFilter) ([]*types.AgentRun, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	agentID := ""
	if filter.AgentID != nil {
		agentID = strings.TrimSpace(*filter.AgentID)
	}
	sessionID := ""
	if filter.SessionID != nil {
		sessionID = strings.TrimSpace(*filter.SessionID)
	}
	statuses := map[types.AgentRunStatus]struct{}{}
	for _, status := range filter.Statuses {
		if status == "" {
			continue
		}
		statuses[status] = struct{}{}
	}

	out := make([]*types.AgentRun, 0, len(f.runs))
	for _, run := range f.runs {
		if run == nil || run.WorkspaceID != workspaceID {
			continue
		}
		if agentID != "" {
			if run.AgentID == nil || strings.TrimSpace(*run.AgentID) != agentID {
				continue
			}
		}
		if sessionID != "" && strings.TrimSpace(run.SessionID) != sessionID {
			continue
		}
		if len(statuses) > 0 {
			if _, ok := statuses[run.Status]; !ok {
				continue
			}
		}
		out = append(out, run)
	}

	sort.Slice(out, func(i, j int) bool {
		if out[i].CreatedAt.Equal(out[j].CreatedAt) {
			return out[i].ID > out[j].ID
		}
		return out[i].CreatedAt.After(out[j].CreatedAt)
	})

	offset := filter.Offset
	if offset < 0 {
		offset = 0
	}
	if offset > len(out) {
		offset = len(out)
	}
	out = out[offset:]

	limit := filter.Limit
	if limit <= 0 {
		limit = len(out)
	}
	if limit < len(out) {
		out = out[:limit]
	}

	result := make([]*types.AgentRun, 0, len(out))
	result = append(result, out...)
	return result, nil
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

func (f *fakeBackend) UpdateAgentRunLifecycle(_ context.Context, runID string, status types.AgentRunStatus, startedAt, endedAt *time.Time, errorMsg *string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	run, ok := f.runs[runID]
	if !ok {
		return &types.ErrAgentRunNotFound{ID: runID}
	}
	run.Status = status
	run.StartedAt = startedAt
	run.EndedAt = endedAt
	run.Error = errorMsg
	return nil
}

func (f *fakeBackend) ListActiveRunsBySession(_ context.Context, workspaceID uint, sessionID string, excludeRunIDs []string, limit int) ([]*types.AgentRun, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" {
		return []*types.AgentRun{}, nil
	}
	exclude := make(map[string]struct{}, len(excludeRunIDs))
	for _, runID := range excludeRunIDs {
		runID = strings.TrimSpace(runID)
		if runID == "" {
			continue
		}
		exclude[runID] = struct{}{}
	}

	out := make([]*types.AgentRun, 0)
	for _, run := range f.runs {
		if run == nil || run.WorkspaceID != workspaceID || !run.Status.IsActive() {
			continue
		}
		if strings.TrimSpace(run.SessionID) != sessionID {
			continue
		}
		if _, skip := exclude[run.ID]; skip {
			continue
		}
		out = append(out, run)
		if limit > 0 && len(out) >= limit {
			break
		}
	}
	return out, nil
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

func (f *fakeBackend) CancelRunExecution(_ context.Context, taskID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	task, ok := f.runExecutions[taskID]
	if !ok {
		return &types.ErrRunExecutionNotFound{ExternalId: taskID}
	}
	task.Status = types.RunExecutionStatusCancelled
	return nil
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

func sessionLeaseKey(workspaceID uint, sessionID string) string {
	return fmt.Sprintf("%d:%s", workspaceID, sessionID)
}

func runInteractionKey(workspaceID uint, runID string) string {
	return fmt.Sprintf("%d:%s", workspaceID, runID)
}

type fakeTerminalIO struct {
	mu            sync.Mutex
	inputs        map[string][][]byte
	publishErr    error
	sessionLeases map[string]string
	interactions  map[string]types.RunInteraction
}

func newFakeTerminalIO() *fakeTerminalIO {
	return &fakeTerminalIO{
		inputs:        map[string][][]byte{},
		sessionLeases: map[string]string{},
		interactions:  map[string]types.RunInteraction{},
	}
}

func (f *fakeTerminalIO) PublishInput(_ context.Context, taskID string, data []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.publishErr != nil {
		return f.publishErr
	}
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

func (f *fakeTerminalIO) ListPendingInputs(_ context.Context, _ string) ([]types.PendingInput, error) {
	return nil, nil
}

func (f *fakeTerminalIO) PublishCancel(_ context.Context, _ string) error { return nil }

func (f *fakeTerminalIO) SubscribeCancel(_ context.Context, _ string) (<-chan struct{}, func(), error) {
	ch := make(chan struct{})
	close(ch)
	return ch, func() {}, nil
}

func (f *fakeTerminalIO) AcquireSessionLease(_ context.Context, workspaceID uint, sessionID string, ownerID string, _ time.Duration) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	key := sessionLeaseKey(workspaceID, sessionID)
	current, exists := f.sessionLeases[key]
	if !exists || current == ownerID {
		f.sessionLeases[key] = ownerID
		return true, nil
	}
	return false, nil
}

func (f *fakeTerminalIO) RenewSessionLease(_ context.Context, workspaceID uint, sessionID string, ownerID string, _ time.Duration) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	key := sessionLeaseKey(workspaceID, sessionID)
	return f.sessionLeases[key] == ownerID, nil
}

func (f *fakeTerminalIO) ReleaseSessionLease(_ context.Context, workspaceID uint, sessionID string, ownerID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	key := sessionLeaseKey(workspaceID, sessionID)
	if f.sessionLeases[key] == ownerID {
		delete(f.sessionLeases, key)
	}
	return nil
}

func (f *fakeTerminalIO) GetSessionLeaseOwner(_ context.Context, workspaceID uint, sessionID string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	key := sessionLeaseKey(workspaceID, sessionID)
	return f.sessionLeases[key], nil
}

func (f *fakeTerminalIO) SetRunInteraction(_ context.Context, workspaceID uint, runID string, state types.RunInteractionState, activeExecutionID string, _ time.Duration) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	key := runInteractionKey(workspaceID, runID)
	f.interactions[key] = types.RunInteraction{
		State:             state,
		ActiveExecutionID: activeExecutionID,
		UpdatedAt:         time.Now().UnixMilli(),
	}
	return nil
}

func (f *fakeTerminalIO) GetRunInteraction(_ context.Context, workspaceID uint, runID string) (*types.RunInteraction, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	key := runInteractionKey(workspaceID, runID)
	interaction, ok := f.interactions[key]
	if !ok {
		return nil, nil
	}
	copy := interaction
	return &copy, nil
}

func (f *fakeTerminalIO) ClearRunInteraction(_ context.Context, workspaceID uint, runID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.interactions, runInteractionKey(workspaceID, runID))
	return nil
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

func TestQueueRouterFollowupReshapesToLatestTask(t *testing.T) {
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
	require.NotNil(t, backend.agentTasks[first.ID].DroppedReason)
	require.Equal(t, types.AgentTaskDropReasonReshapedByQueueMode, *backend.agentTasks[first.ID].DroppedReason)
	require.Equal(t, types.AgentTaskStateQueued, backend.agentTasks[second.ID].State)

	token, err := router.Pop(ctx, 0)
	require.NoError(t, err)
	require.NotEmpty(t, token)

	taskID, err := router.ResolveTaskID(ctx, token)
	require.NoError(t, err)
	require.Equal(t, second.ID, taskID)

	token, err = router.Pop(ctx, 0)
	require.NoError(t, err)
	require.Empty(t, token)
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

func TestAcceptAgentCommandWithActiveSessionRunEnqueuesFollowup(t *testing.T) {
	redisClient, cleanup := newTestRedis(t)
	defer cleanup()

	backend := newFakeBackend()
	agentID := uuid.NewString()
	runID := uuid.NewString()
	originTaskID := uuid.NewString()
	sessionID := "session-active"
	model := "claude-sonnet-4-6"
	backend.profiles[agentID] = &types.AgentProfile{
		ID:          agentID,
		WorkspaceID: 42,
		AgentKey:    "agent-key",
		Name:        "Agent",
		ConfigJSON: map[string]any{
			agentConfigKeyRunner: AgentRunnerClaudeCode,
			agentConfigKeyModel:  model,
		},
		Active: true,
	}
	backend.agentTasks[originTaskID] = &types.AgentTask{
		ID:          originTaskID,
		WorkspaceID: 42,
		AgentID:     &agentID,
		Kind:        types.AgentTaskKindAgentCommand,
		QueueMode:   types.AgentQueueModeQueue,
		State:       types.AgentTaskStateRunning,
		PayloadJSON: map[string]any{
			"message":              "original prompt",
			"session_id":           sessionID,
			agentConfigKeyProvider: AgentProviderClaude,
			agentConfigKeyModel:    model,
		},
	}
	backend.runs[runID] = &types.AgentRun{
		ID:           runID,
		WorkspaceID:  42,
		AgentID:      &agentID,
		OriginTaskID: originTaskID,
		Status:       types.AgentRunStatusRunning,
		SessionID:    sessionID,
		TimeoutMs:    60000,
		Provider:     strPtr(AgentProviderClaude),
		Model:        &model,
		CreatedAt:    time.Now(),
	}

	svc := NewAgentService(context.Background(), backend, nil, redisClient, nil, "ghcr.io/beam/sandbox:latest")
	task, deduped, err := svc.AcceptAgentCommand(context.Background(), 42, AgentCommandParams{
		Message:        "follow up",
		AgentID:        &agentID,
		SessionID:      sessionID,
		IdempotencyKey: "idem-session-active",
	})
	require.NoError(t, err)
	require.False(t, deduped)
	require.NotNil(t, task)
	require.Equal(t, types.AgentTaskKindRunInput, task.Kind)
	require.Equal(t, types.AgentTaskStateQueued, task.State)
	require.NotNil(t, task.TargetRunID)
	require.Equal(t, runID, *task.TargetRunID)
	require.Len(t, backend.agentTasks, 2, "active session should enqueue run input instead of creating a new command task")

	queueLen, err := redisClient.LLen(context.Background(), common.Keys.TaskQueue()).Result()
	require.NoError(t, err)
	require.EqualValues(t, 1, queueLen)
}

func TestAcceptAgentCommandWithTerminalSessionRunRestartsOnSameTask(t *testing.T) {
	redisClient, cleanup := newTestRedis(t)
	defer cleanup()

	backend := newFakeBackend()
	agentID := uuid.NewString()
	originTaskID := uuid.NewString()
	runID := uuid.NewString()
	sessionID := "session-terminal"
	sessionKey := "session-key"
	model := "claude-sonnet-4-6"
	taskQueue := repository.NewRedisTaskQueue(redisClient, "default")
	backend.profiles[agentID] = &types.AgentProfile{
		ID:          agentID,
		WorkspaceID: 42,
		AgentKey:    "agent-key",
		Name:        "Agent",
		ConfigJSON: map[string]any{
			agentConfigKeyRunner: AgentRunnerClaudeCode,
			agentConfigKeyModel:  model,
		},
		Active: true,
	}
	backend.agentTasks[originTaskID] = &types.AgentTask{
		ID:          originTaskID,
		WorkspaceID: 42,
		AgentID:     &agentID,
		Kind:        types.AgentTaskKindAgentCommand,
		QueueMode:   types.AgentQueueModeQueue,
		State:       types.AgentTaskStateDone,
		PayloadJSON: map[string]any{
			"message":              "original prompt",
			"session_id":           sessionID,
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
		SessionID:    sessionID,
		SessionKey:   &sessionKey,
		TimeoutMs:    60000,
		Provider:     strPtr(AgentProviderClaude),
		Model:        &model,
		CreatedAt:    time.Now(),
	}

	svc := NewAgentService(context.Background(), backend, taskQueue, redisClient, nil, "ghcr.io/beam/sandbox:latest")
	task, deduped, err := svc.AcceptAgentCommand(context.Background(), 42, AgentCommandParams{
		Message:        "follow up",
		AgentID:        &agentID,
		SessionID:      sessionID,
		IdempotencyKey: "idem-session-terminal",
	})
	require.NoError(t, err)
	require.False(t, deduped)
	require.NotNil(t, task)
	require.Equal(t, originTaskID, task.ID)
	require.Equal(t, types.AgentTaskStateRunning, task.State)
	require.NotNil(t, task.TargetRunID)
	require.NotEqual(t, runID, *task.TargetRunID)
	require.Len(t, backend.agentTasks, 1, "terminal resume should restart on existing origin task")

	newRun, ok := backend.runs[*task.TargetRunID]
	require.True(t, ok)
	require.Equal(t, originTaskID, newRun.OriginTaskID)
	require.Equal(t, sessionID, newRun.SessionID)
	require.NotNil(t, newRun.SessionKey)
	require.Equal(t, sessionKey, *newRun.SessionKey)

	require.NotEmpty(t, backend.runExecutions)
	require.Len(t, backend.runExecutions, 1)
	for _, exec := range backend.runExecutions {
		require.NotNil(t, exec)
		require.Equal(t, "true", exec.Env["AIRSTORE_AGENT_RESUME_SESSION"])
		require.Equal(t, sessionID, exec.Env["AIRSTORE_AGENT_SESSION_ID"])
	}
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
	task, deduped, outcome, err := svc.AcceptRunInput(context.Background(), 42, runID, types.AgentQueueModeFollowup, "follow up", "")
	require.NoError(t, err)
	require.False(t, deduped)
	require.NotEmpty(t, task.IdempotencyKey)
	require.NotEmpty(t, outcome)
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
	task, deduped, outcome, err := svc.AcceptRunInput(
		context.Background(),
		42,
		runID,
		types.AgentQueueModeFollowup,
		"follow up",
		"",
	)
	require.NoError(t, err)
	require.False(t, deduped)
	require.Equal(t, types.RunInputDeliveryRestarted, outcome)
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
		require.Equal(t, "session-1", exec.Env["AIRSTORE_AGENT_SESSION_ID"])
	}
}

func TestAcceptRunInputRestartBlocksOnActiveSessionConflict(t *testing.T) {
	redisClient, cleanup := newTestRedis(t)
	defer cleanup()

	backend := newFakeBackend()
	agentID := uuid.NewString()
	originTaskID := uuid.NewString()
	runID := uuid.NewString()
	sessionID := "session-1"
	conflictingRunID := uuid.NewString()
	taskQueue := repository.NewRedisTaskQueue(redisClient, "default")

	backend.agentTasks[originTaskID] = &types.AgentTask{
		ID:          originTaskID,
		WorkspaceID: 42,
		AgentID:     &agentID,
		Kind:        types.AgentTaskKindAgentCommand,
		QueueMode:   types.AgentQueueModeQueue,
		State:       types.AgentTaskStateDone,
		PayloadJSON: map[string]any{
			"message":    "original prompt",
			"session_id": sessionID,
			"timeout_ms": 60000,
		},
	}
	backend.runs[runID] = &types.AgentRun{
		ID:           runID,
		WorkspaceID:  42,
		AgentID:      &agentID,
		OriginTaskID: originTaskID,
		Status:       types.AgentRunStatusOK,
		SessionID:    sessionID,
		TimeoutMs:    60000,
	}
	backend.runs[conflictingRunID] = &types.AgentRun{
		ID:           conflictingRunID,
		WorkspaceID:  42,
		AgentID:      &agentID,
		OriginTaskID: uuid.NewString(),
		Status:       types.AgentRunStatusRunning,
		SessionID:    sessionID,
		TimeoutMs:    60000,
	}

	svc := NewAgentService(context.Background(), backend, taskQueue, redisClient, nil, "ghcr.io/beam/sandbox:latest")
	_, _, _, err := svc.AcceptRunInput(
		context.Background(),
		42,
		runID,
		types.AgentQueueModeFollowup,
		"follow up",
		"",
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "session ID session-1 is already in use")
}

func TestAcceptRunInputRestartAllowsSessionAfterConflictClears(t *testing.T) {
	redisClient, cleanup := newTestRedis(t)
	defer cleanup()

	backend := newFakeBackend()
	agentID := uuid.NewString()
	originTaskID := uuid.NewString()
	runID := uuid.NewString()
	sessionID := "session-1"
	conflictingRunID := uuid.NewString()
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
			"session_id":           sessionID,
			agentConfigKeyProvider: AgentProviderClaude,
		},
	}
	backend.runs[runID] = &types.AgentRun{
		ID:           runID,
		WorkspaceID:  42,
		AgentID:      &agentID,
		OriginTaskID: originTaskID,
		Status:       types.AgentRunStatusOK,
		SessionID:    sessionID,
		TimeoutMs:    60000,
		Provider:     strPtr(AgentProviderClaude),
	}
	backend.runs[conflictingRunID] = &types.AgentRun{
		ID:           conflictingRunID,
		WorkspaceID:  42,
		AgentID:      &agentID,
		OriginTaskID: uuid.NewString(),
		Status:       types.AgentRunStatusOK,
		SessionID:    sessionID,
		TimeoutMs:    60000,
		Provider:     strPtr(AgentProviderClaude),
	}

	svc := NewAgentService(context.Background(), backend, taskQueue, redisClient, nil, "ghcr.io/beam/sandbox:latest")
	task, deduped, outcome, err := svc.AcceptRunInput(
		context.Background(),
		42,
		runID,
		types.AgentQueueModeFollowup,
		"follow up",
		"",
	)
	require.NoError(t, err)
	require.False(t, deduped)
	require.Equal(t, types.RunInputDeliveryRestarted, outcome)
	require.NotNil(t, task)
	require.NotNil(t, task.TargetRunID)
	require.NotEqual(t, runID, *task.TargetRunID)
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

	task, deduped, outcome, err := svc.AcceptRunInput(
		context.Background(), 42, runID,
		types.AgentQueueModeFollowup, "follow up", "",
	)
	require.NoError(t, err)
	require.False(t, deduped)
	require.Equal(t, types.RunInputDeliveryDirect, outcome)
	require.NotNil(t, task)
	require.Equal(t, originTaskID, task.ID, "should return the origin task, not create a new one")

	writes := terminalIO.inputs[executionID]
	require.Len(t, writes, 1)
	require.Equal(t, "follow up\n", string(writes[0]))

	taskCountBefore := len(backend.agentTasks)
	require.Equal(t, 1, taskCountBefore, "no new RunInput task should be created")
}

func TestAcceptRunInputDeliversDirectlyWhenInteractionWaiting(t *testing.T) {
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
		SessionID:    "session-waiting-direct",
	}

	terminalIO := newFakeTerminalIO()
	require.NoError(
		t,
		terminalIO.SetRunInteraction(
			context.Background(),
			42,
			runID,
			types.RunInteractionStateWaitingForInput,
			executionID,
			time.Minute,
		),
	)

	svc := NewAgentService(context.Background(), backend, nil, redisClient, nil, "ghcr.io/beam/sandbox:latest")
	svc.terminalIO = terminalIO

	task, deduped, outcome, err := svc.AcceptRunInput(
		context.Background(),
		42,
		runID,
		types.AgentQueueModeFollowup,
		"follow up while waiting",
		"",
	)
	require.NoError(t, err)
	require.False(t, deduped)
	require.Equal(t, types.RunInputDeliveryDirect, outcome)
	require.NotNil(t, task)
	require.Equal(t, originTaskID, task.ID)
	require.Len(t, terminalIO.inputs[executionID], 1)
	require.Equal(t, "follow up while waiting\n", string(terminalIO.inputs[executionID][0]))
	require.Len(t, backend.agentTasks, 1, "direct waiting input should not create run_input task")
}

func TestAcceptRunInputRestartsWhenInteractionClosedEvenIfRunStillRunning(t *testing.T) {
	redisClient, cleanup := newTestRedis(t)
	defer cleanup()

	backend := newFakeBackend()
	agentID := uuid.NewString()
	runID := uuid.NewString()
	originTaskID := uuid.NewString()
	sessionID := "session-closed-restart"

	backend.agentTasks[originTaskID] = &types.AgentTask{
		ID:          originTaskID,
		WorkspaceID: 42,
		AgentID:     &agentID,
		Kind:        types.AgentTaskKindAgentCommand,
		State:       types.AgentTaskStateRunning,
		PayloadJSON: map[string]any{
			"message":    "initial",
			"session_id": sessionID,
			"timeout_ms": 60000,
			"provider":   AgentProviderClaude,
			"model":      "claude-sonnet-4-6",
		},
		TargetRunID: &runID,
	}
	backend.runs[runID] = &types.AgentRun{
		ID:           runID,
		WorkspaceID:  42,
		AgentID:      &agentID,
		OriginTaskID: originTaskID,
		Status:       types.AgentRunStatusRunning, // stale lifecycle while worker already closed
		SessionID:    sessionID,
		TimeoutMs:    60000,
		Provider:     strPtr(AgentProviderClaude),
		Model:        strPtr("claude-sonnet-4-6"),
	}

	terminalIO := newFakeTerminalIO()
	require.NoError(
		t,
		terminalIO.SetRunInteraction(
			context.Background(),
			42,
			runID,
			types.RunInteractionStateClosed,
			"",
			time.Minute,
		),
	)

	taskQueue := repository.NewRedisTaskQueue(redisClient, "default")
	svc := NewAgentService(context.Background(), backend, taskQueue, redisClient, nil, "ghcr.io/beam/sandbox:latest")
	svc.terminalIO = terminalIO

	task, deduped, outcome, err := svc.AcceptRunInput(
		context.Background(),
		42,
		runID,
		types.AgentQueueModeFollowup,
		"continue from closed interaction",
		"",
	)
	require.NoError(t, err)
	require.False(t, deduped)
	require.Equal(t, types.RunInputDeliveryRestarted, outcome)
	require.NotNil(t, task)
	require.NotNil(t, task.TargetRunID)
	require.NotEqual(t, runID, *task.TargetRunID, "closed interaction should force a new run")
}

func TestAcceptRunInputInterruptDispatchesWithoutCreatingTaskForActiveInteractiveRun(t *testing.T) {
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
		TimeoutMs:    60000,
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
	taskQueue := repository.NewRedisTaskQueue(redisClient, "default")
	svc := NewAgentService(context.Background(), backend, taskQueue, redisClient, nil, "ghcr.io/beam/sandbox:latest")
	svc.terminalIO = terminalIO

	task, deduped, outcome, err := svc.AcceptRunInput(
		context.Background(), 42, runID,
		types.AgentQueueModeInterrupt, "send now", "",
	)
	require.NoError(t, err)
	require.False(t, deduped)
	require.Equal(t, types.RunInputDeliveryInterrupted, outcome)
	require.NotNil(t, task)
	require.Equal(t, originTaskID, task.ID, "interrupt should continue the origin task")
	require.Empty(t, terminalIO.inputs[executionID], "interrupt mode should not inject directly")
	require.Len(t, backend.agentTasks, 1, "interrupt should not create extra task records")
	require.Len(t, backend.attempts[runID], 2, "interrupt should schedule a replacement attempt")
	require.NotEmpty(t, backend.runExecutions)
}

func TestBuildRunInputPayloadSetsResumeSession(t *testing.T) {
	run := &types.AgentRun{
		ID:          uuid.NewString(),
		WorkspaceID: 42,
		SessionID:   "session-xyz",
		TimeoutMs:   60000,
	}
	payload := buildRunInputPayload(run, "follow up")
	require.Equal(t, "follow up", payload["message"])
	require.Equal(t, "session-xyz", payload["session_id"])
	require.Equal(t, true, payload["resume_session"])
}

func TestAcceptRunInputFollowupEnqueuedReturnsQueuedOutcome(t *testing.T) {
	redisClient, cleanup := newTestRedis(t)
	defer cleanup()

	backend := newFakeBackend()
	runID := uuid.NewString()
	originTaskID := uuid.NewString()

	backend.runs[runID] = &types.AgentRun{
		ID:           runID,
		WorkspaceID:  42,
		OriginTaskID: originTaskID,
		Status:       types.AgentRunStatusRunning,
		SessionID:    "session-enqueue-test",
		Interactive:  true,
		TimeoutMs:    60000,
	}
	backend.agentTasks[originTaskID] = &types.AgentTask{
		ID:          originTaskID,
		WorkspaceID: 42,
	}

	taskQueue := repository.NewRedisTaskQueue(redisClient, "default")
	svc := NewAgentService(context.Background(), backend, taskQueue, redisClient, nil, "ghcr.io/beam/sandbox:latest")

	_, _, outcome, err := svc.AcceptRunInput(
		context.Background(), 42, runID,
		types.AgentQueueModeFollowup, "test", "",
	)
	require.NoError(t, err)
	require.Equal(t, types.RunInputDeliveryQueued, outcome)
}

func TestAcceptRunInputQueuesWhenInteractionWorking(t *testing.T) {
	redisClient, cleanup := newTestRedis(t)
	defer cleanup()

	backend := newFakeBackend()
	agentID := uuid.NewString()
	runID := uuid.NewString()
	originTaskID := uuid.NewString()
	executionID := uuid.NewString()

	backend.runs[runID] = &types.AgentRun{
		ID:           runID,
		WorkspaceID:  42,
		AgentID:      &agentID,
		OriginTaskID: originTaskID,
		Status:       types.AgentRunStatusRunning,
		SessionID:    "session-working-queue",
		Interactive:  true,
		TimeoutMs:    60000,
	}
	backend.agentTasks[originTaskID] = &types.AgentTask{
		ID:          originTaskID,
		WorkspaceID: 42,
		AgentID:     &agentID,
	}

	terminalIO := newFakeTerminalIO()
	require.NoError(
		t,
		terminalIO.SetRunInteraction(
			context.Background(),
			42,
			runID,
			types.RunInteractionStateWorking,
			executionID,
			time.Minute,
		),
	)

	svc := NewAgentService(context.Background(), backend, nil, redisClient, nil, "ghcr.io/beam/sandbox:latest")
	svc.terminalIO = terminalIO

	task, deduped, outcome, err := svc.AcceptRunInput(
		context.Background(),
		42,
		runID,
		types.AgentQueueModeFollowup,
		"queue me",
		"idem-working-1",
	)
	require.NoError(t, err)
	require.False(t, deduped)
	require.Equal(t, types.RunInputDeliveryQueued, outcome)
	require.NotNil(t, task)
	require.Equal(t, types.AgentTaskKindRunInput, task.Kind)
	require.Empty(t, terminalIO.inputs[executionID], "working state should queue, not inject directly")

	queueLen, err := redisClient.LLen(context.Background(), common.Keys.TaskQueue()).Result()
	require.NoError(t, err)
	require.EqualValues(t, 1, queueLen)
}

func TestAcceptRunInputRestartUsesResumeBarrier(t *testing.T) {
	redisClient, cleanup := newTestRedis(t)
	defer cleanup()

	backend := newFakeBackend()
	agentID := uuid.NewString()
	runID := uuid.NewString()
	originTaskID := uuid.NewString()
	sessionID := "session-resume-barrier"

	backend.agentTasks[originTaskID] = &types.AgentTask{
		ID:          originTaskID,
		WorkspaceID: 42,
		AgentID:     &agentID,
		Kind:        types.AgentTaskKindAgentCommand,
		State:       types.AgentTaskStateDone,
		PayloadJSON: map[string]any{
			"message":    "initial",
			"session_id": sessionID,
			"timeout_ms": 60000,
			"provider":   AgentProviderClaude,
			"model":      "claude-sonnet-4-6",
		},
	}
	backend.runs[runID] = &types.AgentRun{
		ID:           runID,
		WorkspaceID:  42,
		AgentID:      &agentID,
		OriginTaskID: originTaskID,
		Status:       types.AgentRunStatusOK,
		SessionID:    sessionID,
		TimeoutMs:    60000,
		Provider:     strPtr(AgentProviderClaude),
	}

	terminalIO := newFakeTerminalIO()
	terminalIO.sessionLeases[sessionLeaseKey(42, sessionID)] = "other-owner"

	svc := NewAgentService(context.Background(), backend, nil, redisClient, nil, "ghcr.io/beam/sandbox:latest")
	svc.terminalIO = terminalIO

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()

	_, _, _, err := svc.AcceptRunInput(
		ctx,
		42,
		runID,
		types.AgentQueueModeFollowup,
		"resume this",
		"",
	)
	require.Error(t, err)
	require.Contains(t, strings.ToLower(err.Error()), "context deadline exceeded")
}

func TestAcceptRunInputIdenticalMessagesQueueAsDistinctTasks(t *testing.T) {
	redisClient, cleanup := newTestRedis(t)
	defer cleanup()

	backend := newFakeBackend()
	agentID := uuid.NewString()
	runID := uuid.NewString()
	originTaskID := uuid.NewString()
	executionID := uuid.NewString()

	backend.runs[runID] = &types.AgentRun{
		ID:           runID,
		WorkspaceID:  42,
		AgentID:      &agentID,
		OriginTaskID: originTaskID,
		Status:       types.AgentRunStatusRunning,
		SessionID:    "session-identical-queue",
		Interactive:  true,
		TimeoutMs:    60000,
	}
	backend.agentTasks[originTaskID] = &types.AgentTask{
		ID:          originTaskID,
		WorkspaceID: 42,
		AgentID:     &agentID,
	}

	terminalIO := newFakeTerminalIO()
	require.NoError(
		t,
		terminalIO.SetRunInteraction(
			context.Background(),
			42,
			runID,
			types.RunInteractionStateWorking,
			executionID,
			time.Minute,
		),
	)

	svc := NewAgentService(context.Background(), backend, nil, redisClient, nil, "ghcr.io/beam/sandbox:latest")
	svc.terminalIO = terminalIO

	task1, deduped1, outcome1, err1 := svc.AcceptRunInput(
		context.Background(),
		42,
		runID,
		types.AgentQueueModeFollowup,
		"same message",
		"idem-same-1",
	)
	require.NoError(t, err1)
	require.False(t, deduped1)
	require.Equal(t, types.RunInputDeliveryQueued, outcome1)
	require.NotNil(t, task1)

	task2, deduped2, outcome2, err2 := svc.AcceptRunInput(
		context.Background(),
		42,
		runID,
		types.AgentQueueModeFollowup,
		"same message",
		"idem-same-2",
	)
	require.NoError(t, err2)
	require.False(t, deduped2)
	require.Equal(t, types.RunInputDeliveryQueued, outcome2)
	require.NotNil(t, task2)
	require.NotEqual(t, task1.ID, task2.ID, "identical follow-up text must not collapse")
	require.Equal(t, types.AgentTaskStateDropped, backend.agentTasks[task1.ID].State)
	require.NotNil(t, backend.agentTasks[task1.ID].DroppedReason)
	require.Equal(t, types.AgentTaskDropReasonReshapedByQueueMode, *backend.agentTasks[task1.ID].DroppedReason)
	require.Equal(t, types.AgentTaskStateQueued, backend.agentTasks[task2.ID].State)

	queueLen, err := redisClient.LLen(context.Background(), common.Keys.TaskQueue()).Result()
	require.NoError(t, err)
	require.EqualValues(t, 1, queueLen)
}

func TestMaterializeRunBlockedBySessionLease(t *testing.T) {
	redisClient, cleanup := newTestRedis(t)
	defer cleanup()

	backend := newFakeBackend()
	terminalIO := newFakeTerminalIO()
	taskQueue := repository.NewRedisTaskQueue(redisClient, "default")

	svc := NewAgentService(context.Background(), backend, taskQueue, redisClient, nil, "ghcr.io/beam/sandbox:latest")
	svc.terminalIO = terminalIO

	sessionID := "session-lease-gate-test"
	workspaceID := uint(42)

	terminalIO.sessionLeases[sessionLeaseKey(workspaceID, sessionID)] = "task-other"

	task := &types.AgentTask{
		ID:          uuid.NewString(),
		WorkspaceID: workspaceID,
		Kind:        types.AgentTaskKindAgentCommand,
		AgentID:     strPtr("agent-1"),
		PayloadJSON: map[string]any{
			"message":    "hello",
			"session_id": sessionID,
			"timeout_ms": 60000,
			"provider":   AgentProviderClaude,
			"model":      "claude-sonnet-4-6",
		},
	}
	_ = backend.CreateTask(context.Background(), task)

	_, _, _, err := svc.materializeRun(context.Background(), task)
	require.Error(t, err)
	require.Contains(t, err.Error(), "session ID")
	require.Contains(t, err.Error(), "already in use")
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

func TestHandleRunInputTaskFollowupBuffersWhenInteractionWorking(t *testing.T) {
	backend := newFakeBackend()
	runID := uuid.NewString()
	executionID := uuid.NewString()
	backend.runs[runID] = &types.AgentRun{
		ID:          runID,
		WorkspaceID: 42,
		Status:      types.AgentRunStatusRunning,
		SessionID:   "session-working",
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
		PayloadJSON: map[string]any{"message": "queue while working"},
		TargetRunID: &runID,
	}
	backend.agentTasks[taskID] = task

	svc := NewAgentService(context.Background(), backend, nil, nil, nil, "ghcr.io/beam/sandbox:latest")
	terminalIO := newFakeTerminalIO()
	require.NoError(
		t,
		terminalIO.SetRunInteraction(
			context.Background(),
			42,
			runID,
			types.RunInteractionStateWorking,
			executionID,
			time.Minute,
		),
	)
	svc.terminalIO = terminalIO

	err := svc.handleRunInputTask(context.Background(), task)
	require.NoError(t, err)
	require.Equal(t, types.AgentTaskStateDone, backend.agentTasks[taskID].State)
	writes := terminalIO.inputs[executionID]
	require.Len(t, writes, 1)
	require.Equal(t, "queue while working\n", string(writes[0]))
	require.Len(t, backend.attempts[runID], 1, "should not create a replacement attempt while active execution exists")
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

func TestTrySteerRunInputTaskUsesPendingInFlightAttempt(t *testing.T) {
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
		Status:     types.RunExecutionStatusPending,
	}
	backend.attempts[runID] = []*types.AgentRunAttempt{
		{
			ID:          uuid.NewString(),
			RunID:       runID,
			AttemptNo:   2,
			Status:      types.AgentAttemptStatusPending,
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
		PayloadJSON: map[string]any{"message": "queued while turn active"},
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
	require.Equal(t, "queued while turn active\n", string(writes[0]))
}

func TestTrySteerRunInputTaskReturnsPublishError(t *testing.T) {
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
		PayloadJSON: map[string]any{"message": "publish error"},
		TargetRunID: &runID,
	}
	backend.agentTasks[taskID] = task

	svc := NewAgentService(context.Background(), backend, nil, nil, nil, "ghcr.io/beam/sandbox:latest")
	terminalIO := newFakeTerminalIO()
	terminalIO.publishErr = errors.New("redis unavailable")
	svc.terminalIO = terminalIO

	steered, err := svc.trySteerRunInputTask(context.Background(), task)
	require.Error(t, err)
	require.Contains(t, err.Error(), "publish interactive input")
	require.False(t, steered)
	require.Equal(t, types.AgentTaskStateQueued, backend.agentTasks[taskID].State)
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

func TestStaleSessionLeaseAutoCleared(t *testing.T) {
	backend := newFakeBackend()
	terminalIO := newFakeTerminalIO()

	executionID := uuid.NewString()
	staleOwner := "worker-dead:" + executionID

	terminalIO.sessionLeases[sessionLeaseKey(42, "session-stale")] = staleOwner

	svc := NewAgentService(context.Background(), backend, nil, nil, nil, "ghcr.io/beam/sandbox:latest")
	svc.terminalIO = terminalIO

	err := svc.ensureSessionAvailableForNewRun(context.Background(), 42, "session-stale")
	require.NoError(t, err, "stale lease (missing execution) should be auto-cleared")

	owner, _ := terminalIO.GetSessionLeaseOwner(context.Background(), 42, "session-stale")
	require.Empty(t, owner, "stale lease should have been released")
}

func TestStaleSessionLeaseAutoClearedTerminalExecution(t *testing.T) {
	backend := newFakeBackend()
	terminalIO := newFakeTerminalIO()

	executionID := uuid.NewString()
	staleOwner := "worker-dead:" + executionID
	backend.runExecutions[executionID] = &types.RunExecution{
		ExternalId: executionID,
		Status:     types.RunExecutionStatusComplete,
	}

	terminalIO.sessionLeases[sessionLeaseKey(42, "session-stale-terminal")] = staleOwner

	svc := NewAgentService(context.Background(), backend, nil, nil, nil, "ghcr.io/beam/sandbox:latest")
	svc.terminalIO = terminalIO

	err := svc.ensureSessionAvailableForNewRun(context.Background(), 42, "session-stale-terminal")
	require.NoError(t, err, "stale lease (terminal execution) should be auto-cleared")

	owner, _ := terminalIO.GetSessionLeaseOwner(context.Background(), 42, "session-stale-terminal")
	require.Empty(t, owner)
}

func TestActiveSessionLeaseNotForcedClear(t *testing.T) {
	backend := newFakeBackend()
	terminalIO := newFakeTerminalIO()

	executionID := uuid.NewString()
	activeOwner := "worker-alive:" + executionID
	backend.runExecutions[executionID] = &types.RunExecution{
		ExternalId: executionID,
		Status:     types.RunExecutionStatusRunning,
	}

	terminalIO.sessionLeases[sessionLeaseKey(42, "session-active")] = activeOwner

	svc := NewAgentService(context.Background(), backend, nil, nil, nil, "ghcr.io/beam/sandbox:latest")
	svc.terminalIO = terminalIO

	err := svc.ensureSessionAvailableForNewRun(context.Background(), 42, "session-active")
	require.Error(t, err)
	require.Contains(t, err.Error(), "already in use")

	owner, _ := terminalIO.GetSessionLeaseOwner(context.Background(), 42, "session-active")
	require.Equal(t, activeOwner, owner, "active lease must not be force-cleared")
}

func TestWaitForSessionLeaseDrainReconcilesStaleLease(t *testing.T) {
	backend := newFakeBackend()
	terminalIO := newFakeTerminalIO()

	executionID := uuid.NewString()
	staleOwner := "worker-dead:" + executionID

	terminalIO.sessionLeases[sessionLeaseKey(42, "session-drain-stale")] = staleOwner

	svc := NewAgentService(context.Background(), backend, nil, nil, nil, "ghcr.io/beam/sandbox:latest")
	svc.terminalIO = terminalIO

	err := svc.waitForSessionLeaseDrain(context.Background(), 42, "session-drain-stale")
	require.NoError(t, err, "stale lease should be reconciled during drain")
}

func TestIsSessionBusyError(t *testing.T) {
	require.True(t, isSessionBusyError(fmt.Errorf("session ID abc is already in use (lease: x)")))
	require.True(t, isSessionBusyError(fmt.Errorf("session abc still held by worker after drain timeout")))
	require.False(t, isSessionBusyError(fmt.Errorf("missing prompt/message in task payload")))
	require.False(t, isSessionBusyError(nil))
}

func TestExtractLeaseExecutionID(t *testing.T) {
	require.Equal(t, "exec-123", ExtractLeaseExecutionID("worker-1:exec-123"))
	require.Equal(t, "", ExtractLeaseExecutionID("no-colon"))
	require.Equal(t, "", ExtractLeaseExecutionID(""))
	require.Equal(t, "b", ExtractLeaseExecutionID("a:b"))
}
