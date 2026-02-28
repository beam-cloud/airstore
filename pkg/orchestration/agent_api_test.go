package orchestration

import (
	"context"
	"testing"
	"time"

	"github.com/beam-cloud/airstore/pkg/common"
	"github.com/beam-cloud/airstore/pkg/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestValidateRunInputQueueModeSupported(t *testing.T) {
	modes := []types.AgentQueueMode{
		types.AgentQueueModeQueue,
		types.AgentQueueModeFollowup,
		types.AgentQueueModeSteer,
		types.AgentQueueModeSteerBacklog,
		types.AgentQueueModeInterrupt,
	}

	for _, mode := range modes {
		err := types.ValidateRunInputQueueMode(mode)
		require.NoError(t, err, "mode=%s should be supported", mode)
	}
}

func TestValidateRunInputQueueModeUnsupported(t *testing.T) {
	modes := []types.AgentQueueMode{
		types.AgentQueueMode("collect"),
		types.AgentQueueMode("unknown"),
	}

	for _, mode := range modes {
		err := types.ValidateRunInputQueueMode(mode)
		require.Error(t, err, "mode=%s should be rejected", mode)
		require.Contains(t, err.Error(), "not supported")
	}
}

func TestNormalizeRunInputQueueModeSteerBacklog(t *testing.T) {
	mode := types.NormalizeRunInputQueueMode(types.AgentQueueModeSteerBacklog)
	require.Equal(t, types.AgentQueueModeSteer, mode)
}

func TestNormalizeRunInputQueueModeEmptyDefaultsToFollowup(t *testing.T) {
	mode := types.NormalizeRunInputQueueMode("")
	require.Equal(t, types.AgentQueueModeFollowup, mode)
}

func TestEnqueueRunInputTaskRejectsUnsupportedQueueModes(t *testing.T) {
	api := NewAgentAPI(nil, &AgentService{})

	_, _, _, err := api.EnqueueRunInput(
		context.Background(),
		1,
		"run-1",
		types.AgentQueueMode("collect"),
		"hello",
		"idem-1",
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "not supported")
}

func TestStreamTaskEventsResetsCursorsWhenRunBindingChanges(t *testing.T) {
	redisClient, cleanup := newTestRedis(t)
	defer cleanup()

	backend := newFakeBackend()
	taskID := uuid.NewString()
	runID := uuid.NewString()
	backend.agentTasks[taskID] = &types.AgentTask{
		ID:          taskID,
		WorkspaceID: 42,
		State:       types.AgentTaskStateRunning,
		TargetRunID: &runID,
	}
	backend.runs[runID] = &types.AgentRun{
		ID:          runID,
		WorkspaceID: 42,
		Status:      types.AgentRunStatusRunning,
	}

	runtime := NewAgentService(context.Background(), backend, nil, redisClient, nil, "ghcr.io/beam/sandbox:latest")
	require.NoError(t, runtime.publishRunEvent(context.Background(), runID, types.AgentRunEventAccepted, map[string]any{"idx": 1}))
	require.NoError(t, runtime.publishRunEvent(context.Background(), runID, types.AgentRunEventStarted, map[string]any{"idx": 2}))

	api := NewAgentAPI(backend, runtime)
	batch, err := api.StreamTaskEvents(
		context.Background(),
		42,
		taskID,
		100,
		100,
		"run-old",
	)
	require.NoError(t, err)
	require.NotNil(t, batch)
	require.NotNil(t, batch.RunID)
	require.Equal(t, runID, *batch.RunID)
	require.Len(t, batch.RunEvents, 2)
	require.Equal(t, 2, batch.NextRunEventCursor)
	require.EqualValues(t, 0, batch.NextLogCursor)
}

func TestStreamTaskEventsKeepsCursorWhenRunBindingMatches(t *testing.T) {
	redisClient, cleanup := newTestRedis(t)
	defer cleanup()

	backend := newFakeBackend()
	taskID := uuid.NewString()
	runID := uuid.NewString()
	backend.agentTasks[taskID] = &types.AgentTask{
		ID:          taskID,
		WorkspaceID: 42,
		State:       types.AgentTaskStateRunning,
		TargetRunID: &runID,
	}
	backend.runs[runID] = &types.AgentRun{
		ID:          runID,
		WorkspaceID: 42,
		Status:      types.AgentRunStatusRunning,
	}

	runtime := NewAgentService(context.Background(), backend, nil, redisClient, nil, "ghcr.io/beam/sandbox:latest")
	require.NoError(t, runtime.publishRunEvent(context.Background(), runID, types.AgentRunEventAccepted, map[string]any{"idx": 1}))
	require.NoError(t, runtime.publishRunEvent(context.Background(), runID, types.AgentRunEventStarted, map[string]any{"idx": 2}))

	api := NewAgentAPI(backend, runtime)
	batch, err := api.StreamTaskEvents(
		context.Background(),
		42,
		taskID,
		0,
		1,
		runID,
	)
	require.NoError(t, err)
	require.NotNil(t, batch)
	require.Len(t, batch.RunEvents, 1)
	require.Equal(t, 2, batch.NextRunEventCursor)
}

func TestShouldResetTaskLogCursor(t *testing.T) {
	tests := []struct {
		name             string
		cursor           int64
		streamNextCursor int64
		want             bool
	}{
		{
			name:             "resets when cursor is ahead of stream",
			cursor:           120,
			streamNextCursor: 8,
			want:             true,
		},
		{
			name:             "does not reset when cursor matches stream end",
			cursor:           8,
			streamNextCursor: 8,
			want:             false,
		},
		{
			name:             "does not reset when cursor behind stream",
			cursor:           4,
			streamNextCursor: 8,
			want:             false,
		},
		{
			name:             "does not reset when stream has no entries yet",
			cursor:           12,
			streamNextCursor: 0,
			want:             false,
		},
		{
			name:             "does not reset for zero cursor",
			cursor:           0,
			streamNextCursor: 5,
			want:             false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := shouldResetTaskLogCursor(tt.cursor, tt.streamNextCursor)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestValidateAgentCommandParamsRejectsInvalidPolicy(t *testing.T) {
	agentID := "00000000-0000-0000-0000-000000000001"
	params := &AgentCommandParams{
		Message:        "hello",
		AgentID:        &agentID,
		SessionID:      "session-1",
		IdempotencyKey: "idem-1",
		Policy: &RunExecutionPolicy{
			Host: "invalid",
		},
	}

	err := ValidateAgentCommandParams(params)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid policy")
}

func TestValidateAgentCommandParamsAcceptsValidPolicy(t *testing.T) {
	agentID := "00000000-0000-0000-0000-000000000001"
	params := &AgentCommandParams{
		Message:        "hello",
		AgentID:        &agentID,
		SessionID:      "session-1",
		IdempotencyKey: "idem-1",
		Policy: &RunExecutionPolicy{
			Host:            ExecHostSandbox,
			Security:        ExecSecurityAllowlist,
			Ask:             ExecAskOff,
			RuntimeType:     RuntimeTypeGvisor,
			WorkspaceAccess: WorkspaceAccessRW,
			NetworkEnabled:  true,
			Interactive:     false,
			Resources: map[string]any{
				"cpu": 1000,
			},
		},
	}

	err := ValidateAgentCommandParams(params)
	require.NoError(t, err)
}

func TestValidateAgentCommandParamsRejectsMissingAgentID(t *testing.T) {
	params := &AgentCommandParams{
		Message:        "hello",
		SessionID:      "session-1",
		IdempotencyKey: "idem-1",
	}

	err := ValidateAgentCommandParams(params)
	require.Error(t, err)
	require.Contains(t, err.Error(), "agent_id is required")
}

func TestCreateAgentNormalizesRunnerConfig(t *testing.T) {
	backend := newFakeBackend()
	api := NewAgentAPI(backend, nil)

	profile, err := api.CreateAgent(
		context.Background(),
		42,
		"support-agent",
		"Support Agent",
		map[string]any{
			agentConfigKeyRunner: AgentRunnerClaudeCode,
			agentConfigKeyModel:  "claude-sonnet-4-6",
		},
		nil,
	)
	require.NoError(t, err)
	require.Equal(t, AgentRunnerClaudeCode, profile.ConfigJSON[agentConfigKeyRunner])
	require.Equal(t, AgentProviderClaude, profile.ConfigJSON[agentConfigKeyProvider])
	require.Equal(t, "claude-sonnet-4-6", profile.ConfigJSON[agentConfigKeyModel])
}

func TestCreateAgentRejectsUnsupportedRunner(t *testing.T) {
	backend := newFakeBackend()
	api := NewAgentAPI(backend, nil)

	_, err := api.CreateAgent(
		context.Background(),
		42,
		"support-agent",
		"Support Agent",
		map[string]any{agentConfigKeyRunner: "unknown_runner"},
		nil,
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "not supported")
}

func TestDeleteAgentRemovesProfile(t *testing.T) {
	backend := newFakeBackend()
	profileID := uuid.NewString()
	backend.profiles[profileID] = &types.AgentProfile{
		ID:          profileID,
		WorkspaceID: 42,
		AgentKey:    "support-agent",
		Name:        "Support Agent",
		ConfigJSON:  map[string]any{},
		Active:      true,
	}
	api := NewAgentAPI(backend, nil)

	err := api.DeleteAgent(context.Background(), 42, profileID)
	require.NoError(t, err)
	_, err = backend.GetAgentProfile(context.Background(), 42, profileID)
	require.Error(t, err)
	_, ok := err.(*types.ErrAgentProfileNotFound)
	require.True(t, ok)
}

func TestDeleteAgentReturnsNotFound(t *testing.T) {
	backend := newFakeBackend()
	api := NewAgentAPI(backend, nil)

	err := api.DeleteAgent(context.Background(), 42, "missing-agent-id")
	require.Error(t, err)
	_, ok := err.(*types.ErrAgentProfileNotFound)
	require.True(t, ok)
}

func TestArchiveTaskRejectsNonTerminalState(t *testing.T) {
	backend := newFakeBackend()
	taskID := uuid.NewString()
	backend.agentTasks[taskID] = &types.AgentTask{
		ID:          taskID,
		WorkspaceID: 42,
		State:       types.AgentTaskStateRunning,
	}

	api := NewAgentAPI(backend, nil)
	err := api.ArchiveTask(context.Background(), 42, taskID)
	require.Error(t, err)
	require.Contains(t, err.Error(), "idle or terminal")
	require.Nil(t, backend.agentTasks[taskID].ArchivedAt)
}

func TestArchiveTaskMarksTerminalTaskArchived(t *testing.T) {
	backend := newFakeBackend()
	taskID := uuid.NewString()
	backend.agentTasks[taskID] = &types.AgentTask{
		ID:          taskID,
		WorkspaceID: 42,
		State:       types.AgentTaskStateDone,
	}

	api := NewAgentAPI(backend, nil)
	err := api.ArchiveTask(context.Background(), 42, taskID)
	require.NoError(t, err)
	require.NotNil(t, backend.agentTasks[taskID].ArchivedAt)
}

func TestArchiveTaskMarksIdleTaskArchived(t *testing.T) {
	backend := newFakeBackend()
	taskID := uuid.NewString()
	backend.agentTasks[taskID] = &types.AgentTask{
		ID:          taskID,
		WorkspaceID: 42,
		State:       types.AgentTaskStateIdle,
	}

	api := NewAgentAPI(backend, nil)
	err := api.ArchiveTask(context.Background(), 42, taskID)
	require.NoError(t, err)
	require.NotNil(t, backend.agentTasks[taskID].ArchivedAt)
}

func TestCancelTaskRejectsNonRunningState(t *testing.T) {
	backend := newFakeBackend()
	taskID := uuid.NewString()
	backend.agentTasks[taskID] = &types.AgentTask{
		ID:          taskID,
		WorkspaceID: 42,
		State:       types.AgentTaskStateIdle,
	}

	api := NewAgentAPI(backend, nil)
	err := api.CancelTask(context.Background(), 42, taskID)
	require.Error(t, err)
	require.Contains(t, err.Error(), "only running tasks")
	require.Equal(t, types.AgentTaskStateIdle, backend.agentTasks[taskID].State)
}

func TestCancelRunPublishesCancelSignalForInFlightExecution(t *testing.T) {
	redisClient, cleanup := newTestRedis(t)
	defer cleanup()

	backend := newFakeBackend()
	runID := uuid.NewString()
	executionID := uuid.NewString()

	backend.runs[runID] = &types.AgentRun{
		ID:          runID,
		WorkspaceID: 42,
		Status:      types.AgentRunStatusRunning,
	}
	backend.runExecutions[executionID] = &types.RunExecution{
		ExternalId: executionID,
		WorkspaceId: 42,
		Status:     types.RunExecutionStatusRunning,
	}
	backend.attempts[runID] = []*types.AgentRunAttempt{
		{
			ID:          uuid.NewString(),
			RunID:       runID,
			Status:      types.AgentAttemptStatusRunning,
			ExecutionID: &executionID,
		},
	}

	runtime := NewAgentService(context.Background(), backend, nil, redisClient, nil, "ghcr.io/beam/sandbox:latest")
	subCtx, subCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer subCancel()

	cancelCh, cancelCleanup, err := runtime.terminalIO.SubscribeCancel(subCtx, executionID)
	require.NoError(t, err)
	defer cancelCleanup()

	api := NewAgentAPI(backend, runtime)
	require.NoError(t, api.CancelRun(context.Background(), 42, runID))

	select {
	case <-cancelCh:
	case <-time.After(2 * time.Second):
		t.Fatal("expected cancel signal to be published for execution")
	}

	require.Equal(t, types.AgentRunStatusCancelled, backend.runs[runID].Status)
	require.Equal(t, types.RunExecutionStatusCancelled, backend.runExecutions[executionID].Status)
}

func TestPrependTaskPromptLogPrependsPrompt(t *testing.T) {
	task := &types.AgentTask{
		ID: "task-1",
		PayloadJSON: map[string]any{
			"message": "ship this release",
		},
		AcceptedAt: time.UnixMilli(1000),
	}
	logs := []common.TaskLogEntry{
		{TaskID: "task-1", Timestamp: 1010, Stream: "stdout", Data: "working"},
	}

	out := prependTaskPromptLog(task, logs)
	require.Len(t, out, 2)
	require.Equal(t, "user", out[0].Stream)
	require.Equal(t, "ship this release", out[0].Data)
	require.Equal(t, "task_prompt", out[0].ChunkType)
}

func TestPrependTaskPromptLogDoesNotDuplicateExistingUserPrompt(t *testing.T) {
	task := &types.AgentTask{
		ID: "task-1",
		PayloadJSON: map[string]any{
			"prompt": "ship this release",
		},
		AcceptedAt: time.UnixMilli(1000),
	}
	logs := []common.TaskLogEntry{
		{
			TaskID:    "task-1",
			Timestamp: 1005,
			Stream:    "user",
			Data:      "ship this release",
			ChunkType: "user_input",
		},
	}

	out := prependTaskPromptLog(task, logs)
	require.Len(t, out, 1)
	require.Equal(t, "user_input", out[0].ChunkType)
}
