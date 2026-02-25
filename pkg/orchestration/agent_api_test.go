package orchestration

import (
	"context"
	"testing"

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
		err := validateRunInputQueueMode(mode)
		require.NoError(t, err, "mode=%s should be supported", mode)
	}
}

func TestValidateRunInputQueueModeUnsupported(t *testing.T) {
	modes := []types.AgentQueueMode{
		types.AgentQueueMode("collect"),
		types.AgentQueueMode("unknown"),
	}

	for _, mode := range modes {
		err := validateRunInputQueueMode(mode)
		require.Error(t, err, "mode=%s should be rejected", mode)
		require.Contains(t, err.Error(), "not supported")
	}
}

func TestNormalizeRunInputQueueModeSteerBacklog(t *testing.T) {
	mode := normalizeRunInputQueueMode(types.AgentQueueModeSteerBacklog)
	require.Equal(t, types.AgentQueueModeSteer, mode)
}

func TestEnqueueRunInputTaskRejectsUnsupportedQueueModes(t *testing.T) {
	api := NewAgentAPI(nil, &AgentService{})

	_, _, err := api.EnqueueRunInput(
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
