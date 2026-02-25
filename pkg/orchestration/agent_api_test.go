package orchestration

import (
	"context"
	"testing"

	"github.com/beam-cloud/airstore/pkg/types"
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
