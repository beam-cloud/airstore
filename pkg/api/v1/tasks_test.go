package apiv1

import (
	"testing"

	"github.com/beam-cloud/airstore/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestParseTaskStatesAcceptsCanonicalStates(t *testing.T) {
	states, err := parseTaskStates("queued,running,idle,done,dropped,cancelled")
	require.NoError(t, err)
	require.Equal(t, []types.AgentTaskState{
		types.AgentTaskStateQueued,
		types.AgentTaskStateRunning,
		types.AgentTaskStateIdle,
		types.AgentTaskStateDone,
		types.AgentTaskStateDropped,
		types.AgentTaskStateCancelled,
	}, states)
}

func TestParseTaskStatesRejectsRemovedStates(t *testing.T) {
	_, err := parseTaskStates("accepted")
	require.Error(t, err)

	_, err = parseTaskStates("dispatched")
	require.Error(t, err)
}
