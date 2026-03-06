package services

import (
	"testing"

	"github.com/beam-cloud/airstore/pkg/types"
	pb "github.com/beam-cloud/airstore/proto"
	"github.com/stretchr/testify/require"
)

func TestBuildRunResultOutboxPayloadIncludesLLMUsage(t *testing.T) {
	req := &pb.SetTaskResultRequest{
		TaskId:                      "task-1",
		ExitCode:                    0,
		Error:                       "",
		AttemptId:                   "attempt-1",
		LlmInputTokens:              11,
		LlmOutputTokens:             7,
		LlmCacheCreationInputTokens: 3,
		LlmCacheReadInputTokens:     2,
		LlmTotalTokens:              23,
	}
	payload := buildRunResultOutboxPayload(req, req.AttemptId, "dedupe-key")

	require.Equal(t, "task-1", payload[types.OrchestrationOutboxPayloadTaskID])
	require.Equal(t, int64(11), payload[types.OrchestrationOutboxPayloadLLMInputTokens])
	require.Equal(t, int64(7), payload[types.OrchestrationOutboxPayloadLLMOutputTokens])
	require.Equal(t, int64(3), payload[types.OrchestrationOutboxPayloadLLMCacheCreationInputTokens])
	require.Equal(t, int64(2), payload[types.OrchestrationOutboxPayloadLLMCacheReadInputTokens])
	require.Equal(t, int64(23), payload[types.OrchestrationOutboxPayloadLLMTotalTokens])
}
