package services

import (
	"testing"

	"github.com/beam-cloud/airstore/pkg/types"
	pb "github.com/beam-cloud/airstore/proto"
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

	if payload[types.OrchestrationOutboxPayloadTaskID] != "task-1" {
		t.Fatalf("expected task id in payload")
	}
	if payload[types.OrchestrationOutboxPayloadLLMInputTokens] != int64(11) {
		t.Fatalf("expected llm input tokens")
	}
	if payload[types.OrchestrationOutboxPayloadLLMOutputTokens] != int64(7) {
		t.Fatalf("expected llm output tokens")
	}
	if payload[types.OrchestrationOutboxPayloadLLMCacheCreationInputTokens] != int64(3) {
		t.Fatalf("expected llm cache creation tokens")
	}
	if payload[types.OrchestrationOutboxPayloadLLMCacheReadInputTokens] != int64(2) {
		t.Fatalf("expected llm cache read tokens")
	}
	if payload[types.OrchestrationOutboxPayloadLLMTotalTokens] != int64(23) {
		t.Fatalf("expected llm total tokens")
	}
}
