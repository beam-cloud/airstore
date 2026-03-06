package worker

import (
	"testing"

	"github.com/beam-cloud/airstore/pkg/types"
)

func TestClaudeStreamUsageParser_ChunkedLines(t *testing.T) {
	parser := NewClaudeStreamUsageParser()

	_, _ = parser.Write([]byte(`{"type":"message_start","message":{"usage":{"input_tokens":10`))
	_, _ = parser.Write([]byte(`,"output_tokens":4}}}` + "\n"))
	usage := parser.Snapshot()
	if usage == nil {
		t.Fatalf("expected usage snapshot")
	}
	if usage.InputTokens != 10 || usage.OutputTokens != 4 || usage.TotalTokens != 14 {
		t.Fatalf("unexpected usage snapshot: %+v", usage)
	}
}

func TestClaudeStreamUsageParser_MultiplePayloadShapes(t *testing.T) {
	parser := NewClaudeStreamUsageParser()

	_, _ = parser.Write([]byte(`{"type":"result","usage":{"input_tokens":12,"output_tokens":6,"total_tokens":18}}` + "\n"))
	_, _ = parser.Write([]byte(`{"type":"final","result":{"meta":{"usage":{"input_tokens":20,"output_tokens":5,"cache_creation_input_tokens":3,"cache_read_input_tokens":2}}}}` + "\n"))

	usage := parser.Snapshot()
	if usage == nil {
		t.Fatalf("expected usage snapshot")
	}
	if usage.InputTokens != 20 {
		t.Fatalf("expected input tokens 20, got %d", usage.InputTokens)
	}
	if usage.OutputTokens != 5 {
		t.Fatalf("expected output tokens 5, got %d", usage.OutputTokens)
	}
	if usage.CacheCreationInputTokens != 3 {
		t.Fatalf("expected cache creation tokens 3, got %d", usage.CacheCreationInputTokens)
	}
	if usage.CacheReadInputTokens != 2 {
		t.Fatalf("expected cache read tokens 2, got %d", usage.CacheReadInputTokens)
	}
	if usage.TotalTokens != 30 {
		t.Fatalf("expected total tokens 30, got %d", usage.TotalTokens)
	}
}

func TestClaudeStreamUsageParser_FinalLineWithoutTrailingNewline(t *testing.T) {
	parser := NewClaudeStreamUsageParser()
	_, _ = parser.Write([]byte(`{"usage":{"input_tokens":8,"output_tokens":2}}`))

	usage := parser.Snapshot()
	if usage == nil {
		t.Fatalf("expected usage snapshot")
	}
	if usage.TotalTokens != 10 {
		t.Fatalf("expected total tokens 10, got %d", usage.TotalTokens)
	}
}

func TestAddLLMUsage_SumsUsage(t *testing.T) {
	var usage *types.LLMUsage
	usage = AddLLMUsage(usage, &types.LLMUsage{
		InputTokens:  10,
		OutputTokens: 3,
	})
	usage = AddLLMUsage(usage, &types.LLMUsage{
		InputTokens:              2,
		OutputTokens:             5,
		CacheCreationInputTokens: 1,
		CacheReadInputTokens:     1,
	})

	if usage == nil {
		t.Fatalf("expected usage")
	}
	if usage.InputTokens != 12 {
		t.Fatalf("expected input tokens 12, got %d", usage.InputTokens)
	}
	if usage.OutputTokens != 8 {
		t.Fatalf("expected output tokens 8, got %d", usage.OutputTokens)
	}
	if usage.CacheCreationInputTokens != 1 {
		t.Fatalf("expected cache creation tokens 1, got %d", usage.CacheCreationInputTokens)
	}
	if usage.CacheReadInputTokens != 1 {
		t.Fatalf("expected cache read tokens 1, got %d", usage.CacheReadInputTokens)
	}
	if usage.TotalTokens != 22 {
		t.Fatalf("expected total tokens 22, got %d", usage.TotalTokens)
	}
}
