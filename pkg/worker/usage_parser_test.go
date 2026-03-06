package worker

import (
	"testing"

	"github.com/beam-cloud/airstore/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestClaudeStreamUsageParser_ChunkedLines(t *testing.T) {
	parser := NewClaudeStreamUsageParser()

	_, _ = parser.Write([]byte(`{"type":"message_start","message":{"usage":{"input_tokens":10`))
	_, _ = parser.Write([]byte(`,"output_tokens":4}}}` + "\n"))
	usage := parser.Snapshot()
	require.NotNil(t, usage)
	require.EqualValues(t, 10, usage.InputTokens)
	require.EqualValues(t, 4, usage.OutputTokens)
	require.EqualValues(t, 14, usage.TotalTokens)
}

func TestClaudeStreamUsageParser_MultiplePayloadShapes(t *testing.T) {
	parser := NewClaudeStreamUsageParser()

	_, _ = parser.Write([]byte(`{"type":"result","usage":{"input_tokens":12,"output_tokens":6,"total_tokens":18}}` + "\n"))
	_, _ = parser.Write([]byte(`{"type":"final","result":{"meta":{"usage":{"input_tokens":20,"output_tokens":5,"cache_creation_input_tokens":3,"cache_read_input_tokens":2}}}}` + "\n"))

	usage := parser.Snapshot()
	require.NotNil(t, usage)
	require.EqualValues(t, 20, usage.InputTokens)
	require.EqualValues(t, 5, usage.OutputTokens)
	require.EqualValues(t, 3, usage.CacheCreationInputTokens)
	require.EqualValues(t, 2, usage.CacheReadInputTokens)
	require.EqualValues(t, 30, usage.TotalTokens)
}

func TestClaudeStreamUsageParser_FinalLineWithoutTrailingNewline(t *testing.T) {
	parser := NewClaudeStreamUsageParser()
	_, _ = parser.Write([]byte(`{"usage":{"input_tokens":8,"output_tokens":2}}`))

	usage := parser.Snapshot()
	require.NotNil(t, usage)
	require.EqualValues(t, 10, usage.TotalTokens)
}

func TestClaudeStreamUsageParser_SnapshotPreservesPartialBuffer(t *testing.T) {
	parser := NewClaudeStreamUsageParser()

	// Write a partial JSON chunk (no newline, invalid JSON)
	_, _ = parser.Write([]byte(`{"usage":{"input_tokens":8,`))

	// Snapshot mid-stream — partial JSON should be preserved, not discarded
	usage := parser.Snapshot()
	require.Nil(t, usage)

	// Write the rest of the JSON line, completing the object
	_, _ = parser.Write([]byte(`"output_tokens":2}}` + "\n"))

	// Now Snapshot should return the parsed usage
	usage = parser.Snapshot()
	require.NotNil(t, usage)
	require.EqualValues(t, 8, usage.InputTokens)
	require.EqualValues(t, 2, usage.OutputTokens)
	require.EqualValues(t, 10, usage.TotalTokens)
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

	require.NotNil(t, usage)
	require.EqualValues(t, 12, usage.InputTokens)
	require.EqualValues(t, 8, usage.OutputTokens)
	require.EqualValues(t, 1, usage.CacheCreationInputTokens)
	require.EqualValues(t, 1, usage.CacheReadInputTokens)
	require.EqualValues(t, 22, usage.TotalTokens)
}
