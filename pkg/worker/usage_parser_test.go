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

func TestClaudeStreamUsageParser_PrefersModelUsageAndCost(t *testing.T) {
	parser := NewClaudeStreamUsageParser()

	_, _ = parser.Write([]byte(`{"type":"result","total_cost_usd":2.1530685,"usage":{"input_tokens":72,"cache_creation_input_tokens":56974,"cache_read_input_tokens":2390795,"output_tokens":20339},"modelUsage":{"claude-sonnet-4-6":{"inputTokens":72,"outputTokens":20339,"cacheReadInputTokens":2390795,"cacheCreationInputTokens":56974,"webSearchRequests":0,"costUSD":2.06032,"contextWindow":200000,"maxOutputTokens":32000},"claude-haiku-4-5-20251001":{"inputTokens":41096,"outputTokens":2504,"cacheReadInputTokens":0,"cacheCreationInputTokens":31306,"webSearchRequests":0,"costUSD":0.09274849999999998,"contextWindow":200000,"maxOutputTokens":32000}}}` + "\n"))

	usage := parser.Snapshot()
	require.NotNil(t, usage)
	require.EqualValues(t, 41168, usage.InputTokens)
	require.EqualValues(t, 22843, usage.OutputTokens)
	require.EqualValues(t, 88280, usage.CacheCreationInputTokens)
	require.EqualValues(t, 2390795, usage.CacheReadInputTokens)
	require.EqualValues(t, 2543086, usage.TotalTokens)
	require.InDelta(t, 2.1530685, usage.TotalCostUSD, 0.000000001)
	require.EqualValues(t, 2153069, usage.BillingTotalCostMicrousd)
	require.Contains(t, usage.ModelUsage, "claude-haiku-4-5-20251001")
	require.EqualValues(t, 41096, usage.ModelUsage["claude-haiku-4-5-20251001"].InputTokens)
	require.InDelta(t, 0.09274849999999998, usage.ModelUsage["claude-haiku-4-5-20251001"].CostUSD, 0.000000001)
}

func TestClaudeStreamUsageParser_FinalLineWithoutTrailingNewline(t *testing.T) {
	parser := NewClaudeStreamUsageParser()
	_, _ = parser.Write([]byte(`{"usage":{"input_tokens":8,"output_tokens":2}}`))

	usage := parser.Snapshot()
	require.NotNil(t, usage)
	require.EqualValues(t, 10, usage.TotalTokens)
}

func TestClaudeStreamUsageParser_FinalPayloadReplacesEarlierToplineOnly(t *testing.T) {
	parser := NewClaudeStreamUsageParser()

	_, _ = parser.Write([]byte(`{"type":"result","usage":{"input_tokens":72,"cache_creation_input_tokens":56974,"cache_read_input_tokens":2390795,"output_tokens":20339}}` + "\n"))
	_, _ = parser.Write([]byte(`{"type":"final","total_cost_usd":2.1530685,"modelUsage":{"claude-sonnet-4-6":{"inputTokens":72,"outputTokens":20339,"cacheReadInputTokens":2390795,"cacheCreationInputTokens":56974,"costUSD":2.06032},"claude-haiku-4-5-20251001":{"inputTokens":41096,"outputTokens":2504,"cacheCreationInputTokens":31306,"costUSD":0.09274849999999998}}}` + "\n"))

	usage := parser.Snapshot()
	require.NotNil(t, usage)
	require.EqualValues(t, 41168, usage.InputTokens)
	require.EqualValues(t, 22843, usage.OutputTokens)
	require.EqualValues(t, 88280, usage.CacheCreationInputTokens)
	require.EqualValues(t, 2390795, usage.CacheReadInputTokens)
	require.EqualValues(t, 2543086, usage.TotalTokens)
	require.InDelta(t, 2.1530685, usage.TotalCostUSD, 0.000000001)
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

func TestMergeLLMUsage_SumsUsage(t *testing.T) {
	var usage *types.LLMUsage
	usage = types.MergeLLMUsage(usage, &types.LLMUsage{
		InputTokens:  10,
		OutputTokens: 3,
	})
	usage = types.MergeLLMUsage(usage, &types.LLMUsage{
		InputTokens:              2,
		OutputTokens:             5,
		CacheCreationInputTokens: 1,
		CacheReadInputTokens:     1,
		TotalCostUSD:             1.25,
		BillingTotalCostMicrousd: 1250000,
		ModelUsage: map[string]types.LLMModelUsage{
			"claude-haiku-4-5-20251001": {
				InputTokens:     2,
				OutputTokens:    5,
				CostUSD:         1.25,
				ContextWindow:   200000,
				MaxOutputTokens: 32000,
			},
		},
	})
	usage = types.MergeLLMUsage(usage, &types.LLMUsage{
		TotalCostUSD:             0.5,
		BillingTotalCostMicrousd: 500000,
		ModelUsage: map[string]types.LLMModelUsage{
			"claude-haiku-4-5-20251001": {
				InputTokens:     1,
				OutputTokens:    1,
				CostUSD:         0.5,
				ContextWindow:   200000,
				MaxOutputTokens: 64000,
			},
		},
	})

	require.NotNil(t, usage)
	require.EqualValues(t, 13, usage.InputTokens)
	require.EqualValues(t, 9, usage.OutputTokens)
	require.EqualValues(t, 1, usage.CacheCreationInputTokens)
	require.EqualValues(t, 1, usage.CacheReadInputTokens)
	require.EqualValues(t, 24, usage.TotalTokens)
	require.InDelta(t, 1.75, usage.TotalCostUSD, 0.000000001)
	require.EqualValues(t, 1750000, usage.BillingTotalCostMicrousd)
	require.Contains(t, usage.ModelUsage, "claude-haiku-4-5-20251001")
	require.EqualValues(t, 3, usage.ModelUsage["claude-haiku-4-5-20251001"].InputTokens)
	require.EqualValues(t, 6, usage.ModelUsage["claude-haiku-4-5-20251001"].OutputTokens)
	require.EqualValues(t, 64000, usage.ModelUsage["claude-haiku-4-5-20251001"].MaxOutputTokens)
}
