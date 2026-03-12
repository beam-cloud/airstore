package orchestration

import (
	"testing"

	"github.com/beam-cloud/airstore/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestLLMUsageFromStreamValues(t *testing.T) {
	values := map[string]any{
		types.OrchestrationOutboxPayloadLLMInputTokens:              "10",
		types.OrchestrationOutboxPayloadLLMOutputTokens:             5,
		types.OrchestrationOutboxPayloadLLMCacheCreationInputTokens: 2.0,
		types.OrchestrationOutboxPayloadLLMCacheReadInputTokens:     "3",
		types.OrchestrationOutboxPayloadTotalCostUSD:                "2.1530685",
		types.OrchestrationOutboxPayloadLLMModelUsageJSON:           `{"claude-haiku-4-5-20251001":{"input_tokens":10,"output_tokens":5,"cache_creation_input_tokens":2,"cache_read_input_tokens":3,"cost_usd":2.1530685,"context_window":200000,"max_output_tokens":32000}}`,
	}
	usage := llmUsageFromStreamValues(values)
	require.NotNil(t, usage)
	require.EqualValues(t, 10, usage.InputTokens)
	require.EqualValues(t, 5, usage.OutputTokens)
	require.EqualValues(t, 2, usage.CacheCreationInputTokens)
	require.EqualValues(t, 3, usage.CacheReadInputTokens)
	require.EqualValues(t, 20, usage.TotalTokens)
	require.InDelta(t, 2.1530685, usage.TotalCostUSD, 0.000000001)
	require.EqualValues(t, 2153069, usage.BillingTotalCostMicrousd)
	require.Contains(t, usage.ModelUsage, "claude-haiku-4-5-20251001")
}

func TestMergeRunUsageJSON(t *testing.T) {
	base := map[string]any{
		types.AgentRunUsageKeyVersion:                  1,
		types.AgentRunUsageKeyLLMInputTokens:           7,
		types.AgentRunUsageKeyLLMOutputTokens:          4,
		types.AgentRunUsageKeyLLMCacheCreationTokens:   1,
		types.AgentRunUsageKeyLLMCacheReadTokens:       0,
		types.AgentRunUsageKeyLLMTotalTokens:           12,
		types.AgentRunUsageKeyLegacyBillingTotalTokens: 12,
		types.AgentRunUsageKeyModelUsage: map[string]any{
			"claude-sonnet-4-6": map[string]any{
				"input_tokens":                7,
				"output_tokens":               4,
				"cache_creation_input_tokens": 1,
				"total_tokens":                12,
				"cost_usd":                    1.25,
				"context_window":              200000,
				"max_output_tokens":           32000,
			},
		},
	}
	delta := &types.LLMUsage{
		InputTokens:              3,
		OutputTokens:             6,
		CacheCreationInputTokens: 2,
		CacheReadInputTokens:     1,
		TotalCostUSD:             2.1530685,
		BillingTotalCostMicrousd: 2153069,
		ModelUsage: map[string]types.LLMModelUsage{
			"claude-sonnet-4-6": {
				InputTokens:              3,
				OutputTokens:             6,
				CacheCreationInputTokens: 2,
				CacheReadInputTokens:     1,
				CostUSD:                  2.1530685,
				ContextWindow:            200000,
				MaxOutputTokens:          64000,
			},
		},
	}

	merged := mergeRunUsageJSON(base, delta)

	require.EqualValues(t, 10, usageValueFromMap(merged, types.AgentRunUsageKeyLLMInputTokens))
	require.EqualValues(t, 10, usageValueFromMap(merged, types.AgentRunUsageKeyLLMOutputTokens))
	require.EqualValues(t, 3, usageValueFromMap(merged, types.AgentRunUsageKeyLLMCacheCreationTokens))
	require.EqualValues(t, 1, usageValueFromMap(merged, types.AgentRunUsageKeyLLMCacheReadTokens))
	require.EqualValues(t, 24, usageValueFromMap(merged, types.AgentRunUsageKeyLLMTotalTokens))
	require.InDelta(t, 3.4030685, float64ValueFromMap(merged, types.AgentRunUsageKeyTotalCostUSD), 0.000000001)
	require.EqualValues(t, 3403069, usageValueFromMap(merged, types.AgentRunUsageKeyBillingTotalCostMicrousd))
	require.EqualValues(t, int64(types.AgentRunUsageVersion), usageValueFromMap(merged, types.AgentRunUsageKeyVersion))
	require.NotContains(t, merged, types.AgentRunUsageKeyLegacyBillingTotalTokens)

	modelUsage := modelUsageFromValue(merged[types.AgentRunUsageKeyModelUsage])
	require.Contains(t, modelUsage, "claude-sonnet-4-6")
	require.EqualValues(t, 10, modelUsage["claude-sonnet-4-6"].InputTokens)
	require.EqualValues(t, 10, modelUsage["claude-sonnet-4-6"].OutputTokens)
	require.EqualValues(t, 3, modelUsage["claude-sonnet-4-6"].CacheCreationInputTokens)
	require.EqualValues(t, 1, modelUsage["claude-sonnet-4-6"].CacheReadInputTokens)
	require.EqualValues(t, 24, modelUsage["claude-sonnet-4-6"].TotalTokens)
	require.InDelta(t, 3.4030685, modelUsage["claude-sonnet-4-6"].CostUSD, 0.000000001)
	require.EqualValues(t, 64000, modelUsage["claude-sonnet-4-6"].MaxOutputTokens)
}
