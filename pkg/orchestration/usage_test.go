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
	}
	usage := llmUsageFromStreamValues(values)
	require.NotNil(t, usage)
	require.EqualValues(t, 10, usage.InputTokens)
	require.EqualValues(t, 5, usage.OutputTokens)
	require.EqualValues(t, 2, usage.CacheCreationInputTokens)
	require.EqualValues(t, 3, usage.CacheReadInputTokens)
	require.EqualValues(t, 20, usage.TotalTokens)
}

func TestMergeRunUsageJSON(t *testing.T) {
	base := map[string]any{
		types.AgentRunUsageKeyLLMInputTokens:         7,
		types.AgentRunUsageKeyLLMOutputTokens:        4,
		types.AgentRunUsageKeyLLMCacheCreationTokens: 1,
		types.AgentRunUsageKeyLLMCacheReadTokens:     0,
		types.AgentRunUsageKeyLLMTotalTokens:         12,
		types.AgentRunUsageKeyBillingTotalTokens:     12,
	}
	delta := &types.LLMUsage{
		InputTokens:              3,
		OutputTokens:             6,
		CacheCreationInputTokens: 2,
		CacheReadInputTokens:     1,
	}

	merged := mergeRunUsageJSON(base, delta)

	require.EqualValues(t, 10, usageValueFromMap(merged, types.AgentRunUsageKeyLLMInputTokens))
	require.EqualValues(t, 10, usageValueFromMap(merged, types.AgentRunUsageKeyLLMOutputTokens))
	require.EqualValues(t, 3, usageValueFromMap(merged, types.AgentRunUsageKeyLLMCacheCreationTokens))
	require.EqualValues(t, 1, usageValueFromMap(merged, types.AgentRunUsageKeyLLMCacheReadTokens))
	require.EqualValues(t, 24, usageValueFromMap(merged, types.AgentRunUsageKeyLLMTotalTokens))
	require.EqualValues(t, 24, usageValueFromMap(merged, types.AgentRunUsageKeyBillingTotalTokens))
	require.EqualValues(t, int64(types.AgentRunUsageVersion), usageValueFromMap(merged, types.AgentRunUsageKeyVersion))
}
