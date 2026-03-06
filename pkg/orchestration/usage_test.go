package orchestration

import (
	"testing"

	"github.com/beam-cloud/airstore/pkg/types"
)

func TestLLMUsageFromStreamValues(t *testing.T) {
	values := map[string]any{
		types.OrchestrationOutboxPayloadLLMInputTokens:              "10",
		types.OrchestrationOutboxPayloadLLMOutputTokens:             5,
		types.OrchestrationOutboxPayloadLLMCacheCreationInputTokens: 2.0,
		types.OrchestrationOutboxPayloadLLMCacheReadInputTokens:     "3",
	}
	usage := llmUsageFromStreamValues(values)
	if usage == nil {
		t.Fatalf("expected usage, got nil")
	}
	if usage.InputTokens != 10 {
		t.Fatalf("expected input tokens 10, got %d", usage.InputTokens)
	}
	if usage.OutputTokens != 5 {
		t.Fatalf("expected output tokens 5, got %d", usage.OutputTokens)
	}
	if usage.CacheCreationInputTokens != 2 {
		t.Fatalf("expected cache creation 2, got %d", usage.CacheCreationInputTokens)
	}
	if usage.CacheReadInputTokens != 3 {
		t.Fatalf("expected cache read 3, got %d", usage.CacheReadInputTokens)
	}
	if usage.TotalTokens != 20 {
		t.Fatalf("expected total 20, got %d", usage.TotalTokens)
	}
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

	assertUsageValue(t, merged, types.AgentRunUsageKeyLLMInputTokens, 10)
	assertUsageValue(t, merged, types.AgentRunUsageKeyLLMOutputTokens, 10)
	assertUsageValue(t, merged, types.AgentRunUsageKeyLLMCacheCreationTokens, 3)
	assertUsageValue(t, merged, types.AgentRunUsageKeyLLMCacheReadTokens, 1)
	assertUsageValue(t, merged, types.AgentRunUsageKeyLLMTotalTokens, 24)
	assertUsageValue(t, merged, types.AgentRunUsageKeyBillingTotalTokens, 24)
	assertUsageValue(t, merged, types.AgentRunUsageKeyVersion, int64(types.AgentRunUsageVersion))
}

func assertUsageValue(t *testing.T, payload map[string]any, key string, want int64) {
	t.Helper()
	if got := usageValueFromMap(payload, key); got != want {
		t.Fatalf("expected %s=%d, got %d", key, want, got)
	}
}
