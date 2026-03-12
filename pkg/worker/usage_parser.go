package worker

import (
	"bytes"
	"encoding/json"
	"strconv"
	"strings"
	"sync"

	"github.com/beam-cloud/airstore/pkg/types"
)

const (
	usageFieldInputTokens              = "input_tokens"
	usageFieldOutputTokens             = "output_tokens"
	usageFieldCacheCreationInputTokens = "cache_creation_input_tokens"
	usageFieldCacheReadInputTokens     = "cache_read_input_tokens"
	usageFieldTotalTokens              = "total_tokens"
	usageFieldTotalCostUSD             = "total_cost_usd"
	usageFieldModelUsage               = "modelUsage"

	// maxParserBufferSize caps the internal buffer to prevent unbounded growth
	// when the stream contains very long lines without newlines.
	maxParserBufferSize = 1 << 20 // 1 MB
)

// ClaudeStreamUsageParser parses Claude stream-json output and captures the
// latest usage snapshot emitted by the process.
type ClaudeStreamUsageParser struct {
	mu        sync.Mutex
	buffer    []byte
	latest    types.LLMUsage
	hasLatest bool
}

func NewClaudeStreamUsageParser() *ClaudeStreamUsageParser {
	return &ClaudeStreamUsageParser{}
}

func (p *ClaudeStreamUsageParser) Write(chunk []byte) (int, error) {
	if len(chunk) == 0 {
		return 0, nil
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	p.buffer = append(p.buffer, chunk...)
	for {
		line, rest, ok := splitFirstLine(p.buffer)
		if !ok {
			break
		}
		p.buffer = rest
		p.consumeLine(line)
	}
	if len(p.buffer) > maxParserBufferSize {
		// Discard oldest data up to the last newline to keep partial JSON intact.
		if idx := bytes.LastIndexByte(p.buffer, '\n'); idx >= 0 {
			p.buffer = p.buffer[idx+1:]
		} else {
			p.buffer = p.buffer[len(p.buffer)-maxParserBufferSize:]
		}
	}
	return len(chunk), nil
}

func (p *ClaudeStreamUsageParser) Snapshot() *types.LLMUsage {
	p.mu.Lock()
	defer p.mu.Unlock()

	if len(p.buffer) > 0 {
		trailing := bytes.TrimSpace(p.buffer)
		if len(trailing) > 0 && trailing[0] == '{' && json.Valid(trailing) {
			p.buffer = nil
			p.consumeLine(trailing)
		}
	}

	if !p.hasLatest {
		return nil
	}
	copy := p.latest
	copy.ModelUsage = cloneLLMModelUsageMap(p.latest.ModelUsage)
	return &copy
}

func splitFirstLine(buffer []byte) (line []byte, rest []byte, ok bool) {
	idx := bytes.IndexByte(buffer, '\n')
	if idx < 0 {
		return nil, buffer, false
	}
	line = bytes.TrimSpace(buffer[:idx])
	rest = buffer[idx+1:]
	return line, rest, true
}

func (p *ClaudeStreamUsageParser) consumeLine(line []byte) {
	if len(line) == 0 || line[0] != '{' {
		return
	}

	var payload map[string]any
	if err := json.Unmarshal(line, &payload); err != nil {
		return
	}
	usage, ok := extractLLMUsage(payload)
	if !ok || usage.IsZero() {
		return
	}
	p.latest = *usage
	p.hasLatest = true
}

func extractLLMUsage(payload map[string]any) (*types.LLMUsage, bool) {
	modelUsageMap, hasModelUsage := findModelUsageMap(payload)
	usageMap, hasUsage := findUsageMap(payload)

	usage := &types.LLMUsage{}
	if hasModelUsage {
		usage.ModelUsage = parseModelUsage(modelUsageMap)
	}
	if hasUsage && !hasModelUsage {
		usage.InputTokens = int64FromAny(usageMap[usageFieldInputTokens], int64FromAny(usageMap["llm_input_tokens"], 0))
		usage.OutputTokens = int64FromAny(usageMap[usageFieldOutputTokens], int64FromAny(usageMap["llm_output_tokens"], 0))
		usage.CacheCreationInputTokens = int64FromAny(usageMap[usageFieldCacheCreationInputTokens], int64FromAny(usageMap["llm_cache_creation_input_tokens"], 0))
		usage.CacheReadInputTokens = int64FromAny(usageMap[usageFieldCacheReadInputTokens], int64FromAny(usageMap["llm_cache_read_input_tokens"], 0))
		usage.TotalTokens = int64FromAny(usageMap[usageFieldTotalTokens], int64FromAny(usageMap["llm_total_tokens"], 0))
	}

	if cost, ok := findFloat64ByKey(payload, usageFieldTotalCostUSD); ok {
		usage.TotalCostUSD = cost
	}

	normalized := usage.Normalized()
	if normalized.IsZero() {
		return nil, false
	}
	return normalized, true
}

func findUsageMap(value any) (map[string]any, bool) {
	switch typed := value.(type) {
	case map[string]any:
		if rawUsage, ok := typed["usage"]; ok {
			if usageMap := normalizeAnyMap(rawUsage); len(usageMap) > 0 && hasUsageTokenField(usageMap) {
				return usageMap, true
			}
		}
		if hasUsageTokenField(typed) {
			return typed, true
		}
		for _, child := range typed {
			if usage, ok := findUsageMap(child); ok {
				return usage, true
			}
		}
	case []any:
		for _, child := range typed {
			if usage, ok := findUsageMap(child); ok {
				return usage, true
			}
		}
	}
	return nil, false
}

func findModelUsageMap(value any) (map[string]any, bool) {
	switch typed := value.(type) {
	case map[string]any:
		if rawUsage, ok := typed[usageFieldModelUsage]; ok {
			if usageMap := normalizeAnyMap(rawUsage); len(usageMap) > 0 {
				return usageMap, true
			}
		}
		if rawUsage, ok := typed[types.AgentRunUsageKeyModelUsage]; ok {
			if usageMap := normalizeAnyMap(rawUsage); len(usageMap) > 0 {
				return usageMap, true
			}
		}
		for _, child := range typed {
			if usage, ok := findModelUsageMap(child); ok {
				return usage, true
			}
		}
	case []any:
		for _, child := range typed {
			if usage, ok := findModelUsageMap(child); ok {
				return usage, true
			}
		}
	}
	return nil, false
}

func normalizeAnyMap(value any) map[string]any {
	if value == nil {
		return map[string]any{}
	}
	if typed, ok := value.(map[string]any); ok {
		return typed
	}
	marshaled, err := json.Marshal(value)
	if err != nil {
		return map[string]any{}
	}
	out := map[string]any{}
	if err := json.Unmarshal(marshaled, &out); err != nil {
		return map[string]any{}
	}
	return out
}

func parseModelUsage(raw map[string]any) map[string]types.LLMModelUsage {
	if len(raw) == 0 {
		return map[string]types.LLMModelUsage{}
	}

	out := make(map[string]types.LLMModelUsage, len(raw))
	for modelName, value := range raw {
		entry := normalizeAnyMap(value)
		if len(entry) == 0 {
			continue
		}
		modelUsage := types.LLMModelUsage{
			InputTokens:              int64FromAny(entry["inputTokens"], int64FromAny(entry[usageFieldInputTokens], 0)),
			OutputTokens:             int64FromAny(entry["outputTokens"], int64FromAny(entry[usageFieldOutputTokens], 0)),
			CacheCreationInputTokens: int64FromAny(entry["cacheCreationInputTokens"], int64FromAny(entry[usageFieldCacheCreationInputTokens], 0)),
			CacheReadInputTokens:     int64FromAny(entry["cacheReadInputTokens"], int64FromAny(entry[usageFieldCacheReadInputTokens], 0)),
			TotalTokens:              int64FromAny(entry["totalTokens"], int64FromAny(entry[usageFieldTotalTokens], 0)),
			CostUSD:                  float64FromAny(entry["costUSD"], float64FromAny(entry["cost_usd"], 0)),
			WebSearchRequests:        int64FromAny(entry["webSearchRequests"], int64FromAny(entry["web_search_requests"], 0)),
			ContextWindow:            int64FromAny(entry["contextWindow"], int64FromAny(entry["context_window"], 0)),
			MaxOutputTokens:          int64FromAny(entry["maxOutputTokens"], int64FromAny(entry["max_output_tokens"], 0)),
		}
		if modelUsageHasData(modelUsage) {
			out[modelName] = modelUsage.Normalized()
		}
	}
	return out
}

func cloneLLMModelUsageMap(src map[string]types.LLMModelUsage) map[string]types.LLMModelUsage {
	if len(src) == 0 {
		return nil
	}
	out := make(map[string]types.LLMModelUsage, len(src))
	for k, v := range src {
		out[k] = v
	}
	return out
}

func modelUsageHasData(value types.LLMModelUsage) bool {
	return value.InputTokens != 0 ||
		value.OutputTokens != 0 ||
		value.CacheCreationInputTokens != 0 ||
		value.CacheReadInputTokens != 0 ||
		value.TotalTokens != 0 ||
		value.CostUSD != 0 ||
		value.WebSearchRequests != 0 ||
		value.ContextWindow != 0 ||
		value.MaxOutputTokens != 0
}

func hasUsageTokenField(value map[string]any) bool {
	if len(value) == 0 {
		return false
	}
	for _, key := range []string{
		usageFieldInputTokens,
		usageFieldOutputTokens,
		usageFieldCacheCreationInputTokens,
		usageFieldCacheReadInputTokens,
		usageFieldTotalTokens,
		"llm_input_tokens",
		"llm_output_tokens",
		"llm_cache_creation_input_tokens",
		"llm_cache_read_input_tokens",
		"llm_total_tokens",
	} {
		if _, ok := value[key]; ok {
			return true
		}
	}
	return false
}

func findFloat64ByKey(value any, key string) (float64, bool) {
	switch typed := value.(type) {
	case map[string]any:
		if raw, ok := typed[key]; ok {
			return float64FromAny(raw, 0), true
		}
		for _, child := range typed {
			if found, ok := findFloat64ByKey(child, key); ok {
				return found, true
			}
		}
	case []any:
		for _, child := range typed {
			if found, ok := findFloat64ByKey(child, key); ok {
				return found, true
			}
		}
	}
	return 0, false
}

func int64FromAny(value any, fallback int64) int64 {
	switch typed := value.(type) {
	case nil:
		return fallback
	case int:
		return int64(typed)
	case int32:
		return int64(typed)
	case int64:
		return typed
	case float32:
		return int64(typed)
	case float64:
		return int64(typed)
	case json.Number:
		if parsed, err := typed.Int64(); err == nil {
			return parsed
		}
		if parsed, err := typed.Float64(); err == nil {
			return int64(parsed)
		}
	case string:
		trimmed := strings.TrimSpace(typed)
		if trimmed == "" {
			return fallback
		}
		if parsed, err := strconv.ParseInt(trimmed, 10, 64); err == nil {
			return parsed
		}
		if parsed, err := strconv.ParseFloat(trimmed, 64); err == nil {
			return int64(parsed)
		}
	}
	return fallback
}

func float64FromAny(value any, fallback float64) float64 {
	switch typed := value.(type) {
	case nil:
		return fallback
	case int:
		return float64(typed)
	case int32:
		return float64(typed)
	case int64:
		return float64(typed)
	case float32:
		return float64(typed)
	case float64:
		return typed
	case json.Number:
		if parsed, err := typed.Float64(); err == nil {
			return parsed
		}
	case string:
		trimmed := strings.TrimSpace(typed)
		if trimmed == "" {
			return fallback
		}
		if parsed, err := strconv.ParseFloat(trimmed, 64); err == nil {
			return parsed
		}
	}
	return fallback
}
