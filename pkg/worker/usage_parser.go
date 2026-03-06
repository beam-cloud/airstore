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
	return len(chunk), nil
}

func (p *ClaudeStreamUsageParser) Snapshot() *types.LLMUsage {
	p.mu.Lock()
	defer p.mu.Unlock()

	if len(p.buffer) > 0 {
		trailing := bytes.TrimSpace(p.buffer)
		p.buffer = nil
		p.consumeLine(trailing)
	}

	if !p.hasLatest {
		return nil
	}
	usage := p.latest
	usage.TotalTokens = usage.NormalizedTotal()
	return &usage
}

func AddLLMUsage(current *types.LLMUsage, delta *types.LLMUsage) *types.LLMUsage {
	if delta == nil || delta.IsZero() {
		return current
	}
	deltaTotal := delta.NormalizedTotal()
	if current == nil {
		copied := *delta
		copied.TotalTokens = deltaTotal
		return &copied
	}

	merged := *current
	merged.InputTokens += delta.InputTokens
	merged.OutputTokens += delta.OutputTokens
	merged.CacheCreationInputTokens += delta.CacheCreationInputTokens
	merged.CacheReadInputTokens += delta.CacheReadInputTokens
	merged.TotalTokens = current.NormalizedTotal() + deltaTotal
	return &merged
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
	usage.TotalTokens = usage.NormalizedTotal()
	p.latest = *usage
	p.hasLatest = true
}

func extractLLMUsage(payload map[string]any) (*types.LLMUsage, bool) {
	usageMap, ok := findUsageMap(payload)
	if !ok {
		return nil, false
	}

	usage := &types.LLMUsage{
		InputTokens:              int64FromAny(usageMap[usageFieldInputTokens], int64FromAny(usageMap["llm_input_tokens"], 0)),
		OutputTokens:             int64FromAny(usageMap[usageFieldOutputTokens], int64FromAny(usageMap["llm_output_tokens"], 0)),
		CacheCreationInputTokens: int64FromAny(usageMap[usageFieldCacheCreationInputTokens], int64FromAny(usageMap["llm_cache_creation_input_tokens"], 0)),
		CacheReadInputTokens:     int64FromAny(usageMap[usageFieldCacheReadInputTokens], int64FromAny(usageMap["llm_cache_read_input_tokens"], 0)),
		TotalTokens:              int64FromAny(usageMap[usageFieldTotalTokens], int64FromAny(usageMap["llm_total_tokens"], 0)),
	}
	return usage, true
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
