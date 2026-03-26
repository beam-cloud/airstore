package worker

import (
	"encoding/json"
	"strings"
	"sync"
)

// ClaudeCodeAnalyzer implements OutputAnalyzer for the Claude Code
// stream-json format. It tracks tool_use blocks (from "assistant" messages)
// and matches them with tool_result blocks (from "user" messages) to decide
// which completions are worth classifying via the BAML ExtractOutputs function.
type ClaudeCodeAnalyzer struct {
	mu       sync.Mutex
	toolUses map[string]toolUseEntry // keyed by tool_use block id
}

type toolUseEntry struct {
	name  string
	input string
}

func NewClaudeCodeAnalyzer() *ClaudeCodeAnalyzer {
	return &ClaudeCodeAnalyzer{toolUses: make(map[string]toolUseEntry)}
}

// Tools that never produce extractable outputs (read-only, navigational, or routine).
var readOnlyTools = map[string]bool{
	"Read":                  true,
	"ReadFile":              true,
	"ReadLints":             true,
	"Grep":                  true,
	"GrepTool":              true,
	"Search":                true,
	"SemanticSearch":        true,
	"WebSearch":             true,
	"WebFetch":              true,
	"ListDir":               true,
	"Glob":                  true,
	"LS":                    true,
	"TodoWrite":             true,
	"EnterPlanMode":         true,
	"ExitPlanMode":          true,
	"ToolSearch":            true,
	"browser_navigate":      true,
	"browser_click":         true,
	"browser_snapshot":      true,
	"browser_screenshot":    true,
	"browser_scroll":        true,
	"browser_tabs":          true,
	"browser_type":          true,
	"browser_wait":          true,
	"browser_back":          true,
	"browser_forward":       true,
	"browser_hover":         true,
	"browser_select":        true,
	"browser_lock":          true,
	"browser_unlock":        true,
	"browser_fill":          true,
	"browser_press_key":     true,
	"browser_handle_dialog": true,
	"SwitchMode":            true,
	"AskQuestion":           true,
	"Task":                  true,
	"GenerateImage":         true,
}

// bashNoOutputPrefixes lists command prefixes that should not produce task
// outputs when invoked via Bash. The view tool writes directly to the view
// store — creating outputs from those invocations would feed them back into
// the enrichment pipeline and cause duplicates.
var bashNoOutputPrefixes = []string{
	"view ",
	"/workspace/tools/view ",
}

func (a *ClaudeCodeAnalyzer) ShouldAnalyze(payload map[string]any) bool {
	msgType, _ := payload["type"].(string)
	msg, ok := payload["message"].(map[string]any)
	if !ok {
		return false
	}
	content, ok := msg["content"].([]any)
	if !ok {
		return false
	}

	switch msgType {
	case "assistant":
		// Track tool_use blocks for later matching with tool_result.
		for _, block := range content {
			bm, ok := block.(map[string]any)
			if !ok {
				continue
			}
			if bType, _ := bm["type"].(string); bType != "tool_use" {
				continue
			}
			id, _ := bm["id"].(string)
			name, _ := bm["name"].(string)
			if id == "" || name == "" || readOnlyTools[name] {
				continue
			}
			inputJSON := "{}"
			if inp, ok := bm["input"]; ok {
				if b, err := json.Marshal(inp); err == nil {
					inputJSON = truncate(string(b), maxAnalyzedToolInputLen)
				}
			}
			if name == "Bash" && isBashNoOutputCommand(bm) {
				continue
			}
			a.mu.Lock()
			a.toolUses[id] = toolUseEntry{name: name, input: inputJSON}
			a.mu.Unlock()
		}
		return false

	case "user":
		// Check if any tool_result references a tracked (non-read-only) tool_use.
		for _, block := range content {
			bm, ok := block.(map[string]any)
			if !ok {
				continue
			}
			if bType, _ := bm["type"].(string); bType != "tool_result" {
				continue
			}
			toolUseID, _ := bm["tool_use_id"].(string)
			if toolUseID == "" {
				continue
			}
			a.mu.Lock()
			_, tracked := a.toolUses[toolUseID]
			a.mu.Unlock()
			if tracked {
				return true
			}
		}
		return false

	default:
		return false
	}
}

func (a *ClaudeCodeAnalyzer) PrepareInput(payload map[string]any) (toolName, toolInput, toolResult string, ok bool) {
	msg, _ := payload["message"].(map[string]any)
	if msg == nil {
		return "", "", "", false
	}
	content, _ := msg["content"].([]any)

	for _, block := range content {
		bm, _ := block.(map[string]any)
		if bm == nil {
			continue
		}
		if bType, _ := bm["type"].(string); bType != "tool_result" {
			continue
		}
		toolUseID, _ := bm["tool_use_id"].(string)
		if toolUseID == "" {
			continue
		}

		a.mu.Lock()
		entry, tracked := a.toolUses[toolUseID]
		if tracked {
			delete(a.toolUses, toolUseID)
		}
		a.mu.Unlock()
		if !tracked {
			continue
		}

		resultStr := extractResultContent(bm)
		return entry.name, entry.input, truncate(resultStr, maxAnalyzedToolResultLen), true
	}

	return "", "", "", false
}

func isBashNoOutputCommand(toolUseBlock map[string]any) bool {
	inp, ok := toolUseBlock["input"].(map[string]any)
	if !ok {
		return false
	}
	cmd, _ := inp["command"].(string)
	cmd = strings.TrimSpace(cmd)
	for _, prefix := range bashNoOutputPrefixes {
		if strings.HasPrefix(cmd, prefix) {
			return true
		}
	}
	return false
}

func extractResultContent(block map[string]any) string {
	if content, ok := block["content"].(string); ok {
		return content
	}
	if content, ok := block["content"].([]any); ok {
		var parts []string
		for _, c := range content {
			if cm, ok := c.(map[string]any); ok {
				if text, ok := cm["text"].(string); ok {
					parts = append(parts, text)
				}
			}
		}
		return strings.Join(parts, "\n")
	}
	if b, err := json.Marshal(block["content"]); err == nil {
		return string(b)
	}
	return ""
}
