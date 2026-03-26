package worker

import "encoding/json"

// AirAnalyzer implements OutputAnalyzer for the air runner's JSONL event format.
// air emits structured events (tool_call, tool_result, output) — we only need
// to match tool_result events that follow write-capable tools.
type AirAnalyzer struct {
	lastToolCall airToolCall
}

type airToolCall struct {
	tool string
	args string
}

func NewAirAnalyzer() *AirAnalyzer { return &AirAnalyzer{} }

var airReadOnlyTools = map[string]bool{
	"ReadFile":  true,
	"CheckTask": true,
	"KillTask":  true,
	"view":      true,
}

func (a *AirAnalyzer) ShouldAnalyze(payload map[string]any) bool {
	event, _ := payload["event"].(string)
	switch event {
	case "tool_call":
		tool, _ := payload["tool"].(string)
		if airReadOnlyTools[tool] {
			a.lastToolCall = airToolCall{}
			return false
		}
		argsJSON := "{}"
		if args, ok := payload["args"]; ok {
			if b, err := json.Marshal(args); err == nil {
				argsJSON = truncate(string(b), maxAnalyzedToolInputLen)
			}
		}
		a.lastToolCall = airToolCall{tool: tool, args: argsJSON}
		return false

	case "tool_result":
		return a.lastToolCall.tool != ""

	default:
		return false
	}
}

func (a *AirAnalyzer) PrepareInput(payload map[string]any) (toolName, toolInput, toolResult string, ok bool) {
	if a.lastToolCall.tool == "" {
		return "", "", "", false
	}

	tc := a.lastToolCall
	a.lastToolCall = airToolCall{}

	stdout, _ := payload["stdout"].(string)
	stderr, _ := payload["stderr"].(string)
	result := stdout
	if result == "" {
		result = stderr
	}

	return tc.tool, tc.args, truncate(result, maxAnalyzedToolResultLen), true
}
