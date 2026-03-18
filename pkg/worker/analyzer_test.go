package worker

import (
	"testing"
)

func TestAirAnalyzerShouldAnalyze_ToolCallTracking(t *testing.T) {
	a := NewAirAnalyzer()

	if a.ShouldAnalyze(map[string]any{
		"event": "tool_call",
		"tool":  "Bash",
		"args":  map[string]any{"command": "ls"},
	}) {
		t.Fatal("tool_call should not trigger analysis directly")
	}

	if !a.ShouldAnalyze(map[string]any{
		"event":     "tool_result",
		"exit_code": 0,
		"stdout":    "file.txt",
	}) {
		t.Fatal("tool_result after non-read-only tool should trigger analysis")
	}
}

func TestAirAnalyzerShouldAnalyze_ReadOnlyToolIgnored(t *testing.T) {
	a := NewAirAnalyzer()

	a.ShouldAnalyze(map[string]any{
		"event": "tool_call",
		"tool":  "ReadFile",
		"args":  map[string]any{"path": "/tmp/foo"},
	})

	if a.ShouldAnalyze(map[string]any{
		"event":  "tool_result",
		"stdout": "contents",
	}) {
		t.Fatal("tool_result after read-only tool should not trigger analysis")
	}
}

func TestAirAnalyzerPrepareInput(t *testing.T) {
	a := NewAirAnalyzer()

	a.ShouldAnalyze(map[string]any{
		"event": "tool_call",
		"tool":  "WriteFile",
		"args":  map[string]any{"path": "/tmp/out.txt", "content": "hello"},
	})

	toolName, toolInput, toolResult, ok := a.PrepareInput(map[string]any{
		"event":  "tool_result",
		"stdout": "wrote 5 bytes",
	})

	if !ok {
		t.Fatal("expected PrepareInput to succeed")
	}
	if toolName != "WriteFile" {
		t.Fatalf("tool name = %q, want %q", toolName, "WriteFile")
	}
	if toolInput == "" {
		t.Fatal("expected non-empty tool input")
	}
	if toolResult != "wrote 5 bytes" {
		t.Fatalf("tool result = %q, want %q", toolResult, "wrote 5 bytes")
	}
}

func TestAirAnalyzerPrepareInput_FallsBackToStderr(t *testing.T) {
	a := NewAirAnalyzer()

	a.ShouldAnalyze(map[string]any{
		"event": "tool_call",
		"tool":  "Bash",
		"args":  map[string]any{"command": "fail"},
	})

	_, _, toolResult, ok := a.PrepareInput(map[string]any{
		"event":  "tool_result",
		"stdout": "",
		"stderr": "command not found",
	})

	if !ok {
		t.Fatal("expected PrepareInput to succeed")
	}
	if toolResult != "command not found" {
		t.Fatalf("tool result = %q, want stderr fallback", toolResult)
	}
}

func TestAirAnalyzerPrepareInput_NoTrackedTool(t *testing.T) {
	a := NewAirAnalyzer()

	_, _, _, ok := a.PrepareInput(map[string]any{
		"event":  "tool_result",
		"stdout": "orphan result",
	})

	if ok {
		t.Fatal("expected PrepareInput to return false without tracked tool")
	}
}

func TestAirAnalyzerShouldAnalyze_IgnoresNonToolEvents(t *testing.T) {
	a := NewAirAnalyzer()

	for _, event := range []string{"run_start", "run_end", "step", "response", "user_message"} {
		if a.ShouldAnalyze(map[string]any{"event": event}) {
			t.Fatalf("event %q should not trigger analysis", event)
		}
	}
}
