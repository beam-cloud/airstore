package worker

import (
	"strings"
	"testing"
)

func TestClaudePromptEntrypointWithoutModel(t *testing.T) {
	args := claudePromptEntrypoint("hello", defaultClaudePromptEntrypointOptions(""))
	for i := 0; i < len(args)-1; i++ {
		if args[i] == "--model" {
			t.Fatalf("did not expect --model flag when model is empty: %v", args)
		}
	}
}

func TestClaudePromptEntrypointWithModel(t *testing.T) {
	opts := defaultClaudePromptEntrypointOptions("claude-sonnet-4")
	args := claudePromptEntrypoint("hello", opts)
	joined := strings.Join(args, " ")
	if !strings.Contains(joined, "--model claude-sonnet-4") {
		t.Fatalf("expected --model flag in entrypoint, got %v", args)
	}
}

func TestPromptEntrypointBuilderBuild(t *testing.T) {
	args := newPromptEntrypointBuilder("claude", "hello world").
		withFlag("--print").
		withKeyValue("--output-format", "stream-json").
		build()

	want := []string{"claude", "--print", "--output-format", "stream-json", "-p", "hello world"}
	if strings.Join(args, " ") != strings.Join(want, " ") {
		t.Fatalf("unexpected args: got=%v want=%v", args, want)
	}
}
