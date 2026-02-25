package worker

import (
	"strings"
	"testing"

	"github.com/beam-cloud/airstore/pkg/types"
)

func TestPromptEntrypointBuilderBuild(t *testing.T) {
	args := newPromptEntrypointBuilder("claude").
		withFlag("--print").
		withKeyValue("--output-format", "stream-json").
		withPrompt("hello world").
		build()

	want := []string{"claude", "--print", "--output-format", "stream-json", "-p", "hello world"}
	if strings.Join(args, " ") != strings.Join(want, " ") {
		t.Fatalf("unexpected args: got=%v want=%v", args, want)
	}
}

func TestBuildEntrypointIncludesModel(t *testing.T) {
	runner := NewClaudeCodeRunner(ClaudeCodeRunnerOptions{})
	task := types.RunExecution{
		Prompt: "hello",
		Env:    map[string]string{agentModelEnvKey: "claude-sonnet-4"},
	}
	args := runner.BuildEntrypoint(task, task.Env)
	joined := strings.Join(args, " ")
	if !strings.Contains(joined, "--model claude-sonnet-4") {
		t.Fatalf("expected --model flag, got %v", args)
	}
}

func TestBuildEntrypointOmitsEmptyModel(t *testing.T) {
	runner := NewClaudeCodeRunner(ClaudeCodeRunnerOptions{})
	task := types.RunExecution{Prompt: "hello", Env: map[string]string{}}
	args := runner.BuildEntrypoint(task, task.Env)
	for i := 0; i < len(args)-1; i++ {
		if args[i] == "--model" {
			t.Fatalf("did not expect --model flag when model is empty: %v", args)
		}
	}
}

func TestBuildTurnArgsFirstTurn(t *testing.T) {
	runner := NewClaudeCodeRunner(ClaudeCodeRunnerOptions{})
	env := map[string]string{agentModelEnvKey: "claude-sonnet-4"}
	args := runner.BuildTurnArgs("what is this?", env, false)

	joined := strings.Join(args, " ")
	if strings.Contains(joined, "--continue") {
		t.Fatalf("first turn should not have --continue: %v", args)
	}
	if !strings.Contains(joined, "--print") {
		t.Fatalf("expected --print: %v", args)
	}
	if !strings.Contains(joined, "--output-format stream-json") {
		t.Fatalf("expected stream-json output: %v", args)
	}
	if !strings.Contains(joined, "-p") {
		t.Fatalf("expected prompt arg: %v", args)
	}
}

func TestBuildTurnArgsContinueSession(t *testing.T) {
	runner := NewClaudeCodeRunner(ClaudeCodeRunnerOptions{})
	env := map[string]string{agentModelEnvKey: "claude-sonnet-4"}
	args := runner.BuildTurnArgs("follow up", env, true)

	joined := strings.Join(args, " ")
	if !strings.Contains(joined, "--continue") {
		t.Fatalf("expected --continue for follow-up turn: %v", args)
	}
	if !strings.Contains(joined, "--print") {
		t.Fatalf("expected --print: %v", args)
	}
}

func TestBuildTurnArgsInjectsAPIKey(t *testing.T) {
	runner := NewClaudeCodeRunner(ClaudeCodeRunnerOptions{
		AnthropicAPIKey: "sk-test-key",
	})
	env := map[string]string{}
	_ = runner.BuildTurnArgs("hello", env, false)

	if env["ANTHROPIC_API_KEY"] != "sk-test-key" {
		t.Fatalf("expected API key injection, got %q", env["ANTHROPIC_API_KEY"])
	}
}

func TestBuildTurnArgsSetsClaudeConfigDirDefault(t *testing.T) {
	runner := NewClaudeCodeRunner(ClaudeCodeRunnerOptions{})
	env := map[string]string{}
	_ = runner.BuildTurnArgs("hello", env, false)

	if env[claudeConfigDirEnvKey] != claudeConfigDirPath {
		t.Fatalf(
			"expected %s=%q, got %q",
			claudeConfigDirEnvKey,
			claudeConfigDirPath,
			env[claudeConfigDirEnvKey],
		)
	}
}

func TestBuildTurnArgsPreservesExistingClaudeConfigDir(t *testing.T) {
	runner := NewClaudeCodeRunner(ClaudeCodeRunnerOptions{})
	env := map[string]string{
		claudeConfigDirEnvKey: "/workspace/custom-claude",
	}
	_ = runner.BuildTurnArgs("hello", env, false)

	if env[claudeConfigDirEnvKey] != "/workspace/custom-claude" {
		t.Fatalf("expected existing CLAUDE_CONFIG_DIR to be preserved, got %q", env[claudeConfigDirEnvKey])
	}
}

func TestClaudeCodeRunnerImplementsTurnRunner(t *testing.T) {
	var runner AgentExecutionRunner = NewClaudeCodeRunner(ClaudeCodeRunnerOptions{})
	if _, ok := runner.(TurnRunner); !ok {
		t.Fatal("ClaudeCodeRunner should implement TurnRunner")
	}
}

func TestRunnerProviderFromEnvDoesNotAliasProviderName(t *testing.T) {
	env := map[string]string{
		agentProviderEnvKey: "anthropic",
	}
	provider := runnerProviderFromEnv(env)
	if provider != "anthropic" {
		t.Fatalf("expected provider name passthrough, got %q", provider)
	}
}

func TestIsClaudeExecutionTaskRequiresExplicitProvider(t *testing.T) {
	task := types.RunExecution{
		Prompt: "hello",
	}
	if isClaudeExecutionTask(task) {
		t.Fatalf("expected missing provider to be non-claude")
	}
	task.Env = map[string]string{
		agentProviderEnvKey: claudeProviderName,
	}
	if !isClaudeExecutionTask(task) {
		t.Fatalf("expected claude provider to be detected")
	}
}
