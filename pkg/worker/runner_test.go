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

func TestBuildEntrypointIncludesSessionIDWhenValid(t *testing.T) {
	runner := NewClaudeCodeRunner(ClaudeCodeRunnerOptions{})
	task := types.RunExecution{
		Prompt: "hello",
		Env: map[string]string{
			agentModelEnvKey:     "claude-sonnet-4",
			agentSessionIDEnvKey: "550e8400-e29b-41d4-a716-446655440000",
		},
	}

	args := runner.BuildEntrypoint(task, task.Env)
	joined := strings.Join(args, " ")
	if !strings.Contains(joined, "--session-id 550e8400-e29b-41d4-a716-446655440000") {
		t.Fatalf("expected --session-id flag, got %v", args)
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
	args := runner.BuildTurnArgs("what is this?", env, TurnArgModeFirstStart)

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
	args := runner.BuildTurnArgs("follow up", env, TurnArgModeFollowup)

	joined := strings.Join(args, " ")
	if !strings.Contains(joined, "--continue") {
		t.Fatalf("expected --continue for follow-up turn: %v", args)
	}
	if !strings.Contains(joined, "--print") {
		t.Fatalf("expected --print: %v", args)
	}
}

func TestBuildTurnArgsContinueSessionUsesResumeWithSessionID(t *testing.T) {
	runner := NewClaudeCodeRunner(ClaudeCodeRunnerOptions{})
	env := map[string]string{
		agentModelEnvKey:     "claude-sonnet-4",
		agentSessionIDEnvKey: "550e8400-e29b-41d4-a716-446655440000",
	}
	args := runner.BuildTurnArgs("follow up", env, TurnArgModeFollowup)

	joined := strings.Join(args, " ")
	if !strings.Contains(joined, "--resume 550e8400-e29b-41d4-a716-446655440000") {
		t.Fatalf("expected --resume for follow-up turn with session id: %v", args)
	}
	if strings.Contains(joined, "--continue") {
		t.Fatalf("did not expect --continue when resume id is available: %v", args)
	}
}

func TestBuildTurnArgsFirstTurnUsesSessionIDWhenAvailable(t *testing.T) {
	runner := NewClaudeCodeRunner(ClaudeCodeRunnerOptions{})
	env := map[string]string{
		agentModelEnvKey:     "claude-sonnet-4",
		agentSessionIDEnvKey: "550e8400-e29b-41d4-a716-446655440000",
	}
	args := runner.BuildTurnArgs("first turn", env, TurnArgModeFirstStart)

	joined := strings.Join(args, " ")
	if !strings.Contains(joined, "--session-id 550e8400-e29b-41d4-a716-446655440000") {
		t.Fatalf("expected --session-id on first turn when available: %v", args)
	}
	if strings.Contains(joined, "--resume") {
		t.Fatalf("did not expect --resume on first turn: %v", args)
	}
}

func TestBuildTurnArgsContinueSessionFallsBackToContinueWhenSessionIDInvalid(t *testing.T) {
	runner := NewClaudeCodeRunner(ClaudeCodeRunnerOptions{})
	env := map[string]string{
		agentModelEnvKey:     "claude-sonnet-4",
		agentSessionIDEnvKey: "not-a-uuid",
	}
	args := runner.BuildTurnArgs("follow up", env, TurnArgModeFollowup)

	joined := strings.Join(args, " ")
	if !strings.Contains(joined, "--continue") {
		t.Fatalf("expected --continue fallback when session id is invalid: %v", args)
	}
	if strings.Contains(joined, "--resume") {
		t.Fatalf("did not expect --resume when session id is invalid: %v", args)
	}
}

func TestBuildTurnArgsFirstResumeLatestPrefersContinue(t *testing.T) {
	runner := NewClaudeCodeRunner(ClaudeCodeRunnerOptions{})
	env := map[string]string{
		agentModelEnvKey:     "claude-sonnet-4",
		agentSessionIDEnvKey: "550e8400-e29b-41d4-a716-446655440000",
	}
	args := runner.BuildTurnArgs("resume latest", env, TurnArgModeFirstResumeLatest)

	joined := strings.Join(args, " ")
	if !strings.Contains(joined, "--continue") {
		t.Fatalf("expected --continue for VFS-first resume: %v", args)
	}
	if strings.Contains(joined, "--resume") {
		t.Fatalf("did not expect --resume in VFS-first mode: %v", args)
	}
	if strings.Contains(joined, "--session-id") {
		t.Fatalf("did not expect --session-id in VFS-first mode: %v", args)
	}
}

func TestBuildTurnArgsFirstFreshNoSessionOmitsSessionFlags(t *testing.T) {
	runner := NewClaudeCodeRunner(ClaudeCodeRunnerOptions{})
	env := map[string]string{
		agentModelEnvKey:     "claude-sonnet-4",
		agentSessionIDEnvKey: "550e8400-e29b-41d4-a716-446655440000",
	}
	args := runner.BuildTurnArgs("fresh fallback", env, TurnArgModeFirstFreshNoSession)

	joined := strings.Join(args, " ")
	if strings.Contains(joined, "--session-id") || strings.Contains(joined, "--resume") || strings.Contains(joined, "--continue") {
		t.Fatalf("did not expect any session flags in fresh fallback mode: %v", args)
	}
}

func TestBuildTurnArgsInjectsAPIKey(t *testing.T) {
	runner := NewClaudeCodeRunner(ClaudeCodeRunnerOptions{
		AnthropicAPIKey: "sk-test-key",
	})
	env := map[string]string{}
	_ = runner.BuildTurnArgs("hello", env, TurnArgModeFirstStart)

	if env["ANTHROPIC_API_KEY"] != "sk-test-key" {
		t.Fatalf("expected API key injection, got %q", env["ANTHROPIC_API_KEY"])
	}
}

func TestBuildTurnArgsSetsClaudeConfigDirDefault(t *testing.T) {
	runner := NewClaudeCodeRunner(ClaudeCodeRunnerOptions{})
	env := map[string]string{}
	_ = runner.BuildTurnArgs("hello", env, TurnArgModeFirstStart)

	if env[claudeConfigDirEnvKey] != claudeConfigDirPath {
		t.Fatalf(
			"expected %s=%q, got %q",
			claudeConfigDirEnvKey,
			claudeConfigDirPath,
			env[claudeConfigDirEnvKey],
		)
	}
}

func TestBuildTurnArgsSetsClaudeConfigDirFromAgentWorkspaceDir(t *testing.T) {
	runner := NewClaudeCodeRunner(ClaudeCodeRunnerOptions{})
	env := map[string]string{
		agentWorkspaceDirEnvKey: "/workspace/agents/prospect-bot/",
	}
	_ = runner.BuildTurnArgs("hello", env, TurnArgModeFirstStart)

	if env[claudeConfigDirEnvKey] != "/workspace/agents/prospect-bot/.claude" {
		t.Fatalf("expected agent workspace scoped CLAUDE_CONFIG_DIR, got %q", env[claudeConfigDirEnvKey])
	}
}

func TestBuildTurnArgsPreservesExistingClaudeConfigDir(t *testing.T) {
	runner := NewClaudeCodeRunner(ClaudeCodeRunnerOptions{})
	env := map[string]string{
		claudeConfigDirEnvKey: "/workspace/custom-claude",
	}
	_ = runner.BuildTurnArgs("hello", env, TurnArgModeFirstStart)

	if env[claudeConfigDirEnvKey] != "/workspace/custom-claude" {
		t.Fatalf("expected existing CLAUDE_CONFIG_DIR to be preserved, got %q", env[claudeConfigDirEnvKey])
	}
}

func TestBuildTurnArgsDoesNotSetHomeWhenUnset(t *testing.T) {
	runner := NewClaudeCodeRunner(ClaudeCodeRunnerOptions{})
	env := map[string]string{}
	_ = runner.BuildTurnArgs("hello", env, TurnArgModeFirstStart)

	if _, exists := env["HOME"]; exists {
		t.Fatalf("expected HOME to remain unset when not provided, got %q", env["HOME"])
	}
}

func TestBuildTurnArgsPreservesExistingHome(t *testing.T) {
	runner := NewClaudeCodeRunner(ClaudeCodeRunnerOptions{})
	env := map[string]string{
		"HOME": "/home/sandbox",
	}
	_ = runner.BuildTurnArgs("hello", env, TurnArgModeFirstStart)

	if env["HOME"] != "/home/sandbox" {
		t.Fatalf("expected existing HOME to be preserved, got %q", env["HOME"])
	}
}

func TestBuildTurnArgsSetsShellDefault(t *testing.T) {
	runner := NewClaudeCodeRunner(ClaudeCodeRunnerOptions{})
	env := map[string]string{}
	_ = runner.BuildTurnArgs("hello", env, TurnArgModeFirstStart)

	if env["SHELL"] != claudeDefaultShellEnv {
		t.Fatalf("expected SHELL=%q, got %q", claudeDefaultShellEnv, env["SHELL"])
	}
}

func TestBuildTurnArgsPreservesExistingShell(t *testing.T) {
	runner := NewClaudeCodeRunner(ClaudeCodeRunnerOptions{})
	env := map[string]string{"SHELL": "/bin/zsh"}
	_ = runner.BuildTurnArgs("hello", env, TurnArgModeFirstStart)

	if env["SHELL"] != "/bin/zsh" {
		t.Fatalf("expected existing SHELL to be preserved, got %q", env["SHELL"])
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
