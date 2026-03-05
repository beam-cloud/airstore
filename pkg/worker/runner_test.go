package worker

import (
	"strings"
	"testing"
)

func TestClaudeCodeRunnerBuildTurnArgs_FollowupUsesResumeWhenSessionPresent(t *testing.T) {
	runner := NewClaudeCodeRunner(ClaudeCodeRunnerOptions{})
	sessionID := "2f6fd053-0655-4339-9d5d-a9cf43c50985"
	env := map[string]string{
		agentSessionIDEnvKey: sessionID,
		agentModelEnvKey:     "claude-sonnet-4-5",
	}

	args := runner.BuildTurnArgs("hello", env, TurnArgModeFollowup)
	if !argPairExists(args, "--resume", sessionID) {
		t.Fatalf("expected follow-up args to include --resume %s, got %v", sessionID, args)
	}
	if argExists(args, "--continue") {
		t.Fatalf("expected follow-up args to avoid --continue when session is present, got %v", args)
	}
}

func TestClaudeCodeRunnerBuildTurnArgs_FollowupUsesContinueWithoutSession(t *testing.T) {
	runner := NewClaudeCodeRunner(ClaudeCodeRunnerOptions{})
	env := map[string]string{
		agentModelEnvKey: "claude-sonnet-4-5",
	}

	args := runner.BuildTurnArgs("hello", env, TurnArgModeFollowup)
	if !argExists(args, "--continue") {
		t.Fatalf("expected follow-up args to include --continue, got %v", args)
	}
}

func TestDefaultClaudeConfigDir_UsesPersistentStateByDefault(t *testing.T) {
	env := map[string]string{
		agentWorkspaceDirEnvKey: "/workspace/agents/demo",
		agentSessionIDEnvKey:    "2f6fd053-0655-4339-9d5d-a9cf43c50985",
	}

	cfgDir := defaultClaudeConfigDir(env)
	if !strings.HasPrefix(cfgDir, "/workspace/agents/demo/"+claudeStateRootDir+"/") {
		t.Fatalf("expected persistent state root under workspace, got %q", cfgDir)
	}
}

func TestDefaultClaudePersistentConfigDir_RootedUnderWorkspace(t *testing.T) {
	workspaceDir := "/workspace/agents/demo"
	env := map[string]string{
		agentWorkspaceDirEnvKey: workspaceDir,
	}

	cfgDir := defaultClaudePersistentConfigDir(env)
	if !strings.HasPrefix(cfgDir, workspaceDir+"/"+claudeStateRootDir+"/") {
		t.Fatalf("expected persistent state rooted under workspace %q, got %q", workspaceDir, cfgDir)
	}
}

func TestApplySystemPromptFlags_ReplaceMode(t *testing.T) {
	builder := newPromptEntrypointBuilder("claude")
	env := map[string]string{
		agentSystemPromptEnvKey:     "You are a helpful agent.",
		agentSystemPromptModeEnvKey: "replace",
	}

	applySystemPromptFlags(builder, env)
	args := builder.build()

	if !argPairExists(args, "--system-prompt", "You are a helpful agent.") {
		t.Fatalf("expected --system-prompt flag in replace mode, got %v", args)
	}
	if argExists(args, "--append-system-prompt") {
		t.Fatalf("expected no --append-system-prompt in replace mode, got %v", args)
	}
}

func TestApplySystemPromptFlags_AppendMode(t *testing.T) {
	builder := newPromptEntrypointBuilder("claude")
	env := map[string]string{
		agentSystemPromptEnvKey:     "Extra instructions.",
		agentSystemPromptModeEnvKey: "append",
	}

	applySystemPromptFlags(builder, env)
	args := builder.build()

	if !argPairExists(args, "--append-system-prompt", "Extra instructions.") {
		t.Fatalf("expected --append-system-prompt flag in append mode, got %v", args)
	}
	if argExists(args, "--system-prompt") {
		t.Fatalf("expected no --system-prompt in append mode, got %v", args)
	}
}

func TestApplySystemPromptFlags_DefaultMode(t *testing.T) {
	builder := newPromptEntrypointBuilder("claude")
	env := map[string]string{
		agentSystemPromptEnvKey: "Some prompt.",
	}

	applySystemPromptFlags(builder, env)
	args := builder.build()

	if !argPairExists(args, "--append-system-prompt", "Some prompt.") {
		t.Fatalf("expected --append-system-prompt when mode is unset, got %v", args)
	}
}

func TestApplySystemPromptFlags_EmptyPrompt(t *testing.T) {
	builder := newPromptEntrypointBuilder("claude")
	env := map[string]string{
		agentSystemPromptModeEnvKey: "replace",
	}

	applySystemPromptFlags(builder, env)
	args := builder.build()

	if argExists(args, "--system-prompt") || argExists(args, "--append-system-prompt") {
		t.Fatalf("expected no system prompt flags when prompt is empty, got %v", args)
	}
}

func argExists(args []string, want string) bool {
	for _, arg := range args {
		if arg == want {
			return true
		}
	}
	return false
}

func argPairExists(args []string, key, value string) bool {
	for i := 0; i < len(args)-1; i++ {
		if args[i] == key && args[i+1] == value {
			return true
		}
	}
	return false
}
