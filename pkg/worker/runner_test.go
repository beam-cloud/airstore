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
