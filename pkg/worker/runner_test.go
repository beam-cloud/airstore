package worker

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/beam-cloud/airstore/pkg/types"
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

func TestClaudeWorkspaceDirRejectsTraversalOutsideWorkspace(t *testing.T) {
	env := map[string]string{
		agentWorkspaceDirEnvKey: "/workspace/../../etc",
	}

	if got := claudeWorkspaceDir(env); got != types.ContainerWorkDir {
		t.Fatalf("expected invalid workspace dir to fall back to %q, got %q", types.ContainerWorkDir, got)
	}
	if got := defaultClaudeCheckpointPath(env); !strings.HasPrefix(got, types.ContainerWorkDir+"/"+claudeStateRootDir+"/") {
		t.Fatalf("expected checkpoint path to remain rooted under %q, got %q", types.ContainerWorkDir, got)
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

func TestBuildFirstTurnStrategies_ResumeSessionUsesResumeModes(t *testing.T) {
	env := map[string]string{
		agentResumeSessionEnvKey: "true",
		agentSessionIDEnvKey:     "session-123",
	}

	strategies := buildFirstTurnStrategies(env)
	if len(strategies) < 2 {
		t.Fatalf("expected resume strategies, got %v", strategies)
	}
	if strategies[0].mode != TurnArgModeFirstResumeByID {
		t.Fatalf("first strategy = %v, want %v", strategies[0].mode, TurnArgModeFirstResumeByID)
	}
	if strategies[1].mode != TurnArgModeFirstResumeLatest {
		t.Fatalf("second strategy = %v, want %v", strategies[1].mode, TurnArgModeFirstResumeLatest)
	}
}

func TestBuildFirstTurnStrategies_DefaultsToFirstStart(t *testing.T) {
	strategies := buildFirstTurnStrategies(map[string]string{})
	if len(strategies) != 1 || strategies[0].mode != TurnArgModeFirstStart {
		t.Fatalf("expected first_start strategy, got %v", strategies)
	}
}

func TestWriteClaudeSessionCheckpointPersistsManifest(t *testing.T) {
	mountSource := t.TempDir()
	env := map[string]string{
		agentWorkspaceDirEnvKey: "/workspace/demo",
		agentSessionIDEnvKey:    "session-123",
	}
	want := &types.SessionCheckpoint{
		RunID:       "run-123",
		ExecutionID: "exec-123",
		UpdatedAt:   123456789,
	}

	if err := writeClaudeSessionCheckpoint(mountSource, env, want); err != nil {
		t.Fatalf("write checkpoint: %v", err)
	}

	checkpointPath := vfsHostPath(mountSource, defaultClaudeCheckpointPath(env))
	raw, err := os.ReadFile(checkpointPath)
	if err != nil {
		t.Fatalf("read checkpoint: %v", err)
	}

	var got types.SessionCheckpoint
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("unmarshal checkpoint: %v", err)
	}
	if got != *want {
		t.Fatalf("checkpoint mismatch: got %#v want %#v", got, *want)
	}
	if filepath.Base(checkpointPath) != claudeCheckpointFile {
		t.Fatalf("unexpected checkpoint filename %q", checkpointPath)
	}
}

func TestVFSHostPathWithinMountRejectsEscapingPath(t *testing.T) {
	mountSource := t.TempDir()
	_, err := vfsHostPathWithinMount(mountSource, "/tmp/../../etc")
	if err == nil {
		t.Fatal("expected escaping path to be rejected")
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
