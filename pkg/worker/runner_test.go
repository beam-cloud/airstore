package worker

import (
	"bytes"
	"encoding/json"
	"io"
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

// -- Air runner tests ---------------------------------------------------------

func TestAirRunnerName(t *testing.T) {
	r := NewAirRunner(AirRunnerOptions{})
	if got := r.Name(); got != "air" {
		t.Fatalf("Name() = %q, want %q", got, "air")
	}
}

func TestAirRunnerBuildEntrypoint(t *testing.T) {
	r := NewAirRunner(AirRunnerOptions{
		AnthropicAPIKey: "sk-ant-test",
		CerebrasAPIKey:  "sk-cer-test",
		S2Key:           "s2key",
		S2Basin:         "s2basin",
	})
	env := map[string]string{
		agentSessionIDEnvKey: "sess-123",
	}
	task := types.RunExecution{Prompt: "do something useful"}
	args := r.BuildEntrypoint(task, env)

	if args[0] != "air" {
		t.Fatalf("binary = %q, want %q", args[0], "air")
	}
	if !argPairExists(args, "--format", "json") {
		t.Fatalf("expected --format json, got %v", args)
	}
	if !argPairExists(args, "--session", "sess-123") {
		t.Fatalf("expected --session sess-123, got %v", args)
	}
	if !argPairExists(args, "-p", "do something useful") {
		t.Fatalf("expected prompt in args, got %v", args)
	}
	if env["ANTHROPIC_API_KEY"] != "sk-ant-test" {
		t.Fatalf("expected ANTHROPIC_API_KEY injected, got %q", env["ANTHROPIC_API_KEY"])
	}
	if env["S2_KEY"] != "s2key" {
		t.Fatalf("expected S2_KEY injected, got %q", env["S2_KEY"])
	}
}

func TestAirRunnerBuildEntrypoint_ModelPassthrough(t *testing.T) {
	r := NewAirRunner(AirRunnerOptions{})
	env := map[string]string{
		agentModelEnvKey: "airstore-thinking",
	}
	task := types.RunExecution{Prompt: "plan something"}
	args := r.BuildEntrypoint(task, env)

	if !argPairExists(args, "--model", "airstore-thinking") {
		t.Fatalf("expected --model airstore-thinking, got %v", args)
	}
}

func TestAirRunnerBuildTurnArgs_Followup(t *testing.T) {
	r := NewAirRunner(AirRunnerOptions{AnthropicAPIKey: "sk-ant"})
	env := map[string]string{agentSessionIDEnvKey: "sess-abc"}
	args := r.BuildTurnArgs("next prompt", env, TurnArgModeFollowup)

	if !argPairExists(args, "--session", "sess-abc") {
		t.Fatalf("expected --session in follow-up, got %v", args)
	}
	if !argPairExists(args, "-p", "next prompt") {
		t.Fatalf("expected prompt in args, got %v", args)
	}
}

func TestAirRunnerBuildEntrypoint_SystemPrompt(t *testing.T) {
	r := NewAirRunner(AirRunnerOptions{})
	env := map[string]string{
		agentSystemPromptEnvKey: "You are a helpful agent.",
	}
	task := types.RunExecution{Prompt: "hello"}
	args := r.BuildEntrypoint(task, env)

	if !argPairExists(args, "--system", "You are a helpful agent.") {
		t.Fatalf("expected --system flag, got %v", args)
	}
}

func TestAirRunnerInjectEnv_NoOverwrite(t *testing.T) {
	r := NewAirRunner(AirRunnerOptions{
		AnthropicAPIKey: "from-runner",
		CerebrasAPIKey:  "from-runner",
	})
	env := map[string]string{
		"ANTHROPIC_API_KEY": "already-set",
	}
	task := types.RunExecution{Prompt: "test"}
	r.BuildEntrypoint(task, env)

	if env["ANTHROPIC_API_KEY"] != "already-set" {
		t.Fatalf("injectEnv should not overwrite existing keys, got %q", env["ANTHROPIC_API_KEY"])
	}
	if env["CEREBRAS_API_KEY"] != "from-runner" {
		t.Fatalf("expected CEREBRAS_API_KEY to be injected, got %q", env["CEREBRAS_API_KEY"])
	}
}

func TestAirRunnerParseTurnOutput_NeedsInput(t *testing.T) {
	output := []byte(`{"event":"run_start","ts":0.001}
{"event":"step","ts":1.5,"n":1}
{"event":"response","ts":2.0,"step":1,"message":"What email?"}
{"event":"run_end","ts":2.0,"total_steps":1,"status":"waiting_for_input","needs_input":true}
{"status":"waiting_for_input","needs_input":true,"session_id":"abc","response":"What email?"}
`)

	r := NewAirRunner(AirRunnerOptions{})
	result, err := r.ParseTurnOutput(output)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	blocker := requireTurnBlocker(t, result)
	if blocker.InputKind != types.InputKindFreeText {
		t.Fatalf("expected InputKindFreeText, got %q", blocker.InputKind)
	}
	if result.Response != "What email?" {
		t.Fatalf("expected response=%q, got %q", "What email?", result.Response)
	}
}

func TestAirRunnerParseTurnOutput_Complete(t *testing.T) {
	output := []byte(`{"event":"run_start","ts":0.001}
{"event":"step","ts":3.0,"n":2}
{"event":"run_end","ts":3.0,"total_steps":2,"status":"complete","needs_input":false}
{"status":"complete","needs_input":false,"session_id":"def"}
`)

	r := NewAirRunner(AirRunnerOptions{})
	result, err := r.ParseTurnOutput(output)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	requireNoTurnBlocker(t, result)
}

func TestAirRunnerParseTurnOutput_NoTrace(t *testing.T) {
	output := []byte("some random output\nwithout json trace\n")

	r := NewAirRunner(AirRunnerOptions{})
	result, err := r.ParseTurnOutput(output)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	requireNoTurnBlocker(t, result)
}

func TestAirRunnerImplementsInterfaces(t *testing.T) {
	r := NewAirRunner(AirRunnerOptions{})

	var _ AgentExecutionRunner = r
	var _ TurnRunner = r
	var _ OutputParsingRunner = r
	var _ ResponseExtractor = r
}

func TestAirRunnerExtractResponseText(t *testing.T) {
	r := NewAirRunner(AirRunnerOptions{})

	raw := []byte(`{"event":"run_start","ts":0.001,"client":"claude"}
{"event":"step","ts":1.0,"n":1,"reasoning":"thinking"}
{"event":"response","ts":2.0,"step":1,"message":"Here is the answer."}
{"event":"run_end","ts":2.0,"status":"waiting_for_input","needs_input":true}
{"status":"waiting_for_input","needs_input":true,"response":"Here is the answer."}
`)

	got := r.ExtractResponseText(raw, 24000)
	if got != "Here is the answer." {
		t.Fatalf("ExtractResponseText = %q, want %q", got, "Here is the answer.")
	}
}

func TestAirRunnerExtractResponseText_Empty(t *testing.T) {
	r := NewAirRunner(AirRunnerOptions{})

	raw := []byte(`{"event":"run_start","ts":0.001}
{"event":"step","ts":1.0,"n":1,"reasoning":"doing work"}
{"event":"run_end","ts":2.0,"status":"complete","needs_input":false}
{"status":"complete","needs_input":false}
`)

	got := r.ExtractResponseText(raw, 24000)
	if got != "" {
		t.Fatalf("ExtractResponseText = %q, want empty", got)
	}
}

func TestAirRunnerParseTurnOutput_CompleteUsesOutputSummary(t *testing.T) {
	runner := NewAirRunner(AirRunnerOptions{})

	output := `{"event":"run_start","ts":0.001,"session_id":"s1","client":"claude"}
{"event":"step","ts":2.0,"session_id":"s1","n":1,"reasoning":"Sending email"}
{"event":"tool_call","ts":2.0,"session_id":"s1","step":1,"tool":"Bash","args":{"command":"gmail send ..."}}
{"event":"tool_result","ts":3.0,"session_id":"s1","step":1,"exit_code":0,"stdout":"sent"}
{"event":"run_end","ts":4.0,"session_id":"s1","total_steps":1,"status":"complete","needs_input":false}
{"status":"complete","needs_input":false,"session_id":"s1","output":{"summary":"Sent the outreach email and should check for replies later."}}
`

	result, err := runner.ParseTurnOutput([]byte(output))
	if err != nil {
		t.Fatalf("ParseTurnOutput: %v", err)
	}
	requireNoTurnBlocker(t, result)
	if result.Response != "Sent the outreach email and should check for replies later." {
		t.Fatalf("response = %q", result.Response)
	}
}

func TestAirRunnerParseTurnOutput_DraftArtifactsUpgradeResponse(t *testing.T) {
	runner := NewAirRunner(AirRunnerOptions{})

	output := `{"event":"run_end","ts":4.0,"session_id":"s1","total_steps":1,"status":"complete","needs_input":true}
{"status":"complete","needs_input":true,"input_kind":"approve_reject","session_id":"s1","output":{"summary":"Drafted an outreach email.","next_step":"Awaiting your approval before sending.","drafted_responses":[{"channel":"gmail","to":"luke@slai.io","subject":"Beam sandboxes","body":"Hi Mike,\n\nHere is the draft.\n"}]}}
`

	result, err := runner.ParseTurnOutput([]byte(output))
	if err != nil {
		t.Fatalf("ParseTurnOutput: %v", err)
	}
	if result.Response != "Awaiting your approval before sending." {
		t.Fatalf("response = %q", result.Response)
	}
	if got := len(result.Artifacts); got != 1 {
		t.Fatalf("artifact count = %d, want 1", got)
	}
	artifact := result.Artifacts[0]
	if artifact.OutputType != "gmail" {
		t.Fatalf("output_type = %q, want %q", artifact.OutputType, "gmail")
	}
	blocker := requireTurnBlocker(t, result)
	if blocker.InputKind != types.InputKindApproveReject {
		t.Fatalf("kind = %q, want %q", blocker.InputKind, types.InputKindApproveReject)
	}
	if artifact.Blocking == nil || !artifact.Blocking.IsApproval() {
		t.Fatal("expected approval-blocking artifact")
	}
	if artifact.Data["to"] != "luke@slai.io" {
		t.Fatalf("to = %#v", artifact.Data["to"])
	}
}

func TestAirRunnerParseTurnOutput_CompletedDraftResponseDoesNotReblock(t *testing.T) {
	runner := NewAirRunner(AirRunnerOptions{})

	output := `{"event":"run_end","ts":32.0,"session_id":"s1","total_steps":6,"status":"complete","needs_input":false}
{"status":"complete","needs_input":false,"session_id":"s1","output":{"summary":"Drafted and sent the outreach email.","next_step":"Monitor for a reply from Mike and follow up if no response within 3-5 business days.","drafted_responses":[{"channel":"gmail","to":"luke@slai.io","subject":"Beam sandboxes","body":"Hi Mike,\n\nHere is the draft.\n"}]}}
`

	result, err := runner.ParseTurnOutput([]byte(output))
	if err != nil {
		t.Fatalf("ParseTurnOutput: %v", err)
	}
	requireNoTurnBlocker(t, result)
	if result.Response != "Monitor for a reply from Mike and follow up if no response within 3-5 business days." {
		t.Fatalf("response = %q", result.Response)
	}
	if got := len(result.Artifacts); got != 0 {
		t.Fatalf("artifact count = %d, want 0", got)
	}
}

func TestAirRunnerParseTurnOutput_FollowUpMentionsFutureApprovalWithoutBlocking(t *testing.T) {
	runner := NewAirRunner(AirRunnerOptions{})

	output := `{"event":"run_end","ts":18.0,"session_id":"s1","total_steps":3,"status":"complete","needs_input":false}
{"status":"complete","needs_input":false,"session_id":"s1","response":"Email sent successfully. I'm now monitoring this thread for replies. If a reply arrives, I'll read it, summarize it, and draft a response for your approval. Please wake me in 5 minutes to check thread 123 for replies.","output":{"summary":"Sent the outreach email.","drafted_responses":[{"channel":"gmail","to":"luke@slai.io","subject":"Beam sandboxes","body":"Hi Mike,\n\nHere is the draft.\n"}]}}
`

	result, err := runner.ParseTurnOutput([]byte(output))
	if err != nil {
		t.Fatalf("ParseTurnOutput: %v", err)
	}
	requireNoTurnBlocker(t, result)
	if result.Response != "Email sent successfully. I'm now monitoring this thread for replies. If a reply arrives, I'll read it, summarize it, and draft a response for your approval. Please wake me in 5 minutes to check thread 123 for replies." {
		t.Fatalf("response = %q", result.Response)
	}
	if got := len(result.Artifacts); got != 0 {
		t.Fatalf("artifact count = %d, want 0", got)
	}
}

func TestAirRunnerParseTurnOutput_CompletePrefersExplicitResponse(t *testing.T) {
	runner := NewAirRunner(AirRunnerOptions{})

	output := `{"event":"run_end","ts":18.0,"session_id":"s1","total_steps":3,"status":"complete","needs_input":false}
{"status":"complete","needs_input":false,"session_id":"s1","response":"Email sent successfully. Please wake me in 5 minutes to check thread 123 for replies.","output":{"summary":"Sent the outreach email.","next_step":"Monitor for replies later."}}
`

	result, err := runner.ParseTurnOutput([]byte(output))
	if err != nil {
		t.Fatalf("ParseTurnOutput: %v", err)
	}
	requireNoTurnBlocker(t, result)
	if result.Response != "Email sent successfully. Please wake me in 5 minutes to check thread 123 for replies." {
		t.Fatalf("response = %q", result.Response)
	}
}

func TestAirRunnerParseTurnOutput_MultiTurn(t *testing.T) {
	runner := NewAirRunner(AirRunnerOptions{
		AnthropicAPIKey: "test-key",
		S2Key:           "s2-key",
		S2Basin:         "s2-basin",
	})

	var turnBuf bytes.Buffer
	var primary bytes.Buffer
	mw := io.MultiWriter(&primary, &turnBuf)

	airOutput := `{"event":"run_start","ts":0.001,"session_id":"s1","client":"claude"}
{"event":"user_message","ts":0.0,"session_id":"s1","prompt":"send an email"}
{"event":"step","ts":2.5,"session_id":"s1","n":1,"reasoning":"Need recipient details"}
{"event":"response","ts":2.5,"session_id":"s1","step":1,"message":"Who should I send it to?"}
{"event":"run_end","ts":2.5,"session_id":"s1","total_steps":1,"status":"waiting_for_input","needs_input":true}
{"status":"waiting_for_input","needs_input":true,"session_id":"s1","response":"Who should I send it to?","client":"claude","total_steps":1,"elapsed_s":2.5}
`
	mw.Write([]byte(airOutput))

	if primary.Len() != len(airOutput) {
		t.Fatalf("primary writer received %d bytes, want %d", primary.Len(), len(airOutput))
	}

	result, err := runner.ParseTurnOutput(turnBuf.Bytes())
	if err != nil {
		t.Fatalf("ParseTurnOutput: %v", err)
	}
	blocker := requireTurnBlocker(t, result)
	if blocker.InputKind != types.InputKindFreeText {
		t.Fatalf("kind = %q, want %q", blocker.InputKind, types.InputKindFreeText)
	}
	if result.Response != "Who should I send it to?" {
		t.Fatalf("response = %q, want %q", result.Response, "Who should I send it to?")
	}

	turnBuf.Reset()
	secondTurn := `{"event":"run_start","ts":0.001,"session_id":"s1","client":"claude"}
{"event":"step","ts":1.2,"session_id":"s1","n":1,"reasoning":"Sending email"}
{"event":"tool_call","ts":1.2,"session_id":"s1","step":1,"tool":"Bash","args":{"command":"gmail send ..."}}
{"event":"tool_result","ts":2.0,"session_id":"s1","step":1,"exit_code":0,"stdout":"sent"}
{"event":"run_end","ts":3.0,"session_id":"s1","total_steps":2,"status":"complete","needs_input":false}
{"status":"complete","needs_input":false,"session_id":"s1","client":"claude","total_steps":2,"elapsed_s":3.0}
`
	mw.Write([]byte(secondTurn))

	result, err = runner.ParseTurnOutput(turnBuf.Bytes())
	if err != nil {
		t.Fatalf("ParseTurnOutput second turn: %v", err)
	}
	requireNoTurnBlocker(t, result)
}

func TestAirRunnerParseTurnOutput_ApproveReject(t *testing.T) {
	runner := NewAirRunner(AirRunnerOptions{})

	output := `{"event":"run_start","ts":0.001,"session_id":"s1","client":"claude"}
{"event":"step","ts":5.0,"session_id":"s1","n":2,"reasoning":"Drafting email for review"}
{"event":"signal","ts":9.0,"session_id":"s1","step":2,"kind":"CONFIRMATION","message":"Here's the draft -- should I send it?"}
{"event":"response","ts":9.0,"session_id":"s1","step":2,"message":"Here's the draft -- should I send it?"}
{"event":"run_end","ts":9.5,"session_id":"s1","total_steps":2,"status":"waiting_for_input","needs_input":true}
{"status":"waiting_for_input","needs_input":true,"input_kind":"approve_reject","session_id":"s1","response":"Here's the draft -- should I send it?"}
`
	result, err := runner.ParseTurnOutput([]byte(output))
	if err != nil {
		t.Fatalf("ParseTurnOutput: %v", err)
	}
	blocker := requireTurnBlocker(t, result)
	if blocker.InputKind != types.InputKindApproveReject {
		t.Fatalf("kind = %q, want %q", blocker.InputKind, types.InputKindApproveReject)
	}
	if result.Response != "Here's the draft -- should I send it?" {
		t.Fatalf("response = %q", result.Response)
	}
}

func TestAirRunnerEntrypointAndTurnArgs_Integration(t *testing.T) {
	runner := NewAirRunner(AirRunnerOptions{
		AnthropicAPIKey: "sk-test",
		S2Key:           "s2key",
		S2Basin:         "basin",
	})
	sessionID := "sess-integration-test"
	env := map[string]string{
		agentSessionIDEnvKey:    sessionID,
		agentSystemPromptEnvKey: "You help with emails.",
	}

	task := types.RunExecution{Prompt: "send an email"}
	args := runner.BuildEntrypoint(task, env)

	if args[0] != "air" {
		t.Fatalf("entrypoint binary = %q, want %q", args[0], "air")
	}
	if !argPairExists(args, "--format", "json") {
		t.Fatalf("missing --format json in %v", args)
	}
	if !argPairExists(args, "--session", sessionID) {
		t.Fatalf("missing --session in %v", args)
	}
	if !argPairExists(args, "--system", "You help with emails.") {
		t.Fatalf("missing --system in %v", args)
	}

	followUp := runner.BuildTurnArgs("to luke@beam.cloud", env, TurnArgModeFollowup)

	if !argPairExists(followUp, "--session", sessionID) {
		t.Fatalf("follow-up missing --session in %v", followUp)
	}
	if !argPairExists(followUp, "-p", "to luke@beam.cloud") {
		t.Fatalf("follow-up missing prompt in %v", followUp)
	}

	if env["ANTHROPIC_API_KEY"] != "sk-test" {
		t.Fatalf("ANTHROPIC_API_KEY = %q, want %q", env["ANTHROPIC_API_KEY"], "sk-test")
	}
	if env["S2_KEY"] != "s2key" {
		t.Fatalf("S2_KEY = %q, want %q", env["S2_KEY"], "s2key")
	}
}

// -- helpers ------------------------------------------------------------------

func requireTurnBlocker(t *testing.T, result TurnParseResult) *TurnBlockerDirective {
	t.Helper()
	if result.Control == nil || result.Control.Blocker == nil {
		t.Fatal("expected explicit turn blocker")
	}
	return result.Control.Blocker
}

func requireNoTurnBlocker(t *testing.T, result TurnParseResult) {
	t.Helper()
	if result.Control != nil && result.Control.Blocker != nil {
		t.Fatalf("expected no turn blocker, got %#v", result.Control.Blocker)
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
