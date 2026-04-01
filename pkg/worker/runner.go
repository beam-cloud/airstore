package worker

import (
	"strings"

	"github.com/beam-cloud/airstore/pkg/types"
)

const (
	agentProviderEnvKey         = "AIRSTORE_AGENT_PROVIDER"
	agentModelEnvKey            = "AIRSTORE_AGENT_MODEL"
	agentResumeSessionEnvKey    = "AIRSTORE_AGENT_RESUME_SESSION"
	agentSessionIDEnvKey        = "AIRSTORE_AGENT_SESSION_ID"
	agentSystemPromptEnvKey     = "AIRSTORE_AGENT_SYSTEM_PROMPT"
	agentSystemPromptModeEnvKey = "AIRSTORE_AGENT_SYSTEM_PROMPT_MODE"
	agentWorkspaceDirEnvKey     = "AIRSTORE_AGENT_WORKSPACE_DIR"
	approvalPolicyEnvKey        = "AIRSTORE_APPROVAL_POLICY"

	systemPromptModeReplace = "replace"
)

// AgentExecutionRunner builds the process entrypoint for an agent task.
type AgentExecutionRunner interface {
	Name() string
	BuildEntrypoint(task types.RunExecution, env map[string]string) []string
}

// TurnRunner extends AgentExecutionRunner with per-turn execution.
// Runners implementing this interface execute each turn as a separate
// process in the sandbox, with the Go worker managing the lifecycle
// between turns — no shell loop, no stdin pipe.
type TurnRunner interface {
	AgentExecutionRunner
	BuildTurnArgs(prompt string, env map[string]string, mode TurnArgMode) []string
}

// HeartbeatRunner extends AgentExecutionRunner with liveness tracking.
// Runners install hooks inside the sandbox that touch a heartbeat file
// on each lifecycle event (tool use, stop). The worker also touches the
// file on observed output as a belt-and-suspenders fallback.
type HeartbeatRunner interface {
	AgentExecutionRunner
	// SetupHeartbeat writes hook configuration to the VFS so the runner
	// touches a heartbeat file on each tool use / stop. mountSource is
	// the host-side VFS FUSE mount path. env is the task env used to
	// derive CLAUDE_CONFIG_DIR. Returns the host-side heartbeat path.
	SetupHeartbeat(mountSource string, env map[string]string) (string, error)
	// CheckHeartbeat returns true if the heartbeat file at the given
	// host-side path was recently modified.
	CheckHeartbeat(heartbeatPath string) bool
}

// NeedsInputRunner extends AgentExecutionRunner with input-detection.
// A Stop hook dumps the agent's last message to a marker file. The
// worker reads it and calls BAML to classify whether the agent is
// blocked on user input or done.
type NeedsInputRunner interface {
	AgentExecutionRunner
	SetupNeedsInput(mountSource string, env map[string]string) (string, error)
	ReadLastMessage(markerPath string) string
}

// TurnArtifact is a first-class structured output emitted directly by a runner
// as part of its turn result. Artifacts are persisted independently of control
// flow so the worker can keep durable outputs and task-state transitions as
// separate concerns.
type TurnArtifact struct {
	OutputType string
	Title      string
	Summary    string
	Content    string
	URI        string
	Path       string
	Data       map[string]any
	Metadata   map[string]any
	Role       string
	Status     string
	Blocking   *types.TaskOutputBlockingMetadata
}

// TurnBlockerDirective is an explicit instruction that the current turn should
// pause for user input. The worker projects it into blocker/task state after
// persisting any first-class outputs emitted by the turn.
type TurnBlockerDirective struct {
	InputKind types.InputKind
	Summary   string
}

// TurnControl carries the turn's control-flow directive independently from any
// artifacts that were emitted. A turn can either block for input, schedule a
// wake, or complete normally.
type TurnControl struct {
	Blocker    *TurnBlockerDirective
	WakeSignal *types.RunExecutionWakeSignal
}

func (c *TurnControl) IsZero() bool {
	return c == nil || (c.Blocker == nil && c.WakeSignal == nil)
}

// TurnParseResult is the normalized worker-side view of a runner's structured
// turn output. Control flow is explicit via Control; artifacts remain durable
// outputs rather than implicitly driving task state.
type TurnParseResult struct {
	Response  string
	Artifacts []TurnArtifact
	Control   *TurnControl
}

// OutputParsingRunner extends TurnRunner for runners whose stdout carries
// structured turn-outcome signals directly in the process output. The worker
// calls ParseTurnOutput after each turn instead of relying solely on file-based
// markers or a single needs-input bit from the runner.
type OutputParsingRunner interface {
	TurnRunner
	ParseTurnOutput(output []byte) (TurnParseResult, error)
}

// ResponseExtractor extends AgentExecutionRunner with response text
// extraction from raw PTY output. The worker calls this after the
// session completes to extract the assistant's final response for
// output persistence. Runners that don't implement this fall back to
// the default stream-json parser (Claude Code format).
type ResponseExtractor interface {
	ExtractResponseText(raw []byte, limit int) string
}

// AnalyzerProvider exposes the structured-output analyzer that matches a
// runner's stdout format.
type AnalyzerProvider interface {
	AgentExecutionRunner
	OutputAnalyzer() OutputAnalyzer
}

// ClassifierEnvProvider exposes the environment needed for worker-side BAML
// classifier calls associated with a runner.
type ClassifierEnvProvider interface {
	AgentExecutionRunner
	ClassifierEnv() map[string]string
}

func inferProviderFromModel(env map[string]string) string {
	model := strings.ToLower(strings.TrimSpace(env[agentModelEnvKey]))
	if strings.HasPrefix(model, "airstore-") {
		return "air"
	}
	return ""
}

func runnerProviderFromEnv(env map[string]string) string {
	if env == nil {
		return ""
	}
	return strings.ToLower(strings.TrimSpace(env[agentProviderEnvKey]))
}

// promptEntrypointBuilder constructs a command-line argv for a CLI runner.
type promptEntrypointBuilder struct {
	binary string
	args   []string
}

func newPromptEntrypointBuilder(binary string) *promptEntrypointBuilder {
	return &promptEntrypointBuilder{
		binary: strings.TrimSpace(binary),
		args:   []string{},
	}
}

func (b *promptEntrypointBuilder) withFlag(flag string) *promptEntrypointBuilder {
	flag = strings.TrimSpace(flag)
	if flag == "" {
		return b
	}
	b.args = append(b.args, flag)
	return b
}

func (b *promptEntrypointBuilder) withKeyValue(flag, value string) *promptEntrypointBuilder {
	flag = strings.TrimSpace(flag)
	value = strings.TrimSpace(value)
	if flag == "" || value == "" {
		return b
	}
	b.args = append(b.args, flag, value)
	return b
}

func (b *promptEntrypointBuilder) withPrompt(prompt string) *promptEntrypointBuilder {
	if strings.TrimSpace(prompt) == "" {
		return b
	}
	b.args = append(b.args, "-p", prompt)
	return b
}

func (b *promptEntrypointBuilder) build() []string {
	argv := make([]string, 0, len(b.args)+1)
	argv = append(argv, b.binary)
	argv = append(argv, b.args...)
	return argv
}

func applySystemPromptFlags(builder *promptEntrypointBuilder, env map[string]string) {
	sp := strings.TrimSpace(env[agentSystemPromptEnvKey])
	if sp == "" {
		return
	}
	mode := strings.ToLower(strings.TrimSpace(env[agentSystemPromptModeEnvKey]))
	if mode == systemPromptModeReplace {
		builder.withKeyValue("--system-prompt", sp)
	} else {
		builder.withKeyValue("--append-system-prompt", sp)
	}
}

// setEnvDefault sets env[key] = value when value is non-empty and the
// key is not already present. Used by all runners for credential and
// config injection.
func setEnvDefault(env map[string]string, key, value string) {
	if value != "" && env[key] == "" {
		env[key] = value
	}
}

// injectKernelBrowserEnv configures the agent-browser daemon for Kernel
// session reuse. No-op when KERNEL_API_KEY is absent from env.
func injectKernelBrowserEnv(env map[string]string) {
	if env["KERNEL_API_KEY"] == "" {
		return
	}
	setEnvDefault(env, "AGENT_BROWSER_PROVIDER", "kernel")

	// Place daemon socket on tmpfs (/tmp) instead of the overlay filesystem
	// (~/.agent-browser/). gVisor's overlay doesn't reliably support Unix
	// domain sockets, causing the CLI to think no daemon is running and
	// spawning a new one (with a new Kernel session) on every invocation.
	setEnvDefault(env, "AGENT_BROWSER_SOCKET_DIR", "/tmp/agent-browser")

	// Default Kernel timeout is 300s (5 min). Increase to 30 min so the
	// cloud browser session survives between turns in interactive tasks.
	setEnvDefault(env, "KERNEL_TIMEOUT_SECONDS", "1800")
}

type TurnArgMode string

const (
	// TurnArgModeFirstStart is a normal first turn; it preserves explicit session ids.
	TurnArgModeFirstStart TurnArgMode = "first_start"
	// TurnArgModeFirstResumeLatest resumes from latest local/VFS state.
	TurnArgModeFirstResumeLatest TurnArgMode = "first_resume_latest"
	// TurnArgModeFirstResumeByID resumes using an explicit session id.
	TurnArgModeFirstResumeByID TurnArgMode = "first_resume_by_id"
	// TurnArgModeFollowup is used for non-first turns.
	TurnArgModeFollowup TurnArgMode = "followup"
)
