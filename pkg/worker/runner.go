package worker

import (
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

// OutputParsingRunner extends TurnRunner for runners whose stdout carries
// structured turn-outcome signals (needs_input, response) directly in the
// process output. The worker calls ParseTurnOutput after each turn instead
// of relying on file-based markers or BAML classification.
type OutputParsingRunner interface {
	TurnRunner
	ParseTurnOutput(output []byte) (needsInput bool, inputKind types.InputKind, response string, err error)
}

// ResponseExtractor extends AgentExecutionRunner with response text
// extraction from raw PTY output. The worker calls this after the
// session completes to extract the assistant's final response for
// output persistence. Runners that don't implement this fall back to
// the default stream-json parser (Claude Code format).
type ResponseExtractor interface {
	ExtractResponseText(raw []byte, limit int) string
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
