package worker

import (
	"crypto/sha1"
	"encoding/hex"
	"path"
	"strings"

	"github.com/beam-cloud/airstore/pkg/types"
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
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

	claudeConfigDirEnvKey = "CLAUDE_CONFIG_DIR"
	claudeDefaultShellEnv = "/bin/bash"
	claudeStateDirName    = ".claude"
	claudeStateRootDir    = ".airstore/claude"
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

type ClaudeCodeRunnerOptions struct {
	AnthropicAPIKey string
	KernelAPIKey    string
}

type ClaudeCodeRunner struct {
	anthropicAPIKey string
	kernelAPIKey    string
}

func NewClaudeCodeRunner(opts ClaudeCodeRunnerOptions) *ClaudeCodeRunner {
	return &ClaudeCodeRunner{
		anthropicAPIKey: strings.TrimSpace(opts.AnthropicAPIKey),
		kernelAPIKey:    strings.TrimSpace(opts.KernelAPIKey),
	}
}

func (r *ClaudeCodeRunner) Name() string {
	return "claude"
}

func (r *ClaudeCodeRunner) BuildEntrypoint(task types.RunExecution, env map[string]string) []string {
	r.injectEnv(env)
	model := strings.TrimSpace(env[agentModelEnvKey])
	sessionID := claudeSessionIDForCLI(claudeSessionIDFromEnv(env))

	addTaskExecutionContext(
		log.Info().
			Str("model", model).
			Str("prompt", task.Prompt[:min(50, len(task.Prompt))]),
		task,
	).Msg("running claude code task")

	builder := newPromptEntrypointBuilder("claude")
	if sessionID != "" {
		builder.withKeyValue("--session-id", sessionID)
	}
	builder.
		withFlag("--print").
		withFlag("--verbose").
		withKeyValue("--output-format", "stream-json").
		withFlag("--dangerously-skip-permissions").
		withKeyValue("--model", model)
	applySystemPromptFlags(builder, env)
	return builder.withPrompt(task.Prompt).build()
}

// BuildTurnArgs returns the argv for a single interactive turn.
// Each turn runs claude --print as a separate process in the sandbox;
// the Go worker manages the loop between turns.
func (r *ClaudeCodeRunner) BuildTurnArgs(prompt string, env map[string]string, mode TurnArgMode) []string {
	r.injectEnv(env)
	model := strings.TrimSpace(env[agentModelEnvKey])
	sessionID := claudeSessionIDForCLI(claudeSessionIDFromEnv(env))

	builder := newPromptEntrypointBuilder("claude")
	switch mode {
	case TurnArgModeFirstResumeLatest:
		builder.withFlag("--continue")
	case TurnArgModeFirstResumeByID:
		if sessionID != "" {
			builder.withKeyValue("--resume", sessionID)
		} else {
			builder.withFlag("--continue")
		}
	case TurnArgModeFollowup:
		// Follow-up turns should continue from the latest local session state.
		// Explicit session IDs are only used for first-turn steering.
		builder.withFlag("--continue")
	default:
		if sessionID != "" {
			builder.withKeyValue("--session-id", sessionID)
		}
	}
	builder.
		withFlag("--print").
		withFlag("--verbose").
		withKeyValue("--output-format", "stream-json").
		withFlag("--dangerously-skip-permissions").
		withKeyValue("--model", model)
	applySystemPromptFlags(builder, env)
	return builder.withPrompt(prompt).build()
}

func (r *ClaudeCodeRunner) injectEnv(env map[string]string) {
	r.injectAPIKey(env, "ANTHROPIC_API_KEY", r.anthropicAPIKey, true)
	r.injectKernelEnv(env)
	if strings.TrimSpace(env[claudeConfigDirEnvKey]) == "" {
		// Keep state path inside the mounted workspace so resume survives
		// sandbox teardown and worker handoff.
		env[claudeConfigDirEnvKey] = defaultClaudeConfigDir(env)
	}
	if strings.TrimSpace(env["SHELL"]) == "" {
		// Force a stable non-zsh shell for Claude's internal shell snapshots.
		env["SHELL"] = claudeDefaultShellEnv
	}
}

func defaultClaudeConfigDir(env map[string]string) string {
	workspaceDir := types.ContainerWorkDir
	if env != nil {
		if wd := strings.TrimSpace(env[agentWorkspaceDirEnvKey]); wd != "" {
			workspaceDir = wd
		}
	}
	scope := claudeStateScope(workspaceDir)
	if sessionID := claudeSessionIDFromEnv(env); sessionID != "" {
		scope = claudeStateScopeWithSession(workspaceDir, sessionID)
	}
	return path.Join(workspaceDir, claudeStateRootDir, scope, claudeStateDirName)
}

func claudeStateScope(workspaceDir string) string {
	normalized := strings.TrimSpace(strings.TrimSuffix(workspaceDir, "/"))
	if normalized == "" {
		return "default"
	}
	sum := sha1.Sum([]byte(normalized))
	return hex.EncodeToString(sum[:8])
}

func claudeStateScopeWithSession(workspaceDir, sessionID string) string {
	normalized := strings.TrimSpace(strings.TrimSuffix(workspaceDir, "/"))
	if normalized == "" {
		normalized = "default"
	}
	combined := normalized + ":" + strings.TrimSpace(sessionID)
	sum := sha1.Sum([]byte(combined))
	return hex.EncodeToString(sum[:8])
}

func (r *ClaudeCodeRunner) injectKernelEnv(env map[string]string) {
	if r.kernelAPIKey == "" {
		log.Warn().Msg("kernel API key not configured, browser tool will not work")
		return
	}
	r.injectAPIKey(env, "KERNEL_API_KEY", r.kernelAPIKey, false)
	if env["AGENT_BROWSER_PROVIDER"] == "" {
		env["AGENT_BROWSER_PROVIDER"] = "kernel"
	}
	log.Debug().Str("provider", env["AGENT_BROWSER_PROVIDER"]).Msg("kernel browser env injected")
}

func (r *ClaudeCodeRunner) injectAPIKey(env map[string]string, key, value string, overwrite bool) {
	if value == "" {
		return
	}
	if overwrite || env[key] == "" {
		env[key] = value
	}
}

// promptEntrypointBuilder constructs a command-line argv for a CLI tool.
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

func (b *promptEntrypointBuilder) withKeyValue(flag string, value string) *promptEntrypointBuilder {
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

func runnerProviderFromEnv(env map[string]string) string {
	if env == nil {
		return ""
	}
	return strings.ToLower(strings.TrimSpace(env[agentProviderEnvKey]))
}

func claudeSessionIDFromEnv(env map[string]string) string {
	if env == nil {
		return ""
	}
	return strings.TrimSpace(env[agentSessionIDEnvKey])
}

func claudeSessionIDForCLI(sessionID string) string {
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" {
		return ""
	}
	if _, err := uuid.Parse(sessionID); err != nil {
		return ""
	}
	return sessionID
}
