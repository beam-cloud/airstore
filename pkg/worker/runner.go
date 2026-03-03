package worker

import (
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

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

	claudeConfigDirEnvKey  = "CLAUDE_CONFIG_DIR"
	claudeDefaultShellEnv  = "/bin/bash"
	claudeStateDirName     = ".claude"
	claudeStateRootDir     = ".airstore/claude"
	claudeHeartbeatFile    = ".heartbeat"
	claudeHeartbeatFreshFor = 5 * time.Minute
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
		// Follow-up turns stay pinned to the same session when available to avoid
		// accidentally drifting into a fresh Claude context.
		if sessionID != "" {
			builder.withKeyValue("--resume", sessionID)
		} else {
			builder.withFlag("--continue")
		}
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

func (r *ClaudeCodeRunner) SetupHeartbeat(mountSource string, env map[string]string) (string, error) {
	if strings.TrimSpace(mountSource) == "" {
		return "", fmt.Errorf("empty mount source")
	}

	configDir := strings.TrimSpace(env[claudeConfigDirEnvKey])
	if configDir == "" {
		configDir = defaultClaudeConfigDir(env)
	}

	// Heartbeat file sits next to the .claude config dir inside .airstore/claude/<scope>/.
	heartbeatContainerPath := path.Join(path.Dir(configDir), claudeHeartbeatFile)

	hostConfigDir := vfsHostPath(mountSource, configDir)
	hostHeartbeatPath := vfsHostPath(mountSource, heartbeatContainerPath)

	if err := os.MkdirAll(hostConfigDir, 0o755); err != nil {
		return "", fmt.Errorf("mkdir %s: %w", hostConfigDir, err)
	}

	settingsPath := filepath.Join(hostConfigDir, "settings.json")
	if err := os.WriteFile(settingsPath, buildHeartbeatHookSettings(heartbeatContainerPath), 0o644); err != nil {
		return "", fmt.Errorf("write %s: %w", settingsPath, err)
	}

	if f, err := os.Create(hostHeartbeatPath); err == nil {
		f.Close()
	}

	return hostHeartbeatPath, nil
}

func (r *ClaudeCodeRunner) CheckHeartbeat(heartbeatPath string) bool {
	if heartbeatPath == "" {
		return false
	}
	info, err := os.Stat(heartbeatPath)
	if err != nil {
		return false
	}
	return time.Since(info.ModTime()) < claudeHeartbeatFreshFor
}

func buildHeartbeatHookSettings(heartbeatPath string) []byte {
	type hookEntry struct {
		Type    string `json:"type"`
		Command string `json:"command"`
	}
	type matcherGroup struct {
		Hooks []hookEntry `json:"hooks"`
	}
	type settings struct {
		Hooks map[string][]matcherGroup `json:"hooks"`
	}

	cmd := "date +%s > " + heartbeatPath
	group := matcherGroup{Hooks: []hookEntry{{Type: "command", Command: cmd}}}

	s := settings{Hooks: map[string][]matcherGroup{
		"PostToolUse":  {group},
		"Stop":         {group},
		"Notification": {group},
	}}
	b, _ := json.MarshalIndent(s, "", "  ")
	return b
}

func (r *ClaudeCodeRunner) injectEnv(env map[string]string) {
	r.injectAPIKey(env, "ANTHROPIC_API_KEY", r.anthropicAPIKey, true)
	r.injectKernelEnv(env)
	if strings.TrimSpace(env[claudeConfigDirEnvKey]) == "" {
		// Keep Claude state directly on the mounted workspace so behavior is
		// local-like and resume state is natively persistent.
		env[claudeConfigDirEnvKey] = defaultClaudeConfigDir(env)
	}
	if strings.TrimSpace(env["SHELL"]) == "" {
		// Force a stable non-zsh shell for Claude's internal shell snapshots.
		env["SHELL"] = claudeDefaultShellEnv
	}
}

// vfsHostPath maps a container path (under /workspace) to the
// corresponding host-side path on the VFS FUSE mount.
func vfsHostPath(mountSource, containerPath string) string {
	rel := strings.TrimPrefix(containerPath, types.ContainerWorkDir)
	rel = strings.TrimPrefix(rel, "/")
	if rel == "" {
		return mountSource
	}
	return filepath.Join(mountSource, filepath.FromSlash(rel))
}

func defaultClaudeConfigDir(env map[string]string) string {
	return defaultClaudePersistentConfigDir(env)
}

func claudeWorkspaceDir(env map[string]string) string {
	workspaceDir := types.ContainerWorkDir
	if env != nil {
		if wd := strings.TrimSpace(env[agentWorkspaceDirEnvKey]); wd != "" {
			workspaceDir = wd
		}
	}
	return workspaceDir
}

func claudeStateScopeForEnv(env map[string]string) string {
	workspaceDir := claudeWorkspaceDir(env)
	if sessionID := claudeSessionIDFromEnv(env); sessionID != "" {
		return claudeStateScopeWithSession(workspaceDir, sessionID)
	}
	return claudeStateScope(workspaceDir)
}

func defaultClaudePersistentConfigDir(env map[string]string) string {
	return path.Join(
		claudeWorkspaceDir(env),
		claudeStateRootDir,
		claudeStateScopeForEnv(env),
		claudeStateDirName,
	)
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
