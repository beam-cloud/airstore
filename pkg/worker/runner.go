package worker

import (
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"errors"
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

	claudeConfigDirEnvKey       = "CLAUDE_CONFIG_DIR"
	claudeDefaultShellEnv       = "/bin/bash"
	claudeStateDirName          = ".claude"
	claudeStateRootDir          = ".airstore/claude"
	claudeHeartbeatFilePath     = "/tmp/.claude-heartbeat"
	claudeHeartbeatTouchCommand = "touch /tmp/.claude-heartbeat"
	claudeHeartbeatFreshFor     = 5 * time.Minute
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

// HeartbeatRunner extends AgentExecutionRunner with optional liveness hooks.
// Runners can implement this when they have a runner-specific notion of
// "active work" that should defer interactive idle timeout.
type HeartbeatRunner interface {
	AgentExecutionRunner
	// SetupHeartbeat installs heartbeat hooks into the sandbox. overlayRootfs
	// is the overlay merged directory; mountSource is the FUSE filesystem
	// mount point that is bind-mounted over /workspace inside the container.
	// Config files under /workspace must be written to mountSource so the
	// container can see them (the overlay path is hidden by the bind mount).
	SetupHeartbeat(overlayRootfs string, mountSource string, env map[string]string) error
	CheckHeartbeat(overlayRootfs string) bool
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

func (r *ClaudeCodeRunner) SetupHeartbeat(overlayRootfs string, mountSource string, env map[string]string) error {
	overlayRootfs = strings.TrimSpace(overlayRootfs)
	if overlayRootfs == "" {
		return fmt.Errorf("overlay rootfs path is empty")
	}
	if env == nil {
		return fmt.Errorf("environment map is nil")
	}

	r.injectEnv(env)

	configDir := strings.TrimSpace(env[claudeConfigDirEnvKey])
	if configDir == "" {
		return fmt.Errorf("%s is not configured", claudeConfigDirEnvKey)
	}

	// If configDir is under /workspace and we have a FUSE mount source,
	// write to the mount source so the file is visible inside the container.
	// The overlay path for /workspace is hidden by the bind mount.
	settingsDir := resolveHostPath(overlayRootfs, mountSource, configDir)
	if settingsDir == "" {
		return fmt.Errorf("failed to resolve claude settings directory")
	}
	if err := os.MkdirAll(settingsDir, 0o755); err != nil {
		return fmt.Errorf("create claude settings directory: %w", err)
	}

	settingsPath := filepath.Join(settingsDir, "settings.json")
	settings, err := readClaudeSettingsFile(settingsPath)
	if err != nil {
		return fmt.Errorf("read claude settings: %w", err)
	}
	ensureClaudeHeartbeatPostToolHook(settings)

	data, err := json.MarshalIndent(settings, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal claude settings: %w", err)
	}
	data = append(data, '\n')
	if err := os.WriteFile(settingsPath, data, 0o644); err != nil {
		return fmt.Errorf("write claude settings: %w", err)
	}
	return nil
}

// resolveHostPath maps a container-absolute path to its host-side location.
// Paths under /workspace resolve to mountSource (the FUSE bind-mount source);
// all other paths resolve through the overlay rootfs.
func resolveHostPath(overlayRootfs, mountSource, containerPath string) string {
	cleanedContainer := path.Clean("/" + strings.TrimSpace(containerPath))

	mountSource = strings.TrimSpace(mountSource)
	if mountSource != "" {
		workDir := types.ContainerWorkDir
		if strings.HasPrefix(cleanedContainer, workDir+"/") {
			rel := strings.TrimPrefix(cleanedContainer, workDir+"/")
			return filepath.Join(mountSource, filepath.FromSlash(rel))
		}
		if cleanedContainer == workDir {
			return mountSource
		}
	}

	return overlayContainerPath(overlayRootfs, containerPath)
}

func (r *ClaudeCodeRunner) CheckHeartbeat(overlayRootfs string) bool {
	heartbeatPath := overlayContainerPath(overlayRootfs, claudeHeartbeatFilePath)
	if heartbeatPath == "" {
		return false
	}

	info, err := os.Stat(heartbeatPath)
	if err != nil {
		return false
	}
	return time.Since(info.ModTime()) < claudeHeartbeatFreshFor
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

func overlayContainerPath(overlayRootfs, containerPath string) string {
	overlayRootfs = strings.TrimSpace(overlayRootfs)
	if overlayRootfs == "" {
		return ""
	}

	cleanedContainer := path.Clean("/" + strings.TrimSpace(containerPath))
	cleanedContainer = strings.TrimPrefix(cleanedContainer, "/")
	if cleanedContainer == "" {
		return overlayRootfs
	}
	return filepath.Join(overlayRootfs, filepath.FromSlash(cleanedContainer))
}

func readClaudeSettingsFile(filePath string) (map[string]any, error) {
	settings := map[string]any{}

	data, err := os.ReadFile(filePath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return settings, nil
		}
		return nil, err
	}
	if len(strings.TrimSpace(string(data))) == 0 {
		return settings, nil
	}
	if err := json.Unmarshal(data, &settings); err != nil {
		return nil, err
	}
	return settings, nil
}

func ensureClaudeHeartbeatPostToolHook(settings map[string]any) {
	hooks, ok := settings["hooks"].(map[string]any)
	if !ok || hooks == nil {
		hooks = map[string]any{}
		settings["hooks"] = hooks
	}

	postToolUseHooks, ok := hooks["PostToolUse"].([]any)
	if !ok {
		postToolUseHooks = []any{}
	}
	if hasClaudeHeartbeatPostToolHook(postToolUseHooks) {
		hooks["PostToolUse"] = postToolUseHooks
		return
	}

	postToolUseHooks = append(postToolUseHooks, map[string]any{
		"matcher": "",
		"hooks": []any{
			map[string]any{
				"type":    "command",
				"command": claudeHeartbeatTouchCommand,
			},
		},
	})
	hooks["PostToolUse"] = postToolUseHooks
}

func hasClaudeHeartbeatPostToolHook(postToolUseHooks []any) bool {
	for _, rawEntry := range postToolUseHooks {
		entry, ok := rawEntry.(map[string]any)
		if !ok {
			continue
		}
		rawHooks, ok := entry["hooks"].([]any)
		if !ok {
			continue
		}
		for _, rawHook := range rawHooks {
			hook, ok := rawHook.(map[string]any)
			if !ok {
				continue
			}
			hookType, _ := hook["type"].(string)
			hookCommand, _ := hook["command"].(string)
			if strings.EqualFold(strings.TrimSpace(hookType), "command") &&
				strings.TrimSpace(hookCommand) == claudeHeartbeatTouchCommand {
				return true
			}
		}
	}
	return false
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
