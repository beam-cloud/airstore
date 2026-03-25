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
	claudeConfigDirEnvKey   = "CLAUDE_CONFIG_DIR"
	claudeDefaultShellEnv   = "/bin/bash"
	claudeStateDirName      = ".claude"
	claudeStateRootDir      = ".airstore/claude"
	claudeCheckpointFile    = "session-checkpoint.json"
	claudeHeartbeatFile     = ".heartbeat"
	claudeNeedsInputFile    = ".needs_input"
	claudeHeartbeatFreshFor = 5 * time.Minute
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

func (r *ClaudeCodeRunner) OutputAnalyzer() OutputAnalyzer {
	return NewClaudeCodeAnalyzer()
}

func (r *ClaudeCodeRunner) ClassifierEnv() map[string]string {
	env := map[string]string{}
	if key := strings.TrimSpace(r.anthropicAPIKey); key != "" {
		env["ANTHROPIC_API_KEY"] = key
	}
	return env
}

func (r *ClaudeCodeRunner) BuildEntrypoint(task types.RunExecution, env map[string]string) []string {
	r.injectEnv(env)

	model := strings.TrimSpace(env[agentModelEnvKey])
	sessionID := claudeSessionIDForCLI(claudeSessionIDFromEnv(env))

	spMode := strings.ToLower(strings.TrimSpace(env[agentSystemPromptModeEnvKey]))
	spLen := len(strings.TrimSpace(env[agentSystemPromptEnvKey]))
	addTaskExecutionContext(
		log.Info().
			Str("model", model).
			Str("prompt", task.Prompt[:min(50, len(task.Prompt))]).
			Str("system_prompt_mode", spMode).
			Int("system_prompt_len", spLen),
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

	spMode := strings.ToLower(strings.TrimSpace(env[agentSystemPromptModeEnvKey]))
	spLen := len(strings.TrimSpace(env[agentSystemPromptEnvKey]))
	log.Info().
		Str("model", model).
		Str("system_prompt_mode", spMode).
		Int("system_prompt_len", spLen).
		Str("turn_mode", string(mode)).
		Msg("building claude code turn args")

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

// claudeHookPaths resolves the host + container paths for hook marker
// files relative to the CLAUDE_CONFIG_DIR parent directory.
type claudeHookPaths struct {
	hostConfigDir string
	settingsPath  string

	heartbeatContainer string
	heartbeatHost      string

	needsInputContainer string
	needsInputHost      string
}

func resolveClaudeHookPaths(mountSource string, env map[string]string) (*claudeHookPaths, error) {
	if strings.TrimSpace(mountSource) == "" {
		return nil, fmt.Errorf("empty mount source")
	}
	configDir := strings.TrimSpace(env[claudeConfigDirEnvKey])
	if configDir == "" {
		configDir = defaultClaudeConfigDir(env)
	}
	parentDir := path.Dir(configDir)
	hostConfigDir, err := vfsHostPathWithinMount(mountSource, configDir)
	if err != nil {
		return nil, err
	}
	heartbeatHost, err := vfsHostPathWithinMount(mountSource, path.Join(parentDir, claudeHeartbeatFile))
	if err != nil {
		return nil, err
	}
	needsInputHost, err := vfsHostPathWithinMount(mountSource, path.Join(parentDir, claudeNeedsInputFile))
	if err != nil {
		return nil, err
	}
	return &claudeHookPaths{
		hostConfigDir:       hostConfigDir,
		settingsPath:        filepath.Join(hostConfigDir, "settings.json"),
		heartbeatContainer:  path.Join(parentDir, claudeHeartbeatFile),
		heartbeatHost:       heartbeatHost,
		needsInputContainer: path.Join(parentDir, claudeNeedsInputFile),
		needsInputHost:      needsInputHost,
	}, nil
}

func (r *ClaudeCodeRunner) SetupHeartbeat(mountSource string, env map[string]string) (string, error) {
	paths, err := resolveClaudeHookPaths(mountSource, env)
	if err != nil {
		return "", err
	}
	if err := os.MkdirAll(paths.hostConfigDir, 0o755); err != nil {
		return "", fmt.Errorf("mkdir %s: %w", paths.hostConfigDir, err)
	}
	if err := os.WriteFile(paths.settingsPath, buildClaudeHookSettings(paths, false), 0o644); err != nil {
		return "", fmt.Errorf("write %s: %w", paths.settingsPath, err)
	}
	if f, err := os.Create(paths.heartbeatHost); err == nil {
		f.Close()
	}
	return paths.heartbeatHost, nil
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

func (r *ClaudeCodeRunner) SetupNeedsInput(mountSource string, env map[string]string) (string, error) {
	paths, err := resolveClaudeHookPaths(mountSource, env)
	if err != nil {
		return "", err
	}
	if err := os.MkdirAll(paths.hostConfigDir, 0o755); err != nil {
		return "", fmt.Errorf("mkdir %s: %w", paths.hostConfigDir, err)
	}
	scriptHost := filepath.Join(paths.hostConfigDir, "dump-stop-message.js")
	if err := os.WriteFile(scriptHost, stopMessageDumpScript(paths.needsInputContainer), 0o644); err != nil {
		return "", fmt.Errorf("write dump script: %w", err)
	}
	if err := os.WriteFile(paths.settingsPath, buildClaudeHookSettings(paths, true), 0o644); err != nil {
		return "", fmt.Errorf("write %s: %w", paths.settingsPath, err)
	}
	return paths.needsInputHost, nil
}

func (r *ClaudeCodeRunner) ReadLastMessage(markerPath string) string {
	if markerPath == "" {
		return ""
	}
	data, err := os.ReadFile(markerPath)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(data))
}

// stopMessageDumpScript returns a Node.js script invoked by Claude Code's
// "Stop" hook. Claude pipes JSON with last_assistant_message on stdin.
// We extract the tail of that message and write it to markerPath so the
// Go worker can classify the turn via BAML.
func stopMessageDumpScript(markerPath string) []byte {
	const maxChars = 8000
	return []byte(fmt.Sprintf(`"use strict";

const fs = require("fs");
const MARKER = %q;
const chunks = [];

process.stdin.on("data", (chunk) => chunks.push(chunk));
process.stdin.on("end", () => {
  try {
    const msg = (JSON.parse(Buffer.concat(chunks)).last_assistant_message || "").trim();
    fs.writeFileSync(MARKER, msg.slice(-%d));
  } catch (_) {
    fs.writeFileSync(MARKER, "");
  }
});
`, markerPath, maxChars))
}

func buildClaudeHookSettings(paths *claudeHookPaths, includeClassify bool) []byte {
	type hookEntry struct {
		Type    string `json:"type"`
		Command string `json:"command"`
	}
	type matcherGroup struct {
		Matcher string      `json:"matcher,omitempty"`
		Hooks   []hookEntry `json:"hooks"`
	}
	type settings struct {
		Hooks map[string][]matcherGroup `json:"hooks"`
	}

	heartbeatCmd := "date +%s > " + paths.heartbeatContainer
	heartbeat := hookEntry{Type: "command", Command: heartbeatCmd}

	stopHooks := []hookEntry{heartbeat}
	if includeClassify {
		dumpScript := path.Join(path.Dir(paths.needsInputContainer), claudeStateDirName, "dump-stop-message.js")
		stopHooks = append(stopHooks, hookEntry{
			Type:    "command",
			Command: "node " + dumpScript,
		})
	}

	s := settings{Hooks: map[string][]matcherGroup{
		"PostToolUse":  {{Hooks: []hookEntry{heartbeat}}},
		"Notification": {{Hooks: []hookEntry{heartbeat}}},
		"Stop":         {{Hooks: stopHooks}},
	}}
	b, _ := json.MarshalIndent(s, "", "  ")
	return b
}

func (r *ClaudeCodeRunner) injectEnv(env map[string]string) {
	if r.anthropicAPIKey != "" {
		env["ANTHROPIC_API_KEY"] = r.anthropicAPIKey
	}
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

func vfsHostPathWithinMount(mountSource, containerPath string) (string, error) {
	hostPath := filepath.Clean(vfsHostPath(mountSource, containerPath))
	mountRoot := filepath.Clean(mountSource)
	rel, err := filepath.Rel(mountRoot, hostPath)
	if err != nil {
		return "", fmt.Errorf("resolve host path: %w", err)
	}
	if rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("container path %q escapes mount source", containerPath)
	}
	return hostPath, nil
}

func defaultClaudeConfigDir(env map[string]string) string {
	return defaultClaudePersistentConfigDir(env)
}

func claudeWorkspaceDir(env map[string]string) string {
	workspaceDir := types.ContainerWorkDir
	if env != nil {
		if wd := strings.TrimSpace(env[agentWorkspaceDirEnvKey]); wd != "" {
			cleaned := path.Clean(wd)
			if cleaned == types.ContainerWorkDir || strings.HasPrefix(cleaned, types.ContainerWorkDir+"/") {
				workspaceDir = cleaned
			}
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

func defaultClaudeCheckpointPath(env map[string]string) string {
	return path.Join(
		claudeWorkspaceDir(env),
		claudeStateRootDir,
		claudeStateScopeForEnv(env),
		claudeCheckpointFile,
	)
}

func writeClaudeSessionCheckpoint(mountSource string, env map[string]string, checkpoint *types.SessionCheckpoint) error {
	if strings.TrimSpace(mountSource) == "" || checkpoint == nil {
		return fmt.Errorf("mount source and checkpoint are required")
	}
	checkpointPath, err := vfsHostPathWithinMount(mountSource, defaultClaudeCheckpointPath(env))
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(checkpointPath), 0o755); err != nil {
		return fmt.Errorf("mkdir checkpoint dir: %w", err)
	}
	payload, err := json.MarshalIndent(checkpoint, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal checkpoint: %w", err)
	}
	tmpPath := checkpointPath + ".tmp"
	if err := os.WriteFile(tmpPath, payload, 0o644); err != nil {
		return fmt.Errorf("write checkpoint temp: %w", err)
	}
	if err := os.Rename(tmpPath, checkpointPath); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("rename checkpoint: %w", err)
	}
	return nil
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
	setEnvDefault(env, "KERNEL_API_KEY", r.kernelAPIKey)
	injectKernelBrowserEnv(env)
	log.Debug().Str("provider", env["AGENT_BROWSER_PROVIDER"]).Msg("kernel browser env injected")
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
