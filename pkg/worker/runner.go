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

	claudeConfigDirEnvKey   = "CLAUDE_CONFIG_DIR"
	claudeDefaultShellEnv   = "/bin/bash"
	claudeStateDirName      = ".claude"
	claudeStateRootDir      = ".airstore/claude"
	claudeCheckpointFile    = "session-checkpoint.json"
	claudeHeartbeatFile     = ".heartbeat"
	claudeNeedsInputFile    = ".needs_input"
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

// NeedsInputRunner extends AgentExecutionRunner with input-detection.
// A Stop hook dumps the agent's last message to a marker file. The
// worker reads it and calls BAML to classify whether the agent is
// blocked on user input or done.
type NeedsInputRunner interface {
	AgentExecutionRunner
	SetupNeedsInput(mountSource string, env map[string]string) (string, error)
	ReadLastMessage(markerPath string) string
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

// stopMessageDumpScript returns a Node.js script that reads the Stop hook
// JSON from stdin and writes the last 1000 chars of last_assistant_message
// to markerPath. The Go worker then classifies this via BAML.
func stopMessageDumpScript(markerPath string) []byte {
	return []byte(fmt.Sprintf(`const fs=require("fs");
let b=Buffer.alloc(0);
process.stdin.on("data",c=>{b=Buffer.concat([b,c])});
process.stdin.on("end",()=>{
  try {
    const m=(JSON.parse(b).last_assistant_message||"").trim();
    fs.writeFileSync(%q,m.slice(-1000));
  } catch(e) {
    fs.writeFileSync(%q,"");
  }
});`, markerPath, markerPath))
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
