package worker

import (
	"strings"

	"github.com/beam-cloud/airstore/pkg/types"
	"github.com/rs/zerolog/log"
)

const (
	agentProviderEnvKey      = "AIRSTORE_AGENT_PROVIDER"
	agentModelEnvKey         = "AIRSTORE_AGENT_MODEL"
	agentResumeSessionEnvKey = "AIRSTORE_AGENT_RESUME_SESSION"

	claudeProviderName    = "claude"
	claudeConfigDirEnvKey = "CLAUDE_CONFIG_DIR"
	claudeConfigDirPath   = "/workspace/.claude"
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
	BuildTurnArgs(prompt string, env map[string]string, continueSession bool) []string
}

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

	addTaskExecutionContext(
		log.Info().
			Str("model", model).
			Str("prompt", task.Prompt[:min(50, len(task.Prompt))]),
		task,
	).Msg("running claude code task")

	return newPromptEntrypointBuilder("claude").
		withFlag("--print").
		withFlag("--verbose").
		withKeyValue("--output-format", "stream-json").
		withFlag("--dangerously-skip-permissions").
		withKeyValue("--model", model).
		withPrompt(task.Prompt).
		build()
}

// BuildTurnArgs returns the argv for a single interactive turn.
// Each turn runs claude --print as a separate process in the sandbox;
// the Go worker manages the loop between turns.
func (r *ClaudeCodeRunner) BuildTurnArgs(prompt string, env map[string]string, continueSession bool) []string {
	r.injectEnv(env)
	model := strings.TrimSpace(env[agentModelEnvKey])

	builder := newPromptEntrypointBuilder("claude")
	if continueSession {
		builder.withFlag("--continue")
	}
	return builder.
		withFlag("--print").
		withFlag("--verbose").
		withKeyValue("--output-format", "stream-json").
		withFlag("--dangerously-skip-permissions").
		withKeyValue("--model", model).
		withPrompt(prompt).
		build()
}

func (r *ClaudeCodeRunner) injectEnv(env map[string]string) {
	r.injectAPIKey(env, "ANTHROPIC_API_KEY", r.anthropicAPIKey, true)
	r.injectKernelEnv(env)
	if strings.TrimSpace(env[claudeConfigDirEnvKey]) == "" {
		env[claudeConfigDirEnvKey] = claudeConfigDirPath
	}
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

func runnerProviderFromEnv(env map[string]string) string {
	if env == nil {
		return ""
	}
	return strings.ToLower(strings.TrimSpace(env[agentProviderEnvKey]))
}

func providerFromExecutionPolicy(policy map[string]any) string {
	if len(policy) == 0 {
		return ""
	}
	raw, ok := policy["provider"]
	if !ok || raw == nil {
		return ""
	}
	value, ok := raw.(string)
	if !ok {
		return ""
	}
	return strings.ToLower(strings.TrimSpace(value))
}

func isClaudeExecutionTask(task types.RunExecution) bool {
	provider := runnerProviderFromEnv(task.Env)
	if provider == "" {
		provider = providerFromExecutionPolicy(task.ExecutionPolicy)
	}
	return provider == claudeProviderName
}
