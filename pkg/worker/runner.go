package worker

import (
	"strings"

	"github.com/beam-cloud/airstore/pkg/types"
	"github.com/rs/zerolog/log"
)

const (
	agentProviderEnvKey = "AIRSTORE_AGENT_PROVIDER"
	agentModelEnvKey    = "AIRSTORE_AGENT_MODEL"
)

// AgentExecutionRunner builds the process entrypoint for an agent task.
// Different providers/runtimes can implement this interface.
type AgentExecutionRunner interface {
	Name() string
	BuildEntrypoint(task types.RunExecution, env map[string]string) []string
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
	r.injectAPIKey(env, "ANTHROPIC_API_KEY", r.anthropicAPIKey, true)
	r.injectKernelEnv(env)
	model := strings.TrimSpace(env[agentModelEnvKey])

	addTaskExecutionContext(
		log.Info().
			Str("model", model).
			Str("prompt", task.Prompt[:min(50, len(task.Prompt))]),
		task,
	).Msg("running claude code task")

	return claudePromptEntrypoint(task.Prompt, defaultClaudePromptEntrypointOptions(model))
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

type ClaudePromptEntrypointOptions struct {
	Model           string
	Print           bool
	Verbose         bool
	OutputFormat    string
	SkipPermissions bool
}

func defaultClaudePromptEntrypointOptions(model string) ClaudePromptEntrypointOptions {
	return ClaudePromptEntrypointOptions{
		Model:           model,
		Print:           true,
		Verbose:         true,
		OutputFormat:    "stream-json",
		SkipPermissions: true,
	}
}

type promptEntrypointBuilder struct {
	binary string
	args   []string
	prompt string
}

func newPromptEntrypointBuilder(binary string, prompt string) *promptEntrypointBuilder {
	return &promptEntrypointBuilder{
		binary: strings.TrimSpace(binary),
		args:   []string{},
		prompt: prompt,
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

func (b *promptEntrypointBuilder) build() []string {
	argv := make([]string, 0, len(b.args)+3)
	argv = append(argv, b.binary)
	argv = append(argv, b.args...)
	argv = append(argv, "-p", b.prompt)
	return argv
}

func claudePromptEntrypoint(prompt string, opts ClaudePromptEntrypointOptions) []string {
	builder := newPromptEntrypointBuilder("claude", prompt)
	if opts.Print {
		builder.withFlag("--print")
	}
	if opts.Verbose {
		builder.withFlag("--verbose")
	}
	builder.withKeyValue("--output-format", opts.OutputFormat)
	if opts.SkipPermissions {
		builder.withFlag("--dangerously-skip-permissions")
	}
	builder.withKeyValue("--model", opts.Model)
	return builder.build()
}

func runnerProviderFromEnv(env map[string]string) string {
	if env == nil {
		return ""
	}
	return strings.ToLower(strings.TrimSpace(env[agentProviderEnvKey]))
}
