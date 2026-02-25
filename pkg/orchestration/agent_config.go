package orchestration

import "strings"

const (
	AgentRunnerClaudeCode = "claude_code"

	AgentProviderClaude = "claude"

	agentConfigKeyRunner       = "runner"
	agentConfigKeyProvider     = "provider"
	agentConfigKeyModel        = "model"

	agentPayloadKeyAgentConfig = "agent_config"
)

func providerForRunner(runner string) string {
	switch strings.ToLower(strings.TrimSpace(runner)) {
	case AgentRunnerClaudeCode:
		return AgentProviderClaude
	default:
		return ""
	}
}

func isClaudeCompatibleProvider(provider string) bool {
	switch strings.ToLower(strings.TrimSpace(provider)) {
	case AgentProviderClaude:
		return true
	default:
		return false
	}
}
