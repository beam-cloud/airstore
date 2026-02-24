package orchestration

import "strings"

const (
	AgentRunnerClaudeCode = "claude_code"

	AgentProviderClaude    = "claude"
	AgentProviderAnthropic = "anthropic"

	agentConfigKeyRunner       = "runner"
	agentConfigKeyProvider     = "provider"
	agentConfigKeyLLMProvider  = "llm_provider"
	agentConfigKeyModel        = "model"
	agentConfigKeyDefaultModel = "default_model"
	agentConfigKeyLLMModel     = "llm_model"

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
	case AgentProviderClaude, AgentProviderAnthropic:
		return true
	default:
		return false
	}
}
