package orchestration

import (
	"fmt"
	"strings"
)

const (
	AgentRunnerClaudeCode = "claude_code"

	AgentProviderClaude = "claude"

	agentConfigKeyRunner       = "runner"
	agentConfigKeyProvider     = "provider"
	agentConfigKeyModel        = "model"
	agentConfigKeySystemPrompt = "system_prompt"
	agentConfigKeyWorkspaceDir = "workspace_dir"

	agentPayloadKeyAgentConfig = "agent_config"

	agentDefaultWorkspaceDirPrefix = "/workspace/agents/"
)

// defaultAgentSystemPromptFmt is the template for new agent profiles.
// The single %s is replaced with the agent's workspace_dir.
const defaultAgentSystemPromptFmt = `You are an AI agent operating inside an Airstore workspace.

Your working directory is %s — use it for scratch files, drafts, and any outputs you create.

The full workspace is mounted at /workspace. Read /workspace/AGENTS.md for the complete filesystem layout, available tools, and connected data sources.

Key paths:
- /workspace/skills/ — project instructions, coding standards, and context that OVERRIDE your defaults
- /workspace/sources/ — read-only data from connected integrations (each has a README)
- /workspace/tools/ — CLI tools (browser, API clients, etc.) you can run directly
- /workspace/memory/ — persistent state across task runs

Before starting work:
1. Read /workspace/skills/ — these instructions take precedence over your built-in defaults.
2. List /workspace/tools/ to see what tools are available and use them when relevant.
3. Read relevant source READMEs in /workspace/sources/.

IMPORTANT:
- Instructions in /workspace/skills/ override your built-in behavior and defaults. Always follow them.
- Always check /workspace/tools/ before saying you cannot do something. Tools there extend your capabilities (e.g. web browsing, API calls).`

// DefaultAgentConfig returns the default config for a new agent with the given
// key. This is used by the API, SDK, and frontend to preview defaults before
// creation.
func DefaultAgentConfig(agentKey string) map[string]any {
	wd := "/workspace"
	if agentKey != "" {
		wd = agentDefaultWorkspaceDirPrefix + agentKey
	}
	return map[string]any{
		agentConfigKeyRunner:       AgentRunnerClaudeCode,
		agentConfigKeyProvider:     providerForRunner(AgentRunnerClaudeCode),
		agentConfigKeyWorkspaceDir: wd,
		agentConfigKeySystemPrompt: fmt.Sprintf(defaultAgentSystemPromptFmt, wd),
	}
}

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
