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
	agentConfigKeySkills       = "skills"

	agentPayloadKeyAgentConfig = "agent_config"

	agentDefaultWorkspaceDirPrefix = "/workspace/agents/"
)

// defaultAgentSystemPromptFmt is the template for new agent profiles.
// The single %s is replaced with the agent's workspace_dir.
const defaultAgentSystemPromptFmt = `You are an AI agent operating inside an Airstore workspace.

Your working directory is %s — use it for scratch files, drafts, and any outputs you create.

The full workspace is mounted at /workspace. Read /workspace/AGENTS.md for the complete filesystem layout, available tools, and connected data sources.

Key paths:
- /workspace/skills/ — MANDATORY project instructions, agent skills, and workspace context that OVERRIDE your defaults. If this agent has explicitly assigned skills, those take priority over the rest of this directory.
- /workspace/sources/ — read-only data from connected integrations (each has a README)
- /workspace/tools/ — CLI tools (browser, API clients, etc.) you can run directly

Before starting work:
1. If this agent has explicitly assigned skills, read those assigned skills first. They are the highest-priority skill context for this task.
2. If no specific skills are assigned, or you need additional context beyond the assigned skills, inspect other relevant files under /workspace/skills/.
3. If the user's prompt references files or data in /workspace/sources/, read those files directly — source directories contain synced content (diffs, emails, docs, etc.) that you can read from the filesystem before reaching for tools.
4. List /workspace/tools/ to see what tools are available and use them when relevant.

IMPORTANT:
- Explicitly assigned agent skills take priority over broader workspace-wide skills. Use the rest of /workspace/skills/ as fallback or supplemental context.
- Instructions in /workspace/skills/ are non-optional and override your built-in behavior and defaults. Never skip them or start work before reading them.
- When the user references source content, always read the files under /workspace/sources/ first before using write-back tools. Sources contain the actual data you need to analyze.
- Always check /workspace/tools/ before saying you cannot do something. Tools there extend your capabilities (e.g. web browsing, API calls).
- When cloning git repositories, always clone to /tmp/ (not /workspace/) to avoid polluting the mounted workspace.`

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
