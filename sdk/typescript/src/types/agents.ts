/** Runner implementation that executes the agent. */
export type AgentRunner = 'claude_code';

/** LLM provider backing the runner. Inferred from the runner (e.g. claude_code -> claude). */
export type AgentProvider = 'claude';

/** Agent configuration: runner, model, system prompt, and workspace settings. */
export interface AgentConfig {
  runner?: AgentRunner;
  provider?: AgentProvider;
  model?: string;
  system_prompt?: string;
  system_prompt_mode?: string;
  workspace_dir?: string;
  [key: string]: unknown;
}

/** A registered agent profile within a workspace. */
export interface AgentProfile {
  id: string;
  workspace_id: number;
  agent_key: string;
  name: string;
  config_json: AgentConfig;
  active: boolean;
  created_at: string;
  updated_at: string;
}

/** Parameters for creating a new agent profile. */
export interface AgentCreateParams {
  agentKey: string;
  name: string;
  runner?: AgentRunner;
  config?: AgentConfig;
  active?: boolean;
}

/** Parameters for updating an existing agent profile. All fields are optional. */
export interface AgentUpdateParams {
  name?: string;
  config?: AgentConfig;
  active?: boolean;
}
