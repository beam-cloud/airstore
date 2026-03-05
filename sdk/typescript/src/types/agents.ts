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

/**
 * A channel binding that routes inbound messages (email, SMS) to an agent or workspace.
 *
 * When `agent_id` is set, the binding is scoped to that specific agent —
 * inbound messages go directly to that agent as tasks.
 *
 * When `agent_id` is null, the binding is workspace-level — inbound
 * messages are routed to agents automatically via BAML classification.
 */
export interface ChannelBinding {
  id: number;
  workspace_id: number;
  agent_id: string | null;
  channel_type: 'email' | 'sms';
  address: string;
  config_json: Record<string, unknown>;
  active: boolean;
  created_at: string;
  updated_at: string;
}

/** Aggregated task metrics for an agent. */
export interface AgentStats {
  total: number;
  by_state: Record<string, number>;
  avg_run_sec?: number;
}

/** Parameters for upserting channel bindings. */
export interface UpdateChannelsParams {
  channels: Array<{
    channel_type: string;
    address: string;
    active?: boolean;
    config_json?: Record<string, unknown>;
  }>;
}