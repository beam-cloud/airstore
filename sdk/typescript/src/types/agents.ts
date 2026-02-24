export type AgentRunner = 'claude_code';
export type AgentProvider = 'claude' | 'anthropic';

export interface AgentConfig {
  runner?: AgentRunner;
  provider?: AgentProvider;
  model?: string;
  [key: string]: unknown;
}

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

export interface AgentCreateParams {
  agentKey: string;
  name: string;
  runner?: AgentRunner;
  config?: AgentConfig;
  active?: boolean;
}
