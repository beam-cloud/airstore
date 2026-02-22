export interface AgentProfile {
  id: string;
  workspace_id: number;
  agent_key: string;
  name: string;
  config_json: Record<string, unknown>;
  active: boolean;
  created_at: string;
  updated_at: string;
}

export interface AgentCreateParams {
  agentKey: string;
  name: string;
  config?: Record<string, unknown>;
  active?: boolean;
}
