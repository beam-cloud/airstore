export interface Hook {
  external_id: string;
  workspace_id: string;
  path: string;
  prompt: string;
  skill_path: string;
  skill_paths: string[];
  event_types: string[];
  agent_id?: string;
  agent_key?: string;
  agent_name?: string;
  agent_config?: Record<string, unknown>;
  active: boolean;
  created_at: string;
  updated_at: string;
}

export interface HookCreateParams {
  path: string;
  prompt: string;
  skillPath?: string;
  skillPaths?: string[];
  eventTypes?: string[];
  agentName?: string;
  agentConfig?: {
    runner?: string;
    model?: string;
    systemPrompt?: string;
    systemPromptMode?: string;
    workspaceDir?: string;
  };
}

export interface HookUpdateParams {
  prompt?: string;
  active?: boolean;
  skillPath?: string;
  skillPaths?: string[];
  eventTypes?: string[];
  agentName?: string;
  agentConfig?: {
    runner?: string;
    model?: string;
    systemPrompt?: string;
    systemPromptMode?: string;
    workspaceDir?: string;
  };
}
