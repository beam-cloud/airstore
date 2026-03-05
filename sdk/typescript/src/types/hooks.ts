/** A file-system hook that auto-triggers agent tasks on source view changes. */
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

/** Parameters for creating a hook on a source view folder. */
export interface HookCreateParams {
  /** Source view folder path to watch (e.g. `/sources/gmail/Recent Emails`). */
  path: string;
  /** Prompt sent to the agent when an event fires. */
  prompt: string;
  /** Single skill path (prefer `skillPaths`). */
  skillPath?: string;
  /** Skill paths the agent can use when handling hook events. */
  skillPaths?: string[];
  /** File event types to listen for (e.g. `['create', 'write']`). Defaults to all. */
  eventTypes?: string[];
  /** Display name for the auto-created agent. */
  agentName?: string;
  /** Agent configuration overrides. */
  agentConfig?: {
    runner?: string;
    model?: string;
    systemPrompt?: string;
    systemPromptMode?: string;
    workspaceDir?: string;
  };
}

/** Parameters for updating a hook. All fields are optional. */
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
