import type { CoreClient, RequestOptions } from '../client.js';
import type { Hook, HookCreateParams, HookUpdateParams } from '../types/hooks.js';

/**
 * CRUD operations for file-system hooks within a workspace.
 *
 * A hook watches a source view folder for file events (create, write, delete)
 * and automatically submits a task to an agent with the configured prompt.
 */
export class Hooks {
  constructor(private readonly client: CoreClient) {}

  /** Create a hook on a source view folder path. */
  async create(
    workspaceId: string,
    params: HookCreateParams,
    options?: RequestOptions,
  ): Promise<Hook> {
    const body: Record<string, unknown> = {
      path: params.path,
      prompt: params.prompt,
    };
    if (params.skillPaths != null) {
      body.skill_paths = params.skillPaths;
    } else if (params.skillPath != null) {
      body.skill_path = params.skillPath;
    }
    if (params.eventTypes != null) {
      body.event_types = params.eventTypes;
    }
    if (params.agentName != null) {
      body.agent_name = params.agentName;
    }
    if (params.agentConfig) {
      const cfg: Record<string, unknown> = {};
      if (params.agentConfig.runner != null) cfg.runner = params.agentConfig.runner;
      if (params.agentConfig.model != null) cfg.model = params.agentConfig.model;
      if (params.agentConfig.systemPrompt != null) cfg.system_prompt = params.agentConfig.systemPrompt;
      if (params.agentConfig.systemPromptMode != null) cfg.system_prompt_mode = params.agentConfig.systemPromptMode;
      if (params.agentConfig.workspaceDir != null) cfg.workspace_dir = params.agentConfig.workspaceDir;
      if (Object.keys(cfg).length > 0) body.agent_config = cfg;
    }

    return this.client.request<Hook>(
      'POST',
      `/workspaces/${workspaceId}/hooks`,
      body,
      undefined,
      options,
    );
  }

  /** List all hooks in a workspace. */
  async list(
    workspaceId: string,
    options?: RequestOptions,
  ): Promise<Hook[]> {
    return this.client.request<Hook[]>(
      'GET',
      `/workspaces/${workspaceId}/hooks`,
      undefined,
      undefined,
      options,
    );
  }

  /** Retrieve a single hook by ID. */
  async retrieve(
    workspaceId: string,
    hookId: string,
    options?: RequestOptions,
  ): Promise<Hook> {
    return this.client.request<Hook>(
      'GET',
      `/workspaces/${workspaceId}/hooks/${hookId}`,
      undefined,
      undefined,
      options,
    );
  }

  /** Update a hook. Only provided fields are changed. Set `active: false` to disable. */
  async update(
    workspaceId: string,
    hookId: string,
    params: HookUpdateParams,
    options?: RequestOptions,
  ): Promise<Hook> {
    const body: Record<string, unknown> = {};
    if (params.prompt != null) body.prompt = params.prompt;
    if (params.active != null) body.active = params.active;
    if (params.skillPaths != null) {
      body.skill_paths = params.skillPaths;
    } else if (params.skillPath != null) {
      body.skill_path = params.skillPath;
    }
    if (params.eventTypes != null) body.event_types = params.eventTypes;
    if (params.agentName != null) body.agent_name = params.agentName;
    if (params.agentConfig) {
      const cfg: Record<string, unknown> = {};
      if (params.agentConfig.runner != null) cfg.runner = params.agentConfig.runner;
      if (params.agentConfig.model != null) cfg.model = params.agentConfig.model;
      if (params.agentConfig.systemPrompt != null) cfg.system_prompt = params.agentConfig.systemPrompt;
      if (params.agentConfig.systemPromptMode != null) cfg.system_prompt_mode = params.agentConfig.systemPromptMode;
      if (params.agentConfig.workspaceDir != null) cfg.workspace_dir = params.agentConfig.workspaceDir;
      if (Object.keys(cfg).length > 0) body.agent_config = cfg;
    }

    return this.client.request<Hook>(
      'PATCH',
      `/workspaces/${workspaceId}/hooks/${hookId}`,
      body,
      undefined,
      options,
    );
  }

  /** Delete a hook permanently. */
  async delete(
    workspaceId: string,
    hookId: string,
    options?: RequestOptions,
  ): Promise<void> {
    await this.client.request(
      'DELETE',
      `/workspaces/${workspaceId}/hooks/${hookId}`,
      undefined,
      undefined,
      options,
    );
  }
}
