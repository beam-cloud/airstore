import type { CoreClient, RequestOptions } from '../client.js';
import type { AgentConfig, AgentCreateParams, AgentProfile } from '../types/agents.js';

const AGENT_CONFIG_KEY_RUNNER = 'runner';

/**
 * Manage agent profiles in a workspace.
 */
export class Agents {
  constructor(private readonly client: CoreClient) {}

  /**
   * Get the default agent config for a given agent key. Includes the default
   * system prompt and workspace directory.
   */
  async defaults(
    workspaceId: string,
    agentKey?: string,
    options?: RequestOptions,
  ): Promise<AgentConfig> {
    const params = agentKey ? { agent_key: agentKey } : undefined;
    return this.client.request<AgentConfig>(
      'GET',
      `/workspaces/${workspaceId}/agents/defaults`,
      undefined,
      params,
      options,
    );
  }

  async create(
    workspaceId: string,
    params: AgentCreateParams,
    options?: RequestOptions,
  ): Promise<AgentProfile> {
    const config: Record<string, unknown> = { ...(params.config ?? {}) };
    if (params.runner) {
      config[AGENT_CONFIG_KEY_RUNNER] = params.runner;
    }

    return this.client.request<AgentProfile>(
      'POST',
      `/workspaces/${workspaceId}/agents`,
      {
        agent_key: params.agentKey,
        name: params.name,
        config,
        active: params.active,
      },
      undefined,
      options,
    );
  }

  async list(
    workspaceId: string,
    options?: RequestOptions,
  ): Promise<AgentProfile[]> {
    return this.client.request<AgentProfile[]>(
      'GET',
      `/workspaces/${workspaceId}/agents`,
      undefined,
      undefined,
      options,
    );
  }

  async retrieve(
    workspaceId: string,
    agentId: string,
    options?: RequestOptions,
  ): Promise<AgentProfile> {
    return this.client.request<AgentProfile>(
      'GET',
      `/workspaces/${workspaceId}/agents/${agentId}`,
      undefined,
      undefined,
      options,
    );
  }
}
