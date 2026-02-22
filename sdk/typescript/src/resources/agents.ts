import type { CoreClient, RequestOptions } from '../client.js';
import type { AgentCreateParams, AgentProfile } from '../types/agents.js';

/**
 * Manage orchestration agent profiles in a workspace.
 */
export class Agents {
  constructor(private readonly client: CoreClient) {}

  async create(
    workspaceId: string,
    params: AgentCreateParams,
    options?: RequestOptions,
  ): Promise<AgentProfile> {
    return this.client.request<AgentProfile>(
      'POST',
      `/workspaces/${workspaceId}/agents`,
      {
        agent_key: params.agentKey,
        name: params.name,
        config: params.config ?? {},
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
