import type { CoreClient, RequestOptions } from '../client.js';
import type {
  AgentConfig, AgentCreateParams, AgentProfile, AgentStats, AgentUpdateParams,
  ChannelBinding, UpdateChannelsParams,
} from '../types/agents.js';

const AGENT_CONFIG_KEY_RUNNER = 'runner';

/**
 * CRUD operations for agent profiles within a workspace.
 *
 * An agent profile defines the runner, model, system prompt, and other
 * configuration used when a task is dispatched to that agent.
 */
export class Agents {
  constructor(private readonly client: CoreClient) {}

  /** Retrieve the default agent config (system prompt, workspace dir) for a runner key. */
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

  /** Create a new agent profile. Returns the created profile with its server-assigned `id`. */
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

  /** List all agent profiles in a workspace. */
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

  /** Retrieve a single agent profile by ID. */
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

  /** Update an agent profile. Only provided fields are changed (merge semantics for config). */
  async update(
    workspaceId: string,
    agentId: string,
    params: AgentUpdateParams,
    options?: RequestOptions,
  ): Promise<AgentProfile> {
    const body: Record<string, unknown> = {};
    if (params.name != null) body.name = params.name;
    if (params.role != null) body.role = params.role;
    if (params.memoryScope != null) body.memory_scope = params.memoryScope;
    if (params.qualityScore != null) body.quality_score = params.qualityScore;
    if (params.costBudgetUsd != null) body.cost_budget_usd = params.costBudgetUsd;
    if (params.active != null) body.active = params.active;
    if (params.config != null) body.config = params.config;
    return this.client.request<AgentProfile>(
      'PATCH',
      `/workspaces/${workspaceId}/agents/${agentId}`,
      body,
      undefined,
      options,
    );
  }

  /** Delete an agent profile and any hooks bound to it. */
  async delete(
    workspaceId: string,
    agentId: string,
    options?: RequestOptions,
  ): Promise<void> {
    await this.client.request(
      'DELETE',
      `/workspaces/${workspaceId}/agents/${agentId}`,
      undefined,
      undefined,
      options,
    );
  }

  /** Retrieve aggregated task stats for an agent. */
  async stats(
    workspaceId: string,
    agentId: string,
    options?: RequestOptions,
  ): Promise<AgentStats> {
    return this.client.request<AgentStats>(
      'GET',
      `/workspaces/${workspaceId}/agents/${agentId}/stats`,
      undefined,
      undefined,
      options,
    );
  }

  /** List channel bindings for an agent. */
  async listChannels(
    workspaceId: string,
    agentId: string,
    options?: RequestOptions,
  ): Promise<ChannelBinding[]> {
    return this.client.request<ChannelBinding[]>(
      'GET',
      `/workspaces/${workspaceId}/agents/${agentId}/channels`,
      undefined,
      undefined,
      options,
    );
  }

  /** Upsert channel bindings for an agent. */
  async updateChannels(
    workspaceId: string,
    agentId: string,
    params: UpdateChannelsParams,
    options?: RequestOptions,
  ): Promise<ChannelBinding[]> {
    return this.client.request<ChannelBinding[]>(
      'PUT',
      `/workspaces/${workspaceId}/agents/${agentId}/channels`,
      params,
      undefined,
      options,
    );
  }

  /** Remove a channel binding by type. */
  async deleteChannel(
    workspaceId: string,
    agentId: string,
    channelType: string,
    options?: RequestOptions,
  ): Promise<void> {
    await this.client.request(
      'DELETE',
      `/workspaces/${workspaceId}/agents/${agentId}/channels/${channelType}`,
      undefined,
      undefined,
      options,
    );
  }
}
