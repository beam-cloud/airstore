import type { CoreClient, RequestOptions } from '../client.js';
import type { Workspace, WorkspaceCreateParams } from '../types/workspaces.js';
import type {
  ChannelBinding,
  UpdateChannelsParams,
} from '../types/agents.js';

/**
 * Create, list, retrieve, and delete workspaces.
 *
 * Workspaces are the top-level container for connections, source views,
 * agents, members, and the virtual filesystem.
 */
export class Workspaces {
  constructor(private readonly client: CoreClient) {}

  /** Create a new workspace scoped to the token's tenant. */
  async create(
    params: WorkspaceCreateParams,
    options?: RequestOptions,
  ): Promise<Workspace> {
    return this.client.request<Workspace>(
      'POST',
      '/workspaces',
      params,
      undefined,
      options,
    );
  }

  /** List all workspaces accessible to the authenticated token. */
  async list(options?: RequestOptions): Promise<Workspace[]> {
    return this.client.request<Workspace[]>(
      'GET',
      '/workspaces',
      undefined,
      undefined,
      options,
    );
  }

  /** Retrieve a workspace by external ID. */
  async retrieve(id: string, options?: RequestOptions): Promise<Workspace> {
    return this.client.request<Workspace>(
      'GET',
      `/workspaces/${id}`,
      undefined,
      undefined,
      options,
    );
  }

  /** Delete a workspace and all associated data. */
  async del(id: string, options?: RequestOptions): Promise<void> {
    await this.client.request<null>(
      'DELETE',
      `/workspaces/${id}`,
      undefined,
      undefined,
      options,
    );
  }

  /** List workspace-level channel bindings. */
  async listChannels(
    workspaceId: string,
    options?: RequestOptions,
  ): Promise<ChannelBinding[]> {
    return this.client.request<ChannelBinding[]>(
      'GET',
      `/workspaces/${workspaceId}/channels`,
      undefined,
      undefined,
      options,
    );
  }

  /** Upsert workspace-level channel bindings. */
  async updateChannels(
    workspaceId: string,
    params: UpdateChannelsParams,
    options?: RequestOptions,
  ): Promise<ChannelBinding[]> {
    return this.client.request<ChannelBinding[]>(
      'PUT',
      `/workspaces/${workspaceId}/channels`,
      params,
      undefined,
      options,
    );
  }

  /** Remove a workspace-level channel binding by type. */
  async deleteChannel(
    workspaceId: string,
    channelType: string,
    options?: RequestOptions,
  ): Promise<void> {
    await this.client.request<null>(
      'DELETE',
      `/workspaces/${workspaceId}/channels/${channelType}`,
      undefined,
      undefined,
      options,
    );
  }
}
