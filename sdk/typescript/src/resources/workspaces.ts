import type { CoreClient, RequestOptions } from '../client.js';
import type { Workspace, WorkspaceCreateParams } from '../types/workspaces.js';

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
}
