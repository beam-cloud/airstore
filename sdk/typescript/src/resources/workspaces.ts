import type { CoreClient, RequestOptions } from '../client.js';
import type { Workspace, WorkspaceCreateParams } from '../types/workspaces.js';

/**
 * Manage workspaces.
 *
 * Workspaces are the top-level container for connections, source views,
 * members, and the virtual filesystem. When using an organization token,
 * workspaces are automatically scoped to the token's tenant.
 *
 * @example
 * ```ts
 * const ws = await airstore.workspaces.create({ name: "user-123" });
 * console.log(ws.external_id);
 * ```
 */
export class Workspaces {
  constructor(private readonly client: CoreClient) {}

  /**
   * Create a new workspace.
   *
   * When called with an organization token, the workspace is automatically
   * tagged with the token's `tenant_id`.
   *
   * @param params - Workspace creation parameters.
   * @param options - Per-request overrides.
   * @returns The newly created workspace.
   *
   * @throws {AuthenticationError} If the API key is invalid or missing.
   * @throws {PermissionDeniedError} If the token lacks workspace creation rights.
   */
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

  /**
   * List all workspaces accessible to the authenticated token.
   *
   * Organization tokens only see workspaces belonging to their tenant.
   * Cluster admin tokens see all workspaces.
   *
   * @param options - Per-request overrides.
   * @returns Array of workspaces.
   */
  async list(options?: RequestOptions): Promise<Workspace[]> {
    return this.client.request<Workspace[]>(
      'GET',
      '/workspaces',
      undefined,
      undefined,
      options,
    );
  }

  /**
   * Retrieve a workspace by its external ID.
   *
   * @param id - Workspace external ID (UUID).
   * @param options - Per-request overrides.
   * @returns The workspace.
   *
   * @throws {NotFoundError} If the workspace doesn't exist.
   */
  async retrieve(id: string, options?: RequestOptions): Promise<Workspace> {
    return this.client.request<Workspace>(
      'GET',
      `/workspaces/${id}`,
      undefined,
      undefined,
      options,
    );
  }

  /**
   * Delete a workspace and all associated data.
   *
   * @param id - Workspace external ID (UUID).
   * @param options - Per-request overrides.
   *
   * @throws {NotFoundError} If the workspace doesn't exist.
   */
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
