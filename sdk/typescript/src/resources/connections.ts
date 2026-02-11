import type { CoreClient, RequestOptions } from '../client.js';
import type { Connection, ConnectionCreateParams } from '../types/connections.js';

/**
 * Manage connections (integrations) within a workspace.
 *
 * Connections store OAuth tokens or API keys for external services like
 * Gmail, GitHub, Notion, etc. Pass existing tokens directly to avoid
 * requiring users to re-authenticate.
 *
 * @example
 * ```ts
 * await airstore.connections.create("ws_abc", {
 *   integrationType: "gmail",
 *   accessToken: existingAccessToken,
 *   refreshToken: existingRefreshToken,
 * });
 * ```
 */
export class Connections {
  constructor(private readonly client: CoreClient) {}

  /**
   * Create a connection by passing existing OAuth credentials or API keys.
   *
   * @param workspaceId - Workspace external ID.
   * @param params - Connection creation parameters.
   * @param options - Per-request overrides.
   * @returns The created connection.
   */
  async create(
    workspaceId: string,
    params: ConnectionCreateParams,
    options?: RequestOptions,
  ): Promise<Connection> {
    const body: Record<string, unknown> = {
      integration_type: params.integrationType,
    };
    if (params.accessToken !== undefined) body['access_token'] = params.accessToken;
    if (params.refreshToken !== undefined) body['refresh_token'] = params.refreshToken;
    if (params.apiKey !== undefined) body['api_key'] = params.apiKey;
    if (params.scope !== undefined) body['scope'] = params.scope;
    if (params.extra !== undefined) body['extra'] = params.extra;

    return this.client.request<Connection>(
      'POST',
      `/workspaces/${workspaceId}/connections`,
      body,
      undefined,
      options,
    );
  }

  /**
   * List all connections in a workspace.
   *
   * @param workspaceId - Workspace external ID.
   * @param options - Per-request overrides.
   * @returns Array of connections.
   */
  async list(workspaceId: string, options?: RequestOptions): Promise<Connection[]> {
    const result = await this.client.request<Connection[]>(
      'GET',
      `/workspaces/${workspaceId}/connections`,
      undefined,
      undefined,
      options,
    );
    return Array.isArray(result) ? result : [];
  }

  /**
   * Delete a connection.
   *
   * @param workspaceId - Workspace external ID.
   * @param connectionId - Connection external ID.
   * @param options - Per-request overrides.
   *
   * @throws {NotFoundError} If the connection doesn't exist.
   */
  async del(
    workspaceId: string,
    connectionId: string,
    options?: RequestOptions,
  ): Promise<void> {
    await this.client.request<null>(
      'DELETE',
      `/workspaces/${workspaceId}/connections/${connectionId}`,
      undefined,
      undefined,
      options,
    );
  }
}
