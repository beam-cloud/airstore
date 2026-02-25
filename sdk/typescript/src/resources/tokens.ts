import type { CoreClient, RequestOptions } from '../client.js';
import type { Token, TokenCreateParams, TokenCreated } from '../types/tokens.js';

/**
 * Manage workspace-scoped authentication tokens.
 *
 * Tokens are used for CLI mounting (`airstore start --token <token>`)
 * and per-workspace programmatic access.
 *
 * @example
 * ```ts
 * const token = await airstore.tokens.create("ws_abc", {
 *   email: "agent@internal",
 *   name: "vm-mount",
 * });
 * // token.token -> pass to: airstore start --token <this>
 * ```
 */
export class Tokens {
  constructor(private readonly client: CoreClient) {}

  /**
   * Create a workspace-scoped token.
   *
   * Either `memberId` or `email` must be provided. If `email` is given
   * and no member with that email exists, one is auto-created.
   *
   * @param workspaceId - Workspace external ID.
   * @param params - Token creation parameters.
   * @param options - Per-request overrides.
   * @returns The created token with raw value (shown once — store it safely).
   */
  async create(
    workspaceId: string,
    params: TokenCreateParams,
    options?: RequestOptions,
  ): Promise<TokenCreated> {
    const body: Record<string, unknown> = {};
    if (params.memberId !== undefined) body['member_id'] = params.memberId;
    if (params.email !== undefined) body['email'] = params.email;
    if (params.name !== undefined) body['name'] = params.name;
    if (params.expiresIn !== undefined) body['expires_in'] = params.expiresIn;

    return this.client.request<TokenCreated>(
      'POST',
      `/workspaces/${workspaceId}/tokens`,
      body,
      undefined,
      options,
    );
  }

  /**
   * List tokens in a workspace.
   *
   * Raw token values are never returned — only metadata.
   *
   * @param workspaceId - Workspace external ID.
   * @param options - Per-request overrides.
   * @returns Array of token metadata.
   */
  async list(workspaceId: string, options?: RequestOptions): Promise<Token[]> {
    return this.client.request<Token[]>(
      'GET',
      `/workspaces/${workspaceId}/tokens`,
      undefined,
      undefined,
      options,
    );
  }

  /**
   * Revoke (delete) a token. Once revoked, the token can no longer be used.
   *
   * @param workspaceId - Workspace external ID.
   * @param tokenId - Token external ID.
   * @param options - Per-request overrides.
   *
   * @throws {NotFoundError} If the token doesn't exist.
   */
  async revoke(
    workspaceId: string,
    tokenId: string,
    options?: RequestOptions,
  ): Promise<void> {
    await this.client.request<null>(
      'DELETE',
      `/workspaces/${workspaceId}/tokens/${tokenId}`,
      undefined,
      undefined,
      options,
    );
  }
}
