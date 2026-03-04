import type { CoreClient, RequestOptions } from '../client.js';
import type { Member, MemberCreateParams } from '../types/members.js';

/**
 * Manage workspace members.
 *
 * Members are users with roles (admin, member, viewer) in a workspace.
 * Tokens are always associated with a member.
 */
export class Members {
  constructor(private readonly client: CoreClient) {}

  /** Add a member to a workspace. */
  async create(
    workspaceId: string,
    params: MemberCreateParams,
    options?: RequestOptions,
  ): Promise<Member> {
    return this.client.request<Member>(
      'POST',
      `/workspaces/${workspaceId}/members`,
      {
        email: params.email,
        name: params.name,
        role: params.role ?? 'member',
      },
      undefined,
      options,
    );
  }

  /** List members of a workspace. */
  async list(workspaceId: string, options?: RequestOptions): Promise<Member[]> {
    return this.client.request<Member[]>(
      'GET',
      `/workspaces/${workspaceId}/members`,
      undefined,
      undefined,
      options,
    );
  }

  /** Remove a member from a workspace. */
  async del(
    workspaceId: string,
    memberId: string,
    options?: RequestOptions,
  ): Promise<void> {
    await this.client.request<null>(
      'DELETE',
      `/workspaces/${workspaceId}/members/${memberId}`,
      undefined,
      undefined,
      options,
    );
  }
}
