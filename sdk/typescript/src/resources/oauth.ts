import type { CoreClient, RequestOptions } from '../client.js';
import type {
  OAuthSession,
  OAuthSessionCreateParams,
  OAuthSessionStatus,
  OAuthPollOptions,
} from '../types/oauth.js';
import { AirstoreError } from '../errors.js';

/**
 * Manage OAuth sessions for interactive connection setup.
 *
 * Use this when you want users to authorize via browser redirect
 * rather than passing tokens directly.
 */
export class OAuth {
  constructor(private readonly client: CoreClient) {}

  /**
   * Create an OAuth session. Returns a session with an `authorize_url`
   * that the user should be redirected to.
   */
  async createSession(
    params: OAuthSessionCreateParams,
    options?: RequestOptions,
  ): Promise<OAuthSession> {
    const body: Record<string, unknown> = {
      integration_type: params.integrationType,
    };
    if (params.workspaceId !== undefined) body['workspace_id'] = params.workspaceId;
    if (params.returnTo !== undefined) body['return_to'] = params.returnTo;

    return this.client.request<OAuthSession>(
      'POST',
      '/oauth/sessions',
      body,
      undefined,
      options,
    );
  }

  /** Get the current status of an OAuth session. */
  async getSession(
    sessionId: string,
    options?: RequestOptions,
  ): Promise<OAuthSessionStatus> {
    return this.client.request<OAuthSessionStatus>(
      'GET',
      `/oauth/sessions/${sessionId}`,
      undefined,
      undefined,
      options,
    );
  }

  /** Poll an OAuth session until completion, error, or timeout. */
  async poll(
    sessionId: string,
    pollOpts?: OAuthPollOptions,
    options?: RequestOptions,
  ): Promise<OAuthSessionStatus> {
    const timeout = pollOpts?.timeout ?? 300_000;
    const interval = pollOpts?.interval ?? 2_000;
    const deadline = Date.now() + timeout;

    while (Date.now() < deadline) {
      const status = await this.getSession(sessionId, options);

      if (status.status === 'complete') return status;
      if (status.status === 'error') {
        throw new AirstoreError(
          `OAuth session failed: ${status.error ?? 'unknown error'}`,
        );
      }

      await new Promise((resolve) => setTimeout(resolve, interval));
    }

    throw new AirstoreError(`OAuth session timed out after ${timeout}ms`);
  }
}
