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
 *
 * @example
 * ```ts
 * const session = await airstore.oauth.createSession({
 *   integrationType: "gmail",
 *   returnTo: "https://myapp.com/callback",
 * });
 * console.log(session.authorize_url); // redirect user here
 * const completed = await airstore.oauth.poll(session.session_id);
 * ```
 */
export class OAuth {
  constructor(private readonly client: CoreClient) {}

  /**
   * Create an OAuth session to initiate the authorization flow.
   *
   * Returns a session with an `authorize_url` that the user should be
   * redirected to. Once they complete authorization, poll the session
   * for the resulting connection ID.
   *
   * @param params - Session creation parameters.
   * @param options - Per-request overrides.
   * @returns The session with authorize_url to redirect the user to.
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

  /**
   * Get the current status of an OAuth session.
   *
   * @param sessionId - Session ID from createSession.
   * @param options - Per-request overrides.
   * @returns The session status.
   */
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

  /**
   * Poll an OAuth session until completion or timeout.
   *
   * Repeatedly checks session status at the specified interval until
   * the session completes, errors, or the timeout is reached.
   *
   * @param sessionId - Session ID from createSession.
   * @param pollOpts - Polling configuration (timeout, interval).
   * @param options - Per-request overrides.
   * @returns The completed session status with connection_id.
   *
   * @throws {AirstoreError} If the session errors or times out.
   */
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
