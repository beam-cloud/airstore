import type { IntegrationType } from './shared.js';

/**
 * Parameters for creating an OAuth session.
 */
export interface OAuthSessionCreateParams {
  /** The integration provider. */
  integrationType: IntegrationType;
  /** Workspace to associate the connection with (required for org/admin tokens). */
  workspaceId?: string;
  /** URL to redirect after OAuth callback. */
  returnTo?: string;
}

/**
 * An OAuth session for interactive connection setup.
 */
export interface OAuthSession {
  /** Session identifier for polling. */
  session_id: string;
  /** URL to redirect the user to for authorization. */
  authorize_url: string;
}

/**
 * OAuth session status when polling.
 */
export interface OAuthSessionStatus {
  /** Current status. */
  status: 'pending' | 'complete' | 'error';
  /** Error message if status is "error". */
  error?: string;
  /** Connection ID if status is "complete". */
  connection_id?: string;
}

/**
 * Options for the OAuth poll helper.
 */
export interface OAuthPollOptions {
  /** Maximum time to wait in ms. @default 300000 */
  timeout?: number;
  /** Polling interval in ms. @default 2000 */
  interval?: number;
}
