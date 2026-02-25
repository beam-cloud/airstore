import type { IntegrationType } from './shared.js';

/**
 * Parameters for creating a connection to an external service.
 *
 * Provide either OAuth credentials (`accessToken`/`refreshToken`) or an
 * `apiKey`, depending on the integration type.
 */
export interface ConnectionCreateParams {
  /** The integration provider to connect. */
  integrationType: IntegrationType;
  /** OAuth access token from the provider. */
  accessToken?: string;
  /** OAuth refresh token for automatic credential renewal. */
  refreshToken?: string;
  /** API key for key-based integrations (e.g., PostHog). */
  apiKey?: string;
  /** OAuth scope string. */
  scope?: string;
  /** Provider-specific extra fields. */
  extra?: Record<string, string>;
}

/**
 * A connection to an external service within a workspace.
 */
export interface Connection {
  /** Unique identifier. */
  external_id: string;
  /** The workspace this connection belongs to. */
  workspace_id: string;
  /** Integration provider type. */
  integration_type: string;
  /** OAuth scope granted. */
  scope?: string;
  /** When credentials expire. */
  expires_at?: string;
  /** ISO 8601 creation timestamp. */
  created_at: string;
  /** ISO 8601 last update timestamp. */
  updated_at: string;
}
