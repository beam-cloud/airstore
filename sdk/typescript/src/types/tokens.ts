/**
 * Parameters for creating a workspace-scoped mount token.
 */
export interface TokenCreateParams {
  /** Member ID (external) to associate the token with. */
  memberId?: string;
  /** Email to auto-create a member if memberId not provided. */
  email?: string;
  /** Display name for the token. */
  name?: string;
  /** Token expiration in seconds (0 = no expiration). */
  expiresIn?: number;
}

/**
 * A workspace authentication token.
 */
export interface Token {
  /** Unique external identifier. */
  external_id: string;
  /** Display name. */
  name: string;
  /** Token type. */
  token_type: string;
  /** ISO 8601 creation timestamp. */
  created_at: string;
  /** ISO 8601 last used timestamp. */
  last_used_at?: string;
}

/**
 * Response when creating a token (includes the raw token value).
 */
export interface TokenCreated {
  /** The raw token value. Only shown once at creation time. */
  token: string;
  /** Token metadata. */
  info: Token;
  /** Auto-created member ID, if email was provided instead of memberId. */
  member_id?: string;
}
