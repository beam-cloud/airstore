import type { MemberRole } from './shared.js';

/**
 * Parameters for adding a member to a workspace.
 */
export interface MemberCreateParams {
  /** Member email address. */
  email: string;
  /** Display name. */
  name: string;
  /** Role in the workspace. @default "member" */
  role?: MemberRole;
}

/**
 * A workspace member.
 */
export interface Member {
  /** Unique external identifier. */
  external_id: string;
  /** Email address. */
  email: string;
  /** Display name. */
  name: string;
  /** Role in the workspace. */
  role: MemberRole;
  /** ISO 8601 creation timestamp. */
  created_at: string;
  /** ISO 8601 last update timestamp. */
  updated_at: string;
}
