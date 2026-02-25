/**
 * Parameters for creating a new workspace.
 */
export interface WorkspaceCreateParams {
  /** Display name for the workspace. */
  name: string;
}

/**
 * A workspace in Airstore.
 *
 * Workspaces contain connections, source views, members, and a virtual filesystem.
 * When created with an org token, they are automatically scoped to the token's tenant.
 */
export interface Workspace {
  /** Unique external identifier (UUID). */
  external_id: string;
  /** Display name. */
  name: string;
  /** Tenant ID, if workspace was created by an org token. */
  tenant_id?: string;
  /** ISO 8601 creation timestamp. */
  created_at: string;
  /** ISO 8601 last update timestamp. */
  updated_at: string;
}
