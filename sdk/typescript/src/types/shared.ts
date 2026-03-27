/** Per-request overrides. Re-exported from client for convenience. */
export type { RequestOptions, ResponseMeta } from '../client.js';

/**
 * A paginated list response from the Airstore API.
 *
 * Check `hasMore` and pass `nextCursor` to subsequent requests
 * to retrieve additional pages.
 */
export interface PaginatedList<T> {
  /** Items in the current page. */
  data: T[];
  /** Whether more items are available beyond this page. */
  hasMore: boolean;
  /** Cursor to pass to the next request for the following page. */
  nextCursor?: string;
}

/**
 * Supported integration provider types.
 *
 * Each value corresponds to a provider that can be connected to a workspace.
 */
export type IntegrationType =
  | 'gmail'
  | 'gdrive'
  | 'github'
  | 'notion'
  | 'linear'
  | 'slack'
  | 'posthog'
  | 'outlook';

/** Workspace member roles. */
export type MemberRole = 'admin' | 'member' | 'viewer';

/** Source view output format. */
export type OutputFormat = 'folder' | 'file';
