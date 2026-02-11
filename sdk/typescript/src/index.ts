/** @module @airstore/sdk */

// ── Main client ──────────────────────────────────────────────────────────────
export { Airstore, Airstore as default } from './airstore.js';
export type { ClientOptions, RequestOptions, ResponseMeta } from './client.js';

// ── Errors ───────────────────────────────────────────────────────────────────
export {
  AirstoreError,
  APIError,
  AuthenticationError,
  PermissionDeniedError,
  NotFoundError,
  ConflictError,
  UnprocessableEntityError,
  RateLimitError,
  InternalServerError,
  APIConnectionError,
  APIConnectionTimeoutError,
} from './errors.js';

// ── Resource classes ─────────────────────────────────────────────────────────
export { Workspaces } from './resources/workspaces.js';
export { Connections } from './resources/connections.js';
export { SmartFolders } from './resources/smart-folders.js';
export { Tokens } from './resources/tokens.js';
export { Members } from './resources/members.js';
export { OAuth } from './resources/oauth.js';
export { Filesystem } from './resources/filesystem.js';

// ── Types ────────────────────────────────────────────────────────────────────
export type { PaginatedList, IntegrationType, MemberRole, OutputFormat } from './types/shared.js';
export type { Workspace, WorkspaceCreateParams } from './types/workspaces.js';
export type { Connection, ConnectionCreateParams } from './types/connections.js';
export type { SmartFolder, SmartFolderCreateParams, SmartFolderUpdateParams } from './types/smart-folders.js';
export type { Token, TokenCreateParams, TokenCreated } from './types/tokens.js';
export type { Member, MemberCreateParams } from './types/members.js';
export type { OAuthSession, OAuthSessionCreateParams, OAuthSessionStatus, OAuthPollOptions } from './types/oauth.js';
export type { VirtualFile, DirectoryListing, TreeListing } from './types/filesystem.js';

// ── Version ──────────────────────────────────────────────────────────────────
export { VERSION } from './version.js';
