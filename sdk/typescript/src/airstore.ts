import { CoreClient, type ClientOptions } from './client.js';
import { Workspaces } from './resources/workspaces.js';
import { Connections } from './resources/connections.js';
import { SmartFolders } from './resources/smart-folders.js';
import { Tokens } from './resources/tokens.js';
import { Members } from './resources/members.js';
import { OAuth } from './resources/oauth.js';
import { Filesystem } from './resources/filesystem.js';
import { AccessLog } from './resources/access-log.js';

/**
 * The Airstore SDK client.
 *
 * Create an instance with your API key to interact with workspaces,
 * connections, smart folders, tokens, members, the virtual filesystem,
 * and OAuth sessions.
 *
 * @example Basic provisioning flow
 * ```ts
 * import Airstore from '@airstore/sdk';
 *
 * const airstore = new Airstore({ apiKey: 'org_...' });
 *
 * // 1. Create a workspace for a new user
 * const ws = await airstore.workspaces.create({ name: 'user-123' });
 *
 * // 2. Connect their Gmail with existing OAuth tokens
 * await airstore.connections.create(ws.external_id, {
 *   integrationType: 'gmail',
 *   accessToken: existingAccessToken,
 *   refreshToken: existingRefreshToken,
 * });
 *
 * // 3. Set up a smart folder
 * await airstore.smartFolders.create(ws.external_id, {
 *   integration: 'gmail',
 *   name: 'Recent Emails',
 *   guidance: 'Last 7 days of emails from the inbox',
 * });
 *
 * // 4. Generate a mount token for the user's VM
 * const token = await airstore.tokens.create(ws.external_id, {
 *   email: 'agent@internal',
 *   name: 'vm-mount',
 * });
 * // Pass token.token to: airstore start --token <token>
 * ```
 *
 * @example Per-request options
 * ```ts
 * const ws = await airstore.workspaces.list({
 *   timeout: 10_000,
 *   maxRetries: 5,
 * });
 * ```
 */
export class Airstore extends CoreClient {
  /**
   * Manage workspaces.
   *
   * Workspaces are the top-level container for connections, smart folders,
   * members, and the virtual filesystem.
   */
  readonly workspaces: Workspaces;

  /**
   * Manage connections (integrations) within a workspace.
   *
   * Pass existing OAuth tokens or API keys to connect external services
   * like Gmail, GitHub, Notion, etc.
   */
  readonly connections: Connections;

  /**
   * Manage smart folders (filesystem queries).
   *
   * Smart folders use LLM inference to automatically organize and filter
   * data from connected integrations into virtual folders or files.
   */
  readonly smartFolders: SmartFolders;

  /**
   * Manage workspace-scoped authentication tokens.
   *
   * Tokens are used for CLI mounting and per-workspace programmatic access.
   */
  readonly tokens: Tokens;

  /**
   * Manage workspace members.
   *
   * Members are users with roles (admin, member, viewer) in a workspace.
   */
  readonly members: Members;

  /**
   * OAuth session management for interactive connection setup.
   *
   * Use this for browser-redirect flows where users authorize
   * integrations themselves.
   */
  readonly oauth: OAuth;

  /**
   * Read-only access to the workspace virtual filesystem.
   *
   * Browse directories, read files, and inspect metadata across
   * all connected integrations.
   */
  readonly fs: Filesystem;

  /**
   * Query the workspace access log.
   *
   * Every file read is recorded with token counts, compression outcome,
   * and a source_uri that pins content back to its upstream origin.
   */
  readonly accessLog: AccessLog;

  /**
   * Create a new Airstore SDK client.
   *
   * @param opts - Client configuration. At minimum, provide an `apiKey`
   *   or set the `AIRSTORE_API_KEY` environment variable.
   *
   * @throws {AirstoreError} If no API key is provided.
   */
  constructor(opts?: ClientOptions) {
    super(opts);
    this.workspaces = new Workspaces(this);
    this.connections = new Connections(this);
    this.smartFolders = new SmartFolders(this);
    this.tokens = new Tokens(this);
    this.members = new Members(this);
    this.oauth = new OAuth(this);
    this.fs = new Filesystem(this);
    this.accessLog = new AccessLog(this);
  }
}

export default Airstore;
