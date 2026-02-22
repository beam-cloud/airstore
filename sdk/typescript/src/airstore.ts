import { CoreClient, type ClientOptions } from './client.js';
import { Workspaces } from './resources/workspaces.js';
import { Connections } from './resources/connections.js';
import { Views } from './resources/views.js';
import { Tokens } from './resources/tokens.js';
import { Members } from './resources/members.js';
import { OAuth } from './resources/oauth.js';
import { Filesystem } from './resources/filesystem.js';
import { AccessLog } from './resources/access-log.js';
import { Agents } from './resources/agents.js';
import { Tasks } from './resources/tasks.js';
import { Runs } from './resources/runs.js';

/**
 * The Airstore SDK client.
 *
 * @example Basic provisioning flow
 * ```ts
 * import Airstore from '@airstore/sdk';
 *
 * const airstore = new Airstore({ apiKey: 'org_...' });
 *
 * // 1. Create a workspace
 * const ws = await airstore.workspaces.create({ name: 'user-123' });
 *
 * // 2. Connect Gmail
 * await airstore.connections.create(ws.external_id, {
 *   integrationType: 'gmail',
 *   accessToken: existingAccessToken,
 *   refreshToken: existingRefreshToken,
 * });
 *
 * // 3. Create a source view (smart mode — LLM-inferred)
 * const view = await airstore.views.create(ws.external_id, {
 *   integration: 'gmail',
 *   name: 'Recent Emails',
 *   guidance: 'Last 7 days of emails from the inbox',
 * });
 *
 * // 4. Or create a source view (query mode — structured filter)
 * await airstore.views.create(ws.external_id, {
 *   integration: 'gmail',
 *   name: 'Unread from boss',
 *   filter: { from: 'boss@company.com', is_unread: true },
 * });
 *
 * // 5. Sync a view to refresh its metadata
 * const result = await airstore.views.sync(ws.external_id, view.external_id);
 * console.log(result.results_count, result.new_results);
 *
 * // 6. Generate a mount token
 * const token = await airstore.tokens.create(ws.external_id, {
 *   email: 'agent@internal',
 *   name: 'vm-mount',
 * });
 * ```
 */
export class Airstore extends CoreClient {
  /** Manage workspaces. */
  readonly workspaces: Workspaces;
  /** Manage connections (integrations) within a workspace. */
  readonly connections: Connections;
  /** Manage source views — materialized queries over connected data sources. */
  readonly views: Views;
  /** Manage workspace-scoped authentication tokens. */
  readonly tokens: Tokens;
  /** Manage workspace members. */
  readonly members: Members;
  /** OAuth session management for interactive connection setup. */
  readonly oauth: OAuth;
  /** Read-only access to the workspace virtual filesystem. */
  readonly fs: Filesystem;
  /** Query the workspace access log. */
  readonly accessLog: AccessLog;
  /** Manage workspace agent profiles. */
  readonly agents: Agents;
  /** Manage task envelopes (intent tasks). */
  readonly tasks: Tasks;
  /** Read and control run lifecycle state. */
  readonly runs: Runs;

  constructor(opts?: ClientOptions) {
    super(opts);
    this.workspaces = new Workspaces(this);
    this.connections = new Connections(this);
    this.views = new Views(this);
    this.tokens = new Tokens(this);
    this.members = new Members(this);
    this.oauth = new OAuth(this);
    this.fs = new Filesystem(this);
    this.accessLog = new AccessLog(this);
    this.agents = new Agents(this);
    this.tasks = new Tasks(this);
    this.runs = new Runs(this);
  }
}

export default Airstore;
