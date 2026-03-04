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
import { Channels } from './resources/channels.js';
import { Hooks } from './resources/hooks.js';

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
 * // 3. Create a source view
 * const view = await airstore.views.create(ws.external_id, {
 *   integration: 'gmail',
 *   name: 'Recent Emails',
 *   guidance: 'Last 7 days of emails from the inbox',
 * });
 * ```
 *
 * @example Agent task flow
 * ```ts
 * const agent = await airstore.agents.create(ws.external_id, {
 *   agentKey: 'my-agent',
 *   name: 'My Agent',
 *   runner: 'claude_code',
 *   config: { model: 'claude-sonnet-4-6' },
 * });
 *
 * const { task, run_id } = await airstore.tasks.create(ws.external_id, {
 *   message: 'Summarize recent emails',
 *   agentId: agent.id,
 * });
 *
 * // Poll for logs
 * const batch = await airstore.tasks.streamEvents(ws.external_id, task.id);
 * ```
 */
export class Airstore extends CoreClient {
  /** Create, list, retrieve, and delete workspaces. */
  readonly workspaces: Workspaces;
  /** Manage OAuth connections (Gmail, GitHub, etc.) within a workspace. */
  readonly connections: Connections;
  /** Manage source views -- materialized queries over connected data sources. */
  readonly views: Views;
  /** Create, list, and revoke workspace-scoped authentication tokens. */
  readonly tokens: Tokens;
  /** Add, list, and remove workspace members. */
  readonly members: Members;
  /** Interactive OAuth session management for connecting integrations. */
  readonly oauth: OAuth;
  /** Read-only access to the workspace virtual filesystem. */
  readonly fs: Filesystem;
  /** Query the workspace access log (who read what and when). */
  readonly accessLog: AccessLog;
  /** CRUD for agent profiles (runner, model, system prompt configuration). */
  readonly agents: Agents;
  /** Submit tasks, poll logs/events, manage cron schedules, cancel, and archive. */
  readonly tasks: Tasks;
  /** Inspect run lifecycle: status, snapshots, events, and cancellation. */
  readonly runs: Runs;
  /** Send direct messages to agents (new task) or runs (follow-up / steer). */
  readonly channels: Channels;
  /** File-system hooks that auto-trigger agent tasks on source view changes. */
  readonly hooks: Hooks;

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
    this.channels = new Channels(this);
    this.hooks = new Hooks(this);
  }
}

export default Airstore;
