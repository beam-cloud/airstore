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
export { Views } from './resources/views.js';
export { Tokens } from './resources/tokens.js';
export { Members } from './resources/members.js';
export { OAuth } from './resources/oauth.js';
export { Filesystem } from './resources/filesystem.js';
export { AccessLog } from './resources/access-log.js';
export { Agents } from './resources/agents.js';
export { Tasks } from './resources/tasks.js';
export { Runs } from './resources/runs.js';
export { Channels } from './resources/channels.js';
export { Hooks } from './resources/hooks.js';

// ── Typed constants ───────────────────────────────────────────────────────────
export {
  QUEUE_MODE_STEER,
  QUEUE_MODE_STEER_BACKLOG,
  QUEUE_MODE_FOLLOWUP,
  QUEUE_MODE_INTERRUPT,
  QUEUE_MODE_QUEUE,
  EXEC_HOST_SANDBOX,
  EXEC_SECURITY_DENY,
  EXEC_SECURITY_ALLOWLIST,
  EXEC_SECURITY_FULL,
  EXEC_ASK_OFF,
  EXEC_ASK_ON_MISS,
  EXEC_ASK_ALWAYS,
  RUNTIME_TYPE_GVISOR,
  RUNTIME_TYPE_RUNC,
  WORKSPACE_ACCESS_NONE,
  WORKSPACE_ACCESS_RO,
  WORKSPACE_ACCESS_RW,
  RETRY_DEFAULT_MAX_ATTEMPTS,
  RETRY_DEFAULT_DELAY_MS,
} from './types/tasks.js';
// ── Types ────────────────────────────────────────────────────────────────────
export type { PaginatedList, IntegrationType, MemberRole, OutputFormat } from './types/shared.js';
export type { Workspace, WorkspaceCreateParams } from './types/workspaces.js';
export type { Connection, ConnectionCreateParams } from './types/connections.js';
export type {
  SourceView,
  ViewCreateParams,
  ViewUpdateParams,
  ViewMode,
  ViewFilter,
  SyncResult,
  Integration,
  GmailFilter,
  GmailLabel,
  GitHubFilter,
  GitHubResourceType,
  GitHubState,
  GitHubContentType,
  GDriveFilter,
  GDriveMimeType,
  NotionFilter,
  SlackFilter,
  LinearFilter,
  LinearResourceType,
  LinearState,
  LinearPriority,
  PostHogFilter,
  PostHogResourceType,
  WebFilter,
  WebMode,
  IntegrationResource,
} from './types/views.js';
export {
  ViewModes,
  Integrations,
  GmailLabels,
  GitHubResourceTypes,
  GitHubStates,
  GitHubContentTypes,
  GDriveMimeTypes,
  LinearResourceTypes,
  LinearStates,
  LinearPriorities,
  PostHogResourceTypes,
  WebModes,
} from './types/views.js';
export type { Token, TokenCreateParams, TokenCreated } from './types/tokens.js';
export type { Member, MemberCreateParams } from './types/members.js';
export type { OAuthSession, OAuthSessionCreateParams, OAuthSessionStatus, OAuthPollOptions } from './types/oauth.js';
export type { VirtualFile, DirectoryListing, TreeListing } from './types/filesystem.js';
export type {
  AgentProfile,
  AgentCreateParams,
  AgentUpdateParams,
  AgentRunner,
  AgentProvider,
  AgentConfig,
} from './types/agents.js';
export type {
  QueueMode,
  ExecHost,
  ExecSecurity,
  ExecAsk,
  RuntimeType,
  WorkspaceAccess,
  RunRetryPolicy,
  RunExecutionPolicy,
  TaskKind,
  TaskState,
  InputProvenance,
  RoutingContext,
  AgentTask,
  AgentCommandCreateParams,
  TaskListParams,
  TaskListResponse,
  TaskCancelResponse,
  TaskAcceptedResponse,
  TaskLogEntry,
  TaskLogListParams,
  TaskArchiveResponse,
  TaskLogListResponse,
  TaskEventStreamParams,
  TaskEventBatch,
  Schedule,
  ScheduleCreateParams,
  ScheduleUpdateParams,
} from './types/tasks.js';
export type {
  RunStatus,
  AgentRun,
  AgentRunSnapshot,
  RunCancelResponse,
  RunListParams,
  RunListResponse,
} from './types/runs.js';
export type {
  ChannelType,
  SendDirectAgentMessageParams,
  SendDirectAgentMessageResponse,
  SendDirectRunMessageParams,
  SendDirectRunMessageResponse,
} from './types/channels.js';
export type {
  AccessLogRead,
  AccessLogListParams,
  AccessLogListResponse,
  AccessLogSummaryParams,
  AccessLogSummary,
  IntegrationStats,
  PathStats,
} from './types/access-log.js';
export type {
  Hook,
  HookCreateParams,
  HookUpdateParams,
} from './types/hooks.js';

// ── Version ──────────────────────────────────────────────────────────────────
export { VERSION } from './version.js';
