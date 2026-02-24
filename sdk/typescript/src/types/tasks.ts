export type QueueMode =
  | 'steer'
  | 'steer-backlog'
  | 'followup'
  | 'interrupt'
  | 'queue';
export const QUEUE_MODE_STEER: QueueMode = 'steer';
export const QUEUE_MODE_STEER_BACKLOG: QueueMode = 'steer-backlog';
export const QUEUE_MODE_FOLLOWUP: QueueMode = 'followup';
export const QUEUE_MODE_INTERRUPT: QueueMode = 'interrupt';
export const QUEUE_MODE_QUEUE: QueueMode = 'queue';

export type ExecHost = 'sandbox';
export type ExecSecurity = 'deny' | 'allowlist' | 'full';
export type ExecAsk = 'off' | 'on-miss' | 'always';
export type RuntimeType = 'gvisor' | 'runc';
export type WorkspaceAccess = 'none' | 'ro' | 'rw';
export const EXEC_HOST_SANDBOX: ExecHost = 'sandbox';
export const EXEC_SECURITY_DENY: ExecSecurity = 'deny';
export const EXEC_SECURITY_ALLOWLIST: ExecSecurity = 'allowlist';
export const EXEC_SECURITY_FULL: ExecSecurity = 'full';
export const EXEC_ASK_OFF: ExecAsk = 'off';
export const EXEC_ASK_ON_MISS: ExecAsk = 'on-miss';
export const EXEC_ASK_ALWAYS: ExecAsk = 'always';
export const RUNTIME_TYPE_GVISOR: RuntimeType = 'gvisor';
export const RUNTIME_TYPE_RUNC: RuntimeType = 'runc';
export const WORKSPACE_ACCESS_NONE: WorkspaceAccess = 'none';
export const WORKSPACE_ACCESS_RO: WorkspaceAccess = 'ro';
export const WORKSPACE_ACCESS_RW: WorkspaceAccess = 'rw';
export const RETRY_DEFAULT_MAX_ATTEMPTS = 2;
export const RETRY_DEFAULT_DELAY_MS = 0;

export interface RunRetryPolicy {
  maxAttempts?: number;
  delayMs?: number;
}

export interface RunExecutionPolicy {
  host?: ExecHost;
  security?: ExecSecurity;
  ask?: ExecAsk;
  runtimeType?: RuntimeType;
  workspaceAccess?: WorkspaceAccess;
  networkEnabled?: boolean;
  interactive?: boolean;
  resources?: Record<string, number>;
  retry?: RunRetryPolicy;
}

export type TaskKind = 'agent_command' | 'run_input';

export type TaskState = 'accepted' | 'queued' | 'dispatched' | 'done' | 'dropped' | 'cancelled';

export interface RoutingContext {
  to?: string;
  replyTo?: string;
  channel?: string;
  replyChannel?: string;
  accountId?: string;
  replyAccountId?: string;
  threadId?: string;
  groupId?: string;
  groupChannel?: string;
  groupSpace?: string;
}

export interface InputProvenance {
  source?: string;
  messageId?: string;
  channel?: string;
  toolCallId?: string;
  correlationId?: string;
}

export interface AgentTask {
  id: string;
  workspace_id: number;
  agent_id?: string;
  kind: TaskKind;
  queue_mode: QueueMode;
  state: TaskState;
  idempotency_key: string;
  payload_json: Record<string, unknown>;
  routing_json: Record<string, unknown>;
  parent_task_id?: string;
  target_run_id?: string;
  accepted_at: string;
  queued_at?: string;
  dispatched_at?: string;
  dropped_reason?: string;
  created_at: string;
  updated_at: string;
}

export interface AgentCommandCreateParams {
  message: string;
  agentId: string;
  sessionId?: string;
  idempotencyKey?: string;
  sessionKey?: string;
  deliver?: boolean;
  timeoutMs?: number;
  policy?: RunExecutionPolicy;
  lane?: string;
  extraSystemPrompt?: string;
  inputProvenance?: InputProvenance;
  routing?: RoutingContext;
  attachments?: Array<Record<string, unknown>>;
  label?: string;
  spawnedBy?: string;
}

export interface TaskAcceptedResponse {
  accepted: boolean;
  idempotent_hit: boolean;
  task: AgentTask;
  run_id?: string;
}

export interface TaskListParams {
  agentId?: string;
  state?: TaskState | TaskState[];
  createdAfter?: string;
  createdBefore?: string;
  limit?: number;
  cursor?: string;
}

export interface TaskListResponse {
  tasks: AgentTask[];
  next_cursor: string;
  has_more: boolean;
}

export interface TaskCancelResponse {
  status: 'cancelled';
}

export interface TaskLogEntry {
  timestamp: number;
  stream: string;
  data: string;
  chunk_type?: string;
  metadata?: Record<string, unknown>;
}

export interface TaskLogListParams {
  cursor?: number;
}

export interface TaskLogListResponse {
  logs: TaskLogEntry[];
  next_cursor: number;
}

export interface TaskEventStreamParams {
  logCursor?: number;
  runEventCursor?: number;
}

export interface TaskEventBatch {
  task_id: string;
  run_id?: string;
  task?: AgentTask;
  run?: Record<string, unknown>;
  logs: TaskLogEntry[];
  run_events: Array<Record<string, unknown>>;
  next_log_cursor: number;
  next_run_event_cursor: number;
}
