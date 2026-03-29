import type { AgentRun } from './runs.js';

/** How a follow-up message is delivered to a running agent. */
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

/** Retry policy for a run (max attempts and delay between retries). */
export interface RunRetryPolicy {
  maxAttempts?: number;
  delayMs?: number;
}

/** Execution policy controlling sandbox, security, runtime, and resource limits. */
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

/** Lifecycle state of a task. */
export type TaskState =
  | 'queued'
  | 'running'
  | 'waiting'
  | 'sleeping'
  | 'done'
  | 'error'
  | 'dropped'
  | 'cancelled';

/** Routing metadata for multi-channel delivery (Slack, email, etc.). */
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

/** Provenance of the input that created this task. */
export interface InputProvenance {
  source?: string;
  messageId?: string;
  channel?: string;
  toolCallId?: string;
  correlationId?: string;
}

export type TaskPriority = 'urgent' | 'high' | 'normal' | 'low';

export interface WakeAgendaItem {
  seq: number;
  type?: string;
  title: string;
  reason?: string;
}

/** A task representing a unit of intent sent to an agent. */
export interface AgentTask {
  id: string;
  workspace_id: number;
  agent_id?: string;
  agent_name?: string;
  kind?: TaskKind;
  queue_mode: QueueMode;
  state: TaskState;
  priority: TaskPriority;
  idempotency_key: string;
  payload_json: Record<string, unknown>;
  routing_json: Record<string, unknown>;
  parent_task_id?: string;
  target_run_id?: string;
  input_kind?: 'approve_reject' | 'free_text';
  accepted_at: string;
  queued_at?: string;
  dispatched_at?: string;
  deadline?: string;
  dropped_reason?: string;
  budget_usd?: number;
  cost_usd: number;
  archived_at?: string;
  wake_at?: string;
  wake_reason?: string;
  wake_agenda?: WakeAgendaItem[];
  wake_count?: number;
  created_at: string;
  updated_at: string;
}

/** Parameters for creating a new task (agent command). */
export interface AgentCommandCreateParams {
  /** The prompt / instruction to send to the agent. */
  message: string;
  /** ID of the agent profile to target. */
  agentId?: string;
  /** Session ID for grouping related tasks. Auto-generated if omitted. */
  sessionId?: string;
  /** Idempotency key to prevent duplicate task creation. Auto-generated if omitted. */
  idempotencyKey?: string;
  sessionKey?: string;
  deliver?: boolean;
  /** Maximum execution time in milliseconds. */
  timeoutMs?: number;
  /** Execution policy (sandbox, security, runtime settings). */
  policy?: RunExecutionPolicy;
  lane?: string;
  extraSystemPrompt?: string;
  inputProvenance?: InputProvenance;
  routing?: RoutingContext;
  attachments?: Array<Record<string, unknown>>;
  label?: string;
  spawnedBy?: string;
  priority?: TaskPriority;
  budgetUsd?: number;
  /** View/project ID that originated this task, for scoping outputs. */
  sourceViewId?: string;
}

/** Response from task creation. Contains the task and whether it was a duplicate. */
export interface TaskAcceptedResponse {
  accepted: boolean;
  idempotent_hit: boolean;
  task: AgentTask;
  run_id?: string;
}

/** Filters for listing tasks. */
export interface TaskListParams {
  agentId?: string;
  state?: TaskState | TaskState[];
  createdAfter?: string;
  createdBefore?: string;
  limit?: number;
  cursor?: string;
}

/** Paginated list of tasks. */
export interface TaskListResponse {
  tasks: AgentTask[];
  next_cursor: string;
  has_more: boolean;
}

export interface TaskCancelResponse {
  status: 'cancelled';
}

export interface TaskArchiveResponse {
  status: 'archived';
}

export interface TaskUpdateParams {
  priority?: TaskPriority;
  budgetUsd?: number | null;
  payload?: Record<string, unknown>;
  routing?: RoutingContext;
}

/** Structured action for approve/reject input. */
export type TaskInputAction = 'approve' | 'reject';

/** Kind of input the agent is waiting for. */
export type TaskInputKind = 'approve_reject' | 'free_text';

/** Per-item approval/rejection decision. */
export interface ItemDecision {
  outputId: string;
  action: TaskInputAction;
  reason?: string;
}

/** Shared optional fields for task-input submissions. */
interface SubmitTaskInputBase {
  /** Kind of input. Inferred from action if omitted. */
  kind?: TaskInputKind;
  /** Idempotency key to prevent duplicate submissions. */
  idempotencyKey?: string;
  /** Per-item decisions for multi-item approval. */
  items?: ItemDecision[];
}

/**
 * Parameters for submitting follow-up input to a task.
 *
 * At least one of `message` or `action` must be provided.
 */
export type SubmitTaskInputParams =
  | (SubmitTaskInputBase & {
      /** The follow-up message. */
      message: string;
      /** Structured action (approve/reject). When set, message defaults automatically. */
      action?: TaskInputAction;
    })
  | (SubmitTaskInputBase & {
      /** The follow-up message, optional when action is provided. */
      message?: string;
      /** Structured action (approve/reject). When set, message defaults automatically. */
      action: TaskInputAction;
    });

/** Response from submitting task input. */
export interface SubmitTaskInputResponse {
  task: AgentTask;
}

/** A single log entry from a task execution. */
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

/** Cursors for polling the composite task event stream. */
export interface TaskEventStreamParams {
  logCursor?: number;
  runEventCursor?: number;
}

/** Interaction state of an active run (working, waiting for input, or closed). */
export interface RunInteraction {
  state: 'working' | 'waiting_for_input' | 'closed';
  active_execution_id?: string;
  updated_at?: number;
}

/** A lifecycle event emitted by the orchestration engine during a run. */
export interface RunEvent {
  run_id: string;
  event_type: string;
  timestamp: number;
  payload?: Record<string, unknown>;
}

/** Composite batch returned by streamEvents: task/run state, logs, and events. */
export interface TaskEventBatch {
  task_id: string;
  run_id?: string;
  task?: AgentTask;
  run?: AgentRun;
  interaction?: RunInteraction;
  logs: TaskLogEntry[];
  run_events: RunEvent[];
  outputs?: TaskOutput[];
  next_log_cursor: number;
  next_run_event_cursor: number;
}

// ── Scheduled Tasks (cron) ──────────────────────────────────────────────────

/** A cron schedule that periodically submits a task to an agent. */
export interface Schedule {
  external_id: string;
  workspace_id: string;
  agent_id: string;
  agent_name?: string;
  cron_expr: string;
  timezone: string;
  prompt: string;
  skill_paths: string[];
  active: boolean;
  next_run_at: string;
  last_run_at?: string;
  source_view_id?: string;
  created_at: string;
  updated_at: string;
}

/** Parameters for creating a cron schedule. */
export interface ScheduleCreateParams {
  agentId: string;
  /** Cron expression (5-field) or natural language (e.g. "every weekday at 9am"). */
  cronExpr: string;
  /** IANA timezone (defaults to UTC). */
  timezone?: string;
  prompt: string;
  skillPaths?: string[];
}

/** Parameters for updating a cron schedule. All fields are optional. */
export interface ScheduleUpdateParams {
  cronExpr?: string;
  timezone?: string;
  prompt?: string;
  skillPaths?: string[];
  active?: boolean;
}

// ── Task Outputs ────────────────────────────────────────────────────────────

export type OutputType = 'table' | 'email' | 'file' | 'text' | 'link' | 'image' | 'json';

/** A structured output produced by an agent during a task. */
export interface TaskOutput {
  id: string;
  workspace_id: number;
  task_id: string;
  run_id?: string;
  agent_id?: string;
  agent_name?: string;
  output_type: OutputType;
  title: string;
  summary?: string;
  uri?: string;
  schema?: TableColumn[];
  data: Record<string, unknown>;
  metadata?: Record<string, unknown>;
  status?: string;
  archived_at?: string;
  created_at: string;
}

export interface TaskOutputListParams {
  taskId?: string;
  agentId?: string;
  outputType?: OutputType;
  sourceViewId?: string;
  includeArchived?: boolean;
  limit?: number;
  cursor?: string;
}

export interface TaskOutputListResponse {
  outputs: TaskOutput[];
  next_cursor: string;
  has_more: boolean;
}

/** Column definition for table outputs with dynamic schemas. */
export interface TableColumn {
  key: string;
  label: string;
  type: 'string' | 'number' | 'boolean' | 'date';
  display?: 'link' | 'badge' | 'currency' | 'image';
  format?: string;
}

/** Parameters for creating a task output. */
export interface CreateTaskOutputParams {
  output_type: string;
  title: string;
  output_id?: string;
  summary?: string;
  schema?: TableColumn[];
  data: Record<string, unknown>;
  metadata?: Record<string, unknown>;
  run_id?: string;
  agent_id?: string;
}

/** Parameters for appending rows to a table output. */
export interface AppendRowsParams {
  rows: Record<string, unknown>[];
}
