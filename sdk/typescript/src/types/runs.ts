import type {
  ExecAsk,
  ExecHost,
  ExecSecurity,
  RuntimeType,
  WorkspaceAccess,
} from './tasks.js';

/** Lifecycle status of an agent run. */
export type RunStatus = 'accepted' | 'running' | 'ok' | 'error' | 'timeout' | 'cancelled';

/** An execution instance spawned by a task. */
export interface AgentRun {
  id: string;
  workspace_id: number;
  agent_id?: string;
  origin_task_id: string;
  status: RunStatus;
  session_id: string;
  session_key?: string;
  provider?: string;
  model?: string;
  exec_host: ExecHost;
  exec_security: ExecSecurity;
  exec_ask: ExecAsk;
  runtime_type: RuntimeType;
  workspace_access: WorkspaceAccess;
  network_enabled: boolean;
  interactive: boolean;
  timeout_ms: number;
  started_at?: string;
  ended_at?: string;
  error?: string;
  snapshot_ts: number;
  usage_json: Record<string, unknown>;
  delivery_json: Record<string, unknown>;
  created_at: string;
  updated_at: string;
}

/** A point-in-time snapshot of a run's intermediate state. */
export interface AgentRunSnapshot {
  id: number;
  run_id: string;
  seq: number;
  status: RunStatus;
  started_at_ms?: number;
  ended_at_ms?: number;
  error?: string;
  ts: number;
  payload_json: Record<string, unknown>;
  created_at: string;
}

export interface RunCancelResponse {
  status: 'cancelled';
}

/** Filters for listing runs. */
export interface RunListParams {
  agentId?: string;
  status?: RunStatus | RunStatus[];
  sessionId?: string;
  createdAfter?: string;
  createdBefore?: string;
  limit?: number;
  cursor?: string;
}

/** Paginated list of runs. */
export interface RunListResponse {
  runs: AgentRun[];
  next_cursor: string;
  has_more: boolean;
}
