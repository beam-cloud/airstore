import type {
  QueueMode,
  ExecAsk,
  ExecHost,
  ExecSecurity,
  RuntimeType,
  WorkspaceAccess,
} from './tasks.js';

export type RunStatus = 'accepted' | 'running' | 'ok' | 'error' | 'timeout' | 'cancelled';
export type AttemptStatus =
  | 'pending'
  | 'blocked'
  | 'running'
  | 'ok'
  | 'error'
  | 'timeout'
  | 'cancelled';
export type AttemptStrategy = 'primary' | 'retry';
export const ATTEMPT_STRATEGY_PRIMARY: AttemptStrategy = 'primary';
export const ATTEMPT_STRATEGY_RETRY: AttemptStrategy = 'retry';

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

export interface AgentRunAttempt {
  id: string;
  run_id: string;
  attempt_no: number;
  status: AttemptStatus;
  strategy: AttemptStrategy;
  provider?: string;
  model?: string;
  exec_host: ExecHost;
  exec_security: ExecSecurity;
  exec_ask: ExecAsk;
  runtime_type: RuntimeType;
  workspace_access: WorkspaceAccess;
  network_enabled: boolean;
  interactive: boolean;
  execution_id?: string;
  started_at?: string;
  ended_at?: string;
  exit_code?: number;
  error?: string;
  created_at: string;
  updated_at: string;
}

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

export interface RunInputParams {
  message: string;
  idempotencyKey?: string;
  queueMode?: QueueMode;
}

export interface RunCancelResponse {
  status: 'cancelled';
}
