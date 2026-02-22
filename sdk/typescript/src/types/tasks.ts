export type QueueMode =
  | 'steer'
  | 'followup'
  | 'collect'
  | 'steer-backlog'
  | 'interrupt'
  | 'queue';

export type EnvelopeKind = 'agent_command' | 'run_input' | 'followup' | 'cron';

export type EnvelopeState = 'accepted' | 'queued' | 'dispatched' | 'dropped' | 'cancelled';

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

export interface AgentTaskEnvelope {
  id: string;
  workspace_id: number;
  agent_id?: string;
  kind: EnvelopeKind;
  queue_mode: QueueMode;
  state: EnvelopeState;
  idempotency_key: string;
  payload_json: Record<string, unknown>;
  routing_json: Record<string, unknown>;
  parent_envelope_id?: string;
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
  sessionId: string;
  idempotencyKey: string;
  agentId?: string;
  sessionKey?: string;
  deliver?: boolean;
  timeoutMs?: number;
  lane?: string;
  extraSystemPrompt?: string;
  routing?: RoutingContext;
  attachments?: Array<Record<string, unknown>>;
  label?: string;
  spawnedBy?: string;
}

export interface TaskEnvelopeAcceptedResponse {
  accepted: boolean;
  idempotent_hit: boolean;
  envelope: AgentTaskEnvelope;
  run_id?: string;
}
