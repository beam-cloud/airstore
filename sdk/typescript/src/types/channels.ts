import type { AgentCommandCreateParams, QueueMode, TaskAcceptedResponse } from './tasks.js';

export type ChannelType = 'direct';

/**
 * Payload for sending a direct message to an agent.
 * This creates a new task for the target agent.
 */
export interface SendDirectAgentMessageParams
  extends Omit<AgentCommandCreateParams, 'agentId'> {}

/**
 * Payload for sending direct follow-up input to an existing run.
 */
export interface SendDirectRunMessageParams {
  message: string;
  taskId: string;
  queueMode?: QueueMode;
  idempotencyKey?: string;
}

export type SendDirectAgentMessageResponse = TaskAcceptedResponse;
export type SendDirectRunMessageResponse = TaskAcceptedResponse;
