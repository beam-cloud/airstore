import type { CoreClient, RequestOptions } from '../client.js';
import type {
  AgentCommandCreateParams,
  AgentTaskEnvelope,
  TaskEnvelopeAcceptedResponse,
} from '../types/tasks.js';

/**
 * Intent-task (TaskEnvelope) APIs for orchestration.
 *
 * This resource is distinct from the legacy execution `/tasks` endpoint.
 */
export class Tasks {
  constructor(private readonly client: CoreClient) {}

  async create(
    workspaceId: string,
    params: AgentCommandCreateParams,
    options?: RequestOptions,
  ): Promise<TaskEnvelopeAcceptedResponse> {
    return this.client.request<TaskEnvelopeAcceptedResponse>(
      'POST',
      `/workspaces/${workspaceId}/tasks`,
      {
        message: params.message,
        agent_id: params.agentId,
        session_id: params.sessionId,
        session_key: params.sessionKey,
        deliver: params.deliver,
        timeout_ms: params.timeoutMs,
        lane: params.lane,
        extra_system_prompt: params.extraSystemPrompt,
        routing: toRoutingBody(params.routing),
        attachments: params.attachments,
        idempotency_key: params.idempotencyKey,
        label: params.label,
        spawned_by: params.spawnedBy,
      },
      undefined,
      options,
    );
  }

  async retrieve(
    workspaceId: string,
    envelopeId: string,
    options?: RequestOptions,
  ): Promise<AgentTaskEnvelope> {
    return this.client.request<AgentTaskEnvelope>(
      'GET',
      `/workspaces/${workspaceId}/tasks/${envelopeId}`,
      undefined,
      undefined,
      options,
    );
  }
}

function toRoutingBody(routing: AgentCommandCreateParams['routing']): Record<string, unknown> {
  if (!routing) return {};
  return {
    to: routing.to,
    reply_to: routing.replyTo,
    channel: routing.channel,
    reply_channel: routing.replyChannel,
    account_id: routing.accountId,
    reply_account_id: routing.replyAccountId,
    thread_id: routing.threadId,
    group_id: routing.groupId,
    group_channel: routing.groupChannel,
    group_space: routing.groupSpace,
  };
}
