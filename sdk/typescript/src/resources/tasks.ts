import type { CoreClient, RequestOptions } from '../client.js';
import {
  EXEC_ASK_OFF,
  EXEC_HOST_SANDBOX,
  EXEC_SECURITY_ALLOWLIST,
  RETRY_DEFAULT_DELAY_MS,
  RETRY_DEFAULT_MAX_ATTEMPTS,
  RUNTIME_TYPE_GVISOR,
  WORKSPACE_ACCESS_RW,
} from '../types/tasks.js';
import type {
  AgentCommandCreateParams,
  AgentTaskEnvelope,
  RunExecutionPolicy,
  TaskEnvelopeAcceptedResponse,
} from '../types/tasks.js';

/**
 * Task-envelope APIs for agent orchestration.
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
        policy: toPolicyBody(params.policy),
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

function toPolicyBody(policy: RunExecutionPolicy | undefined): Record<string, unknown> | undefined {
  if (!policy) return undefined;
  return {
    host: policy.host ?? EXEC_HOST_SANDBOX,
    security: policy.security ?? EXEC_SECURITY_ALLOWLIST,
    ask: policy.ask ?? EXEC_ASK_OFF,
    runtime_type: policy.runtimeType ?? RUNTIME_TYPE_GVISOR,
    workspace_access: policy.workspaceAccess ?? WORKSPACE_ACCESS_RW,
    network_enabled: policy.networkEnabled ?? true,
    interactive: policy.interactive ?? false,
    resources: policy.resources ?? {},
    retry: {
      max_attempts: policy.retry?.maxAttempts ?? RETRY_DEFAULT_MAX_ATTEMPTS,
      delay_ms: policy.retry?.delayMs ?? RETRY_DEFAULT_DELAY_MS,
    },
  };
}
