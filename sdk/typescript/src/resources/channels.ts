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
import type { InputProvenance, RoutingContext, RunExecutionPolicy } from '../types/tasks.js';
import type {
  SendDirectAgentMessageParams,
  SendDirectAgentMessageResponse,
  SendDirectRunMessageParams,
  SendDirectRunMessageResponse,
} from '../types/channels.js';

/**
 * Direct-channel APIs for sending agent/run messages.
 */
export class Channels {
  constructor(private readonly client: CoreClient) {}

  async sendDirectAgentMessage(
    workspaceId: string,
    agentId: string,
    params: SendDirectAgentMessageParams,
    options?: RequestOptions,
  ): Promise<SendDirectAgentMessageResponse> {
    return this.client.request<SendDirectAgentMessageResponse>(
      'POST',
      `/workspaces/${workspaceId}/channels/direct/agents/${agentId}/messages`,
      {
        message: params.message,
        session_id: params.sessionId,
        session_key: params.sessionKey,
        deliver: params.deliver,
        timeout_ms: params.timeoutMs,
        policy: toPolicyBody(params.policy),
        lane: params.lane,
        extra_system_prompt: params.extraSystemPrompt,
        input_provenance: toInputProvenanceBody(params.inputProvenance),
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

  async sendDirectRunMessage(
    workspaceId: string,
    runId: string,
    params: SendDirectRunMessageParams,
    options?: RequestOptions,
  ): Promise<SendDirectRunMessageResponse> {
    return this.client.request<SendDirectRunMessageResponse>(
      'POST',
      `/workspaces/${workspaceId}/channels/direct/runs/${runId}/messages`,
      {
        message: params.message,
        queue_mode: params.queueMode,
        idempotency_key: params.idempotencyKey,
      },
      undefined,
      options,
    );
  }
}

function toRoutingBody(routing: RoutingContext | undefined): Record<string, unknown> {
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

function toInputProvenanceBody(
  provenance: InputProvenance | undefined,
): Record<string, unknown> | undefined {
  if (!provenance) return undefined;
  return {
    source: provenance.source,
    message_id: provenance.messageId,
    channel: provenance.channel,
    tool_call_id: provenance.toolCallId,
    correlation_id: provenance.correlationId,
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
