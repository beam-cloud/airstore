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

export function toRoutingBody(routing: RoutingContext | undefined): Record<string, unknown> {
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

export function toInputProvenanceBody(
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

export function toPolicyBody(
  policy: RunExecutionPolicy | undefined,
): Record<string, unknown> | undefined {
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
