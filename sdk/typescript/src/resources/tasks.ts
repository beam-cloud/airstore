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
  AgentTask,
  TaskCancelResponse,
  RunExecutionPolicy,
  TaskAcceptedResponse,
  TaskListParams,
  TaskListResponse,
  TaskLogListParams,
  TaskLogListResponse,
  TaskEventStreamParams,
  TaskEventBatch,
} from '../types/tasks.js';

/**
 * Task APIs for agents.
 */
export class Tasks {
  constructor(private readonly client: CoreClient) {}

  async create(
    workspaceId: string,
    params: AgentCommandCreateParams,
    options?: RequestOptions,
  ): Promise<TaskAcceptedResponse> {
    return this.client.request<TaskAcceptedResponse>(
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

  async retrieve(
    workspaceId: string,
    taskId: string,
    options?: RequestOptions,
  ): Promise<AgentTask> {
    return this.client.request<AgentTask>(
      'GET',
      `/workspaces/${workspaceId}/tasks/${taskId}`,
      undefined,
      undefined,
      options,
    );
  }

  async listLogs(
    workspaceId: string,
    taskId: string,
    params?: TaskLogListParams,
    options?: RequestOptions,
  ): Promise<TaskLogListResponse> {
    return this.client.request<TaskLogListResponse>(
      'GET',
      `/workspaces/${workspaceId}/tasks/${taskId}/logs`,
      undefined,
      toTaskLogQuery(params),
      options,
    );
  }

  async streamEvents(
    workspaceId: string,
    taskId: string,
    params?: TaskEventStreamParams,
    options?: RequestOptions,
  ): Promise<TaskEventBatch> {
    return this.client.request<TaskEventBatch>(
      'GET',
      `/workspaces/${workspaceId}/tasks/${taskId}/stream`,
      undefined,
      toTaskEventStreamQuery(params),
      options,
    );
  }

  async list(
    workspaceId: string,
    params?: TaskListParams,
    options?: RequestOptions,
  ): Promise<TaskListResponse> {
    const response = await this.client.request<TaskListResponse | AgentTask[]>(
      'GET',
      `/workspaces/${workspaceId}/tasks`,
      undefined,
      toTaskListQuery(params),
      options,
    );
    if (Array.isArray(response)) {
      return {
        tasks: response,
        next_cursor: '',
        has_more: false,
      };
    }
    return {
      tasks: response.tasks ?? [],
      next_cursor: response.next_cursor ?? '',
      has_more: response.has_more ?? false,
    };
  }

  async cancel(
    workspaceId: string,
    taskId: string,
    options?: RequestOptions,
  ): Promise<TaskCancelResponse> {
    return this.client.request<TaskCancelResponse>(
      'POST',
      `/workspaces/${workspaceId}/tasks/${taskId}/cancel`,
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

function toInputProvenanceBody(
  provenance: AgentCommandCreateParams['inputProvenance'],
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

function toTaskListQuery(params: TaskListParams | undefined): Record<string, string> | undefined {
  if (!params) return undefined;
  const query: Record<string, string> = {};
  if (params.agentId) query['agent_id'] = params.agentId;
  if (params.state) {
    query['state'] = Array.isArray(params.state) ? params.state.join(',') : params.state;
  }
  if (params.createdAfter) query['created_after'] = params.createdAfter;
  if (params.createdBefore) query['created_before'] = params.createdBefore;
  if (params.limit !== undefined) query['limit'] = String(params.limit);
  if (params.cursor) query['cursor'] = params.cursor;
  return Object.keys(query).length > 0 ? query : undefined;
}

function toTaskLogQuery(params: TaskLogListParams | undefined): Record<string, string> | undefined {
  if (!params) return undefined;
  const query: Record<string, string> = {};
  if (params.cursor !== undefined) query['cursor'] = String(params.cursor);
  return Object.keys(query).length > 0 ? query : undefined;
}

function toTaskEventStreamQuery(
  params: TaskEventStreamParams | undefined,
): Record<string, string> | undefined {
  if (!params) return undefined;
  const query: Record<string, string> = {};
  if (params.logCursor !== undefined) query['log_cursor'] = String(params.logCursor);
  if (params.runEventCursor !== undefined) {
    query['run_event_cursor'] = String(params.runEventCursor);
  }
  return Object.keys(query).length > 0 ? query : undefined;
}
