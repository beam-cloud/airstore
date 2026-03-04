import type { CoreClient, RequestOptions } from '../client.js';
import type {
  AgentCommandCreateParams,
  AgentTask,
  TaskCancelResponse,
  TaskAcceptedResponse,
  TaskListParams,
  TaskListResponse,
  TaskLogListParams,
  TaskLogListResponse,
  TaskEventStreamParams,
  TaskEventBatch,
  Schedule,
  ScheduleCreateParams,
  ScheduleUpdateParams,
} from '../types/tasks.js';
import { toInputProvenanceBody, toPolicyBody, toRoutingBody } from './helpers.js';

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
      return { tasks: response, next_cursor: '', has_more: false };
    }
    return {
      tasks: response.tasks ?? [],
      next_cursor: response.next_cursor ?? '',
      has_more: response.has_more ?? false,
    };
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
      params?.cursor !== undefined ? { cursor: String(params.cursor) } : undefined,
      options,
    );
  }

  async streamEvents(
    workspaceId: string,
    taskId: string,
    params?: TaskEventStreamParams,
    options?: RequestOptions,
  ): Promise<TaskEventBatch> {
    const q: Record<string, string> = {};
    if (params?.logCursor !== undefined) q['log_cursor'] = String(params.logCursor);
    if (params?.runEventCursor !== undefined) q['run_event_cursor'] = String(params.runEventCursor);
    return this.client.request<TaskEventBatch>(
      'GET', `/workspaces/${workspaceId}/tasks/${taskId}/stream`,
      undefined, Object.keys(q).length > 0 ? q : undefined, options,
    );
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

  // --- Schedules (cron) ---

  private schedulePath(workspaceId: string, id?: string): string {
    const base = `/workspaces/${workspaceId}/tasks/schedules`;
    return id ? `${base}/${id}` : base;
  }

  async createSchedule(workspaceId: string, params: ScheduleCreateParams, options?: RequestOptions): Promise<Schedule> {
    return this.client.request<Schedule>('POST', this.schedulePath(workspaceId), {
      agent_id: params.agentId, cron_expr: params.cronExpr, prompt: params.prompt,
      ...(params.timezone != null && { timezone: params.timezone }),
      ...(params.skillPaths != null && { skill_paths: params.skillPaths }),
    }, undefined, options);
  }

  async listSchedules(workspaceId: string, options?: RequestOptions): Promise<Schedule[]> {
    return this.client.request<Schedule[]>('GET', this.schedulePath(workspaceId), undefined, undefined, options);
  }

  async retrieveSchedule(workspaceId: string, scheduleId: string, options?: RequestOptions): Promise<Schedule> {
    return this.client.request<Schedule>('GET', this.schedulePath(workspaceId, scheduleId), undefined, undefined, options);
  }

  async updateSchedule(workspaceId: string, scheduleId: string, params: ScheduleUpdateParams, options?: RequestOptions): Promise<Schedule> {
    return this.client.request<Schedule>('PATCH', this.schedulePath(workspaceId, scheduleId), {
      ...(params.cronExpr != null && { cron_expr: params.cronExpr }),
      ...(params.timezone != null && { timezone: params.timezone }),
      ...(params.prompt != null && { prompt: params.prompt }),
      ...(params.skillPaths != null && { skill_paths: params.skillPaths }),
      ...(params.active != null && { active: params.active }),
    }, undefined, options);
  }

  async deleteSchedule(workspaceId: string, scheduleId: string, options?: RequestOptions): Promise<void> {
    await this.client.request('DELETE', this.schedulePath(workspaceId, scheduleId), undefined, undefined, options);
  }
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
