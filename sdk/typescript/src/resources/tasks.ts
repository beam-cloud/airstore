import type { CoreClient, RequestOptions } from '../client.js';
import type {
  AgentCommandCreateParams,
  AgentTask,
  TaskCancelResponse,
  TaskRetryResponse,
  TaskAcceptedResponse,
  TaskArchiveResponse,
  TaskListParams,
  TaskListResponse,
  TaskUpdateParams,
  TaskLogListParams,
  TaskLogListResponse,
  TaskEventStreamParams,
  TaskEventBatch,
  Schedule,
  ScheduleCreateParams,
  ScheduleUpdateParams,
  TaskOutput,
  TaskOutputListParams,
  TaskOutputListResponse,
  CreateTaskOutputParams,
  AppendRowsParams,
  SubmitTaskInputParams,
  SubmitTaskInputResponse,
} from '../types/tasks.js';
import { toInputProvenanceBody, toPolicyBody, toRoutingBody } from './helpers.js';

/**
 * Create, list, and manage agent tasks and their cron schedules.
 *
 * A task is a unit of intent sent to an agent. Creating a task triggers the
 * orchestrator to dispatch a run. Tasks support idempotency, cancellation,
 * archival, log streaming, and cron-based scheduling.
 */
export class Tasks {
  constructor(private readonly client: CoreClient) {}

  /**
   * Submit a new task to an agent. Returns an accepted response containing the
   * task and, when immediately dispatched, a `run_id`. Duplicate submissions
   * with the same `idempotencyKey` return the original task.
   */
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
        priority: params.priority,
        budget_usd: params.budgetUsd,
        source_view_id: params.sourceViewId,
      },
      undefined,
      options,
    );
  }

  /** Retrieve a single task by ID. */
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

  /** List tasks with optional filters (state, agent, date range) and cursor pagination. */
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

  /** Fetch execution logs for a task. Pass `cursor` for incremental reads. */
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

  /**
   * Poll a composite event stream for a task: the current task/run state,
   * new log entries, run events, and pending inputs in a single batch.
   */
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

  /** Cancel a running task and its active run. */
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

  /** Retry a failed or dropped task by re-dispatching it. */
  async retry(
    workspaceId: string,
    taskId: string,
    options?: RequestOptions,
  ): Promise<TaskRetryResponse> {
    return this.client.request<TaskRetryResponse>(
      'POST',
      `/workspaces/${workspaceId}/tasks/${taskId}/retry`,
      undefined,
      undefined,
      options,
    );
  }

  /** Archive a terminal task so it no longer appears in active listings. */
  async archive(
    workspaceId: string,
    taskId: string,
    options?: RequestOptions,
  ): Promise<TaskArchiveResponse> {
    return this.client.request<TaskArchiveResponse>(
      'POST',
      `/workspaces/${workspaceId}/tasks/${taskId}/archive`,
      undefined,
      undefined,
      options,
    );
  }

  /**
   * Submit follow-up input to a task. The backend durably stores the input
   * and delivers it to whichever run is active for the task.
   */
  async submitInput(
    workspaceId: string,
    taskId: string,
    params: SubmitTaskInputParams,
    options?: RequestOptions,
  ): Promise<SubmitTaskInputResponse> {
    const body: Record<string, unknown> = {
      message: params.message,
      action: params.action,
      kind: params.kind,
      idempotency_key: params.idempotencyKey,
    }
    if (params.items?.length) {
      body.items = params.items.map(i => ({
        output_id: i.outputId,
        action: i.action,
        reason: i.reason,
      }))
    }
    return this.client.request<SubmitTaskInputResponse>(
      'POST',
      `/workspaces/${workspaceId}/tasks/${taskId}/input`,
      body,
      undefined,
      options,
    );
  }

  /** Update metadata on an existing task. */
  async update(
    workspaceId: string,
    taskId: string,
    params: TaskUpdateParams,
    options?: RequestOptions,
  ): Promise<AgentTask> {
    return this.client.request<AgentTask>(
      'PATCH',
      `/workspaces/${workspaceId}/tasks/${taskId}`,
      {
        ...(params.priority != null && { priority: params.priority }),
        ...(params.budgetUsd !== undefined && { budget_usd: params.budgetUsd }),
        ...(params.payload != null && { payload_json: params.payload }),
        ...(params.routing != null && { routing_json: toRoutingBody(params.routing) }),
      },
      undefined,
      options,
    );
  }

  // ── Schedules (cron) ──────────────────────────────────────────────────────

  private schedulePath(workspaceId: string, id?: string): string {
    const base = `/workspaces/${workspaceId}/tasks/schedules`;
    return id ? `${base}/${id}` : base;
  }

  /** Create a cron schedule that periodically submits a task to an agent. */
  async createSchedule(workspaceId: string, params: ScheduleCreateParams, options?: RequestOptions): Promise<Schedule> {
    return this.client.request<Schedule>('POST', this.schedulePath(workspaceId), {
      agent_id: params.agentId, cron_expr: params.cronExpr, prompt: params.prompt,
      ...(params.timezone != null && { timezone: params.timezone }),
      ...(params.skillPaths != null && { skill_paths: params.skillPaths }),
    }, undefined, options);
  }

  /** List cron schedules. Optionally filter by view_id. */
  async listSchedules(workspaceId: string, params?: { viewId?: string }, options?: RequestOptions): Promise<Schedule[]> {
    const qp = params?.viewId ? `?view_id=${encodeURIComponent(params.viewId)}` : '';
    return this.client.request<Schedule[]>('GET', this.schedulePath(workspaceId) + qp, undefined, undefined, options);
  }

  /** Retrieve a single schedule by ID. */
  async retrieveSchedule(workspaceId: string, scheduleId: string, options?: RequestOptions): Promise<Schedule> {
    return this.client.request<Schedule>('GET', this.schedulePath(workspaceId, scheduleId), undefined, undefined, options);
  }

  /** Update a schedule. Only provided fields are changed. Set `active: false` to pause. */
  async updateSchedule(workspaceId: string, scheduleId: string, params: ScheduleUpdateParams, options?: RequestOptions): Promise<Schedule> {
    return this.client.request<Schedule>('PATCH', this.schedulePath(workspaceId, scheduleId), {
      ...(params.cronExpr != null && { cron_expr: params.cronExpr }),
      ...(params.timezone != null && { timezone: params.timezone }),
      ...(params.prompt != null && { prompt: params.prompt }),
      ...(params.skillPaths != null && { skill_paths: params.skillPaths }),
      ...(params.active != null && { active: params.active }),
    }, undefined, options);
  }

  /** Permanently delete a schedule. */
  async deleteSchedule(workspaceId: string, scheduleId: string, options?: RequestOptions): Promise<void> {
    await this.client.request('DELETE', this.schedulePath(workspaceId, scheduleId), undefined, undefined, options);
  }

  // ── Task Outputs ──────────────────────────────────────────────────────────

  private outputPath(workspaceId: string, taskId: string, outputId?: string): string {
    const base = `/workspaces/${workspaceId}/tasks/${taskId}/outputs`;
    return outputId ? `${base}/${outputId}` : base;
  }

  /** List all outputs for a task. */
  async listOutputs(workspaceId: string, taskId: string, options?: RequestOptions): Promise<TaskOutput[]> {
    const resp = await this.client.request<{ outputs: TaskOutput[] }>(
      'GET', this.outputPath(workspaceId, taskId), undefined, undefined, options,
    );
    return resp.outputs ?? [];
  }

  /** List recent outputs across a workspace. */
  async listWorkspaceOutputs(
    workspaceId: string,
    params?: TaskOutputListParams,
    options?: RequestOptions,
  ): Promise<TaskOutputListResponse> {
    const response = await this.client.request<TaskOutputListResponse>(
      'GET',
      `/workspaces/${workspaceId}/outputs`,
      undefined,
      toTaskOutputListQuery(params),
      options,
    );
    return {
      outputs: response.outputs ?? [],
      next_cursor: response.next_cursor ?? '',
      has_more: response.has_more ?? false,
    };
  }

  /** Create a structured output for a task. */
  async createOutput(workspaceId: string, taskId: string, params: CreateTaskOutputParams, options?: RequestOptions): Promise<TaskOutput> {
    return this.client.request<TaskOutput>(
      'POST', this.outputPath(workspaceId, taskId), params, undefined, options,
    );
  }

  /** Retrieve a single output by ID (includes full data). */
  async getOutput(workspaceId: string, taskId: string, outputId: string, options?: RequestOptions): Promise<TaskOutput> {
    return this.client.request<TaskOutput>(
      'GET', this.outputPath(workspaceId, taskId, outputId), undefined, undefined, options,
    );
  }

  /** Append rows to a table output. */
  async appendOutputRows(workspaceId: string, taskId: string, outputId: string, params: AppendRowsParams, options?: RequestOptions): Promise<void> {
    await this.client.request(
      'POST', `${this.outputPath(workspaceId, taskId, outputId)}/rows`, params, undefined, options,
    );
  }

  /** Delete an output. */
  async deleteOutput(workspaceId: string, taskId: string, outputId: string, options?: RequestOptions): Promise<void> {
    await this.client.request(
      'DELETE', this.outputPath(workspaceId, taskId, outputId), undefined, undefined, options,
    );
  }

  /** Archive (dismiss) a single output. */
  async archiveOutput(workspaceId: string, outputId: string, options?: RequestOptions): Promise<void> {
    await this.client.request(
      'POST', `/workspaces/${workspaceId}/outputs/${outputId}/archive`, undefined, undefined, options,
    );
  }

  /** Archive all unarchived outputs in the workspace. */
  async archiveAllOutputs(workspaceId: string, options?: RequestOptions): Promise<{ archived: number }> {
    return this.client.request<{ archived: number }>(
      'POST', `/workspaces/${workspaceId}/outputs/archive-all`, undefined, undefined, options,
    );
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

function toTaskOutputListQuery(params: TaskOutputListParams | undefined): Record<string, string> | undefined {
  if (!params) return undefined;
  const query: Record<string, string> = {};
  if (params.taskId) query['task_id'] = params.taskId;
  if (params.agentId) query['agent_id'] = params.agentId;
  if (params.outputType) query['output_type'] = params.outputType;
  if (params.sourceViewId) query['source_view_id'] = params.sourceViewId;
  if (params.includeArchived) query['include_archived'] = 'true';
  if (params.limit !== undefined) query['limit'] = String(params.limit);
  if (params.cursor) query['cursor'] = params.cursor;
  return Object.keys(query).length > 0 ? query : undefined;
}
