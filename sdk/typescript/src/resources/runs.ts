import { attachResponseMeta, type CoreClient, type RequestOptions } from '../client.js';
import type {
  AgentRun,
  RunListParams,
  RunListResponse,
  AgentRunSnapshot,
  RunCancelResponse,
} from '../types/runs.js';

/**
 * Read and manage the lifecycle of agent runs.
 *
 * A run is the actual execution instance spawned by a task. Use this resource
 * to inspect run status, list snapshots (intermediate state), retrieve
 * execution events, or cancel a running execution.
 */
export class Runs {
  constructor(private readonly client: CoreClient) {}

  /** List runs with optional filters (agent, status, session, date range) and cursor pagination. */
  async list(workspaceId: string, options?: RequestOptions): Promise<RunListResponse>;
  async list(
    workspaceId: string,
    params?: RunListParams,
    options?: RequestOptions,
  ): Promise<RunListResponse>;
  async list(
    workspaceId: string,
    paramsOrOptions?: RunListParams | RequestOptions,
    maybeOptions?: RequestOptions,
  ): Promise<RunListResponse> {
    const secondArgIsOptions = shouldTreatSecondArgAsOptions(paramsOrOptions, maybeOptions);
    const params = secondArgIsOptions
      ? undefined
      : (paramsOrOptions as RunListParams | undefined);
    const options = secondArgIsOptions
      ? (paramsOrOptions as RequestOptions | undefined)
      : maybeOptions;

    const response = await this.client.request<RunListResponse | AgentRun[]>(
      'GET',
      `/workspaces/${workspaceId}/runs`,
      undefined,
      toRunListQuery(params),
      options,
    );
    if (Array.isArray(response)) {
      return attachResponseMeta(
        { runs: response, next_cursor: '', has_more: false },
        response.lastResponse,
      );
    }
    return attachResponseMeta(
      {
        runs: response.runs ?? [],
        next_cursor: response.next_cursor ?? '',
        has_more: response.has_more ?? false,
      },
      response.lastResponse,
    );
  }

  /** Retrieve a single run by ID. */
  async retrieve(
    workspaceId: string,
    runId: string,
    options?: RequestOptions,
  ): Promise<AgentRun> {
    return this.client.request<AgentRun>(
      'GET',
      `/workspaces/${workspaceId}/runs/${runId}`,
      undefined,
      undefined,
      options,
    );
  }

  /** List snapshots (intermediate state captures) for a run, ordered by sequence number. */
  async listSnapshots(
    workspaceId: string,
    runId: string,
    options?: RequestOptions,
  ): Promise<AgentRunSnapshot[]> {
    return this.client.request<AgentRunSnapshot[]>(
      'GET',
      `/workspaces/${workspaceId}/runs/${runId}/snapshots`,
      undefined,
      undefined,
      options,
    );
  }

  /** List execution events emitted during a run. */
  async listEvents(
    workspaceId: string,
    runId: string,
    options?: RequestOptions,
  ): Promise<Array<Record<string, unknown>>> {
    return this.client.request<Array<Record<string, unknown>>>(
      'GET',
      `/workspaces/${workspaceId}/runs/${runId}/events`,
      undefined,
      undefined,
      options,
    );
  }

  /** Cancel an active run. */
  async cancel(
    workspaceId: string,
    runId: string,
    options?: RequestOptions,
  ): Promise<RunCancelResponse> {
    return this.client.request<RunCancelResponse>(
      'POST',
      `/workspaces/${workspaceId}/runs/${runId}/cancel`,
      undefined,
      undefined,
      options,
    );
  }
}

function toRunListQuery(params: RunListParams | undefined): Record<string, string> | undefined {
  if (!params) return undefined;
  const query: Record<string, string> = {};
  if (params.agentId) query['agent_id'] = params.agentId;
  if (params.status) {
    query['status'] = Array.isArray(params.status) ? params.status.join(',') : params.status;
  }
  if (params.sessionId) query['session_id'] = params.sessionId;
  if (params.createdAfter) query['created_after'] = params.createdAfter;
  if (params.createdBefore) query['created_before'] = params.createdBefore;
  if (params.limit !== undefined) query['limit'] = String(params.limit);
  if (params.cursor) query['cursor'] = params.cursor;
  return Object.keys(query).length > 0 ? query : undefined;
}

function shouldTreatSecondArgAsOptions(
  paramsOrOptions: RunListParams | RequestOptions | undefined,
  options: RequestOptions | undefined,
): paramsOrOptions is RequestOptions {
  if (options !== undefined) return false;
  if (!paramsOrOptions || typeof paramsOrOptions !== 'object') return false;
  return !hasRunListParamKeys(paramsOrOptions);
}

function hasRunListParamKeys(value: RunListParams | RequestOptions): value is RunListParams {
  return (
    'agentId' in value ||
    'status' in value ||
    'sessionId' in value ||
    'createdAfter' in value ||
    'createdBefore' in value ||
    'limit' in value ||
    'cursor' in value
  );
}
