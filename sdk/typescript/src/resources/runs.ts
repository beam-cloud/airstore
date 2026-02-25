import type { CoreClient, RequestOptions } from '../client.js';
import type {
  AgentRun,
  RunListParams,
  RunListResponse,
  AgentRunSnapshot,
  RunCancelResponse,
} from '../types/runs.js';

/**
 * Read and control run lifecycle state.
 */
export class Runs {
  constructor(private readonly client: CoreClient) {}

  async list(
    workspaceId: string,
    params?: RunListParams,
    options?: RequestOptions,
  ): Promise<RunListResponse> {
    const response = await this.client.request<RunListResponse | AgentRun[]>(
      'GET',
      `/workspaces/${workspaceId}/runs`,
      undefined,
      toRunListQuery(params),
      options,
    );
    if (Array.isArray(response)) {
      return { runs: response, next_cursor: '', has_more: false };
    }
    return {
      runs: response.runs ?? [],
      next_cursor: response.next_cursor ?? '',
      has_more: response.has_more ?? false,
    };
  }

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
