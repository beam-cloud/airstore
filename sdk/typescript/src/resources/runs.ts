import type { CoreClient, RequestOptions } from '../client.js';
import type {
  AgentRun,
  AgentRunAttempt,
  AgentRunSnapshot,
  RunCancelResponse,
  RunInputParams,
} from '../types/runs.js';
import type { TaskEnvelopeAcceptedResponse } from '../types/tasks.js';

/**
 * Read and control orchestration runs.
 */
export class Runs {
  constructor(private readonly client: CoreClient) {}

  async list(workspaceId: string, options?: RequestOptions): Promise<AgentRun[]> {
    return this.client.request<AgentRun[]>(
      'GET',
      `/workspaces/${workspaceId}/runs`,
      undefined,
      undefined,
      options,
    );
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

  async listAttempts(
    workspaceId: string,
    runId: string,
    options?: RequestOptions,
  ): Promise<AgentRunAttempt[]> {
    return this.client.request<AgentRunAttempt[]>(
      'GET',
      `/workspaces/${workspaceId}/runs/${runId}/attempts`,
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

  async input(
    workspaceId: string,
    runId: string,
    params: RunInputParams,
    options?: RequestOptions,
  ): Promise<TaskEnvelopeAcceptedResponse> {
    return this.client.request<TaskEnvelopeAcceptedResponse>(
      'POST',
      `/workspaces/${workspaceId}/runs/${runId}/input`,
      {
        message: params.message,
        idempotency_key: params.idempotencyKey,
        queue_mode: params.queueMode,
      },
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
