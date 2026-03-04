import type { CoreClient, RequestOptions } from '../client.js';
import type {
  AccessLogListParams,
  AccessLogListResponse,
  AccessLogSummaryParams,
  AccessLogSummary,
} from '../types/access-log.js';

/**
 * Query the workspace access log.
 *
 * Every file read through the virtual filesystem is recorded with token
 * counts, compression outcome, timing, and a `source_uri` that pins
 * the content back to its upstream origin.
 */
export class AccessLog {
  constructor(private readonly client: CoreClient) {}

  /** List access log reads with optional time-window and cursor pagination. */
  async list(
    workspaceId: string,
    params?: AccessLogListParams,
    options?: RequestOptions,
  ): Promise<AccessLogListResponse> {
    const query: Record<string, string> = {};
    if (params?.start !== undefined) query.start = String(params.start);
    if (params?.end !== undefined) query.end = String(params.end);
    if (params?.cursor !== undefined) query.cursor = params.cursor;
    if (params?.limit !== undefined) query.limit = String(params.limit);
    if (params?.session !== undefined) query.session = params.session;

    return this.client.request<AccessLogListResponse>(
      'GET',
      `/workspaces/${workspaceId}/access-log`,
      undefined,
      query,
      options,
    );
  }

  /** Get an aggregated summary: totals, compression ratio, per-integration breakdown. */
  async summary(
    workspaceId: string,
    params?: AccessLogSummaryParams,
    options?: RequestOptions,
  ): Promise<AccessLogSummary> {
    const query: Record<string, string> = {};
    if (params?.start !== undefined) query.start = String(params.start);
    if (params?.end !== undefined) query.end = String(params.end);
    if (params?.session !== undefined) query.session = params.session;

    return this.client.request<AccessLogSummary>(
      'GET',
      `/workspaces/${workspaceId}/access-log/summary`,
      undefined,
      query,
      options,
    );
  }

  /** Read content directly from an upstream source using a `source_uri`. */
  async read(
    workspaceId: string,
    sourceUri: string,
    options?: RequestOptions,
  ): Promise<ArrayBuffer> {
    const resp = await this.client.rawRequest(
      'GET',
      `/workspaces/${workspaceId}/access-log/read`,
      { params: { uri: sourceUri }, ...options },
    );
    return resp.arrayBuffer();
  }
}
