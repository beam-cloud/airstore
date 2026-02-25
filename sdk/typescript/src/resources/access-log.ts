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
 *
 * @example List recent reads
 * ```ts
 * const page = await airstore.accessLog.list("ws-abc", {
 *   start: Date.now() - 3600_000, // last hour
 *   limit: 50,
 * });
 * for (const r of page.reads) {
 *   console.log(r.source_uri, r.original_tokens, "->", r.compressed_tokens);
 * }
 * ```
 *
 * @example Get a summary
 * ```ts
 * const s = await airstore.accessLog.summary("ws-abc", {
 *   start: Date.now() - 86400_000,
 * });
 * console.log(`${s.total_reads} reads, ${s.compression_ratio} ratio`);
 * ```
 *
 * @example Replay a read by source URI
 * ```ts
 * const content = await airstore.accessLog.read("ws-abc", "github://abc123");
 * ```
 */
export class AccessLog {
  constructor(private readonly client: CoreClient) {}

  /**
   * List access log reads for a workspace.
   *
   * Supports cursor-based pagination and optional time-window filtering.
   *
   * @param workspaceId - Workspace external ID (UUID).
   * @param params - Filtering and pagination options.
   * @param options - Per-request overrides.
   */
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

  /**
   * Get an aggregated summary of access log reads.
   *
   * Returns totals, compression ratio, per-integration breakdown,
   * outcome distribution, and top paths by token consumption.
   *
   * @param workspaceId - Workspace external ID (UUID).
   * @param params - Time window and session options.
   * @param options - Per-request overrides.
   */
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

  /**
   * Read content directly from an upstream source using a `source_uri`.
   *
   * This bypasses the source-view/query layer entirely. Even if the
   * materialized view has changed, the source_uri lets you pull the
   * exact content that was originally read.
   *
   * @param workspaceId - Workspace external ID (UUID).
   * @param sourceUri - Source URI from an access log entry (e.g., "github://abc123").
   * @param options - Per-request overrides.
   * @returns The raw content as an ArrayBuffer.
   */
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
