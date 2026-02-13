/**
 * A single read recorded in the access log.
 */
export interface AccessLogRead {
  /** Unix millisecond timestamp of the read. */
  ts: number;
  /** Workspace external ID. */
  workspace_id: string;
  /** Session ID (defaults to workspace ID). */
  session_id: string;
  /** Virtual filesystem path that was read. */
  path: string;
  /** Integration that served the content (e.g., "github", "gmail"). */
  integration: string;
  /** Canonical upstream reference (e.g., "github://abc123", "gmail://msg-id"). */
  source_uri: string;
  /** Query path within the integration. */
  query_path?: string;
  /** Provider-specific result ID. */
  result_id?: string;
  /** Original content size in bytes. */
  original_bytes: number;
  /** Compressed content size in bytes. */
  compressed_bytes: number;
  /** Original token count. */
  original_tokens: number;
  /** Compressed token count. */
  compressed_tokens: number;
  /** Compression strategy used (e.g., "strip", "distill", "chain"). */
  strategy: string;
  /** Outcome ("compressed", "cache_hit", "passthrough", "timeout", "error", "skipped"). */
  outcome: string;
  /** Time spent on compression in milliseconds. */
  compression_ms: number;
  /** Error message, if any. */
  error_msg?: string;
}

/**
 * Parameters for listing access log reads.
 */
export interface AccessLogListParams {
  /** Start of time window (unix ms). */
  start?: number;
  /** End of time window (unix ms). */
  end?: number;
  /** Pagination cursor (S2 sequence number). */
  cursor?: string;
  /** Maximum reads to return (1-1000, default 100). */
  limit?: number;
  /** Custom session ID. Defaults to workspace ID. */
  session?: string;
}

/**
 * Response from listing access log reads.
 */
export interface AccessLogListResponse {
  /** The reads in this page. */
  reads: AccessLogRead[];
  /** Cursor for the next page. */
  next_cursor: string;
  /** Whether more reads exist beyond this page. */
  has_more: boolean;
}

/**
 * Per-integration breakdown.
 */
export interface IntegrationStats {
  events: number;
  original_tokens: number;
  compressed_tokens: number;
}

/**
 * A path ranked by total token consumption.
 */
export interface PathStats {
  path: string;
  source_uri: string;
  events: number;
  total_tokens: number;
}

/**
 * Parameters for the access log summary.
 */
export interface AccessLogSummaryParams {
  /** Start of time window (unix ms). */
  start?: number;
  /** End of time window (unix ms). */
  end?: number;
  /** Custom session ID. Defaults to workspace ID. */
  session?: string;
}

/**
 * Aggregated summary of access log reads within a time window.
 */
export interface AccessLogSummary {
  total_reads: number;
  total_original_tokens: number;
  total_compressed_tokens: number;
  compression_ratio: number;
  by_integration: Record<string, IntegrationStats>;
  by_outcome: Record<string, number>;
  top_paths: PathStats[];
}
