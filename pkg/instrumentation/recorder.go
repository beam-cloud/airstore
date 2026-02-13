package instrumentation

import "context"

// AccessEvent records a single file access through the filesystem.
// Every read produces exactly one event regardless of compression outcome.
type AccessEvent struct {
	EventID          string `json:"event_id,omitempty"`
	Timestamp        int64  `json:"ts"`
	WorkspaceID      string `json:"workspace_id"`
	SessionID        string `json:"session_id"`
	Path             string `json:"path"`
	CacheSource      string `json:"cache_source,omitempty"` // backend_rpc, open_content, content_cache, prefetch, dirty_buffer, etc.
	Offset           int64  `json:"offset,omitempty"`
	RequestedBytes   int    `json:"requested_bytes,omitempty"`
	ReadBytes        int    `json:"read_bytes,omitempty"`
	LatencyMs        int64  `json:"latency_ms,omitempty"`
	MountID          string `json:"mount_id,omitempty"`
	AccessOrigin     string `json:"access_origin,omitempty"` // "fuse" when emitted by mount collector
	Integration      string `json:"integration"`
	SourceURI        string `json:"source_uri"` // canonical upstream ref, e.g. "github://abc123" or "gmail://msg-id"
	QueryPath        string `json:"query_path"`
	ResultID         string `json:"result_id"`
	OriginalBytes    int    `json:"original_bytes"`
	CompressedBytes  int    `json:"compressed_bytes"`
	OriginalTokens   int    `json:"original_tokens"`
	CompressedTokens int    `json:"compressed_tokens"`
	Strategy         string `json:"strategy"` // requested strategy
	Outcome          string `json:"outcome"`  // "compressed", "cache_hit", "passthrough", "timeout", "error", "skipped"
	CompressionMs    int64  `json:"compression_ms"`
	FetchMs          int64  `json:"fetch_ms,omitempty"` // e2e content fetch duration (e.g. time to fetch from source during Open)
	ErrorMsg         string `json:"error_msg,omitempty"` // populated on outcome=error or timeout
}

// AccessRecorder records file access events. Implementations must be safe
// for concurrent use and should be non-blocking (buffer internally).
type AccessRecorder interface {
	Record(ctx context.Context, event AccessEvent) error // Record enqueues an access event for async delivery.
	Flush() error                                        // Flush drains any buffered events. Called on graceful shutdown.
}
