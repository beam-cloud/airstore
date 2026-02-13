package instrumentation

import "context"

// AccessEvent records a single file access through the filesystem.
// Every read produces exactly one event regardless of compression outcome.
type AccessEvent struct {
	Timestamp        int64  `json:"ts"`
	WorkspaceID      string `json:"workspace_id"`
	SessionID        string `json:"session_id"`
	Path             string `json:"path"`
	Integration      string `json:"integration"`
	SourceURI        string `json:"source_uri"`         // canonical upstream ref, e.g. "github://abc123" or "gmail://msg-id"
	QueryPath        string `json:"query_path"`
	ResultID         string `json:"result_id"`
	OriginalBytes    int    `json:"original_bytes"`
	CompressedBytes  int    `json:"compressed_bytes"`
	OriginalTokens   int    `json:"original_tokens"`
	CompressedTokens int    `json:"compressed_tokens"`
	Strategy         string `json:"strategy"` // requested strategy
	Outcome          string `json:"outcome"`  // "compressed", "cache_hit", "passthrough", "timeout", "error", "skipped"
	CompressionMs    int64  `json:"compression_ms"`
	ErrorMsg         string `json:"error_msg,omitempty"` // populated on outcome=error or timeout
}

// AccessRecorder records file access events. Implementations must be safe
// for concurrent use and should be non-blocking (buffer internally).
type AccessRecorder interface {
	Record(ctx context.Context, event AccessEvent) error // Record enqueues an access event for async delivery.
	Flush() error                                        // Flush drains any buffered events. Called on graceful shutdown.
}
