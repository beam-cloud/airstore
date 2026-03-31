package types

// ViewContextEntry is a single record in a view's persistent context stream.
// The stream is append-only (S2-backed); compaction entries summarize prior
// entries up to a sequence frontier.
type ViewContextEntry struct {
	ID           string         `json:"id"`
	ViewID       string         `json:"view_id"`
	Timestamp    int64          `json:"timestamp"`
	EntryType    string         `json:"entry_type"`
	Content      string         `json:"content"`
	SourceTaskID string         `json:"source_task_id,omitempty"`
	Metadata     map[string]any `json:"metadata,omitempty"`
	CompactUpTo  int64          `json:"compact_up_to,omitempty"`
	SeqNum       int64          `json:"seq_num,omitempty"`
}

const (
	ViewContextEntryFeedback   = "feedback"
	ViewContextEntryNote       = "note"
	ViewContextEntryLink       = "link"
	ViewContextEntryCompaction = "compaction"
)
