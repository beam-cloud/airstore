package index

import (
	"encoding/json"
	"time"
)

// EntryType indicates whether an entry is a file or directory
type EntryType string

const (
	EntryTypeFile EntryType = "file"
	EntryTypeDir  EntryType = "dir"
)

// IndexEntry represents a single item stored in the local index.
// This could be an email, a file, a page, etc.
type IndexEntry struct {
	// WorkspaceID is the workspace this entry belongs to
	WorkspaceID string `json:"workspace_id"`

	// Integration is the source integration name (e.g., "gmail", "gdrive", "notion")
	Integration string `json:"integration"`

	// EntityID is the source-specific unique identifier (e.g., Gmail message ID)
	EntityID string `json:"entity_id"`

	// Path is the virtual filesystem path where this entry appears
	// e.g., "messages/inbox/Meeting_abc123/body.txt"
	Path string `json:"path"`

	// Name is the display name (e.g., filename, email subject)
	Name string `json:"name"`

	// Type indicates whether this is a file or directory
	Type EntryType `json:"type"`

	// Size is the content size in bytes (0 for directories)
	Size int64 `json:"size"`

	// ModTime is the last modification time
	ModTime time.Time `json:"mod_time"`

	// Searchable content fields

	// Title is the primary searchable title (subject, filename, page title)
	Title string `json:"title"`

	// Body is the full text content (for small items like emails)
	// For large files, this may be empty and content fetched on-demand
	Body string `json:"body"`

	// Metadata is a JSON blob of extra provider-specific fields
	// e.g., from, to, labels for Gmail; author, created for Notion
	Metadata string `json:"metadata"`

	// ContentRef is a reference to fetch actual content for large/binary files
	// For files stored in the index, this is empty
	ContentRef string `json:"content_ref"`

	// ParentPath is the parent directory path (for hierarchical listing)
	ParentPath string `json:"parent_path"`
}

// SetMetadata serializes a struct to the Metadata field
func (e *IndexEntry) SetMetadata(v any) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	e.Metadata = string(data)
	return nil
}

// GetMetadata deserializes the Metadata field into a struct
func (e *IndexEntry) GetMetadata(v any) error {
	if e.Metadata == "" {
		return nil
	}
	return json.Unmarshal([]byte(e.Metadata), v)
}

// IsFile returns true if this entry is a file
func (e *IndexEntry) IsFile() bool {
	return e.Type == EntryTypeFile
}

// IsDir returns true if this entry is a directory
func (e *IndexEntry) IsDir() bool {
	return e.Type == EntryTypeDir
}

// Content returns the body content, or an empty string if content must be fetched
func (e *IndexEntry) Content() string {
	return e.Body
}

// NeedsFetch returns true if content must be fetched from the source
func (e *IndexEntry) NeedsFetch() bool {
	return e.ContentRef != "" && e.Body == ""
}
