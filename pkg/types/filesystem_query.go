package types

import "time"

// ViewOutputFormat specifies how source view results are materialized.
type ViewOutputFormat string

const (
	ViewOutputFolder ViewOutputFormat = "folder" // Each result as a file in a directory
	ViewOutputFile   ViewOutputFormat = "file"   // All results in a single file
)

// ViewMode distinguishes how a view's query was created.
type ViewMode string

const (
	ViewModeSmart ViewMode = "smart" // Created via LLM inference from name + guidance
	ViewModeQuery ViewMode = "query" // Created via structured filter
)

// FilesystemQuery represents a source view that materializes as filesystem content.
// A view can be created via LLM inference (smart mode) or structured filters (query mode).
// Reading the folder executes the view's query against the source provider.
type FilesystemQuery struct {
	Id             uint             `json:"id" db:"id"`
	ExternalId     string           `json:"external_id" db:"external_id"`
	WorkspaceId    uint             `json:"workspace_id" db:"workspace_id"`
	Integration    string           `json:"integration" db:"integration"`         // "gmail", "gdrive", "notion"
	Path           string           `json:"path" db:"path"`                       // Full path: "/sources/gmail/unread-emails"
	Name           string           `json:"name" db:"name"`                       // Folder/file name: "unread-emails"
	QuerySpec      string           `json:"query_spec" db:"query_spec"`           // JSON query params
	Guidance       string           `json:"guidance" db:"guidance"`               // Optional user-provided context (smart mode)
	OutputFormat   ViewOutputFormat `json:"output_format" db:"output_format"`     // "folder" or "file"
	FileExt        string           `json:"file_ext" db:"file_ext"`               // For files: ".json", ".md"
	FilenameFormat string           `json:"filename_format" db:"filename_format"` // Template for result filenames
	CacheTTL       int              `json:"cache_ttl" db:"cache_ttl"`             // Seconds, 0 = always live
	Mode           ViewMode         `json:"mode" db:"mode"`                       // "smart" or "query"
	Filter         string           `json:"filter,omitempty" db:"filter"`         // JSON structured filter (query mode, round-trip editing)
	CreatedAt      time.Time        `json:"created_at" db:"created_at"`
	UpdatedAt      time.Time        `json:"updated_at" db:"updated_at"`
	LastExecuted   *time.Time       `json:"last_executed,omitempty" db:"last_executed"`
}

// IsFolder returns true if results materialize as a directory.
func (q *FilesystemQuery) IsFolder() bool {
	return q.OutputFormat == ViewOutputFolder
}

// IsFile returns true if results materialize as a single file.
func (q *FilesystemQuery) IsFile() bool {
	return q.OutputFormat == ViewOutputFile
}

// IsLive returns true if the query should be re-executed on every access.
func (q *FilesystemQuery) IsLive() bool {
	return q.CacheTTL == 0
}

// SourceView is the canonical name for FilesystemQuery.
type SourceView = FilesystemQuery
