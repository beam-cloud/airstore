package types

import "time"

// QueryOutputFormat specifies how filesystem query results are materialized.
type QueryOutputFormat string

const (
	QueryOutputFolder QueryOutputFormat = "folder" // Each result as a file in a directory
	QueryOutputFile   QueryOutputFormat = "file"   // All results in a single file
)

// FilesystemQuery represents a user-defined query that materializes as filesystem content.
// When a user creates a folder like /sources/gmail/unread-emails, an LLM infers
// the appropriate query and stores it here. Reading the folder executes the query.
type FilesystemQuery struct {
	Id             uint              `json:"id" db:"id"`
	ExternalId     string            `json:"external_id" db:"external_id"`
	WorkspaceId    uint              `json:"workspace_id" db:"workspace_id"`
	Integration    string            `json:"integration" db:"integration"`       // "gmail", "gdrive", "notion"
	Path           string            `json:"path" db:"path"`                     // Full path: "/sources/gmail/unread-emails"
	Name           string            `json:"name" db:"name"`                     // Folder/file name: "unread-emails"
	QuerySpec      string            `json:"query_spec" db:"query_spec"`         // JSON query params from LLM
	Guidance       string            `json:"guidance" db:"guidance"`             // Optional user-provided context
	OutputFormat   QueryOutputFormat `json:"output_format" db:"output_format"`   // "folder" or "file"
	FileExt        string            `json:"file_ext" db:"file_ext"`             // For files: ".json", ".md"
	FilenameFormat string            `json:"filename_format" db:"filename_format"` // Template for result filenames (e.g., "{date}_{subject}_{id}.txt")
	CacheTTL       int               `json:"cache_ttl" db:"cache_ttl"`           // Seconds, 0 = always live
	CreatedAt      time.Time         `json:"created_at" db:"created_at"`
	UpdatedAt      time.Time         `json:"updated_at" db:"updated_at"`
	LastExecuted   *time.Time        `json:"last_executed,omitempty" db:"last_executed"`
}

// IsFolder returns true if results materialize as a directory.
func (q *FilesystemQuery) IsFolder() bool {
	return q.OutputFormat == QueryOutputFolder
}

// IsFile returns true if results materialize as a single file.
func (q *FilesystemQuery) IsFile() bool {
	return q.OutputFormat == QueryOutputFile
}

// IsLive returns true if the query should be re-executed on every access.
func (q *FilesystemQuery) IsLive() bool {
	return q.CacheTTL == 0
}

// Aliases for backward compatibility during migration
type SmartQuery = FilesystemQuery
type SmartQueryOutputFormat = QueryOutputFormat

const (
	SmartQueryOutputFolder = QueryOutputFolder
	SmartQueryOutputFile   = QueryOutputFile
)
