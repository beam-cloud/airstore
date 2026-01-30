package repository

import (
	"context"
	"time"

	"github.com/beam-cloud/airstore/pkg/types"
)

// QueryResult represents a materialized search result from a filesystem query.
type QueryResult struct {
	ID       string            `json:"id"`       // Provider-specific ID (e.g., Gmail message ID)
	Filename string            `json:"filename"` // Generated filename for the result
	Metadata map[string]string `json:"metadata"` // Key-value metadata (date, subject, from, etc.)
	Size     int64             `json:"size"`     // Content size in bytes
	Mtime    int64             `json:"mtime"`    // Last modified timestamp (Unix)
}

// SearchHit represents a full-text search match across materialized content.
type SearchHit struct {
	WorkspaceID uint    `json:"workspace_id"`
	QueryPath   string  `json:"query_path"` // Path of the query that produced this result
	ResultID    string  `json:"result_id"`  // Provider-specific ID
	Filename    string  `json:"filename"`
	Snippet     string  `json:"snippet"` // Text snippet with match highlighted
	Score       float64 `json:"score"`   // Relevance score
}

// FilesystemStore is the unified interface for all filesystem operations.
// It consolidates query definitions (Postgres/memory), query results (Elasticsearch/memory),
// and filesystem metadata (Redis/memory) into a single interface.
type FilesystemStore interface {
	// ===== Query Definitions =====

	// CreateQuery stores a new filesystem query definition.
	CreateQuery(ctx context.Context, query *types.FilesystemQuery) (*types.FilesystemQuery, error)

	// GetQuery retrieves a query by workspace and path.
	GetQuery(ctx context.Context, workspaceId uint, path string) (*types.FilesystemQuery, error)

	// GetQueryByExternalId retrieves a query by its external ID.
	GetQueryByExternalId(ctx context.Context, externalId string) (*types.FilesystemQuery, error)

	// ListQueries returns all queries under a parent path.
	ListQueries(ctx context.Context, workspaceId uint, parentPath string) ([]*types.FilesystemQuery, error)

	// UpdateQuery updates an existing query definition.
	UpdateQuery(ctx context.Context, query *types.FilesystemQuery) error

	// DeleteQuery removes a query by external ID.
	DeleteQuery(ctx context.Context, externalId string) error

	// ===== Query Results =====

	// GetQueryResults retrieves cached results for a query path.
	GetQueryResults(ctx context.Context, workspaceId uint, queryPath string) ([]QueryResult, error)

	// StoreQueryResults caches query results with the specified TTL.
	StoreQueryResults(ctx context.Context, workspaceId uint, queryPath string, results []QueryResult, ttl time.Duration) error

	// ===== Result Content =====

	// GetResultContent retrieves cached content for a specific result.
	GetResultContent(ctx context.Context, workspaceId uint, queryPath, resultID string) ([]byte, error)

	// StoreResultContent caches content for a specific result.
	StoreResultContent(ctx context.Context, workspaceId uint, queryPath, resultID string, content []byte) error

	// ===== Full-Text Search =====

	// SearchContent performs full-text search across all materialized content.
	SearchContent(ctx context.Context, workspaceId uint, query string, limit int) ([]SearchHit, error)

	// IndexContent indexes content for full-text search.
	IndexContent(ctx context.Context, workspaceId uint, queryPath, resultID, filename string, content []byte) error

	// ===== Filesystem Metadata =====

	// GetFileMeta retrieves file metadata.
	GetFileMeta(ctx context.Context, path string) (*types.FileMeta, error)

	// GetDirMeta retrieves directory metadata.
	GetDirMeta(ctx context.Context, path string) (*types.DirMeta, error)

	// SaveFileMeta stores file metadata.
	SaveFileMeta(ctx context.Context, meta *types.FileMeta) error

	// SaveDirMeta stores directory metadata.
	SaveDirMeta(ctx context.Context, meta *types.DirMeta) error

	// DeleteFileMeta removes file metadata.
	DeleteFileMeta(ctx context.Context, path string) error

	// DeleteDirMeta removes directory metadata.
	DeleteDirMeta(ctx context.Context, path string) error

	// ListDir returns directory entries.
	ListDir(ctx context.Context, path string) ([]types.DirEntry, error)

	// SaveDirListing stores a directory listing.
	SaveDirListing(ctx context.Context, path string, entries []types.DirEntry) error

	// ===== Symlinks =====

	// GetSymlink retrieves a symlink target.
	GetSymlink(ctx context.Context, path string) (string, error)

	// SaveSymlink stores a symlink.
	SaveSymlink(ctx context.Context, path, target string) error

	// DeleteSymlink removes a symlink.
	DeleteSymlink(ctx context.Context, path string) error

	// ===== Cache Invalidation =====

	// InvalidatePath removes a specific path from all caches.
	InvalidatePath(ctx context.Context, path string) error

	// InvalidatePrefix removes all paths with a given prefix.
	InvalidatePrefix(ctx context.Context, prefix string) error

	// InvalidateQuery removes all cached data for a query (results + content).
	InvalidateQuery(ctx context.Context, workspaceId uint, queryPath string) error
}
