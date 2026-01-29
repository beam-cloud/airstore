package index

import (
	"context"
	"time"
)

// IndexStore is the storage abstraction for the local index.
// It supports both SQLite (embedded) and Elasticsearch (external) backends.
type IndexStore interface {
	// Write operations (called by syncers)

	// Upsert inserts or updates an entry in the index
	Upsert(ctx context.Context, entry *IndexEntry) error

	// Delete removes an entry from the index
	Delete(ctx context.Context, integration, entityID string) error

	// DeleteByIntegration removes all entries for an integration
	DeleteByIntegration(ctx context.Context, integration string) error

	// Read operations (called by FUSE)

	// List returns direct children under a path prefix (for directory listing)
	List(ctx context.Context, integration, pathPrefix string) ([]*IndexEntry, error)

	// ListAll returns ALL entries for an integration (for sync/export)
	ListAll(ctx context.Context, integration string) ([]*IndexEntry, error)

	// Get retrieves a single entry by integration and entity ID
	Get(ctx context.Context, integration, entityID string) (*IndexEntry, error)

	// GetByPath retrieves an entry by its virtual filesystem path
	GetByPath(ctx context.Context, path string) (*IndexEntry, error)

	// Search performs full-text search across entries
	Search(ctx context.Context, integration, query string) ([]*IndexEntry, error)

	// Sync status

	// LastSync returns the last sync time for an integration
	LastSync(ctx context.Context, integration string) (time.Time, error)

	// SetLastSync records the last sync time for an integration
	SetLastSync(ctx context.Context, integration string, t time.Time) error

	// Lifecycle

	// Close closes the store and releases resources
	Close() error
}
