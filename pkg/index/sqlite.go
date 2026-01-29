package index

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	_ "codeberg.org/emersion/go-sqlite3-fts5" // Register FTS5 extension
	_ "github.com/mattn/go-sqlite3"
	"github.com/rs/zerolog/log"
)

// SQLiteIndexStore implements IndexStore using SQLite with FTS5 for full-text search.
// This is the default embedded store that requires no external dependencies.
type SQLiteIndexStore struct {
	db                *sql.DB
	useFallbackSearch bool // true if FTS5 is not available
}

// NewSQLiteIndexStore creates a new SQLite-backed index store.
// The dbPath should be a path to the SQLite database file.
// Use ":memory:" for an in-memory database (useful for testing).
// If the file doesn't exist, it will be created along with parent directories.
func NewSQLiteIndexStore(dbPath string) (*SQLiteIndexStore, error) {
	// Create parent directories if needed (skip for in-memory databases)
	if dbPath != ":memory:" && !strings.HasPrefix(dbPath, ":memory:") {
		dir := filepath.Dir(dbPath)
		if dir != "" && dir != "." {
			if err := os.MkdirAll(dir, 0755); err != nil {
				return nil, fmt.Errorf("failed to create index directory %s: %w", dir, err)
			}
		}
		log.Info().Str("path", dbPath).Msg("opening sqlite index store")
	}

	db, err := sql.Open("sqlite3", dbPath+"?_journal_mode=WAL&_synchronous=NORMAL&_busy_timeout=5000")
	if err != nil {
		return nil, fmt.Errorf("failed to open sqlite db: %w", err)
	}

	store := &SQLiteIndexStore{db: db}
	if err := store.migrate(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to migrate sqlite db: %w", err)
	}

	return store, nil
}

// migrate creates the necessary tables and indexes
func (s *SQLiteIndexStore) migrate() error {
	// Main entries table (with workspace_id for multi-tenant namespacing)
	_, err := s.db.Exec(`
		CREATE TABLE IF NOT EXISTS entries (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			workspace_id TEXT NOT NULL DEFAULT '',
			integration TEXT NOT NULL,
			entity_id TEXT NOT NULL,
			path TEXT NOT NULL,
			parent_path TEXT NOT NULL,
			name TEXT NOT NULL,
			type TEXT NOT NULL,
			size INTEGER NOT NULL DEFAULT 0,
			mod_time INTEGER NOT NULL,
			title TEXT NOT NULL DEFAULT '',
			body TEXT NOT NULL DEFAULT '',
			metadata TEXT NOT NULL DEFAULT '{}',
			content_ref TEXT NOT NULL DEFAULT '',
			created_at INTEGER NOT NULL DEFAULT (strftime('%s', 'now')),
			updated_at INTEGER NOT NULL DEFAULT (strftime('%s', 'now')),
			UNIQUE(workspace_id, integration, entity_id)
		)
	`)
	if err != nil {
		return fmt.Errorf("failed to create entries table: %w", err)
	}

	// Add workspace_id column if it doesn't exist (migration for existing databases)
	s.db.Exec(`ALTER TABLE entries ADD COLUMN workspace_id TEXT NOT NULL DEFAULT ''`)

	// Indexes for common queries (including workspace_id)
	indexes := []string{
		"CREATE INDEX IF NOT EXISTS idx_entries_workspace ON entries(workspace_id)",
		"CREATE INDEX IF NOT EXISTS idx_entries_integration ON entries(integration)",
		"CREATE INDEX IF NOT EXISTS idx_entries_workspace_integration ON entries(workspace_id, integration)",
		"CREATE INDEX IF NOT EXISTS idx_entries_path ON entries(path)",
		"CREATE INDEX IF NOT EXISTS idx_entries_workspace_path ON entries(workspace_id, path)",
		"CREATE INDEX IF NOT EXISTS idx_entries_parent_path ON entries(integration, parent_path)",
		"CREATE INDEX IF NOT EXISTS idx_entries_workspace_parent ON entries(workspace_id, integration, parent_path)",
		"CREATE INDEX IF NOT EXISTS idx_entries_integration_entity ON entries(integration, entity_id)",
	}

	for _, idx := range indexes {
		if _, err := s.db.Exec(idx); err != nil {
			return fmt.Errorf("failed to create index: %w", err)
		}
	}

	// Try to create FTS5 virtual table for full-text search
	// If FTS5 is not available, we'll fall back to LIKE-based search
	_, err = s.db.Exec(`
		CREATE VIRTUAL TABLE IF NOT EXISTS entries_fts USING fts5(
			integration,
			entity_id,
			title,
			body,
			content=entries,
			content_rowid=id,
			tokenize='porter unicode61'
		)
	`)
	if err != nil {
		// FTS5 not available, log and continue with fallback search
		log.Warn().Err(err).Msg("FTS5 not available, using fallback search")
		s.useFallbackSearch = true
	} else {
		// Triggers to keep FTS in sync
		triggers := []string{
			`CREATE TRIGGER IF NOT EXISTS entries_ai AFTER INSERT ON entries BEGIN
				INSERT INTO entries_fts(rowid, integration, entity_id, title, body)
				VALUES (new.id, new.integration, new.entity_id, new.title, new.body);
			END`,
			`CREATE TRIGGER IF NOT EXISTS entries_ad AFTER DELETE ON entries BEGIN
				INSERT INTO entries_fts(entries_fts, rowid, integration, entity_id, title, body)
				VALUES ('delete', old.id, old.integration, old.entity_id, old.title, old.body);
			END`,
			`CREATE TRIGGER IF NOT EXISTS entries_au AFTER UPDATE ON entries BEGIN
				INSERT INTO entries_fts(entries_fts, rowid, integration, entity_id, title, body)
				VALUES ('delete', old.id, old.integration, old.entity_id, old.title, old.body);
				INSERT INTO entries_fts(rowid, integration, entity_id, title, body)
				VALUES (new.id, new.integration, new.entity_id, new.title, new.body);
			END`,
		}

		for _, trigger := range triggers {
			if _, err := s.db.Exec(trigger); err != nil {
				log.Warn().Err(err).Msg("failed to create FTS trigger")
			}
		}
	}

	// Sync status table
	_, err = s.db.Exec(`
		CREATE TABLE IF NOT EXISTS sync_status (
			integration TEXT PRIMARY KEY,
			last_sync INTEGER NOT NULL,
			sync_token TEXT NOT NULL DEFAULT ''
		)
	`)
	if err != nil {
		return fmt.Errorf("failed to create sync_status table: %w", err)
	}

	return nil
}

// Upsert inserts or updates an entry in the index
func (s *SQLiteIndexStore) Upsert(ctx context.Context, entry *IndexEntry) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO entries (workspace_id, integration, entity_id, path, parent_path, name, type, size, mod_time, title, body, metadata, content_ref, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, strftime('%s', 'now'))
		ON CONFLICT(workspace_id, integration, entity_id) DO UPDATE SET
			path = excluded.path,
			parent_path = excluded.parent_path,
			name = excluded.name,
			type = excluded.type,
			size = excluded.size,
			mod_time = excluded.mod_time,
			title = excluded.title,
			body = excluded.body,
			metadata = excluded.metadata,
			content_ref = excluded.content_ref,
			updated_at = strftime('%s', 'now')
	`, entry.WorkspaceID, entry.Integration, entry.EntityID, entry.Path, entry.ParentPath, entry.Name, string(entry.Type),
		entry.Size, entry.ModTime.Unix(), entry.Title, entry.Body, entry.Metadata, entry.ContentRef)

	if err != nil {
		return fmt.Errorf("failed to upsert entry: %w", err)
	}
	return nil
}

// Delete removes an entry from the index
func (s *SQLiteIndexStore) Delete(ctx context.Context, integration, entityID string) error {
	_, err := s.db.ExecContext(ctx, `
		DELETE FROM entries WHERE integration = ? AND entity_id = ?
	`, integration, entityID)
	if err != nil {
		return fmt.Errorf("failed to delete entry: %w", err)
	}
	return nil
}

// DeleteByIntegration removes all entries for an integration
func (s *SQLiteIndexStore) DeleteByIntegration(ctx context.Context, integration string) error {
	_, err := s.db.ExecContext(ctx, `
		DELETE FROM entries WHERE integration = ?
	`, integration)
	if err != nil {
		return fmt.Errorf("failed to delete entries: %w", err)
	}
	return nil
}

// List returns direct children under a path prefix (for directory listing)
func (s *SQLiteIndexStore) List(ctx context.Context, integration, pathPrefix string) ([]*IndexEntry, error) {
	// Normalize path prefix for matching
	if pathPrefix != "" && !strings.HasSuffix(pathPrefix, "/") {
		pathPrefix = pathPrefix + "/"
	}

	rows, err := s.db.QueryContext(ctx, `
		SELECT workspace_id, integration, entity_id, path, parent_path, name, type, size, mod_time, title, body, metadata, content_ref
		FROM entries
		WHERE integration = ? AND parent_path = ?
		ORDER BY type DESC, name ASC
	`, integration, strings.TrimSuffix(pathPrefix, "/"))
	if err != nil {
		return nil, fmt.Errorf("failed to list entries: %w", err)
	}
	defer rows.Close()

	return s.scanEntries(rows)
}

// ListAll returns ALL entries for an integration (for sync/export)
func (s *SQLiteIndexStore) ListAll(ctx context.Context, integration string) ([]*IndexEntry, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT workspace_id, integration, entity_id, path, parent_path, name, type, size, mod_time, title, body, metadata, content_ref
		FROM entries
		WHERE integration = ?
		ORDER BY path ASC
	`, integration)
	if err != nil {
		return nil, fmt.Errorf("failed to list all entries: %w", err)
	}
	defer rows.Close()

	return s.scanEntries(rows)
}

// Get retrieves a single entry by integration and entity ID
func (s *SQLiteIndexStore) Get(ctx context.Context, integration, entityID string) (*IndexEntry, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT workspace_id, integration, entity_id, path, parent_path, name, type, size, mod_time, title, body, metadata, content_ref
		FROM entries
		WHERE integration = ? AND entity_id = ?
	`, integration, entityID)

	return s.scanEntry(row)
}

// GetByPath retrieves an entry by its virtual filesystem path
func (s *SQLiteIndexStore) GetByPath(ctx context.Context, path string) (*IndexEntry, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT workspace_id, integration, entity_id, path, parent_path, name, type, size, mod_time, title, body, metadata, content_ref
		FROM entries
		WHERE path = ?
	`, path)

	return s.scanEntry(row)
}

// Search performs full-text search across entries
func (s *SQLiteIndexStore) Search(ctx context.Context, integration, query string) ([]*IndexEntry, error) {
	log.Debug().Str("integration", integration).Str("query", query).Bool("fallback", s.useFallbackSearch).Msg("searching index")

	var rows *sql.Rows
	var err error

	if s.useFallbackSearch {
		// Fallback to LIKE-based search
		likeQuery := "%" + query + "%"
		rows, err = s.db.QueryContext(ctx, `
			SELECT workspace_id, integration, entity_id, path, parent_path, name, type, size, mod_time, title, body, metadata, content_ref
			FROM entries
			WHERE integration = ? AND (title LIKE ? OR body LIKE ?)
			LIMIT 1000
		`, integration, likeQuery, likeQuery)
	} else {
		// Use FTS5 search
		ftsQuery := escapeFTSQuery(query)
		rows, err = s.db.QueryContext(ctx, `
			SELECT e.workspace_id, e.integration, e.entity_id, e.path, e.parent_path, e.name, e.type, e.size, e.mod_time, e.title, e.body, e.metadata, e.content_ref
			FROM entries e
			JOIN entries_fts f ON e.id = f.rowid
			WHERE f.integration = ? AND entries_fts MATCH ?
			ORDER BY rank
			LIMIT 1000
		`, integration, ftsQuery)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to search entries: %w", err)
	}
	defer rows.Close()

	return s.scanEntries(rows)
}

// LastSync returns the last sync time for an integration
func (s *SQLiteIndexStore) LastSync(ctx context.Context, integration string) (time.Time, error) {
	var lastSync int64
	err := s.db.QueryRowContext(ctx, `
		SELECT last_sync FROM sync_status WHERE integration = ?
	`, integration).Scan(&lastSync)

	if err == sql.ErrNoRows {
		return time.Time{}, nil
	}
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to get last sync: %w", err)
	}

	return time.Unix(lastSync, 0), nil
}

// SetLastSync records the last sync time for an integration
func (s *SQLiteIndexStore) SetLastSync(ctx context.Context, integration string, t time.Time) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO sync_status (integration, last_sync) VALUES (?, ?)
		ON CONFLICT(integration) DO UPDATE SET last_sync = excluded.last_sync
	`, integration, t.Unix())
	if err != nil {
		return fmt.Errorf("failed to set last sync: %w", err)
	}
	return nil
}

// Close closes the store and releases resources
func (s *SQLiteIndexStore) Close() error {
	return s.db.Close()
}

// scanEntry scans a single entry from a row
func (s *SQLiteIndexStore) scanEntry(row *sql.Row) (*IndexEntry, error) {
	var entry IndexEntry
	var entryType string
	var modTime int64

	err := row.Scan(
		&entry.WorkspaceID, &entry.Integration, &entry.EntityID, &entry.Path, &entry.ParentPath,
		&entry.Name, &entryType, &entry.Size, &modTime,
		&entry.Title, &entry.Body, &entry.Metadata, &entry.ContentRef,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to scan entry: %w", err)
	}

	entry.Type = EntryType(entryType)
	entry.ModTime = time.Unix(modTime, 0)

	return &entry, nil
}

// scanEntries scans multiple entries from rows
func (s *SQLiteIndexStore) scanEntries(rows *sql.Rows) ([]*IndexEntry, error) {
	var entries []*IndexEntry

	for rows.Next() {
		var entry IndexEntry
		var entryType string
		var modTime int64

		err := rows.Scan(
			&entry.WorkspaceID, &entry.Integration, &entry.EntityID, &entry.Path, &entry.ParentPath,
			&entry.Name, &entryType, &entry.Size, &modTime,
			&entry.Title, &entry.Body, &entry.Metadata, &entry.ContentRef,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan entry: %w", err)
		}

		entry.Type = EntryType(entryType)
		entry.ModTime = time.Unix(modTime, 0)
		entries = append(entries, &entry)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating rows: %w", err)
	}

	return entries, nil
}

// escapeFTSQuery escapes special characters for FTS5 queries
func escapeFTSQuery(query string) string {
	// For simple queries, wrap in quotes to search as phrase
	// For more complex queries, we'd need to parse and handle operators
	query = strings.TrimSpace(query)
	if query == "" {
		return "\"\""
	}

	// If query contains operators, pass through
	if strings.ContainsAny(query, "\"+-*()") {
		return query
	}

	// Simple term search - escape and wrap in quotes for exact phrase match
	// or use * for prefix match
	return "\"" + strings.ReplaceAll(query, "\"", "\"\"") + "\"*"
}

// Stats returns statistics about the index
func (s *SQLiteIndexStore) Stats(ctx context.Context) (map[string]int64, error) {
	stats := make(map[string]int64)

	rows, err := s.db.QueryContext(ctx, `
		SELECT integration, COUNT(*) as count FROM entries GROUP BY integration
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var integration string
		var count int64
		if err := rows.Scan(&integration, &count); err != nil {
			return nil, err
		}
		stats[integration] = count
	}

	return stats, nil
}
