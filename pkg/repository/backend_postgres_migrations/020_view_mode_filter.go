package backend_postgres_migrations

import (
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigration(upViewModeFilter, downViewModeFilter)
}

func upViewModeFilter(tx *sql.Tx) error {
	// Add mode and filter columns to support structured query mode.
	// mode: "smart" (default, LLM-inferred) or "query" (structured filter).
	// filter: JSON blob storing the per-integration filter for round-trip editing.
	_, err := tx.Exec(`
		ALTER TABLE filesystem_queries
		ADD COLUMN IF NOT EXISTS mode VARCHAR(16) NOT NULL DEFAULT 'smart',
		ADD COLUMN IF NOT EXISTS filter JSONB DEFAULT NULL
	`)
	return err
}

func downViewModeFilter(tx *sql.Tx) error {
	_, err := tx.Exec(`
		ALTER TABLE filesystem_queries
		DROP COLUMN IF EXISTS filter,
		DROP COLUMN IF EXISTS mode
	`)
	return err
}
