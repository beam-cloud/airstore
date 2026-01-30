package backend_postgres_migrations

import (
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigration(upFilenameFormat, downFilenameFormat)
}

func upFilenameFormat(tx *sql.Tx) error {
	// Add filename_format column to filesystem_queries table
	// This stores the LLM-generated template for result filenames
	_, err := tx.Exec(`
		ALTER TABLE filesystem_queries
		ADD COLUMN IF NOT EXISTS filename_format TEXT DEFAULT ''
	`)
	return err
}

func downFilenameFormat(tx *sql.Tx) error {
	_, err := tx.Exec(`
		ALTER TABLE filesystem_queries
		DROP COLUMN IF EXISTS filename_format
	`)
	return err
}
