package backend_postgres_migrations

import (
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigration(upTaskName, downTaskName)
}

func upTaskName(tx *sql.Tx) error {
	stmts := []string{
		// Add name column for human-readable task slugs
		`ALTER TABLE task ADD COLUMN IF NOT EXISTS name VARCHAR(100)`,

		// Backfill existing tasks: use first 50 chars of external_id as slug
		// (proper slug generation happens in Go; this is a safe default)
		`UPDATE task SET name = CONCAT('task-', LEFT(REPLACE(external_id::text, '-', ''), 8)) WHERE name IS NULL`,

		// Index for name-based lookups
		`CREATE INDEX IF NOT EXISTS idx_task_name ON task(name)`,
	}

	for _, stmt := range stmts {
		if _, err := tx.Exec(stmt); err != nil {
			return err
		}
	}

	return nil
}

func downTaskName(tx *sql.Tx) error {
	stmts := []string{
		`DROP INDEX IF EXISTS idx_task_name`,
		`ALTER TABLE task DROP COLUMN IF EXISTS name`,
	}

	for _, stmt := range stmts {
		if _, err := tx.Exec(stmt); err != nil {
			return err
		}
	}

	return nil
}
