package backend_postgres_migrations

import (
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigration(upTaskSourceWatches, downTaskSourceWatches)
}

func upTaskSourceWatches(tx *sql.Tx) error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS task_source_watches (
			id              SERIAL PRIMARY KEY,
			workspace_id    INT    NOT NULL,
			task_id         UUID   NOT NULL,
			integration     TEXT   NOT NULL,
			correlation_key TEXT   NOT NULL,
			reason          TEXT,
			created_at      TIMESTAMPTZ DEFAULT NOW(),
			UNIQUE (workspace_id, task_id, integration, correlation_key)
		)`,
		`CREATE INDEX IF NOT EXISTS idx_tsw_lookup
		 ON task_source_watches (workspace_id, integration, correlation_key)`,
		`CREATE INDEX IF NOT EXISTS idx_tsw_task
		 ON task_source_watches (task_id)`,

		// Clean up legacy source watch queries and hooks
		`DELETE FROM filesystem_queries WHERE path LIKE '%/__followup__%'`,
		`DELETE FROM filesystem_hooks WHERE system_managed = true AND delivery_mode = 'task_input'`,
	}

	for _, stmt := range stmts {
		if _, err := tx.Exec(stmt); err != nil {
			return err
		}
	}
	return nil
}

func downTaskSourceWatches(tx *sql.Tx) error {
	stmts := []string{
		`DROP INDEX IF EXISTS idx_tsw_task`,
		`DROP INDEX IF EXISTS idx_tsw_lookup`,
		`DROP TABLE IF EXISTS task_source_watches`,
	}

	for _, stmt := range stmts {
		if _, err := tx.Exec(stmt); err != nil {
			return err
		}
	}
	return nil
}
