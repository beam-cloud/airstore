package backend_postgres_migrations

import (
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigration(upScheduleSourceView, downScheduleSourceView)
}

func upScheduleSourceView(tx *sql.Tx) error {
	stmts := []string{
		`ALTER TABLE scheduled_task
		   ADD COLUMN IF NOT EXISTS source_view_id TEXT`,
		`CREATE INDEX IF NOT EXISTS idx_scheduled_task_source_view
		 ON scheduled_task (workspace_id, source_view_id)
		 WHERE source_view_id IS NOT NULL`,
	}

	for _, stmt := range stmts {
		if _, err := tx.Exec(stmt); err != nil {
			return err
		}
	}
	return nil
}

func downScheduleSourceView(tx *sql.Tx) error {
	stmts := []string{
		`DROP INDEX IF EXISTS idx_scheduled_task_source_view`,
		`ALTER TABLE scheduled_task DROP COLUMN IF EXISTS source_view_id`,
	}

	for _, stmt := range stmts {
		if _, err := tx.Exec(stmt); err != nil {
			return err
		}
	}
	return nil
}
