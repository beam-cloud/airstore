package backend_postgres_migrations

import (
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigration(upTaskOutputArchived, downTaskOutputArchived)
}

func upTaskOutputArchived(tx *sql.Tx) error {
	stmts := []string{
		`ALTER TABLE task_output ADD COLUMN IF NOT EXISTS archived_at TIMESTAMPTZ`,
		`CREATE INDEX IF NOT EXISTS idx_task_output_workspace_archived ON task_output (workspace_id, archived_at) WHERE archived_at IS NULL`,
	}
	for _, stmt := range stmts {
		if _, err := tx.Exec(stmt); err != nil {
			return err
		}
	}
	return nil
}

func downTaskOutputArchived(tx *sql.Tx) error {
	stmts := []string{
		`DROP INDEX IF EXISTS idx_task_output_workspace_archived`,
		`ALTER TABLE task_output DROP COLUMN IF EXISTS archived_at`,
	}
	for _, stmt := range stmts {
		if _, err := tx.Exec(stmt); err != nil {
			return err
		}
	}
	return nil
}
