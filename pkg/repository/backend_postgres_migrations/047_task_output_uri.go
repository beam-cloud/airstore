package backend_postgres_migrations

import (
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigration(upTaskOutputURI, downTaskOutputURI)
}

func upTaskOutputURI(tx *sql.Tx) error {
	stmts := []string{
		`ALTER TABLE task_output ADD COLUMN IF NOT EXISTS uri TEXT`,
		`CREATE INDEX IF NOT EXISTS idx_task_output_workspace_created ON task_output (workspace_id, created_at DESC)`,
	}
	for _, stmt := range stmts {
		if _, err := tx.Exec(stmt); err != nil {
			return err
		}
	}
	return nil
}

func downTaskOutputURI(tx *sql.Tx) error {
	stmts := []string{
		`DROP INDEX IF EXISTS idx_task_output_workspace_created`,
		`ALTER TABLE task_output DROP COLUMN IF EXISTS uri`,
	}
	for _, stmt := range stmts {
		if _, err := tx.Exec(stmt); err != nil {
			return err
		}
	}
	return nil
}
