package backend_postgres_migrations

import (
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigration(upTaskOutputStatus, downTaskOutputStatus)
}

func upTaskOutputStatus(tx *sql.Tx) error {
	stmts := []string{
		`ALTER TABLE task_output ADD COLUMN status TEXT NOT NULL DEFAULT 'active'`,
		`ALTER TABLE task_output ADD CONSTRAINT chk_task_output_status CHECK (status IN ('active', 'pending', 'approved', 'rejected', 'superseded'))`,
		`CREATE INDEX idx_task_output_status ON task_output(status) WHERE status != 'active'`,
	}
	for _, stmt := range stmts {
		if _, err := tx.Exec(stmt); err != nil {
			return err
		}
	}
	return nil
}

func downTaskOutputStatus(tx *sql.Tx) error {
	stmts := []string{
		`DROP INDEX IF EXISTS idx_task_output_status`,
		`ALTER TABLE task_output DROP CONSTRAINT IF EXISTS chk_task_output_status`,
		`ALTER TABLE task_output DROP COLUMN IF EXISTS status`,
	}
	for _, stmt := range stmts {
		if _, err := tx.Exec(stmt); err != nil {
			return err
		}
	}
	return nil
}
