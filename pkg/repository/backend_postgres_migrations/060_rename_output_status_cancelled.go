package backend_postgres_migrations

import (
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigration(up060, down060)
}

func up060(tx *sql.Tx) error {
	stmts := []string{
		`ALTER TABLE task_output DROP CONSTRAINT IF EXISTS chk_task_output_status`,
		`UPDATE task_output SET status = 'cancelled' WHERE status = 'superseded'`,
		`ALTER TABLE task_output ADD CONSTRAINT chk_task_output_status CHECK (status IN ('active', 'pending', 'approved', 'rejected', 'cancelled'))`,
	}
	for _, stmt := range stmts {
		if _, err := tx.Exec(stmt); err != nil {
			return err
		}
	}
	return nil
}

func down060(tx *sql.Tx) error {
	stmts := []string{
		`UPDATE task_output SET status = 'superseded' WHERE status = 'cancelled'`,
		`ALTER TABLE task_output DROP CONSTRAINT IF EXISTS chk_task_output_status`,
		`ALTER TABLE task_output ADD CONSTRAINT chk_task_output_status CHECK (status IN ('active', 'pending', 'approved', 'rejected', 'superseded'))`,
	}
	for _, stmt := range stmts {
		if _, err := tx.Exec(stmt); err != nil {
			return err
		}
	}
	return nil
}
