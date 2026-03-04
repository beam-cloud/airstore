package backend_postgres_migrations

import (
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigration(upScheduleTimezone, downScheduleTimezone)
}

func upScheduleTimezone(tx *sql.Tx) error {
	stmts := []string{
		`ALTER TABLE scheduled_task
		 ADD COLUMN IF NOT EXISTS timezone TEXT NOT NULL DEFAULT 'UTC';`,
	}

	for _, stmt := range stmts {
		if _, err := tx.Exec(stmt); err != nil {
			return err
		}
	}
	return nil
}

func downScheduleTimezone(tx *sql.Tx) error {
	stmts := []string{
		`ALTER TABLE scheduled_task DROP COLUMN IF EXISTS timezone;`,
	}

	for _, stmt := range stmts {
		if _, err := tx.Exec(stmt); err != nil {
			return err
		}
	}
	return nil
}
