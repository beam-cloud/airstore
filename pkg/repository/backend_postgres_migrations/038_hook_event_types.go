package backend_postgres_migrations

import (
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigration(upHookEventTypes, downHookEventTypes)
}

func upHookEventTypes(tx *sql.Tx) error {
	stmts := []string{
		`ALTER TABLE filesystem_hooks
		 ADD COLUMN IF NOT EXISTS event_types TEXT[] NOT NULL DEFAULT '{fs.create}'::text[];`,
	}

	for _, stmt := range stmts {
		if _, err := tx.Exec(stmt); err != nil {
			return err
		}
	}
	return nil
}

func downHookEventTypes(tx *sql.Tx) error {
	stmts := []string{
		`ALTER TABLE filesystem_hooks DROP COLUMN IF EXISTS event_types;`,
	}

	for _, stmt := range stmts {
		if _, err := tx.Exec(stmt); err != nil {
			return err
		}
	}
	return nil
}
