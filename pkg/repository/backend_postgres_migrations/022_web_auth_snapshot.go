package backend_postgres_migrations

import (
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigration(upWebAuthSnapshot, downWebAuthSnapshot)
}

func upWebAuthSnapshot(tx *sql.Tx) error {
	_, err := tx.Exec(`ALTER TABLE filesystem_queries ADD COLUMN IF NOT EXISTS web_auth_snapshot JSONB`)
	return err
}

func downWebAuthSnapshot(tx *sql.Tx) error {
	_, err := tx.Exec(`ALTER TABLE filesystem_queries DROP COLUMN IF EXISTS web_auth_snapshot`)
	return err
}
