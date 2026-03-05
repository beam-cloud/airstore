package backend_postgres_migrations

import (
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigration(upTaskType, downTaskType)
}

func upTaskType(tx *sql.Tx) error {
	_, err := tx.Exec(`ALTER TABLE task ADD COLUMN IF NOT EXISTS type TEXT NOT NULL DEFAULT 'background'`)
	return err
}

func downTaskType(tx *sql.Tx) error {
	_, err := tx.Exec(`ALTER TABLE task DROP COLUMN IF EXISTS type`)
	return err
}
