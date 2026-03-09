package backend_postgres_migrations

import (
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigration(upTaskInputKind, downTaskInputKind)
}

func upTaskInputKind(tx *sql.Tx) error {
	_, err := tx.Exec(`ALTER TABLE agent_task ADD COLUMN IF NOT EXISTS input_kind TEXT`)
	return err
}

func downTaskInputKind(tx *sql.Tx) error {
	_, err := tx.Exec(`ALTER TABLE agent_task DROP COLUMN IF EXISTS input_kind`)
	return err
}
