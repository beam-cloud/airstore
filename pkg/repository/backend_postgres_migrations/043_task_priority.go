package backend_postgres_migrations

import (
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigration(upTaskPriority, downTaskPriority)
}

func upTaskPriority(tx *sql.Tx) error {
	_, err := tx.Exec(`ALTER TABLE agent_task ADD COLUMN IF NOT EXISTS priority TEXT NOT NULL DEFAULT 'normal';`)
	return err
}

func downTaskPriority(tx *sql.Tx) error {
	_, err := tx.Exec(`ALTER TABLE agent_task DROP COLUMN IF EXISTS priority;`)
	return err
}
