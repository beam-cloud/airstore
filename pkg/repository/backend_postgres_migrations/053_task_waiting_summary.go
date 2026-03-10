package backend_postgres_migrations

import (
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigration(upTaskWaitingSummary, downTaskWaitingSummary)
}

func upTaskWaitingSummary(tx *sql.Tx) error {
	_, err := tx.Exec(`ALTER TABLE agent_task ADD COLUMN IF NOT EXISTS waiting_summary TEXT`)
	return err
}

func downTaskWaitingSummary(tx *sql.Tx) error {
	_, err := tx.Exec(`ALTER TABLE agent_task DROP COLUMN IF EXISTS waiting_summary`)
	return err
}
