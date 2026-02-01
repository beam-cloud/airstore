package backend_postgres_migrations

import (
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigration(upTaskPrompt, downTaskPrompt)
}

func upTaskPrompt(tx *sql.Tx) error {
	// Add prompt column to task table for Claude Code tasks
	_, err := tx.Exec(`ALTER TABLE task ADD COLUMN IF NOT EXISTS prompt TEXT`)
	return err
}

func downTaskPrompt(tx *sql.Tx) error {
	_, err := tx.Exec(`ALTER TABLE task DROP COLUMN IF EXISTS prompt`)
	return err
}
