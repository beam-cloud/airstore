package backend_postgres_migrations

import (
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigration(upTaskMember, downTaskMember)
}

func upTaskMember(tx *sql.Tx) error {
	// Add created_by_member_id column to task table
	_, err := tx.Exec(`ALTER TABLE task ADD COLUMN IF NOT EXISTS created_by_member_id INTEGER REFERENCES workspace_member(id) ON DELETE SET NULL`)
	return err
}

func downTaskMember(tx *sql.Tx) error {
	_, err := tx.Exec(`ALTER TABLE task DROP COLUMN IF EXISTS created_by_member_id`)
	return err
}
