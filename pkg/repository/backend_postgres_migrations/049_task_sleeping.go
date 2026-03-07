package backend_postgres_migrations

import (
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigration(upTaskSleeping, downTaskSleeping)
}

func upTaskSleeping(tx *sql.Tx) error {
	stmts := []string{
		`ALTER TYPE agent_task_state ADD VALUE IF NOT EXISTS 'sleeping'`,
		`ALTER TABLE agent_task ADD COLUMN IF NOT EXISTS wake_at TIMESTAMPTZ`,
		`ALTER TABLE agent_task ADD COLUMN IF NOT EXISTS wake_reason TEXT`,
		`ALTER TABLE agent_task ADD COLUMN IF NOT EXISTS wake_count INTEGER NOT NULL DEFAULT 0`,
	}
	for _, stmt := range stmts {
		if _, err := tx.Exec(stmt); err != nil {
			return err
		}
	}
	return nil
}

func downTaskSleeping(tx *sql.Tx) error {
	stmts := []string{
		`ALTER TABLE agent_task DROP COLUMN IF EXISTS wake_count`,
		`ALTER TABLE agent_task DROP COLUMN IF EXISTS wake_reason`,
		`ALTER TABLE agent_task DROP COLUMN IF EXISTS wake_at`,
	}
	for _, stmt := range stmts {
		if _, err := tx.Exec(stmt); err != nil {
			return err
		}
	}
	return nil
}
