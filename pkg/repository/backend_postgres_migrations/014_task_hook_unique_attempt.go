package backend_postgres_migrations

import (
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigration(upTaskHookUniqueAttempt, downTaskHookUniqueAttempt)
}

func upTaskHookUniqueAttempt(tx *sql.Tx) error {
	// Prevent duplicate retry attempts across replicas.
	// Two pollers seeing the same failed task can't both create attempt N+1.
	_, err := tx.Exec(`CREATE UNIQUE INDEX IF NOT EXISTS idx_task_hook_attempt ON task(hook_id, attempt) WHERE hook_id IS NOT NULL AND status != 'cancelled'`)
	return err
}

func downTaskHookUniqueAttempt(tx *sql.Tx) error {
	_, err := tx.Exec(`DROP INDEX IF EXISTS idx_task_hook_attempt`)
	return err
}
