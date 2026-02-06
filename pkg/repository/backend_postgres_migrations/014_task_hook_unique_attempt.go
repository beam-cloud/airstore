package backend_postgres_migrations

import (
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigration(upTaskHookUniqueAttempt, downTaskHookUniqueAttempt)
}

func upTaskHookUniqueAttempt(tx *sql.Tx) error {
	// Only one non-terminal task per hook at a time.
	// Prevents duplicate fires and duplicate retries across replicas.
	// Completed/failed/cancelled tasks don't count -- the hook can fire again.
	_, err := tx.Exec(`CREATE UNIQUE INDEX IF NOT EXISTS idx_task_hook_one_active ON task(hook_id) WHERE hook_id IS NOT NULL AND status NOT IN ('complete', 'failed', 'cancelled')`)
	return err
}

func downTaskHookUniqueAttempt(tx *sql.Tx) error {
	_, err := tx.Exec(`DROP INDEX IF EXISTS idx_task_hook_one_active`)
	return err
}
