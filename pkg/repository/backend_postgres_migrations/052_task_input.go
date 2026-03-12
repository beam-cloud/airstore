package backend_postgres_migrations

import (
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigration(upTaskInput, downTaskInput)
}

func upTaskInput(tx *sql.Tx) error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS task_input (
		   id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
		   workspace_id BIGINT NOT NULL,
		   task_id UUID NOT NULL REFERENCES agent_task(id) ON DELETE CASCADE,
		   session_id TEXT NOT NULL DEFAULT '',
		   seq INT NOT NULL DEFAULT 0,
		   kind TEXT NOT NULL DEFAULT 'free_text',
		   action TEXT,
		   message TEXT NOT NULL DEFAULT '',
		   idempotency_key TEXT NOT NULL,
		   status TEXT NOT NULL DEFAULT 'pending',
		   claimed_by_run_id UUID,
		   claimed_by_execution_id TEXT,
		   created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
		   claimed_at TIMESTAMPTZ,
		   consumed_at TIMESTAMPTZ
		 )`,
		`CREATE UNIQUE INDEX IF NOT EXISTS idx_task_input_idempotency
		   ON task_input(task_id, idempotency_key)`,
		`CREATE INDEX IF NOT EXISTS idx_task_input_pending
		   ON task_input(task_id, seq)
		   WHERE status = 'pending'`,
		`CREATE INDEX IF NOT EXISTS idx_task_input_task
		   ON task_input(task_id, created_at DESC)`,
	}
	for _, stmt := range stmts {
		if _, err := tx.Exec(stmt); err != nil {
			return err
		}
	}
	return nil
}

func downTaskInput(tx *sql.Tx) error {
	_, err := tx.Exec(`DROP TABLE IF EXISTS task_input`)
	return err
}
