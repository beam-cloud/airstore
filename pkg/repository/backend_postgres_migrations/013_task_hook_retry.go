package backend_postgres_migrations

import (
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigration(upTaskHookRetry, downTaskHookRetry)
}

func upTaskHookRetry(tx *sql.Tx) error {
	stmts := []string{
		`ALTER TABLE task ADD COLUMN IF NOT EXISTS hook_id INTEGER REFERENCES filesystem_hooks(id) ON DELETE SET NULL`,
		`ALTER TABLE task ADD COLUMN IF NOT EXISTS attempt INTEGER NOT NULL DEFAULT 1`,
		`ALTER TABLE task ADD COLUMN IF NOT EXISTS max_attempts INTEGER NOT NULL DEFAULT 1`,
		`CREATE INDEX IF NOT EXISTS idx_task_hook_active ON task(hook_id) WHERE hook_id IS NOT NULL AND status NOT IN ('complete', 'failed', 'cancelled')`,
	}
	for _, stmt := range stmts {
		if _, err := tx.Exec(stmt); err != nil {
			return err
		}
	}
	return nil
}

func downTaskHookRetry(tx *sql.Tx) error {
	stmts := []string{
		`DROP INDEX IF EXISTS idx_task_hook_active`,
		`ALTER TABLE task DROP COLUMN IF EXISTS max_attempts`,
		`ALTER TABLE task DROP COLUMN IF EXISTS attempt`,
		`ALTER TABLE task DROP COLUMN IF EXISTS hook_id`,
	}
	for _, stmt := range stmts {
		if _, err := tx.Exec(stmt); err != nil {
			return err
		}
	}
	return nil
}
