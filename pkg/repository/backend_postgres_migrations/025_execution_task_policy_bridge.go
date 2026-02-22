package backend_postgres_migrations

import (
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigration(upExecutionTaskPolicyBridge, downExecutionTaskPolicyBridge)
}

func upExecutionTaskPolicyBridge(tx *sql.Tx) error {
	stmts := []string{
		`ALTER TABLE task
		   ADD COLUMN IF NOT EXISTS run_attempt_id UUID REFERENCES agent_run_attempt(id) ON DELETE SET NULL,
		   ADD COLUMN IF NOT EXISTS timeout_ms INT NULL CHECK (timeout_ms IS NULL OR timeout_ms >= 0),
		   ADD COLUMN IF NOT EXISTS exec_host TEXT NULL CHECK (exec_host IN ('sandbox', 'gateway', 'node')),
		   ADD COLUMN IF NOT EXISTS exec_security TEXT NULL CHECK (exec_security IN ('deny', 'allowlist', 'full')),
		   ADD COLUMN IF NOT EXISTS exec_ask TEXT NULL CHECK (exec_ask IN ('off', 'on-miss', 'always')),
		   ADD COLUMN IF NOT EXISTS runtime_type TEXT NULL,
		   ADD COLUMN IF NOT EXISTS workspace_access TEXT NULL CHECK (workspace_access IN ('none', 'ro', 'rw')),
		   ADD COLUMN IF NOT EXISTS network_enabled BOOLEAN NULL,
		   ADD COLUMN IF NOT EXISTS execution_policy_json JSONB NOT NULL DEFAULT '{}'::jsonb;`,
		`CREATE UNIQUE INDEX IF NOT EXISTS uq_task_run_attempt_id
		 ON task (run_attempt_id)
		 WHERE run_attempt_id IS NOT NULL;`,
	}
	for _, stmt := range stmts {
		if _, err := tx.Exec(stmt); err != nil {
			return err
		}
	}
	return nil
}

func downExecutionTaskPolicyBridge(tx *sql.Tx) error {
	stmts := []string{
		`DROP INDEX IF EXISTS uq_task_run_attempt_id;`,
		`ALTER TABLE task
		   DROP COLUMN IF EXISTS execution_policy_json,
		   DROP COLUMN IF EXISTS network_enabled,
		   DROP COLUMN IF EXISTS workspace_access,
		   DROP COLUMN IF EXISTS runtime_type,
		   DROP COLUMN IF EXISTS exec_ask,
		   DROP COLUMN IF EXISTS exec_security,
		   DROP COLUMN IF EXISTS exec_host,
		   DROP COLUMN IF EXISTS timeout_ms,
		   DROP COLUMN IF EXISTS run_attempt_id;`,
	}
	for _, stmt := range stmts {
		if _, err := tx.Exec(stmt); err != nil {
			return err
		}
	}
	return nil
}
