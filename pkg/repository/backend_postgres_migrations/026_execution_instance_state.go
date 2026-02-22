package backend_postgres_migrations

import (
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigration(upExecutionInstanceState, downExecutionInstanceState)
}

func upExecutionInstanceState(tx *sql.Tx) error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS agent_execution_instance (
		   id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
		   instance_key TEXT NOT NULL UNIQUE,
		   workspace_id INT NOT NULL REFERENCES workspace(id) ON DELETE CASCADE,
		   agent_id UUID NULL REFERENCES agent_profile(id) ON DELETE SET NULL,
		   lane TEXT NULL,
		   execution_class_key TEXT NOT NULL,
		   pool_name TEXT NOT NULL DEFAULT 'default',
		   active BOOLEAN NOT NULL DEFAULT TRUE,
		   status TEXT NOT NULL DEFAULT 'healthy' CHECK (status IN ('healthy', 'warning', 'degraded')),
		   failed_attempt_threshold INT NOT NULL DEFAULT 5 CHECK (failed_attempt_threshold > 0),
		   desired_dispatch_concurrency INT NOT NULL DEFAULT 0 CHECK (desired_dispatch_concurrency >= 0),
		   running_attempts INT NOT NULL DEFAULT 0 CHECK (running_attempts >= 0),
		   pending_attempts INT NOT NULL DEFAULT 0 CHECK (pending_attempts >= 0),
		   stopping_attempts INT NOT NULL DEFAULT 0 CHECK (stopping_attempts >= 0),
		   last_event_at TIMESTAMPTZ NULL,
		   created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
		   updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
		 );`,
		`CREATE INDEX IF NOT EXISTS idx_agent_execution_instance_workspace_agent
		 ON agent_execution_instance (workspace_id, agent_id, updated_at DESC);`,
		`CREATE INDEX IF NOT EXISTS idx_agent_execution_instance_status
		 ON agent_execution_instance (status, updated_at DESC);`,
	}
	for _, stmt := range stmts {
		if _, err := tx.Exec(stmt); err != nil {
			return err
		}
	}
	return nil
}

func downExecutionInstanceState(tx *sql.Tx) error {
	_, err := tx.Exec(`DROP TABLE IF EXISTS agent_execution_instance;`)
	return err
}
