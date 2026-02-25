package backend_postgres_migrations

import (
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigration(upAgentOrchestrationIndexes, downAgentOrchestrationIndexes)
}

func upAgentOrchestrationIndexes(tx *sql.Tx) error {
	stmts := []string{
		`CREATE UNIQUE INDEX IF NOT EXISTS uq_agent_task_envelope_idempotency_with_agent
		 ON agent_task_envelope (workspace_id, agent_id, idempotency_key)
		 WHERE agent_id IS NOT NULL;`,
		`CREATE UNIQUE INDEX IF NOT EXISTS uq_agent_task_envelope_idempotency_without_agent
		 ON agent_task_envelope (workspace_id, idempotency_key)
		 WHERE agent_id IS NULL;`,
		`CREATE INDEX IF NOT EXISTS idx_agent_task_envelope_state_accepted_at
		 ON agent_task_envelope (state, accepted_at)
		 WHERE state IN ('accepted', 'queued');`,
		`CREATE INDEX IF NOT EXISTS idx_agent_task_envelope_workspace_state_created
		 ON agent_task_envelope (workspace_id, state, created_at DESC);`,
		`CREATE INDEX IF NOT EXISTS idx_agent_task_envelope_target_run
		 ON agent_task_envelope (target_run_id)
		 WHERE target_run_id IS NOT NULL;`,
		`CREATE INDEX IF NOT EXISTS idx_agent_task_envelope_parent_envelope
		 ON agent_task_envelope (parent_envelope_id)
		 WHERE parent_envelope_id IS NOT NULL;`,
		`CREATE INDEX IF NOT EXISTS idx_agent_run_workspace_created
		 ON agent_run (workspace_id, created_at DESC);`,
		`CREATE INDEX IF NOT EXISTS idx_agent_run_status_updated
		 ON agent_run (status, updated_at DESC);`,
		`CREATE INDEX IF NOT EXISTS idx_agent_run_agent_created
		 ON agent_run (agent_id, created_at DESC)
		 WHERE agent_id IS NOT NULL;`,
		`CREATE UNIQUE INDEX IF NOT EXISTS uq_agent_run_attempt_execution_task_external_id
		 ON agent_run_attempt (execution_task_external_id)
		 WHERE execution_task_external_id IS NOT NULL;`,
		`CREATE INDEX IF NOT EXISTS idx_agent_run_attempt_run_status_attempt_no
		 ON agent_run_attempt (run_id, status, attempt_no DESC);`,
		`CREATE INDEX IF NOT EXISTS idx_agent_run_snapshot_run_ts_desc
		 ON agent_run_snapshot (run_id, ts DESC);`,
	}
	for _, stmt := range stmts {
		if _, err := tx.Exec(stmt); err != nil {
			return err
		}
	}
	return nil
}

func downAgentOrchestrationIndexes(tx *sql.Tx) error {
	stmts := []string{
		`DROP INDEX IF EXISTS idx_agent_run_snapshot_run_ts_desc;`,
		`DROP INDEX IF EXISTS idx_agent_run_attempt_run_status_attempt_no;`,
		`DROP INDEX IF EXISTS uq_agent_run_attempt_execution_task_external_id;`,
		`DROP INDEX IF EXISTS idx_agent_run_agent_created;`,
		`DROP INDEX IF EXISTS idx_agent_run_status_updated;`,
		`DROP INDEX IF EXISTS idx_agent_run_workspace_created;`,
		`DROP INDEX IF EXISTS idx_agent_task_envelope_parent_envelope;`,
		`DROP INDEX IF EXISTS idx_agent_task_envelope_target_run;`,
		`DROP INDEX IF EXISTS idx_agent_task_envelope_workspace_state_created;`,
		`DROP INDEX IF EXISTS idx_agent_task_envelope_state_accepted_at;`,
		`DROP INDEX IF EXISTS uq_agent_task_envelope_idempotency_without_agent;`,
		`DROP INDEX IF EXISTS uq_agent_task_envelope_idempotency_with_agent;`,
	}
	for _, stmt := range stmts {
		if _, err := tx.Exec(stmt); err != nil {
			return err
		}
	}
	return nil
}
