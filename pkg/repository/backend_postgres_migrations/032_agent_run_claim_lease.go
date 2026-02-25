package backend_postgres_migrations

import (
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigration(upAgentRunClaimLease, downAgentRunClaimLease)
}

func upAgentRunClaimLease(tx *sql.Tx) error {
	stmts := []string{
		`ALTER TABLE agent_run
		   ADD COLUMN IF NOT EXISTS claimed_by_worker_id TEXT,
		   ADD COLUMN IF NOT EXISTS claim_heartbeat_at TIMESTAMPTZ,
		   ADD COLUMN IF NOT EXISTS claim_expires_at TIMESTAMPTZ;`,
		`CREATE INDEX IF NOT EXISTS idx_agent_run_active_claim_expires
		   ON agent_run (status, claim_expires_at)
		 WHERE run_attempt_id IS NOT NULL
		   AND status IN ('accepted'::agent_run_status, 'running'::agent_run_status)
		   AND claim_expires_at IS NOT NULL;`,
		`CREATE INDEX IF NOT EXISTS idx_agent_run_claimed_worker_active
		   ON agent_run (claimed_by_worker_id, status)
		 WHERE run_attempt_id IS NOT NULL
		   AND claimed_by_worker_id IS NOT NULL
		   AND status IN ('accepted'::agent_run_status, 'running'::agent_run_status);`,
		`CREATE INDEX IF NOT EXISTS idx_agent_run_active_unclaimed_updated
		   ON agent_run (status, updated_at)
		 WHERE run_attempt_id IS NOT NULL
		   AND claimed_by_worker_id IS NULL
		   AND status IN ('accepted'::agent_run_status, 'running'::agent_run_status);`,
	}

	for _, stmt := range stmts {
		if _, err := tx.Exec(stmt); err != nil {
			return err
		}
	}
	return nil
}

func downAgentRunClaimLease(tx *sql.Tx) error {
	stmts := []string{
		`DROP INDEX IF EXISTS idx_agent_run_claimed_worker_active;`,
		`DROP INDEX IF EXISTS idx_agent_run_active_claim_expires;`,
		`DROP INDEX IF EXISTS idx_agent_run_active_unclaimed_updated;`,
		`ALTER TABLE agent_run
		   DROP COLUMN IF EXISTS claim_expires_at,
		   DROP COLUMN IF EXISTS claim_heartbeat_at,
		   DROP COLUMN IF EXISTS claimed_by_worker_id;`,
	}

	for _, stmt := range stmts {
		if _, err := tx.Exec(stmt); err != nil {
			return err
		}
	}
	return nil
}
