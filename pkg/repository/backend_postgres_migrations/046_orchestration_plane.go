package backend_postgres_migrations

import (
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigration(upOrchestrationPlane, downOrchestrationPlane)
}

func upOrchestrationPlane(tx *sql.Tx) error {
	stmts := []string{
		`DO $$ BEGIN
		   IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'objective_status') THEN
		     CREATE TYPE objective_status AS ENUM ('draft', 'active', 'at_risk', 'completed', 'archived');
		   END IF;
		 END $$;`,
		`DO $$ BEGIN
		   IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'task_review_status') THEN
		     CREATE TYPE task_review_status AS ENUM ('not_required', 'pending', 'approved', 'rejected');
		   END IF;
		 END $$;`,
		`DO $$ BEGIN
		   IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'task_approval_status') THEN
		     CREATE TYPE task_approval_status AS ENUM ('pending', 'approved', 'rejected', 'cancelled');
		   END IF;
		 END $$;`,
		`ALTER TYPE agent_task_state ADD VALUE IF NOT EXISTS 'review' AFTER 'waiting';`,
		`CREATE TABLE IF NOT EXISTS objective (
		   id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
		   workspace_id INTEGER NOT NULL REFERENCES workspace(id) ON DELETE CASCADE,
		   title TEXT NOT NULL,
		   description TEXT NOT NULL DEFAULT '',
		   owner TEXT NOT NULL DEFAULT '',
		   priority TEXT NOT NULL DEFAULT 'normal',
		   status objective_status NOT NULL DEFAULT 'draft',
		   target_metric TEXT NOT NULL DEFAULT '',
		   target_value NUMERIC(12,2),
		   current_value NUMERIC(12,2),
		   progress_pct NUMERIC(5,2) NOT NULL DEFAULT 0,
		   deadline TIMESTAMPTZ,
		   budget_usd NUMERIC(12,2),
		   cost_usd NUMERIC(12,2) NOT NULL DEFAULT 0,
		   metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
		   created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
		   updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
		 );`,
		`ALTER TABLE agent_profile
		   ADD COLUMN IF NOT EXISTS role TEXT NOT NULL DEFAULT 'generalist';`,
		`ALTER TABLE agent_profile
		   ADD COLUMN IF NOT EXISTS memory_scope TEXT NOT NULL DEFAULT 'workspace';`,
		`ALTER TABLE agent_profile
		   ADD COLUMN IF NOT EXISTS quality_score NUMERIC(5,2);`,
		`ALTER TABLE agent_profile
		   ADD COLUMN IF NOT EXISTS cost_budget_usd NUMERIC(12,2);`,
		`ALTER TABLE agent_task
		   ADD COLUMN IF NOT EXISTS objective_id UUID REFERENCES objective(id) ON DELETE SET NULL;`,
		`ALTER TABLE agent_task
		   ADD COLUMN IF NOT EXISTS contract_kind TEXT NOT NULL DEFAULT 'standard';`,
		`ALTER TABLE agent_task
		   ADD COLUMN IF NOT EXISTS review_status task_review_status NOT NULL DEFAULT 'not_required';`,
		`ALTER TABLE agent_task
		   ADD COLUMN IF NOT EXISTS requires_review BOOLEAN NOT NULL DEFAULT FALSE;`,
		`ALTER TABLE agent_task
		   ADD COLUMN IF NOT EXISTS contract_json JSONB NOT NULL DEFAULT '{}'::jsonb;`,
		`ALTER TABLE agent_task
		   ADD COLUMN IF NOT EXISTS deadline TIMESTAMPTZ;`,
		`ALTER TABLE agent_task
		   ADD COLUMN IF NOT EXISTS budget_usd NUMERIC(12,2);`,
		`ALTER TABLE agent_task
		   ADD COLUMN IF NOT EXISTS cost_usd NUMERIC(12,2) NOT NULL DEFAULT 0;`,
		`ALTER TABLE agent_run
		   ADD COLUMN IF NOT EXISTS cost_usd NUMERIC(12,2) NOT NULL DEFAULT 0;`,
		`CREATE TABLE IF NOT EXISTS task_approval (
		   id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
		   workspace_id INTEGER NOT NULL REFERENCES workspace(id) ON DELETE CASCADE,
		   task_id UUID NOT NULL REFERENCES agent_task(id) ON DELETE CASCADE,
		   run_id UUID REFERENCES agent_run(id) ON DELETE SET NULL,
		   output_id UUID REFERENCES task_output(id) ON DELETE SET NULL,
		   kind TEXT NOT NULL,
		   status task_approval_status NOT NULL DEFAULT 'pending',
		   title TEXT NOT NULL,
		   summary TEXT NOT NULL DEFAULT '',
		   requested_action TEXT NOT NULL DEFAULT '',
		   requested_by TEXT NOT NULL DEFAULT 'system',
		   requested_by_type TEXT NOT NULL DEFAULT 'system',
		   resolved_by TEXT,
		   resolution_note TEXT,
		   metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
		   resolved_at TIMESTAMPTZ,
		   created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
		   updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
		 );`,
		`CREATE INDEX IF NOT EXISTS idx_objective_workspace_status_created
		   ON objective(workspace_id, status, created_at DESC);`,
		`CREATE INDEX IF NOT EXISTS idx_agent_task_objective
		   ON agent_task(objective_id);`,
		`CREATE INDEX IF NOT EXISTS idx_agent_task_review_status
		   ON agent_task(review_status, state, created_at DESC);`,
		`CREATE INDEX IF NOT EXISTS idx_agent_run_origin_task_created
		   ON agent_run(origin_task_id, created_at DESC);`,
		`CREATE INDEX IF NOT EXISTS idx_task_approval_workspace_status_created
		   ON task_approval(workspace_id, status, created_at DESC);`,
		`CREATE INDEX IF NOT EXISTS idx_task_approval_task_created
		   ON task_approval(task_id, created_at DESC);`,
	}

	for _, stmt := range stmts {
		if _, err := tx.Exec(stmt); err != nil {
			return err
		}
	}
	return nil
}

func downOrchestrationPlane(tx *sql.Tx) error {
	stmts := []string{
		`DROP INDEX IF EXISTS idx_task_approval_task_created;`,
		`DROP INDEX IF EXISTS idx_task_approval_workspace_status_created;`,
		`DROP INDEX IF EXISTS idx_agent_run_origin_task_created;`,
		`DROP INDEX IF EXISTS idx_agent_task_review_status;`,
		`DROP INDEX IF EXISTS idx_agent_task_objective;`,
		`DROP INDEX IF EXISTS idx_objective_workspace_status_created;`,
		`DROP TABLE IF EXISTS task_approval;`,
		`ALTER TABLE agent_run DROP COLUMN IF EXISTS cost_usd;`,
		`ALTER TABLE agent_task DROP COLUMN IF EXISTS cost_usd;`,
		`ALTER TABLE agent_task DROP COLUMN IF EXISTS budget_usd;`,
		`ALTER TABLE agent_task DROP COLUMN IF EXISTS deadline;`,
		`ALTER TABLE agent_task DROP COLUMN IF EXISTS contract_json;`,
		`ALTER TABLE agent_task DROP COLUMN IF EXISTS requires_review;`,
		`ALTER TABLE agent_task DROP COLUMN IF EXISTS review_status;`,
		`ALTER TABLE agent_task DROP COLUMN IF EXISTS contract_kind;`,
		`ALTER TABLE agent_task DROP COLUMN IF EXISTS objective_id;`,
		`ALTER TABLE agent_profile DROP COLUMN IF EXISTS cost_budget_usd;`,
		`ALTER TABLE agent_profile DROP COLUMN IF EXISTS quality_score;`,
		`ALTER TABLE agent_profile DROP COLUMN IF EXISTS memory_scope;`,
		`ALTER TABLE agent_profile DROP COLUMN IF EXISTS role;`,
		`DROP TABLE IF EXISTS objective;`,
		`DROP TYPE IF EXISTS task_approval_status;`,
		`DROP TYPE IF EXISTS task_review_status;`,
		`DROP TYPE IF EXISTS objective_status;`,
	}

	for _, stmt := range stmts {
		if _, err := tx.Exec(stmt); err != nil {
			return err
		}
	}
	return nil
}
