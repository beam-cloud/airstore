package backend_postgres_migrations

import (
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigration(upRemoveReviewObjectivesContract, downRemoveReviewObjectivesContract)
}

func upRemoveReviewObjectivesContract(tx *sql.Tx) error {
	stmts := []string{
		// Drop task_approval table
		`DROP TABLE IF EXISTS task_approval`,

		// Drop review/objective/contract columns from agent_task
		`ALTER TABLE agent_task DROP COLUMN IF EXISTS review_status`,
		`ALTER TABLE agent_task DROP COLUMN IF EXISTS requires_review`,
		`ALTER TABLE agent_task DROP COLUMN IF EXISTS contract_json`,
		`ALTER TABLE agent_task DROP COLUMN IF EXISTS contract_kind`,
		`ALTER TABLE agent_task DROP COLUMN IF EXISTS objective_id`,

		// Drop objective table
		`DROP TABLE IF EXISTS objective`,

		// Drop enums
		`DROP TYPE IF EXISTS task_review_status`,
		`DROP TYPE IF EXISTS objective_status`,

		// Drop indexes that reference removed columns
		`DROP INDEX IF EXISTS idx_agent_task_review_status`,
		`DROP INDEX IF EXISTS idx_task_approval_workspace_status_created`,
		`DROP INDEX IF EXISTS idx_task_approval_task_created`,
		`DROP INDEX IF EXISTS idx_objective_workspace_status_created`,
		`DROP INDEX IF EXISTS idx_agent_task_objective`,
	}
	for _, stmt := range stmts {
		if _, err := tx.Exec(stmt); err != nil {
			return err
		}
	}
	return nil
}

func downRemoveReviewObjectivesContract(tx *sql.Tx) error {
	stmts := []string{
		// Re-create enums
		`DO $$ BEGIN CREATE TYPE task_review_status AS ENUM ('not_required','pending','approved','rejected'); EXCEPTION WHEN duplicate_object THEN NULL; END $$`,
		`DO $$ BEGIN CREATE TYPE objective_status AS ENUM ('draft','active','at_risk','completed','archived'); EXCEPTION WHEN duplicate_object THEN NULL; END $$`,

		// Re-create objective table
		`CREATE TABLE IF NOT EXISTS objective (
			id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
			workspace_id BIGINT NOT NULL REFERENCES workspace(id),
			title TEXT NOT NULL,
			description TEXT NOT NULL DEFAULT '',
			owner TEXT NOT NULL DEFAULT '',
			priority TEXT NOT NULL DEFAULT 'normal',
			status objective_status NOT NULL DEFAULT 'draft',
			target_metric TEXT NOT NULL DEFAULT '',
			target_value DOUBLE PRECISION,
			current_value DOUBLE PRECISION,
			progress_pct DOUBLE PRECISION NOT NULL DEFAULT 0,
			deadline TIMESTAMPTZ,
			budget_usd DOUBLE PRECISION,
			cost_usd DOUBLE PRECISION NOT NULL DEFAULT 0,
			metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
			created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
		)`,

		// Re-add columns to agent_task
		`ALTER TABLE agent_task ADD COLUMN IF NOT EXISTS objective_id UUID REFERENCES objective(id) ON DELETE SET NULL`,
		`ALTER TABLE agent_task ADD COLUMN IF NOT EXISTS contract_kind TEXT NOT NULL DEFAULT 'standard'`,
		`ALTER TABLE agent_task ADD COLUMN IF NOT EXISTS review_status task_review_status NOT NULL DEFAULT 'not_required'`,
		`ALTER TABLE agent_task ADD COLUMN IF NOT EXISTS requires_review BOOLEAN NOT NULL DEFAULT FALSE`,
		`ALTER TABLE agent_task ADD COLUMN IF NOT EXISTS contract_json JSONB NOT NULL DEFAULT '{}'::jsonb`,

		// Re-create task_approval table
		`CREATE TABLE IF NOT EXISTS task_approval (
			id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
			workspace_id BIGINT NOT NULL REFERENCES workspace(id),
			task_id UUID NOT NULL REFERENCES agent_task(id) ON DELETE CASCADE,
			run_id UUID,
			output_id UUID,
			kind TEXT NOT NULL DEFAULT 'review',
			status TEXT NOT NULL DEFAULT 'pending',
			title TEXT NOT NULL DEFAULT '',
			summary TEXT NOT NULL DEFAULT '',
			requested_action TEXT NOT NULL DEFAULT '',
			requested_by TEXT NOT NULL DEFAULT '',
			requested_by_type TEXT NOT NULL DEFAULT '',
			resolved_by TEXT,
			resolution_note TEXT,
			metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
			resolved_at TIMESTAMPTZ,
			created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
		)`,

		// Re-create indexes
		`CREATE INDEX IF NOT EXISTS idx_agent_task_review_status ON agent_task(review_status)`,
		`CREATE INDEX IF NOT EXISTS idx_task_approval_workspace_status_created ON task_approval(workspace_id, status, created_at DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_task_approval_task_created ON task_approval(task_id, created_at DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_objective_workspace_status_created ON objective(workspace_id, status, created_at DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_agent_task_objective ON agent_task(objective_id)`,
	}
	for _, stmt := range stmts {
		if _, err := tx.Exec(stmt); err != nil {
			return err
		}
	}
	return nil
}
