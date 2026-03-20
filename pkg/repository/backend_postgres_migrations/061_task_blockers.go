package backend_postgres_migrations

import (
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigration(upTaskBlockers, downTaskBlockers)
}

func upTaskBlockers(tx *sql.Tx) error {
	if _, err := tx.Exec(`
		CREATE TABLE IF NOT EXISTS task_blocker (
			id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
			workspace_id BIGINT NOT NULL REFERENCES workspace(id) ON DELETE CASCADE,
			task_id UUID NOT NULL REFERENCES agent_task(id) ON DELETE CASCADE,
			run_id UUID REFERENCES agent_run(id) ON DELETE SET NULL,
			kind TEXT NOT NULL,
			input_kind TEXT,
			status TEXT NOT NULL,
			wait_group_id TEXT,
			payload_json JSONB NOT NULL DEFAULT '{}'::jsonb,
			resolution_json JSONB NOT NULL DEFAULT '{}'::jsonb,
			output_ids_json JSONB NOT NULL DEFAULT '[]'::jsonb,
			revision INTEGER NOT NULL DEFAULT 1,
			resolved_at TIMESTAMPTZ,
			created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
		)
	`); err != nil {
		return err
	}
	if _, err := tx.Exec(`ALTER TABLE agent_task ADD COLUMN IF NOT EXISTS current_blocker_id UUID REFERENCES task_blocker(id) ON DELETE SET NULL`); err != nil {
		return err
	}
	if _, err := tx.Exec(`CREATE INDEX IF NOT EXISTS idx_task_blocker_task_created ON task_blocker(task_id, created_at DESC)`); err != nil {
		return err
	}
	if _, err := tx.Exec(`CREATE INDEX IF NOT EXISTS idx_task_blocker_open_task ON task_blocker(task_id) WHERE status = 'open'`); err != nil {
		return err
	}
	if _, err := tx.Exec(`CREATE INDEX IF NOT EXISTS idx_agent_task_current_blocker ON agent_task(current_blocker_id)`); err != nil {
		return err
	}
	return nil
}

func downTaskBlockers(tx *sql.Tx) error {
	if _, err := tx.Exec(`ALTER TABLE agent_task DROP COLUMN IF EXISTS current_blocker_id`); err != nil {
		return err
	}
	if _, err := tx.Exec(`DROP TABLE IF EXISTS task_blocker`); err != nil {
		return err
	}
	return nil
}
