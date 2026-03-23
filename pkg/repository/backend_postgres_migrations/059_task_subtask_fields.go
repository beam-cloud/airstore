package backend_postgres_migrations

import (
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigrationNoTx(upTaskSubtaskFields, downTaskSubtaskFields)
}

func upTaskSubtaskFields(db *sql.DB) error {
	stmts := []string{
		`ALTER TYPE agent_task_state ADD VALUE IF NOT EXISTS 'error'`,
		`CREATE INDEX IF NOT EXISTS idx_agent_task_parent_active
			ON agent_task (parent_envelope_id)
			WHERE parent_envelope_id IS NOT NULL
			AND state IN ('queued', 'running', 'waiting', 'sleeping', 'idle')`,
		`CREATE TABLE IF NOT EXISTS task_spawn_binding (
			task_id UUID PRIMARY KEY REFERENCES agent_task(id) ON DELETE CASCADE,
			source_output_id TEXT NOT NULL,
			entity_label TEXT,
			created_at TIMESTAMPTZ DEFAULT NOW()
		)`,
		`CREATE INDEX IF NOT EXISTS idx_task_spawn_binding_output
			ON task_spawn_binding (source_output_id)`,
	}
	for _, stmt := range stmts {
		if _, err := db.Exec(stmt); err != nil {
			return err
		}
	}
	return nil
}

func downTaskSubtaskFields(db *sql.DB) error {
	stmts := []string{
		`DROP TABLE IF EXISTS task_spawn_binding`,
		`DROP INDEX IF EXISTS idx_agent_task_parent_active`,
	}
	for _, stmt := range stmts {
		if _, err := db.Exec(stmt); err != nil {
			return err
		}
	}
	return nil
}
