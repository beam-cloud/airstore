package backend_postgres_migrations

import (
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigration(upTaskOutputs, downTaskOutputs)
}

func upTaskOutputs(tx *sql.Tx) error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS task_output (
		   id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
		   workspace_id INTEGER NOT NULL REFERENCES workspace(id) ON DELETE CASCADE,
		   task_id UUID NOT NULL REFERENCES agent_task(id) ON DELETE CASCADE,
		   run_id UUID NULL REFERENCES agent_run(id) ON DELETE SET NULL,
		   agent_id UUID NULL REFERENCES agent_profile(id) ON DELETE SET NULL,
		   output_type TEXT NOT NULL,
		   title TEXT NOT NULL,
		   summary TEXT,
		   data_json JSONB NOT NULL DEFAULT '{}'::jsonb,
		   metadata_json JSONB,
		   created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
		 );`,
		`CREATE INDEX IF NOT EXISTS idx_task_output_task ON task_output(task_id);`,
		`CREATE INDEX IF NOT EXISTS idx_task_output_workspace ON task_output(workspace_id);`,
	}
	for _, stmt := range stmts {
		if _, err := tx.Exec(stmt); err != nil {
			return err
		}
	}
	return nil
}

func downTaskOutputs(tx *sql.Tx) error {
	_, err := tx.Exec(`DROP TABLE IF EXISTS task_output;`)
	return err
}
