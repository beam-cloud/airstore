package backend_postgres_migrations

import (
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigration(upScheduledTasks, downScheduledTasks)
}

func upScheduledTasks(tx *sql.Tx) error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS scheduled_task (
		   id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
		   external_id UUID NOT NULL UNIQUE DEFAULT uuid_generate_v4(),
		   workspace_id INT NOT NULL REFERENCES workspace(id) ON DELETE CASCADE,
		   agent_id UUID NOT NULL REFERENCES agent_profile(id) ON DELETE CASCADE,
		   cron_expr TEXT NOT NULL,
		   prompt TEXT NOT NULL,
		   skill_paths TEXT[] NOT NULL DEFAULT '{}'::text[],
		   active BOOLEAN NOT NULL DEFAULT TRUE,
		   next_run_at TIMESTAMPTZ NOT NULL,
		   last_run_at TIMESTAMPTZ,
		   token_id INT REFERENCES token(id) ON DELETE SET NULL,
		   encrypted_token BYTEA,
		   created_by_member_id INT REFERENCES workspace_member(id) ON DELETE SET NULL,
		   created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
		   updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
		 );`,
		`CREATE INDEX IF NOT EXISTS idx_scheduled_task_due
		 ON scheduled_task (next_run_at, id)
		 WHERE active = TRUE;`,
		`CREATE INDEX IF NOT EXISTS idx_scheduled_task_workspace
		 ON scheduled_task (workspace_id);`,
		`ALTER TABLE agent_task
		 ADD COLUMN IF NOT EXISTS scheduled_task_id UUID REFERENCES scheduled_task(id) ON DELETE SET NULL;`,
	}

	for _, stmt := range stmts {
		if _, err := tx.Exec(stmt); err != nil {
			return err
		}
	}
	return nil
}

func downScheduledTasks(tx *sql.Tx) error {
	stmts := []string{
		`ALTER TABLE agent_task DROP COLUMN IF EXISTS scheduled_task_id;`,
		`DROP INDEX IF EXISTS idx_scheduled_task_workspace;`,
		`DROP INDEX IF EXISTS idx_scheduled_task_due;`,
		`DROP TABLE IF EXISTS scheduled_task;`,
	}

	for _, stmt := range stmts {
		if _, err := tx.Exec(stmt); err != nil {
			return err
		}
	}
	return nil
}
