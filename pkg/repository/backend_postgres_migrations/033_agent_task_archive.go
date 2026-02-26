package backend_postgres_migrations

import (
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigration(upAgentTaskArchive, downAgentTaskArchive)
}

func upAgentTaskArchive(tx *sql.Tx) error {
	stmts := []string{
		`ALTER TABLE agent_task
		   ADD COLUMN IF NOT EXISTS archived_at TIMESTAMPTZ;`,
		`CREATE INDEX IF NOT EXISTS idx_agent_task_workspace_unarchived_created
		   ON agent_task (workspace_id, created_at DESC, id DESC)
		 WHERE archived_at IS NULL;`,
		`CREATE INDEX IF NOT EXISTS idx_agent_task_archived_at
		   ON agent_task (archived_at)
		 WHERE archived_at IS NOT NULL;`,
	}
	for _, stmt := range stmts {
		if _, err := tx.Exec(stmt); err != nil {
			return err
		}
	}
	return nil
}

func downAgentTaskArchive(tx *sql.Tx) error {
	stmts := []string{
		`DROP INDEX IF EXISTS idx_agent_task_archived_at;`,
		`DROP INDEX IF EXISTS idx_agent_task_workspace_unarchived_created;`,
		`ALTER TABLE agent_task
		   DROP COLUMN IF EXISTS archived_at;`,
	}
	for _, stmt := range stmts {
		if _, err := tx.Exec(stmt); err != nil {
			return err
		}
	}
	return nil
}
