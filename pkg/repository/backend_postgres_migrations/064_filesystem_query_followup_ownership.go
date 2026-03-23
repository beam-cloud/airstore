package backend_postgres_migrations

import (
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigration(upFilesystemQueryFollowupOwnership, downFilesystemQueryFollowupOwnership)
}

func upFilesystemQueryFollowupOwnership(tx *sql.Tx) error {
	stmts := []string{
		`ALTER TABLE filesystem_queries
		   ADD COLUMN IF NOT EXISTS system_managed BOOLEAN NOT NULL DEFAULT FALSE,
		   ADD COLUMN IF NOT EXISTS lifecycle TEXT NOT NULL DEFAULT 'persistent',
		   ADD COLUMN IF NOT EXISTS owner_task_id UUID REFERENCES agent_task(id) ON DELETE CASCADE,
		   ADD COLUMN IF NOT EXISTS owner_run_id UUID REFERENCES agent_run(id) ON DELETE SET NULL`,
		`UPDATE filesystem_queries
		 SET lifecycle = 'persistent'
		 WHERE NULLIF(BTRIM(lifecycle), '') IS NULL`,
		`UPDATE filesystem_queries AS q
		 SET system_managed = TRUE,
		     lifecycle = 'task_followup',
		     owner_task_id = COALESCE(q.owner_task_id, h.target_task_id),
		     owner_run_id = COALESCE(q.owner_run_id, t.target_run_id)
		 FROM filesystem_hooks AS h
		 LEFT JOIN agent_task AS t
		   ON t.id = h.target_task_id
		 WHERE h.workspace_id = q.workspace_id
		   AND LOWER(h.path) = LOWER(q.path)
		   AND h.system_managed = TRUE
		   AND h.target_task_id IS NOT NULL`,
		`CREATE INDEX IF NOT EXISTS idx_filesystem_queries_owner_task
		 ON filesystem_queries (workspace_id, owner_task_id)
		 WHERE owner_task_id IS NOT NULL`,
		`CREATE INDEX IF NOT EXISTS idx_filesystem_queries_system_managed
		 ON filesystem_queries (workspace_id, system_managed)
		 WHERE system_managed = TRUE`,
	}

	for _, stmt := range stmts {
		if _, err := tx.Exec(stmt); err != nil {
			return err
		}
	}
	return nil
}

func downFilesystemQueryFollowupOwnership(tx *sql.Tx) error {
	stmts := []string{
		`DROP INDEX IF EXISTS idx_filesystem_queries_system_managed`,
		`DROP INDEX IF EXISTS idx_filesystem_queries_owner_task`,
		`ALTER TABLE filesystem_queries
		   DROP COLUMN IF EXISTS owner_run_id,
		   DROP COLUMN IF EXISTS owner_task_id,
		   DROP COLUMN IF EXISTS lifecycle,
		   DROP COLUMN IF EXISTS system_managed`,
	}

	for _, stmt := range stmts {
		if _, err := tx.Exec(stmt); err != nil {
			return err
		}
	}
	return nil
}
