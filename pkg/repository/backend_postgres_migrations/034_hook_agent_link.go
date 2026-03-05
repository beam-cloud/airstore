package backend_postgres_migrations

import (
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigration(upHookAgentLink, downHookAgentLink)
}

func upHookAgentLink(tx *sql.Tx) error {
	stmts := []string{
		`ALTER TABLE filesystem_hooks
			ADD COLUMN IF NOT EXISTS agent_id UUID REFERENCES agent_profile(id) ON DELETE SET NULL`,
		`ALTER TABLE filesystem_hooks
			ADD COLUMN IF NOT EXISTS skill_paths TEXT[] NOT NULL DEFAULT '{}'::text[]`,
		`UPDATE filesystem_hooks
			SET skill_paths = ARRAY[skill_path]
			WHERE COALESCE(array_length(skill_paths, 1), 0) = 0
			  AND btrim(COALESCE(skill_path, '')) <> ''`,
		`CREATE INDEX IF NOT EXISTS idx_filesystem_hooks_agent_id
			ON filesystem_hooks(agent_id)
			WHERE agent_id IS NOT NULL`,
	}

	for _, stmt := range stmts {
		if _, err := tx.Exec(stmt); err != nil {
			return err
		}
	}
	return nil
}

func downHookAgentLink(tx *sql.Tx) error {
	stmts := []string{
		`DROP INDEX IF EXISTS idx_filesystem_hooks_agent_id`,
		`ALTER TABLE filesystem_hooks DROP COLUMN IF EXISTS agent_id`,
		`ALTER TABLE filesystem_hooks DROP COLUMN IF EXISTS skill_paths`,
	}

	for _, stmt := range stmts {
		if _, err := tx.Exec(stmt); err != nil {
			return err
		}
	}
	return nil
}
