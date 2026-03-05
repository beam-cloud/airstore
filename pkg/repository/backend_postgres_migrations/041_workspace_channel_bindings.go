package backend_postgres_migrations

import (
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigration(upWorkspaceChannelBindings, downWorkspaceChannelBindings)
}

func upWorkspaceChannelBindings(tx *sql.Tx) error {
	stmts := []string{
		`ALTER TABLE agent_channel_binding ALTER COLUMN agent_id DROP NOT NULL;`,
		`CREATE INDEX IF NOT EXISTS idx_channel_binding_workspace
		 ON agent_channel_binding(workspace_id) WHERE agent_id IS NULL;`,
	}
	for _, stmt := range stmts {
		if _, err := tx.Exec(stmt); err != nil {
			return err
		}
	}
	return nil
}

func downWorkspaceChannelBindings(tx *sql.Tx) error {
	stmts := []string{
		`DROP INDEX IF EXISTS idx_channel_binding_workspace;`,
		`DELETE FROM agent_channel_binding WHERE agent_id IS NULL;`,
		`ALTER TABLE agent_channel_binding ALTER COLUMN agent_id SET NOT NULL;`,
	}
	for _, stmt := range stmts {
		if _, err := tx.Exec(stmt); err != nil {
			return err
		}
	}
	return nil
}
