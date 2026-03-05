package backend_postgres_migrations

import (
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigration(upAgentChannelBindings, downAgentChannelBindings)
}

func upAgentChannelBindings(tx *sql.Tx) error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS agent_channel_binding (
			id          BIGSERIAL PRIMARY KEY,
			workspace_id BIGINT NOT NULL,
			agent_id    TEXT NOT NULL,
			channel_type TEXT NOT NULL,
			address     TEXT NOT NULL,
			config_json JSONB DEFAULT '{}',
			active      BOOLEAN DEFAULT true,
			created_at  TIMESTAMPTZ DEFAULT now(),
			updated_at  TIMESTAMPTZ DEFAULT now(),
			UNIQUE(channel_type, address)
		);`,
		`CREATE INDEX IF NOT EXISTS idx_agent_channel_binding_agent
		 ON agent_channel_binding(workspace_id, agent_id);`,
	}
	for _, stmt := range stmts {
		if _, err := tx.Exec(stmt); err != nil {
			return err
		}
	}
	return nil
}

func downAgentChannelBindings(tx *sql.Tx) error {
	stmts := []string{
		`DROP INDEX IF EXISTS idx_agent_channel_binding_agent;`,
		`DROP TABLE IF EXISTS agent_channel_binding;`,
	}
	for _, stmt := range stmts {
		if _, err := tx.Exec(stmt); err != nil {
			return err
		}
	}
	return nil
}
