package backend_postgres_migrations

import (
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigration(upWorkspaceTool, downWorkspaceTool)
}

func upWorkspaceTool(tx *sql.Tx) error {
	stmts := []string{
		// Workspace tool provider table
		// Stores per-workspace MCP servers and other tool providers
		`CREATE TABLE workspace_tool (
			id SERIAL PRIMARY KEY,
			external_id UUID DEFAULT uuid_generate_v4() UNIQUE NOT NULL,
			workspace_id INT NOT NULL REFERENCES workspace(id) ON DELETE CASCADE,
			name VARCHAR(255) NOT NULL,
			provider_type VARCHAR(50) NOT NULL DEFAULT 'mcp',
			config JSONB NOT NULL,
			manifest JSONB,
			created_by_member_id INT REFERENCES workspace_member(id) ON DELETE SET NULL,
			created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
			UNIQUE(workspace_id, name)
		)`,

		// Indexes
		`CREATE INDEX idx_workspace_tool_workspace ON workspace_tool(workspace_id)`,
		`CREATE INDEX idx_workspace_tool_name ON workspace_tool(name)`,
		`CREATE INDEX idx_workspace_tool_provider_type ON workspace_tool(provider_type)`,
	}

	for _, stmt := range stmts {
		if _, err := tx.Exec(stmt); err != nil {
			return err
		}
	}

	return nil
}

func downWorkspaceTool(tx *sql.Tx) error {
	stmts := []string{
		`DROP TABLE IF EXISTS workspace_tool`,
	}

	for _, stmt := range stmts {
		if _, err := tx.Exec(stmt); err != nil {
			return err
		}
	}

	return nil
}
