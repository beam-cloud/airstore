package backend_postgres_migrations

import (
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigration(upWorkspaceToolSettings, downWorkspaceToolSettings)
}

func upWorkspaceToolSettings(tx *sql.Tx) error {
	stmts := []string{
		// Workspace tool settings table
		// Stores per-workspace tool enabled/disabled state
		// By default, tools are enabled (not in this table)
		// Only disabled tools have entries here
		`CREATE TABLE workspace_tool_setting (
			id SERIAL PRIMARY KEY,
			workspace_id INT NOT NULL REFERENCES workspace(id) ON DELETE CASCADE,
			tool_name VARCHAR(255) NOT NULL,
			enabled BOOLEAN NOT NULL DEFAULT true,
			created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
			UNIQUE(workspace_id, tool_name)
		)`,

		// Indexes
		`CREATE INDEX idx_workspace_tool_setting_workspace ON workspace_tool_setting(workspace_id)`,
		`CREATE INDEX idx_workspace_tool_setting_tool ON workspace_tool_setting(tool_name)`,
	}

	for _, stmt := range stmts {
		if _, err := tx.Exec(stmt); err != nil {
			return err
		}
	}

	return nil
}

func downWorkspaceToolSettings(tx *sql.Tx) error {
	stmts := []string{
		`DROP TABLE IF EXISTS workspace_tool_setting`,
	}

	for _, stmt := range stmts {
		if _, err := tx.Exec(stmt); err != nil {
			return err
		}
	}

	return nil
}
