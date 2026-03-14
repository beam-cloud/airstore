package backend_postgres_migrations

import (
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigration(upViews, downViews)
}

func upViews(tx *sql.Tx) error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS workspace_view (
			id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
			workspace_id INT NOT NULL REFERENCES workspace(id) ON DELETE CASCADE,
			name TEXT NOT NULL,
			description TEXT,
			definition_json JSONB NOT NULL DEFAULT '{}'::jsonb,
			created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
		)`,
		`CREATE INDEX IF NOT EXISTS idx_workspace_view_workspace
		 ON workspace_view (workspace_id)`,
	}

	for _, stmt := range stmts {
		if _, err := tx.Exec(stmt); err != nil {
			return err
		}
	}
	return nil
}

func downViews(tx *sql.Tx) error {
	stmts := []string{
		`DROP INDEX IF EXISTS idx_workspace_view_workspace`,
		`DROP TABLE IF EXISTS workspace_view`,
	}
	for _, stmt := range stmts {
		if _, err := tx.Exec(stmt); err != nil {
			return err
		}
	}
	return nil
}
