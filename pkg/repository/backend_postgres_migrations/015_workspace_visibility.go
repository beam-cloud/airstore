package backend_postgres_migrations

import (
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigration(upWorkspaceVisibility, downWorkspaceVisibility)
}

func upWorkspaceVisibility(tx *sql.Tx) error {
	stmts := []string{
		`ALTER TABLE workspace ADD COLUMN IF NOT EXISTS visibility VARCHAR(20) NOT NULL DEFAULT 'private'`,
		`ALTER TABLE workspace ADD COLUMN IF NOT EXISTS slug VARCHAR(255) UNIQUE`,
		`CREATE INDEX IF NOT EXISTS idx_workspace_slug ON workspace(slug) WHERE slug IS NOT NULL`,
		`CREATE INDEX IF NOT EXISTS idx_workspace_visibility ON workspace(visibility) WHERE visibility = 'public'`,
	}

	for _, stmt := range stmts {
		if _, err := tx.Exec(stmt); err != nil {
			return err
		}
	}
	return nil
}

func downWorkspaceVisibility(tx *sql.Tx) error {
	stmts := []string{
		`DROP INDEX IF EXISTS idx_workspace_visibility`,
		`DROP INDEX IF EXISTS idx_workspace_slug`,
		`ALTER TABLE workspace DROP COLUMN IF EXISTS slug`,
		`ALTER TABLE workspace DROP COLUMN IF EXISTS visibility`,
	}

	for _, stmt := range stmts {
		if _, err := tx.Exec(stmt); err != nil {
			return err
		}
	}
	return nil
}
