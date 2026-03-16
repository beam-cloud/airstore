package backend_postgres_migrations

import (
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigration(upViewSourceDraftID, downViewSourceDraftID)
}

func upViewSourceDraftID(tx *sql.Tx) error {
	stmts := []string{
		`ALTER TABLE workspace_view
			ADD COLUMN IF NOT EXISTS source_draft_id TEXT`,
		`CREATE UNIQUE INDEX IF NOT EXISTS idx_workspace_view_source_draft
			ON workspace_view (source_draft_id)
			WHERE source_draft_id IS NOT NULL`,
	}
	for _, stmt := range stmts {
		if _, err := tx.Exec(stmt); err != nil {
			return err
		}
	}
	return nil
}

func downViewSourceDraftID(tx *sql.Tx) error {
	stmts := []string{
		`DROP INDEX IF EXISTS idx_workspace_view_source_draft`,
		`ALTER TABLE workspace_view
			DROP COLUMN IF EXISTS source_draft_id`,
	}
	for _, stmt := range stmts {
		if _, err := tx.Exec(stmt); err != nil {
			return err
		}
	}
	return nil
}
