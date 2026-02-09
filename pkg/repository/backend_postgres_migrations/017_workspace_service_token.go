package backend_postgres_migrations

import (
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigration(upWorkspaceServiceToken, downWorkspaceServiceToken)
}

func upWorkspaceServiceToken(tx *sql.Tx) error {
	_, err := tx.Exec(`ALTER TYPE token_type ADD VALUE IF NOT EXISTS 'workspace_service'`)
	return err
}

func downWorkspaceServiceToken(tx *sql.Tx) error {
	// Enum values cannot be removed in Postgres; no-op.
	return nil
}
