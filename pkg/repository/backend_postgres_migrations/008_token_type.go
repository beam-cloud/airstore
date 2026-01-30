package backend_postgres_migrations

import (
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigration(upTokenType, downTokenType)
}

func upTokenType(tx *sql.Tx) error {
	stmts := []string{
		// Token type enum
		`CREATE TYPE workspace_token_type AS ENUM ('workspace_member')`,

		// Add token_type column with default
		`ALTER TABLE workspace_token ADD COLUMN token_type workspace_token_type NOT NULL DEFAULT 'workspace_member'`,
	}

	for _, stmt := range stmts {
		if _, err := tx.Exec(stmt); err != nil {
			return err
		}
	}

	return nil
}

func downTokenType(tx *sql.Tx) error {
	stmts := []string{
		`ALTER TABLE workspace_token DROP COLUMN IF EXISTS token_type`,
		`DROP TYPE IF EXISTS workspace_token_type`,
	}

	for _, stmt := range stmts {
		if _, err := tx.Exec(stmt); err != nil {
			return err
		}
	}

	return nil
}
