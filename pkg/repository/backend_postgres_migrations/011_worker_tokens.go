package backend_postgres_migrations

import (
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigration(upWorkerTokens, downWorkerTokens)
}

func upWorkerTokens(tx *sql.Tx) error {
	stmts := []string{
		// Rename table from workspace_token to token
		`ALTER TABLE workspace_token RENAME TO token`,

		// Rename enum type
		`ALTER TYPE workspace_token_type RENAME TO token_type`,

		// Add 'worker' to the token type enum
		`ALTER TYPE token_type ADD VALUE IF NOT EXISTS 'worker'`,

		// Make workspace_id nullable for worker tokens
		`ALTER TABLE token ALTER COLUMN workspace_id DROP NOT NULL`,

		// Make member_id nullable for worker tokens
		`ALTER TABLE token ALTER COLUMN member_id DROP NOT NULL`,

		// Add pool_name column for worker tokens (optional, null = all pools)
		`ALTER TABLE token ADD COLUMN IF NOT EXISTS pool_name VARCHAR(255)`,

		// Rename indexes to match new table name
		`ALTER INDEX IF EXISTS idx_workspace_token_workspace RENAME TO idx_token_workspace`,
		`ALTER INDEX IF EXISTS idx_workspace_token_member RENAME TO idx_token_member`,
		`ALTER INDEX IF EXISTS idx_workspace_token_hash RENAME TO idx_token_hash`,

		// Index for looking up worker tokens by pool
		`CREATE INDEX IF NOT EXISTS idx_token_pool ON token(pool_name) WHERE pool_name IS NOT NULL`,
	}

	for _, stmt := range stmts {
		if _, err := tx.Exec(stmt); err != nil {
			return err
		}
	}

	return nil
}

func downWorkerTokens(tx *sql.Tx) error {
	stmts := []string{
		`DROP INDEX IF EXISTS idx_token_pool`,
		`ALTER TABLE token DROP COLUMN IF EXISTS pool_name`,
		`ALTER INDEX IF EXISTS idx_token_workspace RENAME TO idx_workspace_token_workspace`,
		`ALTER INDEX IF EXISTS idx_token_member RENAME TO idx_workspace_token_member`,
		`ALTER INDEX IF EXISTS idx_token_hash RENAME TO idx_workspace_token_hash`,
		`ALTER TYPE token_type RENAME TO workspace_token_type`,
		`ALTER TABLE token RENAME TO workspace_token`,
	}

	for _, stmt := range stmts {
		if _, err := tx.Exec(stmt); err != nil {
			return err
		}
	}

	return nil
}
