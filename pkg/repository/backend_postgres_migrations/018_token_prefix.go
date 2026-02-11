package backend_postgres_migrations

import (
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigration(upTokenPrefix, downTokenPrefix)
}

func upTokenPrefix(tx *sql.Tx) error {
	stmts := []string{
		// Add token_prefix column for fast lookup (first 16 hex chars of raw token)
		`ALTER TABLE token ADD COLUMN IF NOT EXISTS token_prefix VARCHAR(16)`,

		// Create index for fast prefix-based lookups
		`CREATE INDEX IF NOT EXISTS idx_token_prefix ON token(token_prefix) WHERE token_prefix IS NOT NULL`,
	}

	for _, stmt := range stmts {
		if _, err := tx.Exec(stmt); err != nil {
			return err
		}
	}

	return nil
}

func downTokenPrefix(tx *sql.Tx) error {
	stmts := []string{
		`DROP INDEX IF EXISTS idx_token_prefix`,
		`ALTER TABLE token DROP COLUMN IF EXISTS token_prefix`,
	}

	for _, stmt := range stmts {
		if _, err := tx.Exec(stmt); err != nil {
			return err
		}
	}

	return nil
}
