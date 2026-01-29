package backend_postgres_migrations

import (
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigration(upRenameFilesystemQueries, downRenameFilesystemQueries)
}

func upRenameFilesystemQueries(tx *sql.Tx) error {
	stmts := []string{
		// Rename table
		`ALTER TABLE smart_queries RENAME TO filesystem_queries`,

		// Rename indexes
		`ALTER INDEX idx_smart_queries_workspace RENAME TO idx_filesystem_queries_workspace`,
		`ALTER INDEX idx_smart_queries_integration RENAME TO idx_filesystem_queries_integration`,
		`ALTER INDEX idx_smart_queries_path RENAME TO idx_filesystem_queries_path`,
	}

	for _, stmt := range stmts {
		if _, err := tx.Exec(stmt); err != nil {
			return err
		}
	}

	return nil
}

func downRenameFilesystemQueries(tx *sql.Tx) error {
	stmts := []string{
		// Revert rename
		`ALTER TABLE filesystem_queries RENAME TO smart_queries`,

		// Revert index names
		`ALTER INDEX idx_filesystem_queries_workspace RENAME TO idx_smart_queries_workspace`,
		`ALTER INDEX idx_filesystem_queries_integration RENAME TO idx_smart_queries_integration`,
		`ALTER INDEX idx_filesystem_queries_path RENAME TO idx_smart_queries_path`,
	}

	for _, stmt := range stmts {
		if _, err := tx.Exec(stmt); err != nil {
			return err
		}
	}

	return nil
}
