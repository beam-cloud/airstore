package backend_postgres_migrations

import (
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigration(upWorkspaceBYOK, downWorkspaceBYOK)
}

func upWorkspaceBYOK(tx *sql.Tx) error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS workspace_secrets (
			id         SERIAL PRIMARY KEY,
			workspace_id INTEGER NOT NULL REFERENCES workspace(id) ON DELETE CASCADE,
			key        TEXT NOT NULL,
			value_encoded BYTEA NOT NULL,
			created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
			UNIQUE (workspace_id, key)
		);`,
	}

	for _, stmt := range stmts {
		if _, err := tx.Exec(stmt); err != nil {
			return err
		}
	}
	return nil
}

func downWorkspaceBYOK(tx *sql.Tx) error {
	stmts := []string{
		`DROP TABLE IF EXISTS workspace_secrets;`,
	}

	for _, stmt := range stmts {
		if _, err := tx.Exec(stmt); err != nil {
			return err
		}
	}
	return nil
}
