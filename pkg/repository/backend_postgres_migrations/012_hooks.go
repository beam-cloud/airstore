package backend_postgres_migrations

import (
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigration(upHooks, downHooks)
}

func upHooks(tx *sql.Tx) error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS filesystem_hooks (
			id SERIAL PRIMARY KEY,
			external_id UUID NOT NULL DEFAULT gen_random_uuid() UNIQUE,
			workspace_id INTEGER NOT NULL REFERENCES workspace(id) ON DELETE CASCADE,
			path TEXT NOT NULL,
			prompt TEXT NOT NULL DEFAULT '',
			active BOOLEAN NOT NULL DEFAULT true,
			created_by_member_id INTEGER REFERENCES workspace_member(id) ON DELETE SET NULL,
			token_id INTEGER REFERENCES token(id) ON DELETE SET NULL,
			encrypted_token BYTEA NOT NULL DEFAULT '',
			created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
		)`,

		`CREATE INDEX IF NOT EXISTS idx_hooks_workspace ON filesystem_hooks(workspace_id)`,
		`CREATE INDEX IF NOT EXISTS idx_hooks_workspace_active ON filesystem_hooks(workspace_id) WHERE active = true`,
		`CREATE INDEX IF NOT EXISTS idx_hooks_external_id ON filesystem_hooks(external_id)`,
		`CREATE UNIQUE INDEX IF NOT EXISTS idx_hooks_workspace_path ON filesystem_hooks(workspace_id, path)`,
	}

	for _, stmt := range stmts {
		if _, err := tx.Exec(stmt); err != nil {
			return err
		}
	}

	return nil
}

func downHooks(tx *sql.Tx) error {
	_, err := tx.Exec(`DROP TABLE IF EXISTS filesystem_hooks`)
	return err
}
