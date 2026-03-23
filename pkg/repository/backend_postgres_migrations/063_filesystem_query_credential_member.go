package backend_postgres_migrations

import (
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigration(upFilesystemQueryCredentialMember, downFilesystemQueryCredentialMember)
}

func upFilesystemQueryCredentialMember(tx *sql.Tx) error {
	if _, err := tx.Exec(`
		ALTER TABLE filesystem_queries
		ADD COLUMN IF NOT EXISTS credential_member_id INTEGER REFERENCES workspace_member(id) ON DELETE SET NULL
	`); err != nil {
		return err
	}
	if _, err := tx.Exec(`
		UPDATE filesystem_queries
		SET credential_member_id = NULLIF(BTRIM(query_spec ->> 'credential_member_id'), '')::INTEGER
		WHERE credential_member_id IS NULL
		  AND query_spec IS NOT NULL
		  AND jsonb_typeof(query_spec) = 'object'
		  AND (query_spec ? 'credential_member_id')
		  AND NULLIF(BTRIM(query_spec ->> 'credential_member_id'), '') IS NOT NULL
		  AND BTRIM(query_spec ->> 'credential_member_id') ~ '^[0-9]+$'
	`); err != nil {
		return err
	}
	if _, err := tx.Exec(`
		CREATE INDEX IF NOT EXISTS idx_filesystem_queries_credential_member
		ON filesystem_queries (workspace_id, credential_member_id)
		WHERE credential_member_id IS NOT NULL
	`); err != nil {
		return err
	}
	return nil
}

func downFilesystemQueryCredentialMember(tx *sql.Tx) error {
	if _, err := tx.Exec(`DROP INDEX IF EXISTS idx_filesystem_queries_credential_member`); err != nil {
		return err
	}
	if _, err := tx.Exec(`ALTER TABLE filesystem_queries DROP COLUMN IF EXISTS credential_member_id`); err != nil {
		return err
	}
	return nil
}
