package backend_postgres_migrations

import (
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigration(upCredits, downCredits)
}

func upCredits(tx *sql.Tx) error {
	stmts := []string{
		// Credit account: one per workspace, holds current balance.
		`CREATE TABLE IF NOT EXISTS credit_account (
		   id SERIAL PRIMARY KEY,
		   external_id UUID NOT NULL DEFAULT uuid_generate_v4() UNIQUE,
		   workspace_id INT NOT NULL REFERENCES workspace(id) ON DELETE CASCADE UNIQUE,
		   balance BIGINT NOT NULL DEFAULT 0,
		   created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
		   updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
		 );`,

		// Credit ledger: immutable append-only transaction log.
		`CREATE TABLE IF NOT EXISTS credit_ledger (
		   id SERIAL PRIMARY KEY,
		   external_id UUID NOT NULL DEFAULT uuid_generate_v4() UNIQUE,
		   account_id INT NOT NULL REFERENCES credit_account(id) ON DELETE CASCADE,
		   workspace_id INT NOT NULL REFERENCES workspace(id) ON DELETE CASCADE,
		   type TEXT NOT NULL CHECK (type IN ('grant', 'usage', 'adjustment', 'expiry')),
		   amount BIGINT NOT NULL,
		   balance_after BIGINT NOT NULL,
		   description TEXT NOT NULL DEFAULT '',
		   reference_id TEXT NULL,
		   reference_type TEXT NULL,
		   created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
		 );`,

		// Index for fast ledger lookups by workspace.
		`CREATE INDEX IF NOT EXISTS idx_credit_ledger_workspace_id
		   ON credit_ledger (workspace_id, created_at DESC);`,

		// Index for reference lookups (e.g. find the charge for a specific run).
		`CREATE INDEX IF NOT EXISTS idx_credit_ledger_reference
		   ON credit_ledger (reference_type, reference_id)
		   WHERE reference_id IS NOT NULL;`,
	}

	for _, stmt := range stmts {
		if _, err := tx.Exec(stmt); err != nil {
			return err
		}
	}
	return nil
}

func downCredits(tx *sql.Tx) error {
	stmts := []string{
		`DROP TABLE IF EXISTS credit_ledger;`,
		`DROP TABLE IF EXISTS credit_account;`,
	}
	for _, stmt := range stmts {
		if _, err := tx.Exec(stmt); err != nil {
			return err
		}
	}
	return nil
}
