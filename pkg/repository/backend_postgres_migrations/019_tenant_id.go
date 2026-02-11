package backend_postgres_migrations

import (
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigration(upTenantId, downTenantId)
}

func upTenantId(tx *sql.Tx) error {
	// Add 'organization' to the token_type enum so org tokens can be created.
	// NOTE: ALTER TYPE ... ADD VALUE cannot run inside a transaction in Postgres <12.
	// goose runs each migration in a transaction, but ADD VALUE IF NOT EXISTS is
	// safe in Postgres 12+ which we require. If this fails on older Postgres,
	// run this ALTER manually outside a transaction first.
	if _, err := tx.Exec(`ALTER TYPE token_type ADD VALUE IF NOT EXISTS 'organization'`); err != nil {
		return err
	}

	stmts := []string{
		// Add tenant_id column to workspace for tenant scoping
		`ALTER TABLE workspace ADD COLUMN IF NOT EXISTS tenant_id VARCHAR(255)`,

		// Add tenant_id column to token for organization tokens
		`ALTER TABLE token ADD COLUMN IF NOT EXISTS tenant_id VARCHAR(255)`,

		// Partial indexes for fast tenant-scoped lookups
		`CREATE INDEX IF NOT EXISTS idx_workspace_tenant_id ON workspace(tenant_id) WHERE tenant_id IS NOT NULL`,
		`CREATE INDEX IF NOT EXISTS idx_token_tenant_id ON token(tenant_id) WHERE tenant_id IS NOT NULL`,
	}

	for _, stmt := range stmts {
		if _, err := tx.Exec(stmt); err != nil {
			return err
		}
	}

	return nil
}

func downTenantId(tx *sql.Tx) error {
	stmts := []string{
		`DROP INDEX IF EXISTS idx_token_tenant_id`,
		`DROP INDEX IF EXISTS idx_workspace_tenant_id`,
		`ALTER TABLE token DROP COLUMN IF EXISTS tenant_id`,
		`ALTER TABLE workspace DROP COLUMN IF EXISTS tenant_id`,
	}

	for _, stmt := range stmts {
		if _, err := tx.Exec(stmt); err != nil {
			return err
		}
	}

	return nil
}
