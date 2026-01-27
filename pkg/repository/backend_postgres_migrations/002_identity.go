package backend_postgres_migrations

import (
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigration(upIdentity, downIdentity)
}

func upIdentity(tx *sql.Tx) error {
	stmts := []string{
		// Member role enum
		`CREATE TYPE member_role AS ENUM ('admin', 'member', 'viewer')`,

		// Workspace members
		`CREATE TABLE workspace_member (
			id SERIAL PRIMARY KEY,
			external_id UUID DEFAULT uuid_generate_v4() UNIQUE NOT NULL,
			workspace_id INT NOT NULL REFERENCES workspace(id) ON DELETE CASCADE,
			email VARCHAR(255) NOT NULL,
			name VARCHAR(255),
			role member_role NOT NULL DEFAULT 'member',
			created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
			UNIQUE(workspace_id, email)
		)`,

		// Workspace tokens
		`CREATE TABLE workspace_token (
			id SERIAL PRIMARY KEY,
			external_id UUID DEFAULT uuid_generate_v4() UNIQUE NOT NULL,
			workspace_id INT NOT NULL REFERENCES workspace(id) ON DELETE CASCADE,
			member_id INT NOT NULL REFERENCES workspace_member(id) ON DELETE CASCADE,
			token_hash VARCHAR(255) NOT NULL,
			name VARCHAR(255),
			expires_at TIMESTAMP WITH TIME ZONE,
			created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
			last_used_at TIMESTAMP WITH TIME ZONE
		)`,

		// Integration connections
		`CREATE TABLE integration_connection (
			id SERIAL PRIMARY KEY,
			external_id UUID DEFAULT uuid_generate_v4() UNIQUE NOT NULL,
			workspace_id INT NOT NULL REFERENCES workspace(id) ON DELETE CASCADE,
			member_id INT REFERENCES workspace_member(id) ON DELETE CASCADE,
			integration_type VARCHAR(50) NOT NULL,
			credentials BYTEA NOT NULL,
			scope VARCHAR(512),
			expires_at TIMESTAMP WITH TIME ZONE,
			created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
		)`,

		// Indexes
		`CREATE INDEX idx_workspace_member_workspace ON workspace_member(workspace_id)`,
		`CREATE INDEX idx_workspace_member_email ON workspace_member(email)`,
		`CREATE INDEX idx_workspace_token_workspace ON workspace_token(workspace_id)`,
		`CREATE INDEX idx_workspace_token_member ON workspace_token(member_id)`,
		`CREATE INDEX idx_workspace_token_hash ON workspace_token(token_hash)`,
		`CREATE INDEX idx_integration_connection_workspace ON integration_connection(workspace_id)`,
		`CREATE INDEX idx_integration_connection_member ON integration_connection(member_id)`,
		`CREATE INDEX idx_integration_connection_type ON integration_connection(integration_type)`,

		// Unique constraint for integration connections
		// Uses COALESCE to handle NULL member_id (shared connections)
		`CREATE UNIQUE INDEX idx_integration_connection_unique 
			ON integration_connection(workspace_id, COALESCE(member_id, 0), integration_type)`,
	}

	for _, stmt := range stmts {
		if _, err := tx.Exec(stmt); err != nil {
			return err
		}
	}

	return nil
}

func downIdentity(tx *sql.Tx) error {
	stmts := []string{
		`DROP TABLE IF EXISTS integration_connection`,
		`DROP TABLE IF EXISTS workspace_token`,
		`DROP TABLE IF EXISTS workspace_member`,
		`DROP TYPE IF EXISTS member_role`,
	}

	for _, stmt := range stmts {
		if _, err := tx.Exec(stmt); err != nil {
			return err
		}
	}

	return nil
}
