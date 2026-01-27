package backend_postgres_migrations

import (
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigration(upInitial, downInitial)
}

func upInitial(tx *sql.Tx) error {
	// Ensure UUID extension is available
	if _, err := tx.Exec(`CREATE EXTENSION IF NOT EXISTS "uuid-ossp"`); err != nil {
		return err
	}

	createStatements := []string{
		// Task status enum (lowercase to match Go constants)
		`CREATE TYPE task_status AS ENUM ('pending', 'scheduled', 'running', 'complete', 'failed', 'cancelled');`,

		// Workspaces table
		`CREATE TABLE IF NOT EXISTS workspace (
			id SERIAL PRIMARY KEY,
			external_id UUID DEFAULT uuid_generate_v4() UNIQUE NOT NULL,
			name VARCHAR(255) NOT NULL UNIQUE,
			created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
		);`,

		// Tasks table
		`CREATE TABLE IF NOT EXISTS task (
			id SERIAL PRIMARY KEY,
			external_id UUID DEFAULT uuid_generate_v4() UNIQUE NOT NULL,
			workspace_id INT NOT NULL REFERENCES workspace(id) ON DELETE CASCADE,
			status task_status NOT NULL DEFAULT 'pending',
			image VARCHAR(512) NOT NULL,
			entrypoint TEXT[] DEFAULT '{}',
			env JSONB DEFAULT '{}',
			exit_code INTEGER,
			error TEXT,
			created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
			started_at TIMESTAMP WITH TIME ZONE,
			finished_at TIMESTAMP WITH TIME ZONE
		);`,

		// Indexes
		`CREATE INDEX idx_task_workspace_id ON task(workspace_id);`,
		`CREATE INDEX idx_task_status ON task(status);`,
		`CREATE INDEX idx_task_created_at ON task(created_at DESC);`,
		`CREATE INDEX idx_task_external_id ON task(external_id);`,
	}

	for _, stmt := range createStatements {
		if _, err := tx.Exec(stmt); err != nil {
			return err
		}
	}

	return nil
}

func downInitial(tx *sql.Tx) error {
	dropStatements := []string{
		"DROP TABLE IF EXISTS task;",
		"DROP TABLE IF EXISTS workspace;",
		"DROP TYPE IF EXISTS task_status;",
	}

	for _, stmt := range dropStatements {
		if _, err := tx.Exec(stmt); err != nil {
			return err
		}
	}

	return nil
}
