package backend_postgres_migrations

import (
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigration(upSmartQueries, downSmartQueries)
}

func upSmartQueries(tx *sql.Tx) error {
	stmts := []string{
		// Smart queries table
		// Stores LLM-inferred queries that materialize as filesystem content
		`CREATE TABLE smart_queries (
			id SERIAL PRIMARY KEY,
			external_id UUID NOT NULL UNIQUE DEFAULT gen_random_uuid(),
			workspace_id INT NOT NULL REFERENCES workspace(id) ON DELETE CASCADE,
			integration VARCHAR(64) NOT NULL,
			path TEXT NOT NULL,
			name VARCHAR(255) NOT NULL,
			query_spec JSONB NOT NULL,
			guidance TEXT DEFAULT '',
			output_format VARCHAR(32) NOT NULL DEFAULT 'folder',
			file_ext VARCHAR(32) DEFAULT '',
			cache_ttl INT DEFAULT 0,
			created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
			last_executed TIMESTAMP WITH TIME ZONE,
			UNIQUE(workspace_id, path)
		)`,

		// Indexes
		`CREATE INDEX idx_smart_queries_workspace ON smart_queries(workspace_id)`,
		`CREATE INDEX idx_smart_queries_integration ON smart_queries(workspace_id, integration)`,
		`CREATE INDEX idx_smart_queries_path ON smart_queries(workspace_id, path)`,
	}

	for _, stmt := range stmts {
		if _, err := tx.Exec(stmt); err != nil {
			return err
		}
	}

	return nil
}

func downSmartQueries(tx *sql.Tx) error {
	stmts := []string{
		`DROP TABLE IF EXISTS smart_queries`,
	}

	for _, stmt := range stmts {
		if _, err := tx.Exec(stmt); err != nil {
			return err
		}
	}

	return nil
}
