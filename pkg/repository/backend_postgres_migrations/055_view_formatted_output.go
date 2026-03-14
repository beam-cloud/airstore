package backend_postgres_migrations

import (
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigration(upViewFormattedOutput, downViewFormattedOutput)
}

func upViewFormattedOutput(tx *sql.Tx) error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS view_formatted_output (
			id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
			view_id UUID NOT NULL REFERENCES workspace_view(id) ON DELETE CASCADE,
			output_id UUID NOT NULL REFERENCES task_output(id) ON DELETE CASCADE,
			formatted_json JSONB NOT NULL,
			created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
			UNIQUE(view_id, output_id)
		)`,
	}

	for _, stmt := range stmts {
		if _, err := tx.Exec(stmt); err != nil {
			return err
		}
	}
	return nil
}

func downViewFormattedOutput(tx *sql.Tx) error {
	stmts := []string{
		`DROP TABLE IF EXISTS view_formatted_output`,
	}
	for _, stmt := range stmts {
		if _, err := tx.Exec(stmt); err != nil {
			return err
		}
	}
	return nil
}
