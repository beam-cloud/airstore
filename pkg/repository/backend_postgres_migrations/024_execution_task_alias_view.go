package backend_postgres_migrations

import (
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigration(upExecutionTaskAliasView, downExecutionTaskAliasView)
}

func upExecutionTaskAliasView(tx *sql.Tx) error {
	_, err := tx.Exec(`
		CREATE OR REPLACE VIEW execution_task AS
		SELECT * FROM task;

		COMMENT ON VIEW execution_task IS
		'Execution substrate alias. Agent orchestration source-of-truth lives in envelope/run tables.';
	`)
	return err
}

func downExecutionTaskAliasView(tx *sql.Tx) error {
	_, err := tx.Exec(`DROP VIEW IF EXISTS execution_task;`)
	return err
}
