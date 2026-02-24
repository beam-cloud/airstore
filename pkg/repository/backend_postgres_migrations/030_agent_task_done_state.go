package backend_postgres_migrations

import (
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigration(upAgentTaskDoneState, downAgentTaskDoneState)
}

func upAgentTaskDoneState(tx *sql.Tx) error {
	stmt := `
		DO $$
		BEGIN
		  IF EXISTS (SELECT 1 FROM pg_type WHERE typname = 'agent_task_state')
		     AND NOT EXISTS (
		       SELECT 1
		       FROM pg_enum e
		       JOIN pg_type t ON t.oid = e.enumtypid
		       WHERE t.typname = 'agent_task_state'
		         AND e.enumlabel = 'done'
		     ) THEN
		    ALTER TYPE agent_task_state ADD VALUE 'done';
		  END IF;
		END $$;
	`
	_, err := tx.Exec(stmt)
	return err
}

func downAgentTaskDoneState(tx *sql.Tx) error {
	// Postgres enum values cannot be dropped safely in-place.
	return nil
}
