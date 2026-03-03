package backend_postgres_migrations

import (
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigration(upAgentTaskKindCleanup, downAgentTaskKindCleanup)
}

func upAgentTaskKindCleanup(tx *sql.Tx) error {
	stmt := `
		DO $$
		BEGIN
		  IF NOT EXISTS (
		    SELECT 1 FROM information_schema.tables
		    WHERE table_schema = current_schema() AND table_name = 'agent_task'
		  ) THEN
		    RETURN;
		  END IF;

		  IF NOT EXISTS (
		    SELECT 1 FROM information_schema.columns
		    WHERE table_schema = current_schema()
		      AND table_name = 'agent_task'
		      AND column_name = 'kind'
		  ) THEN
		    RETURN;
		  END IF;

		  UPDATE agent_task
		  SET state = CASE
		                WHEN state IN ('queued', 'running', 'idle')
		                  THEN 'dropped'::agent_task_state
		                ELSE state
		              END,
		      dropped_reason = CASE
		                         WHEN state IN ('queued', 'running', 'idle')
		                           THEN COALESCE(dropped_reason, 'legacy_task_kind_removed')
		                         ELSE dropped_reason
		                       END,
		      archived_at = COALESCE(archived_at, CURRENT_TIMESTAMP),
		      updated_at = CURRENT_TIMESTAMP
		  WHERE kind::text <> 'agent_command';
		END $$;
	`
	_, err := tx.Exec(stmt)
	return err
}

func downAgentTaskKindCleanup(tx *sql.Tx) error {
	// Data archival/drop is intentionally one-way.
	return nil
}
