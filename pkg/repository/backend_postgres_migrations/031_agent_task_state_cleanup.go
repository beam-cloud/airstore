package backend_postgres_migrations

import (
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigration(upAgentTaskStateCleanup, downAgentTaskStateCleanup)
}

func upAgentTaskStateCleanup(tx *sql.Tx) error {
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
		    SELECT 1 FROM pg_type WHERE typname = 'agent_task_state'
		  ) THEN
		    RETURN;
		  END IF;

		  -- The old queueability index predicate references enum values
		  -- we are removing ('accepted'), so rebuild it after remapping.
		  DROP INDEX IF EXISTS idx_agent_task_state_accepted_at;

		  ALTER TABLE agent_task
		    ALTER COLUMN state DROP DEFAULT;

		  IF NOT EXISTS (
		    SELECT 1 FROM pg_type WHERE typname = 'agent_task_state_legacy_031'
		  ) THEN
		    ALTER TYPE agent_task_state RENAME TO agent_task_state_legacy_031;
		  END IF;

		  IF NOT EXISTS (
		    SELECT 1 FROM pg_type WHERE typname = 'agent_task_state'
		  ) THEN
		    CREATE TYPE agent_task_state AS ENUM (
		      'queued',
		      'running',
		      'idle',
		      'done',
		      'dropped',
		      'cancelled'
		    );
		  END IF;

		  ALTER TABLE agent_task
		    ALTER COLUMN state TYPE agent_task_state
		    USING (
		      CASE state::text
		        WHEN 'accepted' THEN 'queued'
		        WHEN 'dispatched' THEN 'running'
		        ELSE state::text
		      END
		    )::agent_task_state;

		  ALTER TABLE agent_task
		    ALTER COLUMN state SET DEFAULT 'queued'::agent_task_state;

		  CREATE INDEX IF NOT EXISTS idx_agent_task_state_accepted_at
		    ON agent_task (state, accepted_at)
		    WHERE state IN ('queued');

		  DROP TYPE IF EXISTS agent_task_state_legacy_031;
		END $$;
	`
	_, err := tx.Exec(stmt)
	return err
}

func downAgentTaskStateCleanup(tx *sql.Tx) error {
	// This migration intentionally performs an enum hard-cleanup.
	// Reintroducing removed enum values is not safe as an automatic down migration.
	return nil
}
