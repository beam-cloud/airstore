package backend_postgres_migrations

import (
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigration(upAgentTaskSemanticRename, downAgentTaskSemanticRename)
}

func upAgentTaskSemanticRename(tx *sql.Tx) error {
	stmts := []string{
		`DO $$ BEGIN
		   IF EXISTS (SELECT 1 FROM pg_type WHERE typname = 'agent_envelope_kind')
		      AND NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'agent_task_kind') THEN
		     ALTER TYPE agent_envelope_kind RENAME TO agent_task_kind;
		   END IF;
		 END $$;`,
		`DO $$ BEGIN
		   IF EXISTS (SELECT 1 FROM pg_type WHERE typname = 'agent_envelope_state')
		      AND NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'agent_task_state') THEN
		     ALTER TYPE agent_envelope_state RENAME TO agent_task_state;
		   END IF;
		 END $$;`,
		`DO $$ BEGIN
		   IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'agent_task_envelope')
		      AND NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'agent_task') THEN
		     ALTER TABLE agent_task_envelope RENAME TO agent_task;
		   END IF;
		 END $$;`,
		`DO $$ BEGIN
		   IF EXISTS (
		     SELECT 1 FROM pg_constraint
		     WHERE conname = 'chk_agent_task_envelope_idempotency_key_nonempty'
		   ) THEN
		     ALTER TABLE agent_task
		       RENAME CONSTRAINT chk_agent_task_envelope_idempotency_key_nonempty
		       TO chk_agent_task_idempotency_key_nonempty;
		   END IF;
		 END $$;`,
		`DO $$ BEGIN
		   IF EXISTS (
		     SELECT 1 FROM pg_constraint
		     WHERE conname = 'fk_agent_task_envelope_target_run'
		   ) THEN
		     ALTER TABLE agent_task
		       RENAME CONSTRAINT fk_agent_task_envelope_target_run
		       TO fk_agent_task_target_run;
		   END IF;
		 END $$;`,
		`DO $$ BEGIN
		   IF EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'uq_agent_task_envelope_idempotency_with_agent') THEN
		     ALTER INDEX uq_agent_task_envelope_idempotency_with_agent
		       RENAME TO uq_agent_task_idempotency_with_agent;
		   END IF;
		 END $$;`,
		`DO $$ BEGIN
		   IF EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'uq_agent_task_envelope_idempotency_without_agent') THEN
		     ALTER INDEX uq_agent_task_envelope_idempotency_without_agent
		       RENAME TO uq_agent_task_idempotency_without_agent;
		   END IF;
		 END $$;`,
		`DO $$ BEGIN
		   IF EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_agent_task_envelope_state_accepted_at') THEN
		     ALTER INDEX idx_agent_task_envelope_state_accepted_at
		       RENAME TO idx_agent_task_state_accepted_at;
		   END IF;
		 END $$;`,
		`DO $$ BEGIN
		   IF EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_agent_task_envelope_workspace_state_created') THEN
		     ALTER INDEX idx_agent_task_envelope_workspace_state_created
		       RENAME TO idx_agent_task_workspace_state_created;
		   END IF;
		 END $$;`,
		`DO $$ BEGIN
		   IF EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_agent_task_envelope_target_run') THEN
		     ALTER INDEX idx_agent_task_envelope_target_run
		       RENAME TO idx_agent_task_target_run;
		   END IF;
		 END $$;`,
		`DO $$ BEGIN
		   IF EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_agent_task_envelope_parent_envelope') THEN
		     ALTER INDEX idx_agent_task_envelope_parent_envelope
		       RENAME TO idx_agent_task_parent_envelope;
		   END IF;
		 END $$;`,
	}
	for _, stmt := range stmts {
		if _, err := tx.Exec(stmt); err != nil {
			return err
		}
	}
	return nil
}

func downAgentTaskSemanticRename(tx *sql.Tx) error {
	stmts := []string{
		`DO $$ BEGIN
		   IF EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_agent_task_parent_envelope') THEN
		     ALTER INDEX idx_agent_task_parent_envelope
		       RENAME TO idx_agent_task_envelope_parent_envelope;
		   END IF;
		 END $$;`,
		`DO $$ BEGIN
		   IF EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_agent_task_target_run') THEN
		     ALTER INDEX idx_agent_task_target_run
		       RENAME TO idx_agent_task_envelope_target_run;
		   END IF;
		 END $$;`,
		`DO $$ BEGIN
		   IF EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_agent_task_workspace_state_created') THEN
		     ALTER INDEX idx_agent_task_workspace_state_created
		       RENAME TO idx_agent_task_envelope_workspace_state_created;
		   END IF;
		 END $$;`,
		`DO $$ BEGIN
		   IF EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_agent_task_state_accepted_at') THEN
		     ALTER INDEX idx_agent_task_state_accepted_at
		       RENAME TO idx_agent_task_envelope_state_accepted_at;
		   END IF;
		 END $$;`,
		`DO $$ BEGIN
		   IF EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'uq_agent_task_idempotency_without_agent') THEN
		     ALTER INDEX uq_agent_task_idempotency_without_agent
		       RENAME TO uq_agent_task_envelope_idempotency_without_agent;
		   END IF;
		 END $$;`,
		`DO $$ BEGIN
		   IF EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'uq_agent_task_idempotency_with_agent') THEN
		     ALTER INDEX uq_agent_task_idempotency_with_agent
		       RENAME TO uq_agent_task_envelope_idempotency_with_agent;
		   END IF;
		 END $$;`,
		`DO $$ BEGIN
		   IF EXISTS (
		     SELECT 1 FROM pg_constraint
		     WHERE conname = 'fk_agent_task_target_run'
		   ) THEN
		     ALTER TABLE agent_task
		       RENAME CONSTRAINT fk_agent_task_target_run
		       TO fk_agent_task_envelope_target_run;
		   END IF;
		 END $$;`,
		`DO $$ BEGIN
		   IF EXISTS (
		     SELECT 1 FROM pg_constraint
		     WHERE conname = 'chk_agent_task_idempotency_key_nonempty'
		   ) THEN
		     ALTER TABLE agent_task
		       RENAME CONSTRAINT chk_agent_task_idempotency_key_nonempty
		       TO chk_agent_task_envelope_idempotency_key_nonempty;
		   END IF;
		 END $$;`,
		`DO $$ BEGIN
		   IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'agent_task')
		      AND NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'agent_task_envelope') THEN
		     ALTER TABLE agent_task RENAME TO agent_task_envelope;
		   END IF;
		 END $$;`,
		`DO $$ BEGIN
		   IF EXISTS (SELECT 1 FROM pg_type WHERE typname = 'agent_task_state')
		      AND NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'agent_envelope_state') THEN
		     ALTER TYPE agent_task_state RENAME TO agent_envelope_state;
		   END IF;
		 END $$;`,
		`DO $$ BEGIN
		   IF EXISTS (SELECT 1 FROM pg_type WHERE typname = 'agent_task_kind')
		      AND NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'agent_envelope_kind') THEN
		     ALTER TYPE agent_task_kind RENAME TO agent_envelope_kind;
		   END IF;
		 END $$;`,
	}
	for _, stmt := range stmts {
		if _, err := tx.Exec(stmt); err != nil {
			return err
		}
	}
	return nil
}
