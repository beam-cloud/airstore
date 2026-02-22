package backend_postgres_migrations

import (
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigration(upExecutionTaskPolicyBridge, downExecutionTaskPolicyBridge)
}

func upExecutionTaskPolicyBridge(tx *sql.Tx) error {
	stmts := []string{
		`DO $$ BEGIN
		   IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'agent_envelope_kind') THEN
		     CREATE TYPE agent_envelope_kind AS ENUM ('agent_command', 'run_input', 'followup', 'cron');
		   END IF;
		 END $$;`,
		`DO $$ BEGIN
		   IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'agent_queue_mode') THEN
		     CREATE TYPE agent_queue_mode AS ENUM ('steer', 'followup', 'collect', 'steer-backlog', 'interrupt', 'queue');
		   END IF;
		 END $$;`,
		`DO $$ BEGIN
		   IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'agent_envelope_state') THEN
		     CREATE TYPE agent_envelope_state AS ENUM ('accepted', 'queued', 'dispatched', 'dropped', 'cancelled');
		   END IF;
		 END $$;`,
		`DO $$ BEGIN
		   IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'agent_run_status') THEN
		     CREATE TYPE agent_run_status AS ENUM ('accepted', 'running', 'ok', 'error', 'timeout', 'cancelled');
		   END IF;
		 END $$;`,
		`DO $$ BEGIN
		   IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'agent_attempt_status') THEN
		     CREATE TYPE agent_attempt_status AS ENUM ('pending', 'blocked', 'running', 'ok', 'error', 'timeout', 'cancelled');
		   END IF;
		 END $$;`,
		`CREATE TABLE IF NOT EXISTS agent_profile (
		   id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
		   workspace_id INT NOT NULL REFERENCES workspace(id) ON DELETE CASCADE,
		   agent_key TEXT NOT NULL,
		   name TEXT NOT NULL,
		   config_json JSONB NOT NULL DEFAULT '{}'::jsonb,
		   active BOOLEAN NOT NULL DEFAULT TRUE,
		   created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
		   updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
		   CONSTRAINT uq_agent_profile_workspace_key UNIQUE (workspace_id, agent_key)
		 );`,
		`CREATE TABLE IF NOT EXISTS agent_task_envelope (
		   id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
		   workspace_id INT NOT NULL REFERENCES workspace(id) ON DELETE CASCADE,
		   agent_id UUID NULL REFERENCES agent_profile(id) ON DELETE SET NULL,
		   kind agent_envelope_kind NOT NULL,
		   queue_mode agent_queue_mode NOT NULL DEFAULT 'queue',
		   state agent_envelope_state NOT NULL DEFAULT 'accepted',
		   idempotency_key TEXT NOT NULL,
		   payload_json JSONB NOT NULL DEFAULT '{}'::jsonb,
		   routing_json JSONB NOT NULL DEFAULT '{}'::jsonb,
		   parent_envelope_id UUID NULL REFERENCES agent_task_envelope(id) ON DELETE SET NULL,
		   target_run_id UUID NULL,
		   accepted_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
		   queued_at TIMESTAMPTZ NULL,
		   dispatched_at TIMESTAMPTZ NULL,
		   dropped_reason TEXT NULL,
		   created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
		   updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
		   CONSTRAINT chk_agent_task_envelope_idempotency_key_nonempty CHECK (char_length(trim(idempotency_key)) > 0)
		 );`,
		`CREATE TABLE IF NOT EXISTS agent_run (
		   id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
		   workspace_id INT NOT NULL REFERENCES workspace(id) ON DELETE CASCADE,
		   agent_id UUID NULL REFERENCES agent_profile(id) ON DELETE SET NULL,
		   origin_envelope_id UUID NOT NULL REFERENCES agent_task_envelope(id) ON DELETE RESTRICT,
		   status agent_run_status NOT NULL DEFAULT 'accepted',
		   session_id TEXT NOT NULL,
		   session_key TEXT NULL,
		   provider TEXT NULL,
		   model TEXT NULL,
		   exec_host TEXT NOT NULL DEFAULT 'sandbox' CHECK (exec_host IN ('sandbox', 'gateway', 'node')),
		   exec_security TEXT NOT NULL DEFAULT 'allowlist' CHECK (exec_security IN ('deny', 'allowlist', 'full')),
		   exec_ask TEXT NOT NULL DEFAULT 'off' CHECK (exec_ask IN ('off', 'on-miss', 'always')),
		   runtime_type TEXT NOT NULL DEFAULT 'gvisor',
		   workspace_access TEXT NOT NULL DEFAULT 'rw' CHECK (workspace_access IN ('none', 'ro', 'rw')),
		   network_enabled BOOLEAN NOT NULL DEFAULT TRUE,
		   interactive BOOLEAN NOT NULL DEFAULT FALSE,
		   timeout_ms INT NOT NULL,
		   started_at TIMESTAMPTZ NULL,
		   ended_at TIMESTAMPTZ NULL,
		   error TEXT NULL,
		   snapshot_ts BIGINT NOT NULL DEFAULT 0,
		   usage_json JSONB NOT NULL DEFAULT '{}'::jsonb,
		   delivery_json JSONB NOT NULL DEFAULT '{}'::jsonb,
		   created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
		   updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
		   CONSTRAINT uq_agent_run_origin_envelope UNIQUE (origin_envelope_id),
		   CONSTRAINT chk_agent_run_timeout_ms_nonnegative CHECK (timeout_ms >= 0)
		 );`,
		`DO $$ BEGIN
		   IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_agent_task_envelope_target_run') THEN
		     ALTER TABLE agent_task_envelope
		       ADD CONSTRAINT fk_agent_task_envelope_target_run
		       FOREIGN KEY (target_run_id)
		       REFERENCES agent_run(id)
		       ON DELETE SET NULL
		       DEFERRABLE INITIALLY DEFERRED;
		   END IF;
		 END $$;`,
		`CREATE TABLE IF NOT EXISTS agent_run_attempt (
		   id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
		   run_id UUID NOT NULL REFERENCES agent_run(id) ON DELETE CASCADE,
		   attempt_no INT NOT NULL,
		   status agent_attempt_status NOT NULL DEFAULT 'pending',
		   strategy TEXT NOT NULL DEFAULT 'primary',
		   provider TEXT NULL,
		   model TEXT NULL,
		   exec_host TEXT NOT NULL DEFAULT 'sandbox' CHECK (exec_host IN ('sandbox', 'gateway', 'node')),
		   exec_security TEXT NOT NULL DEFAULT 'allowlist' CHECK (exec_security IN ('deny', 'allowlist', 'full')),
		   exec_ask TEXT NOT NULL DEFAULT 'off' CHECK (exec_ask IN ('off', 'on-miss', 'always')),
		   runtime_type TEXT NOT NULL DEFAULT 'gvisor',
		   workspace_access TEXT NOT NULL DEFAULT 'rw' CHECK (workspace_access IN ('none', 'ro', 'rw')),
		   network_enabled BOOLEAN NOT NULL DEFAULT TRUE,
		   interactive BOOLEAN NOT NULL DEFAULT FALSE,
		   execution_task_external_id UUID NULL REFERENCES task(external_id) ON DELETE SET NULL,
		   started_at TIMESTAMPTZ NULL,
		   ended_at TIMESTAMPTZ NULL,
		   exit_code INT NULL,
		   error TEXT NULL,
		   created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
		   updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
		   CONSTRAINT uq_agent_run_attempt_run_attempt_no UNIQUE (run_id, attempt_no),
		   CONSTRAINT chk_agent_run_attempt_attempt_no_positive CHECK (attempt_no > 0)
		 );`,
		`CREATE TABLE IF NOT EXISTS agent_run_snapshot (
		   id BIGSERIAL PRIMARY KEY,
		   run_id UUID NOT NULL REFERENCES agent_run(id) ON DELETE CASCADE,
		   seq BIGINT NOT NULL,
		   status agent_run_status NOT NULL,
		   started_at_ms BIGINT NULL,
		   ended_at_ms BIGINT NULL,
		   error TEXT NULL,
		   ts BIGINT NOT NULL,
		   payload_json JSONB NOT NULL DEFAULT '{}'::jsonb,
		   created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
		   CONSTRAINT uq_agent_run_snapshot_run_seq UNIQUE (run_id, seq)
		 );`,
		`ALTER TABLE task
		   ADD COLUMN IF NOT EXISTS run_attempt_id UUID NULL,
		   ADD COLUMN IF NOT EXISTS timeout_ms INT NULL CHECK (timeout_ms IS NULL OR timeout_ms >= 0),
		   ADD COLUMN IF NOT EXISTS exec_host TEXT NULL CHECK (exec_host IN ('sandbox', 'gateway', 'node')),
		   ADD COLUMN IF NOT EXISTS exec_security TEXT NULL CHECK (exec_security IN ('deny', 'allowlist', 'full')),
		   ADD COLUMN IF NOT EXISTS exec_ask TEXT NULL CHECK (exec_ask IN ('off', 'on-miss', 'always')),
		   ADD COLUMN IF NOT EXISTS runtime_type TEXT NULL,
		   ADD COLUMN IF NOT EXISTS workspace_access TEXT NULL CHECK (workspace_access IN ('none', 'ro', 'rw')),
		   ADD COLUMN IF NOT EXISTS network_enabled BOOLEAN NULL,
		   ADD COLUMN IF NOT EXISTS execution_policy_json JSONB NOT NULL DEFAULT '{}'::jsonb;`,
		`DO $$ BEGIN
		   IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'agent_run_attempt') THEN
		     IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_task_run_attempt_id') THEN
		       ALTER TABLE task
		         ADD CONSTRAINT fk_task_run_attempt_id
		         FOREIGN KEY (run_attempt_id)
		         REFERENCES agent_run_attempt(id)
		         ON DELETE SET NULL;
		     END IF;
		   END IF;
		 END $$;`,
		`CREATE UNIQUE INDEX IF NOT EXISTS uq_task_run_attempt_id
		 ON task (run_attempt_id)
		 WHERE run_attempt_id IS NOT NULL;`,
	}
	for _, stmt := range stmts {
		if _, err := tx.Exec(stmt); err != nil {
			return err
		}
	}
	return nil
}

func downExecutionTaskPolicyBridge(tx *sql.Tx) error {
	stmts := []string{
		`DROP INDEX IF EXISTS uq_task_run_attempt_id;`,
		`ALTER TABLE IF EXISTS task DROP CONSTRAINT IF EXISTS fk_task_run_attempt_id;`,
		`ALTER TABLE task
		   DROP COLUMN IF EXISTS execution_policy_json,
		   DROP COLUMN IF EXISTS network_enabled,
		   DROP COLUMN IF EXISTS workspace_access,
		   DROP COLUMN IF EXISTS runtime_type,
		   DROP COLUMN IF EXISTS exec_ask,
		   DROP COLUMN IF EXISTS exec_security,
		   DROP COLUMN IF EXISTS exec_host,
		   DROP COLUMN IF EXISTS timeout_ms,
		   DROP COLUMN IF EXISTS run_attempt_id;`,
	}
	for _, stmt := range stmts {
		if _, err := tx.Exec(stmt); err != nil {
			return err
		}
	}
	return nil
}
