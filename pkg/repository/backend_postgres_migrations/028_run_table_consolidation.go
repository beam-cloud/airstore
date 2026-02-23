package backend_postgres_migrations

import (
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigration(upRunTableConsolidation, downRunTableConsolidation)
}

func upRunTableConsolidation(tx *sql.Tx) error {
	stmts := []string{
		`DO $$ BEGIN
		   IF EXISTS (
		     SELECT 1
		     FROM information_schema.columns
		     WHERE table_schema = current_schema()
		       AND table_name = 'agent_run'
		       AND column_name = 'origin_envelope_id'
		   ) THEN
		     ALTER TABLE agent_run RENAME COLUMN origin_envelope_id TO origin_task_id;
		   ELSIF NOT EXISTS (
		     SELECT 1
		     FROM information_schema.columns
		     WHERE table_schema = current_schema()
		       AND table_name = 'agent_run'
		       AND column_name = 'origin_task_id'
		   ) THEN
		     RAISE EXCEPTION 'migration 028 expected agent_run.origin_envelope_id or agent_run.origin_task_id';
		   END IF;
		 END $$;`,
		`ALTER TABLE agent_run DROP CONSTRAINT IF EXISTS uq_agent_run_origin_envelope;`,
		`ALTER TABLE agent_run DROP CONSTRAINT IF EXISTS agent_run_origin_envelope_id_fkey;`,
		`ALTER TABLE agent_run DROP CONSTRAINT IF EXISTS agent_run_origin_task_id_fkey;`,
		`ALTER TABLE agent_run_attempt DROP CONSTRAINT IF EXISTS agent_run_attempt_execution_task_external_id_fkey;`,
		`ALTER TABLE agent_run
		   ADD COLUMN IF NOT EXISTS created_by_member_id INTEGER REFERENCES workspace_member(id) ON DELETE SET NULL,
		   ADD COLUMN IF NOT EXISTS type TEXT NOT NULL DEFAULT 'background',
		   ADD COLUMN IF NOT EXISTS prompt TEXT,
		   ADD COLUMN IF NOT EXISTS image VARCHAR(512) NOT NULL DEFAULT '',
		   ADD COLUMN IF NOT EXISTS entrypoint TEXT[] NOT NULL DEFAULT '{}',
		   ADD COLUMN IF NOT EXISTS env JSONB NOT NULL DEFAULT '{}'::jsonb,
		   ADD COLUMN IF NOT EXISTS hook_id INTEGER REFERENCES filesystem_hooks(id) ON DELETE SET NULL,
		   ADD COLUMN IF NOT EXISTS attempt INTEGER NOT NULL DEFAULT 1,
		   ADD COLUMN IF NOT EXISTS max_attempts INTEGER NOT NULL DEFAULT 1,
		   ADD COLUMN IF NOT EXISTS run_attempt_id UUID NULL,
		   ADD COLUMN IF NOT EXISTS exit_code INTEGER,
		   ADD COLUMN IF NOT EXISTS execution_policy_json JSONB NOT NULL DEFAULT '{}'::jsonb;`,
		`CREATE INDEX IF NOT EXISTS idx_agent_run_hook_active
		 ON agent_run(hook_id)
		 WHERE hook_id IS NOT NULL AND status IN ('accepted', 'running');`,
		`CREATE UNIQUE INDEX IF NOT EXISTS uq_agent_run_run_attempt_id
		 ON agent_run(run_attempt_id)
		 WHERE run_attempt_id IS NOT NULL;`,
		`UPDATE agent_run AS run
		 SET created_by_member_id = COALESCE(latest.created_by_member_id, run.created_by_member_id),
		     status = CASE latest.status::text
		       WHEN 'pending' THEN 'accepted'::agent_run_status
		       WHEN 'scheduled' THEN 'accepted'::agent_run_status
		       WHEN 'running' THEN 'running'::agent_run_status
		       WHEN 'complete' THEN 'ok'::agent_run_status
		       WHEN 'failed' THEN 'error'::agent_run_status
		       WHEN 'cancelled' THEN 'cancelled'::agent_run_status
		       ELSE run.status
		     END,
		     type = COALESCE(NULLIF(latest.type, ''), run.type),
		     prompt = COALESCE(latest.prompt, run.prompt),
		     image = COALESCE(NULLIF(latest.image, ''), run.image),
		     entrypoint = COALESCE(latest.entrypoint, run.entrypoint),
		     env = COALESCE(latest.env, run.env),
		     hook_id = COALESCE(latest.hook_id, run.hook_id),
		     attempt = GREATEST(COALESCE(latest.attempt, 1), COALESCE(run.attempt, 1)),
		     max_attempts = GREATEST(COALESCE(latest.max_attempts, 1), COALESCE(run.max_attempts, 1)),
		     run_attempt_id = COALESCE(latest.run_attempt_id, run.run_attempt_id),
		     timeout_ms = COALESCE(latest.timeout_ms, run.timeout_ms),
		     exec_host = COALESCE(latest.exec_host, run.exec_host),
		     exec_security = COALESCE(latest.exec_security, run.exec_security),
		     exec_ask = COALESCE(latest.exec_ask, run.exec_ask),
		     runtime_type = COALESCE(latest.runtime_type, run.runtime_type),
		     workspace_access = COALESCE(latest.workspace_access, run.workspace_access),
		     network_enabled = COALESCE(latest.network_enabled, run.network_enabled),
		     exit_code = COALESCE(latest.exit_code, run.exit_code),
		     error = COALESCE(NULLIF(latest.error, ''), run.error),
		     started_at = COALESCE(latest.started_at, run.started_at),
		     ended_at = COALESCE(latest.finished_at, run.ended_at),
		     execution_policy_json = COALESCE(latest.execution_policy_json, run.execution_policy_json),
		     updated_at = CURRENT_TIMESTAMP
		 FROM (
		   SELECT DISTINCT ON (attempt.run_id)
		     attempt.run_id,
		     attempt.id AS run_attempt_id,
		     task.created_by_member_id,
		     task.status,
		     task.type,
		     task.prompt,
		     task.image,
		     task.entrypoint,
		     task.env,
		     task.hook_id,
		     task.attempt,
		     task.max_attempts,
		     task.timeout_ms,
		     task.exec_host,
		     task.exec_security,
		     task.exec_ask,
		     task.runtime_type,
		     task.workspace_access,
		     task.network_enabled,
		     task.exit_code,
		     task.error,
		     task.started_at,
		     task.finished_at,
		     task.execution_policy_json
		   FROM agent_run_attempt AS attempt
		   JOIN task ON task.external_id = attempt.execution_task_external_id
		   ORDER BY attempt.run_id, attempt.attempt_no DESC, attempt.created_at DESC
		 ) AS latest
		 WHERE run.id = latest.run_id;`,
		`INSERT INTO agent_run (
		   id,
		   workspace_id,
		   origin_task_id,
		   status,
		   session_id,
		   exec_host,
		   exec_security,
		   exec_ask,
		   runtime_type,
		   workspace_access,
		   network_enabled,
		   interactive,
		   timeout_ms,
		   created_by_member_id,
		   type,
		   prompt,
		   image,
		   entrypoint,
		   env,
		   hook_id,
		   attempt,
		   max_attempts,
		   run_attempt_id,
		   exit_code,
		   error,
		   execution_policy_json,
		   started_at,
		   ended_at,
		   created_at,
		   updated_at
		 )
		 SELECT
		   task.external_id,
		   task.workspace_id,
		   task.external_id,
		   CASE task.status::text
		     WHEN 'pending' THEN 'accepted'::agent_run_status
		     WHEN 'scheduled' THEN 'accepted'::agent_run_status
		     WHEN 'running' THEN 'running'::agent_run_status
		     WHEN 'complete' THEN 'ok'::agent_run_status
		     WHEN 'failed' THEN 'error'::agent_run_status
		     WHEN 'cancelled' THEN 'cancelled'::agent_run_status
		     ELSE 'error'::agent_run_status
		   END,
		   task.external_id::text,
		   COALESCE(task.exec_host, 'sandbox'),
		   COALESCE(task.exec_security, 'allowlist'),
		   COALESCE(task.exec_ask, 'off'),
		   COALESCE(task.runtime_type, 'gvisor'),
		   COALESCE(task.workspace_access, 'rw'),
		   COALESCE(task.network_enabled, TRUE),
		   FALSE,
		   COALESCE(task.timeout_ms, 0),
		   task.created_by_member_id,
		   COALESCE(NULLIF(task.type, ''), 'background'),
		   task.prompt,
		   COALESCE(task.image, ''),
		   COALESCE(task.entrypoint, '{}'::text[]),
		   COALESCE(task.env, '{}'::jsonb),
		   task.hook_id,
		   COALESCE(task.attempt, 1),
		   COALESCE(task.max_attempts, 1),
		   task.external_id,
		   task.exit_code,
		   NULLIF(task.error, ''),
		   COALESCE(task.execution_policy_json, '{}'::jsonb),
		   task.started_at,
		   task.finished_at,
		   task.created_at,
		   CURRENT_TIMESTAMP
		 FROM task
		 LEFT JOIN agent_run_attempt AS attempt
		   ON attempt.execution_task_external_id = task.external_id
		 LEFT JOIN agent_run AS run
		   ON run.id = COALESCE(attempt.run_id, task.external_id)
		 WHERE run.id IS NULL;`,
		`UPDATE agent_run_attempt
		 SET execution_task_external_id = run_id
		 WHERE execution_task_external_id IS DISTINCT FROM run_id;`,
		`UPDATE agent_run AS run
		 SET run_attempt_id = latest.id
		 FROM (
		   SELECT DISTINCT ON (run_id) run_id, id
		   FROM agent_run_attempt
		   ORDER BY run_id, attempt_no DESC, created_at DESC
		 ) AS latest
		 WHERE latest.run_id = run.id
		   AND run.run_attempt_id IS DISTINCT FROM latest.id;`,
		`DROP VIEW execution_task;`,
		`DROP TABLE task;`,
		`DROP TYPE task_status;`,
		`DROP TABLE agent_run_attempt;`,
		`DROP TYPE agent_attempt_status;`,
	}

	for _, stmt := range stmts {
		if _, err := tx.Exec(stmt); err != nil {
			return err
		}
	}
	return nil
}

func downRunTableConsolidation(tx *sql.Tx) error {
	stmts := []string{
		`DO $$ BEGIN
		   IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'task_status') THEN
		     CREATE TYPE task_status AS ENUM ('pending', 'scheduled', 'running', 'complete', 'failed', 'cancelled');
		   END IF;
		 END $$;`,
		`DO $$ BEGIN
		   IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'agent_attempt_status') THEN
		     CREATE TYPE agent_attempt_status AS ENUM ('pending', 'blocked', 'running', 'ok', 'error', 'timeout', 'cancelled');
		   END IF;
		 END $$;`,
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
		   finished_at TIMESTAMP WITH TIME ZONE,
		   prompt TEXT,
		   created_by_member_id INTEGER REFERENCES workspace_member(id) ON DELETE SET NULL,
		   hook_id INTEGER REFERENCES filesystem_hooks(id) ON DELETE SET NULL,
		   attempt INTEGER NOT NULL DEFAULT 1,
		   max_attempts INTEGER NOT NULL DEFAULT 1,
		   type TEXT NOT NULL DEFAULT 'background',
		   run_attempt_id UUID NULL,
		   timeout_ms INT NULL CHECK (timeout_ms IS NULL OR timeout_ms >= 0),
		   exec_host TEXT NULL CHECK (exec_host IN ('sandbox', 'gateway', 'node')),
		   exec_security TEXT NULL CHECK (exec_security IN ('deny', 'allowlist', 'full')),
		   exec_ask TEXT NULL CHECK (exec_ask IN ('off', 'on-miss', 'always')),
		   runtime_type TEXT NULL,
		   workspace_access TEXT NULL CHECK (workspace_access IN ('none', 'ro', 'rw')),
		   network_enabled BOOLEAN NULL,
		   execution_policy_json JSONB NOT NULL DEFAULT '{}'::jsonb
		 );`,
		`CREATE OR REPLACE VIEW execution_task AS SELECT * FROM task;`,
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
		   execution_task_external_id UUID NULL,
		   started_at TIMESTAMPTZ NULL,
		   ended_at TIMESTAMPTZ NULL,
		   exit_code INT NULL,
		   error TEXT NULL,
		   created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
		   updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
		   CONSTRAINT uq_agent_run_attempt_run_attempt_no UNIQUE (run_id, attempt_no),
		   CONSTRAINT chk_agent_run_attempt_attempt_no_positive CHECK (attempt_no > 0)
		 );`,
		`INSERT INTO agent_run_attempt (
		   id, run_id, attempt_no, status, strategy, provider, model,
		   exec_host, exec_security, exec_ask, runtime_type, workspace_access,
		   network_enabled, interactive, execution_task_external_id,
		   started_at, ended_at, exit_code, error, created_at, updated_at
		 )
		 SELECT
		   COALESCE(run.run_attempt_id, uuid_generate_v4()),
		   run.id,
		   COALESCE(run.attempt, 1),
		   CASE run.status
		     WHEN 'accepted' THEN 'pending'::agent_attempt_status
		     WHEN 'running' THEN 'running'::agent_attempt_status
		     WHEN 'ok' THEN 'ok'::agent_attempt_status
		     WHEN 'error' THEN 'error'::agent_attempt_status
		     WHEN 'timeout' THEN 'timeout'::agent_attempt_status
		     WHEN 'cancelled' THEN 'cancelled'::agent_attempt_status
		     ELSE 'error'::agent_attempt_status
		   END,
		   CASE WHEN COALESCE(run.attempt, 1) > 1 THEN 'retry' ELSE 'primary' END,
		   run.provider,
		   run.model,
		   run.exec_host,
		   run.exec_security,
		   run.exec_ask,
		   run.runtime_type,
		   run.workspace_access,
		   run.network_enabled,
		   run.interactive,
		   run.id,
		   run.started_at,
		   run.ended_at,
		   run.exit_code,
		   run.error,
		   run.created_at,
		   run.updated_at
		 FROM agent_run AS run
		 WHERE run.image IS NOT NULL
		   AND run.image <> ''
		 ON CONFLICT (id) DO NOTHING;`,
		`INSERT INTO task (
		   external_id, workspace_id, status, image, entrypoint, env, exit_code, error,
		   created_at, started_at, finished_at, prompt, created_by_member_id, hook_id,
		   attempt, max_attempts, type, run_attempt_id, timeout_ms, exec_host, exec_security,
		   exec_ask, runtime_type, workspace_access, network_enabled, execution_policy_json
		 )
		 SELECT
		   run.id,
		   run.workspace_id,
		   CASE run.status
		     WHEN 'accepted' THEN 'pending'::task_status
		     WHEN 'running' THEN 'running'::task_status
		     WHEN 'ok' THEN 'complete'::task_status
		     WHEN 'error' THEN 'failed'::task_status
		     WHEN 'timeout' THEN 'failed'::task_status
		     WHEN 'cancelled' THEN 'cancelled'::task_status
		     ELSE 'failed'::task_status
		   END,
		   run.image,
		   run.entrypoint,
		   run.env,
		   run.exit_code,
		   run.error,
		   run.created_at,
		   run.started_at,
		   run.ended_at,
		   run.prompt,
		   run.created_by_member_id,
		   run.hook_id,
		   run.attempt,
		   run.max_attempts,
		   run.type,
		   run.run_attempt_id,
		   run.timeout_ms,
		   run.exec_host,
		   run.exec_security,
		   run.exec_ask,
		   run.runtime_type,
		   run.workspace_access,
		   run.network_enabled,
		   run.execution_policy_json
		 FROM agent_run AS run
		 WHERE run.image IS NOT NULL AND run.image <> ''
		 ON CONFLICT (external_id) DO NOTHING;`,
	}

	for _, stmt := range stmts {
		if _, err := tx.Exec(stmt); err != nil {
			return err
		}
	}
	return nil
}
