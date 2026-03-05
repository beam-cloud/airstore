package backend_postgres_migrations

import (
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigration(upAgentChannelsCleanup, downAgentChannelsCleanup)
}

func upAgentChannelsCleanup(tx *sql.Tx) error {
	stmts := []string{
		`DROP INDEX IF EXISTS idx_agent_task_routing_thread;`,
		`DROP INDEX IF EXISTS idx_agent_task_routing_channel;`,
		`DROP TABLE IF EXISTS agent_message;`,
		`DROP TABLE IF EXISTS agent_thread;`,
		`DROP TABLE IF EXISTS agent_channel;`,
	}

	for _, stmt := range stmts {
		if _, err := tx.Exec(stmt); err != nil {
			return err
		}
	}
	return nil
}

func downAgentChannelsCleanup(tx *sql.Tx) error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS agent_channel (
		   id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
		   workspace_id INT NOT NULL REFERENCES workspace(id) ON DELETE CASCADE,
		   agent_id UUID NULL REFERENCES agent_profile(id) ON DELETE SET NULL,
		   integration_type TEXT NOT NULL DEFAULT 'airstore',
		   external_id TEXT NOT NULL,
		   name TEXT NOT NULL,
		   active BOOLEAN NOT NULL DEFAULT TRUE,
		   metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
		   created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
		   updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
		   CONSTRAINT uq_agent_channel_workspace_external UNIQUE (workspace_id, integration_type, external_id),
		   CONSTRAINT chk_agent_channel_external_id_nonempty CHECK (char_length(trim(external_id)) > 0),
		   CONSTRAINT chk_agent_channel_name_nonempty CHECK (char_length(trim(name)) > 0)
		 );`,
		`CREATE TABLE IF NOT EXISTS agent_thread (
		   id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
		   workspace_id INT NOT NULL REFERENCES workspace(id) ON DELETE CASCADE,
		   channel_id UUID NOT NULL REFERENCES agent_channel(id) ON DELETE CASCADE,
		   agent_id UUID NULL REFERENCES agent_profile(id) ON DELETE SET NULL,
		   external_id TEXT NOT NULL,
		   title TEXT NULL,
		   status TEXT NOT NULL DEFAULT 'open' CHECK (status IN ('open', 'paused', 'closed')),
		   target_run_id UUID NULL REFERENCES agent_run(id) ON DELETE SET NULL,
		   session_id TEXT NULL,
		   metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
		   last_message_at TIMESTAMPTZ NULL,
		   created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
		   updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
		   CONSTRAINT uq_agent_thread_channel_external UNIQUE (channel_id, external_id),
		   CONSTRAINT chk_agent_thread_external_id_nonempty CHECK (char_length(trim(external_id)) > 0)
		 );`,
		`CREATE TABLE IF NOT EXISTS agent_message (
		   id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
		   workspace_id INT NOT NULL REFERENCES workspace(id) ON DELETE CASCADE,
		   channel_id UUID NOT NULL REFERENCES agent_channel(id) ON DELETE CASCADE,
		   thread_id UUID NOT NULL REFERENCES agent_thread(id) ON DELETE CASCADE,
		   agent_id UUID NULL REFERENCES agent_profile(id) ON DELETE SET NULL,
		   task_id UUID NULL REFERENCES agent_task(id) ON DELETE SET NULL,
		   run_id UUID NULL REFERENCES agent_run(id) ON DELETE SET NULL,
		   direction TEXT NOT NULL CHECK (direction IN ('inbound', 'outbound', 'system')),
		   body TEXT NOT NULL,
		   provider_message_id TEXT NULL,
		   delivery_state TEXT NOT NULL DEFAULT 'accepted' CHECK (delivery_state IN ('accepted', 'sent', 'failed')),
		   metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
		   created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
		   updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
		   CONSTRAINT chk_agent_message_body_nonempty CHECK (char_length(trim(body)) > 0)
		 );`,
		`CREATE INDEX IF NOT EXISTS idx_agent_channel_workspace_created
		 ON agent_channel(workspace_id, created_at DESC, id DESC);`,
		`CREATE INDEX IF NOT EXISTS idx_agent_thread_workspace_channel_created
		 ON agent_thread(workspace_id, channel_id, created_at DESC, id DESC);`,
		`CREATE INDEX IF NOT EXISTS idx_agent_thread_workspace_last_message
		 ON agent_thread(workspace_id, last_message_at DESC NULLS LAST, created_at DESC, id DESC);`,
		`CREATE INDEX IF NOT EXISTS idx_agent_message_thread_created
		 ON agent_message(thread_id, created_at DESC, id DESC);`,
		`CREATE INDEX IF NOT EXISTS idx_agent_message_workspace_created
		 ON agent_message(workspace_id, created_at DESC, id DESC);`,
		`CREATE INDEX IF NOT EXISTS idx_agent_task_routing_channel
		 ON agent_task ((routing_json->>'channel'));`,
		`CREATE INDEX IF NOT EXISTS idx_agent_task_routing_thread
		 ON agent_task ((routing_json->>'thread_id'));`,
	}

	for _, stmt := range stmts {
		if _, err := tx.Exec(stmt); err != nil {
			return err
		}
	}
	return nil
}
