package backend_postgres_migrations

import (
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigration(upOrchestrationOutboxInbox, downOrchestrationOutboxInbox)
}

func upOrchestrationOutboxInbox(tx *sql.Tx) error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS orchestration_outbox (
		   id BIGSERIAL PRIMARY KEY,
		   event_type TEXT NOT NULL,
		   dedupe_key TEXT NOT NULL UNIQUE,
		   payload_json JSONB NOT NULL DEFAULT '{}'::jsonb,
		   available_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
		   published_at TIMESTAMPTZ NULL,
		   attempts INT NOT NULL DEFAULT 0,
		   last_error TEXT NULL,
		   created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
		   updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
		 );`,
		`CREATE INDEX IF NOT EXISTS idx_orchestration_outbox_ready
		 ON orchestration_outbox (available_at, id)
		 WHERE published_at IS NULL;`,
		`CREATE TABLE IF NOT EXISTS orchestration_inbox_results (
		   result_key TEXT PRIMARY KEY,
		   stream_id TEXT NOT NULL,
		   created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
		 );`,
		`CREATE TABLE IF NOT EXISTS orchestration_retry_guard (
		   guard_key TEXT PRIMARY KEY,
		   created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
		 );`,
	}

	for _, stmt := range stmts {
		if _, err := tx.Exec(stmt); err != nil {
			return err
		}
	}
	return nil
}

func downOrchestrationOutboxInbox(tx *sql.Tx) error {
	stmts := []string{
		`DROP TABLE IF EXISTS orchestration_retry_guard;`,
		`DROP TABLE IF EXISTS orchestration_inbox_results;`,
		`DROP INDEX IF EXISTS idx_orchestration_outbox_ready;`,
		`DROP TABLE IF EXISTS orchestration_outbox;`,
	}

	for _, stmt := range stmts {
		if _, err := tx.Exec(stmt); err != nil {
			return err
		}
	}
	return nil
}
