package backend_postgres_migrations

import (
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigration(upTaskWakeAgenda, downTaskWakeAgenda)
}

func upTaskWakeAgenda(tx *sql.Tx) error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS task_wake_agenda_item (
		   task_id UUID NOT NULL REFERENCES agent_task(id) ON DELETE CASCADE,
		   seq INT NOT NULL,
		   item_type TEXT NOT NULL DEFAULT '',
		   title TEXT NOT NULL DEFAULT '',
		   reason TEXT NOT NULL DEFAULT '',
		   created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
		   PRIMARY KEY (task_id, seq)
		 )`,
	}
	for _, stmt := range stmts {
		if _, err := tx.Exec(stmt); err != nil {
			return err
		}
	}
	return nil
}

func downTaskWakeAgenda(tx *sql.Tx) error {
	_, err := tx.Exec(`DROP TABLE IF EXISTS task_wake_agenda_item`)
	return err
}
