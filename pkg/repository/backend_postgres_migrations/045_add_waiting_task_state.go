package backend_postgres_migrations

import (
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigration(upAddWaitingTaskState, downAddWaitingTaskState)
}

func upAddWaitingTaskState(tx *sql.Tx) error {
	_, err := tx.Exec(`ALTER TYPE agent_task_state ADD VALUE IF NOT EXISTS 'waiting' AFTER 'running';`)
	return err
}

func downAddWaitingTaskState(tx *sql.Tx) error {
	return nil
}
