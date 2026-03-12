package backend_postgres_migrations

import (
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigration(upRemoveIdleTaskState, downRemoveIdleTaskState)
}

func upRemoveIdleTaskState(tx *sql.Tx) error {
	_, err := tx.Exec(`UPDATE agent_task SET state = 'done' WHERE state = 'idle';`)
	return err
}

func downRemoveIdleTaskState(tx *sql.Tx) error {
	return nil
}
