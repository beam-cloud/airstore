package backend_postgres_migrations

import (
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigration(upFollowupBaseline, downFollowupBaseline)
}

func upFollowupBaseline(tx *sql.Tx) error {
	_, err := tx.Exec(`ALTER TABLE filesystem_queries ADD COLUMN IF NOT EXISTS baseline_item_ids TEXT[] DEFAULT NULL`)
	return err
}

func downFollowupBaseline(tx *sql.Tx) error {
	_, err := tx.Exec(`ALTER TABLE filesystem_queries DROP COLUMN IF EXISTS baseline_item_ids`)
	return err
}
