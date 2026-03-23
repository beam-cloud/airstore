package backend_postgres_migrations

import (
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigration(upHookDeliveryMode, downHookDeliveryMode)
}

func upHookDeliveryMode(tx *sql.Tx) error {
	if _, err := tx.Exec(`ALTER TABLE filesystem_hooks ADD COLUMN IF NOT EXISTS delivery_mode TEXT NOT NULL DEFAULT 'spawn_task'`); err != nil {
		return err
	}
	if _, err := tx.Exec(`ALTER TABLE filesystem_hooks ADD COLUMN IF NOT EXISTS target_task_id UUID REFERENCES agent_task(id) ON DELETE CASCADE`); err != nil {
		return err
	}
	if _, err := tx.Exec(`ALTER TABLE filesystem_hooks ADD COLUMN IF NOT EXISTS system_managed BOOLEAN NOT NULL DEFAULT FALSE`); err != nil {
		return err
	}
	if _, err := tx.Exec(`ALTER TABLE filesystem_hooks ADD COLUMN IF NOT EXISTS one_shot BOOLEAN NOT NULL DEFAULT FALSE`); err != nil {
		return err
	}
	if _, err := tx.Exec(`CREATE INDEX IF NOT EXISTS idx_filesystem_hooks_target_task ON filesystem_hooks(target_task_id)`); err != nil {
		return err
	}
	if _, err := tx.Exec(`CREATE INDEX IF NOT EXISTS idx_filesystem_hooks_system_managed ON filesystem_hooks(system_managed, workspace_id)`); err != nil {
		return err
	}
	return nil
}

func downHookDeliveryMode(tx *sql.Tx) error {
	if _, err := tx.Exec(`DROP INDEX IF EXISTS idx_filesystem_hooks_system_managed`); err != nil {
		return err
	}
	if _, err := tx.Exec(`DROP INDEX IF EXISTS idx_filesystem_hooks_target_task`); err != nil {
		return err
	}
	if _, err := tx.Exec(`ALTER TABLE filesystem_hooks DROP COLUMN IF EXISTS one_shot`); err != nil {
		return err
	}
	if _, err := tx.Exec(`ALTER TABLE filesystem_hooks DROP COLUMN IF EXISTS system_managed`); err != nil {
		return err
	}
	if _, err := tx.Exec(`ALTER TABLE filesystem_hooks DROP COLUMN IF EXISTS target_task_id`); err != nil {
		return err
	}
	if _, err := tx.Exec(`ALTER TABLE filesystem_hooks DROP COLUMN IF EXISTS delivery_mode`); err != nil {
		return err
	}
	return nil
}
