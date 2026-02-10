package backend_postgres_migrations

import (
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigration(upHookSkill, downHookSkill)
}

func upHookSkill(tx *sql.Tx) error {
	// Add skill_path column to hooks table.
	// This is the airstore path to the skill folder (e.g., /skills/email-triage).
	// When a hook fires, the skill's SKILL.md is read and used as task instructions.
	// The existing prompt field becomes optional additional context.
	_, err := tx.Exec(`ALTER TABLE filesystem_hooks ADD COLUMN IF NOT EXISTS skill_path TEXT NOT NULL DEFAULT ''`)
	return err
}

func downHookSkill(tx *sql.Tx) error {
	_, err := tx.Exec(`ALTER TABLE filesystem_hooks DROP COLUMN IF EXISTS skill_path`)
	return err
}
