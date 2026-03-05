package types

import "time"

type ScheduledTask struct {
	ID                string     `json:"id" db:"id"`
	ExternalID        string     `json:"external_id" db:"external_id"`
	WorkspaceID       uint       `json:"workspace_id" db:"workspace_id"`
	AgentID           string     `json:"agent_id" db:"agent_id"`
	CronExpr          string     `json:"cron_expr" db:"cron_expr"`
	Timezone          string     `json:"timezone" db:"timezone"`
	Prompt            string     `json:"prompt" db:"prompt"`
	SkillPaths        []string   `json:"skill_paths" db:"skill_paths"`
	Active            bool       `json:"active" db:"active"`
	NextRunAt         time.Time  `json:"next_run_at" db:"next_run_at"`
	LastRunAt         *time.Time `json:"last_run_at,omitempty" db:"last_run_at"`
	TokenID           *uint      `json:"-" db:"token_id"`
	EncryptedToken    []byte     `json:"-" db:"encrypted_token"`
	CreatedByMemberID *uint      `json:"created_by_member_id,omitempty" db:"created_by_member_id"`
	CreatedAt         time.Time  `json:"created_at" db:"created_at"`
	UpdatedAt         time.Time  `json:"updated_at" db:"updated_at"`
}

type ErrScheduledTaskNotFound struct {
	ExternalID string
}

func (e *ErrScheduledTaskNotFound) Error() string {
	return "scheduled task not found: " + e.ExternalID
}
