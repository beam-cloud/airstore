package types

import "time"

// HookTrigger specifies what filesystem event fires a hook.
type HookTrigger string

const (
	HookTriggerOnCreate   HookTrigger = "on_create"   // New file created under path
	HookTriggerOnWrite    HookTrigger = "on_write"     // File written/modified under path
	HookTriggerOnChange   HookTrigger = "on_change"    // Source query results changed
	HookTriggerOnSchedule HookTrigger = "on_schedule"  // Cron schedule (future)
)

// Hook is metadata on a filesystem path that implicitly creates tasks
// when events occur. A hook on "/inbox/contracts" with trigger on_create
// will spawn a task whenever a file is dropped into that folder.
type Hook struct {
	Id                uint        `json:"id" db:"id"`
	ExternalId        string      `json:"external_id" db:"external_id"`
	WorkspaceId       uint        `json:"workspace_id" db:"workspace_id"`
	Path              string      `json:"path" db:"path"`           // Watched path: "/inbox/contracts"
	Prompt            string      `json:"prompt" db:"prompt"`       // Task instructions
	Trigger           HookTrigger `json:"trigger" db:"trigger"`     // What fires this hook
	Schedule          string      `json:"schedule,omitempty" db:"schedule"` // Cron expression (future)
	Active            bool        `json:"active" db:"active"`
	CreatedByMemberId *uint       `json:"created_by_member_id,omitempty" db:"created_by_member_id"`
	TokenId           *uint       `json:"-" db:"token_id"`          // FK to token used at creation (for validity checks)
	EncryptedToken    []byte      `json:"-" db:"encrypted_token"`   // Encrypted raw token for task execution
	CreatedAt         time.Time   `json:"created_at" db:"created_at"`
	UpdatedAt         time.Time   `json:"updated_at" db:"updated_at"`
}

// ErrHookNotFound is returned when a hook cannot be found.
type ErrHookNotFound struct {
	ExternalId string
}

func (e *ErrHookNotFound) Error() string {
	return "hook not found: " + e.ExternalId
}
