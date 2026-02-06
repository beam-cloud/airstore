package types

import "time"

// Hook is metadata on a filesystem path that creates tasks when something
// changes at that path. A hook on "/skills" fires when files are created
// or modified there. A hook on "/sources/gmail/inbox" fires when new
// query results appear. The event type (create, modify, source change)
// is passed as context in the task prompt -- not as a filter.
type Hook struct {
	Id                uint      `json:"id" db:"id"`
	ExternalId        string    `json:"external_id" db:"external_id"`
	WorkspaceId       uint      `json:"workspace_id" db:"workspace_id"`
	Path              string    `json:"path" db:"path"`
	Prompt            string    `json:"prompt" db:"prompt"`
	Active            bool      `json:"active" db:"active"`
	CreatedByMemberId *uint     `json:"created_by_member_id,omitempty" db:"created_by_member_id"`
	TokenId           *uint     `json:"-" db:"token_id"`
	EncryptedToken    []byte    `json:"-" db:"encrypted_token"`
	CreatedAt         time.Time `json:"created_at" db:"created_at"`
	UpdatedAt         time.Time `json:"updated_at" db:"updated_at"`
}

// ErrHookNotFound is returned when a hook cannot be found.
type ErrHookNotFound struct {
	ExternalId string
}

func (e *ErrHookNotFound) Error() string {
	return "hook not found: " + e.ExternalId
}
