package types

import "time"

// Workspace represents a workspace that contains tasks
type Workspace struct {
	Id         uint      `json:"id" db:"id"`                   // Internal ID for joins
	ExternalId string    `json:"external_id" db:"external_id"` // External UUID for API
	Name       string    `json:"name" db:"name"`
	CreatedAt  time.Time `json:"created_at" db:"created_at"`
	UpdatedAt  time.Time `json:"updated_at" db:"updated_at"`
}

// ErrWorkspaceNotFound is returned when a workspace cannot be found
type ErrWorkspaceNotFound struct {
	ExternalId string
	Name       string
}

func (e *ErrWorkspaceNotFound) Error() string {
	if e.ExternalId != "" {
		return "workspace not found: " + e.ExternalId
	}
	return "workspace not found: " + e.Name
}
