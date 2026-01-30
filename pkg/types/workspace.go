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

// WorkspaceToolSetting represents the enabled/disabled state of a single tool for a workspace
type WorkspaceToolSetting struct {
	Id          uint      `json:"id" db:"id"`
	WorkspaceId uint      `json:"workspace_id" db:"workspace_id"`
	ToolName    string    `json:"tool_name" db:"tool_name"`
	Enabled     bool      `json:"enabled" db:"enabled"`
	CreatedAt   time.Time `json:"created_at" db:"created_at"`
	UpdatedAt   time.Time `json:"updated_at" db:"updated_at"`
}

// WorkspaceToolSettings is a collection of tool settings for a workspace
// Provides helper methods for checking tool state
type WorkspaceToolSettings struct {
	WorkspaceId uint
	// DisabledTools is a set of tool names that are disabled
	// Tools not in this map are enabled by default
	DisabledTools map[string]bool
}

// NewWorkspaceToolSettings creates a new WorkspaceToolSettings instance
func NewWorkspaceToolSettings(workspaceId uint) *WorkspaceToolSettings {
	return &WorkspaceToolSettings{
		WorkspaceId:   workspaceId,
		DisabledTools: make(map[string]bool),
	}
}

// IsDisabled returns true if the tool is disabled for this workspace
func (s *WorkspaceToolSettings) IsDisabled(toolName string) bool {
	if s == nil || s.DisabledTools == nil {
		return false
	}
	return s.DisabledTools[toolName]
}

// IsEnabled returns true if the tool is enabled for this workspace
func (s *WorkspaceToolSettings) IsEnabled(toolName string) bool {
	return !s.IsDisabled(toolName)
}

// SetEnabled sets the enabled state for a tool
func (s *WorkspaceToolSettings) SetEnabled(toolName string, enabled bool) {
	if s.DisabledTools == nil {
		s.DisabledTools = make(map[string]bool)
	}
	if enabled {
		delete(s.DisabledTools, toolName)
	} else {
		s.DisabledTools[toolName] = true
	}
}
