package types

import (
	"encoding/json"
	"time"
)

// WorkspaceToolProviderType represents the type of tool provider
type WorkspaceToolProviderType string

const (
	// ProviderTypeMCP is an MCP server (stdio or remote)
	ProviderTypeMCP WorkspaceToolProviderType = "mcp"
	// Future: ProviderTypeBuiltin, ProviderTypeHTTP, etc.
)

// WorkspaceTool represents a workspace-scoped tool provider stored in the database
type WorkspaceTool struct {
	Id                uint                      `json:"id" db:"id"`
	ExternalId        string                    `json:"external_id" db:"external_id"`
	WorkspaceId       uint                      `json:"workspace_id" db:"workspace_id"`
	Name              string                    `json:"name" db:"name"`
	ProviderType      WorkspaceToolProviderType `json:"provider_type" db:"provider_type"`
	Config            json.RawMessage           `json:"config" db:"config"`           // Serialized MCPServerConfig
	Manifest          json.RawMessage           `json:"manifest,omitempty" db:"manifest"` // Cached tools/list output
	CreatedByMemberId *uint                     `json:"created_by_member_id,omitempty" db:"created_by_member_id"`
	CreatedAt         time.Time                 `json:"created_at" db:"created_at"`
	UpdatedAt         time.Time                 `json:"updated_at" db:"updated_at"`
}

// GetMCPConfig deserializes the config as MCPServerConfig
func (t *WorkspaceTool) GetMCPConfig() (*MCPServerConfig, error) {
	if t.ProviderType != ProviderTypeMCP {
		return nil, nil
	}
	var cfg MCPServerConfig
	if err := json.Unmarshal(t.Config, &cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}

// SetMCPConfig serializes an MCPServerConfig into the config field
func (t *WorkspaceTool) SetMCPConfig(cfg *MCPServerConfig) error {
	data, err := json.Marshal(cfg)
	if err != nil {
		return err
	}
	t.Config = data
	t.ProviderType = ProviderTypeMCP
	return nil
}

// WorkspaceToolOrigin indicates where a tool comes from
type WorkspaceToolOrigin string

const (
	ToolOriginGlobal    WorkspaceToolOrigin = "global"
	ToolOriginWorkspace WorkspaceToolOrigin = "workspace"
)

// ResolvedTool represents a tool with its origin and metadata for listing
type ResolvedTool struct {
	Name        string              `json:"name"`
	Help        string              `json:"help,omitempty"`
	Origin      WorkspaceToolOrigin `json:"origin"`
	ExternalId  string              `json:"external_id,omitempty"` // Only for workspace tools
	Enabled     bool                `json:"enabled"`
	ToolCount   int                 `json:"tool_count,omitempty"` // For MCP servers
}

// CreateWorkspaceToolRequest is the API request to create a workspace tool
type CreateWorkspaceToolRequest struct {
	Name         string           `json:"name"`
	ProviderType string           `json:"provider_type"` // "mcp"
	MCP          *MCPServerConfig `json:"mcp,omitempty"`
}

// WorkspaceToolResponse is the API response for a workspace tool
type WorkspaceToolResponse struct {
	ExternalId   string    `json:"external_id"`
	Name         string    `json:"name"`
	ProviderType string    `json:"provider_type"`
	ToolCount    int       `json:"tool_count,omitempty"`
	CreatedAt    time.Time `json:"created_at"`
	UpdatedAt    time.Time `json:"updated_at"`
}

// ErrWorkspaceToolNotFound is returned when a workspace tool cannot be found
type ErrWorkspaceToolNotFound struct {
	Name       string
	ExternalId string
}

func (e *ErrWorkspaceToolNotFound) Error() string {
	if e.ExternalId != "" {
		return "workspace tool not found: " + e.ExternalId
	}
	return "workspace tool not found: " + e.Name
}

// ErrWorkspaceToolExists is returned when a workspace tool already exists
type ErrWorkspaceToolExists struct {
	Name string
}

func (e *ErrWorkspaceToolExists) Error() string {
	return "workspace tool already exists: " + e.Name
}
