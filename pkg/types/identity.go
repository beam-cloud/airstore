package types

import "time"

// MemberRole represents the role of a member in a workspace
type MemberRole string

const (
	RoleAdmin  MemberRole = "admin"
	RoleMember MemberRole = "member"
	RoleViewer MemberRole = "viewer"
)

// WorkspaceMember represents a user who belongs to a workspace
type WorkspaceMember struct {
	Id          uint       `db:"id" json:"id"`
	ExternalId  string     `db:"external_id" json:"external_id"`
	WorkspaceId uint       `db:"workspace_id" json:"workspace_id"`
	Email       string     `db:"email" json:"email"`
	Name        string     `db:"name" json:"name"`
	Role        MemberRole `db:"role" json:"role"`
	CreatedAt   time.Time  `db:"created_at" json:"created_at"`
	UpdatedAt   time.Time  `db:"updated_at" json:"updated_at"`
}

// WorkspaceToken represents an authentication token that maps to a workspace and member
type WorkspaceToken struct {
	Id          uint       `db:"id" json:"id"`
	ExternalId  string     `db:"external_id" json:"external_id"`
	WorkspaceId uint       `db:"workspace_id" json:"workspace_id"`
	MemberId    uint       `db:"member_id" json:"member_id"`
	TokenHash   string     `db:"token_hash" json:"-"`
	Name        string     `db:"name" json:"name"`
	ExpiresAt   *time.Time `db:"expires_at" json:"expires_at,omitempty"`
	CreatedAt   time.Time  `db:"created_at" json:"created_at"`
	LastUsedAt  *time.Time `db:"last_used_at" json:"last_used_at,omitempty"`
}

// IntegrationConnection stores OAuth tokens or API keys for an integration
type IntegrationConnection struct {
	Id              uint       `db:"id" json:"id"`
	ExternalId      string     `db:"external_id" json:"external_id"`
	WorkspaceId     uint       `db:"workspace_id" json:"workspace_id"`
	MemberId        *uint      `db:"member_id" json:"member_id,omitempty"` // nil = workspace-shared
	IntegrationType string     `db:"integration_type" json:"integration_type"`
	Credentials     []byte     `db:"credentials" json:"-"` // Encrypted
	Scope           string     `db:"scope" json:"scope,omitempty"`
	ExpiresAt       *time.Time `db:"expires_at" json:"expires_at,omitempty"`
	CreatedAt       time.Time  `db:"created_at" json:"created_at"`
	UpdatedAt       time.Time  `db:"updated_at" json:"updated_at"`
}

// IsShared returns true if this is a workspace-shared connection
func (c *IntegrationConnection) IsShared() bool {
	return c.MemberId == nil
}

// IntegrationCredentials contains decrypted credentials for tool execution
type IntegrationCredentials struct {
	AccessToken  string            `json:"access_token,omitempty"`
	RefreshToken string            `json:"refresh_token,omitempty"`
	APIKey       string            `json:"api_key,omitempty"`
	ExpiresAt    *time.Time        `json:"expires_at,omitempty"`
	Extra        map[string]string `json:"extra,omitempty"`
}

// TokenValidationResult is returned when validating a workspace token
type TokenValidationResult struct {
	WorkspaceId   uint
	WorkspaceExt  string
	WorkspaceName string
	MemberId      uint
	MemberExt     string
	MemberEmail   string
	MemberRole    MemberRole
}
