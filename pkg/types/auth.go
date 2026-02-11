package types

// TokenType represents the type of authentication token.
type TokenType string

const (
	TokenTypeClusterAdmin     TokenType = "cluster_admin"
	TokenTypeWorkspaceMember  TokenType = "workspace_member"
	TokenTypeWorker           TokenType = "worker"
	TokenTypeWorkspaceService TokenType = "workspace_service"
	TokenTypeOrganization     TokenType = "organization"
)

// AuthInfo contains identity information for authenticated requests.
type AuthInfo struct {
	TokenType TokenType
	TokenId   uint   // Internal ID of the token used for authentication
	TenantId  string // Set when the token has a tenant_id (organization tokens)
	Workspace *WorkspaceInfo
	Member    *MemberInfo
	Worker    *WorkerInfo
}

type WorkspaceInfo struct {
	Id         uint
	ExternalId string
	Name       string
}

type MemberInfo struct {
	Id         uint
	ExternalId string
	Email      string
	Role       MemberRole
}

type WorkerInfo struct {
	PoolName string
}

func (a *AuthInfo) IsClusterAdmin() bool {
	return a != nil && a.TokenType == TokenTypeClusterAdmin
}

func (a *AuthInfo) IsWorkspaceMember() bool {
	return a != nil && a.TokenType == TokenTypeWorkspaceMember && a.Workspace != nil
}

func (a *AuthInfo) IsWorkspaceService() bool {
	return a != nil && a.TokenType == TokenTypeWorkspaceService && a.Workspace != nil
}

func (a *AuthInfo) IsWorker() bool {
	return a != nil && a.TokenType == TokenTypeWorker
}

func (a *AuthInfo) IsOrganization() bool {
	return a != nil && a.TokenType == TokenTypeOrganization && a.TenantId != ""
}

func (a *AuthInfo) HasWorkspaceAccess(workspaceExtId string) bool {
	if a == nil {
		return false
	}
	if a.IsClusterAdmin() {
		return true
	}
	if a.IsOrganization() {
		// Organization tokens get workspace access via middleware tenant_id check,
		// which populates a.Workspace. If Workspace is set, tenant match passed.
		return a.Workspace != nil && a.Workspace.ExternalId == workspaceExtId
	}
	if a.IsWorkspaceService() && a.Workspace.ExternalId == workspaceExtId {
		return true
	}
	return a.IsWorkspaceMember() && a.Workspace.ExternalId == workspaceExtId
}

func (a *AuthInfo) IsAdmin() bool {
	if a == nil {
		return false
	}
	if a.IsClusterAdmin() {
		return true
	}
	if a.IsOrganization() {
		return true // org tokens are admin-level within their tenant
	}
	return a.IsWorkspaceMember() && a.Member != nil && a.Member.Role == RoleAdmin
}

func (a *AuthInfo) CanWrite() bool {
	if a == nil {
		return false
	}
	if a.IsClusterAdmin() {
		return true
	}
	if a.IsOrganization() {
		return true // org tokens have full write access within their tenant
	}
	if a.IsWorkspaceService() {
		return true // service tokens have full workspace write access
	}
	if a.IsWorkspaceMember() && a.Member != nil {
		return a.Member.Role == RoleAdmin || a.Member.Role == RoleMember
	}
	return false
}

func (a *AuthInfo) CanAccessPool(poolName string) bool {
	if a == nil || !a.IsWorker() {
		return false
	}
	if a.Worker == nil || a.Worker.PoolName == "" {
		return true
	}
	return a.Worker.PoolName == poolName
}
