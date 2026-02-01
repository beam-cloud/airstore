package types

// TokenType represents the type of authentication token.
type TokenType string

const (
	TokenTypeClusterAdmin    TokenType = "cluster_admin"
	TokenTypeWorkspaceMember TokenType = "workspace_member"
	TokenTypeWorker          TokenType = "worker"
)

// AuthInfo contains identity information for authenticated requests.
type AuthInfo struct {
	TokenType TokenType
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

func (a *AuthInfo) IsWorker() bool {
	return a != nil && a.TokenType == TokenTypeWorker
}

func (a *AuthInfo) HasWorkspaceAccess(workspaceExtId string) bool {
	if a == nil {
		return false
	}
	if a.IsClusterAdmin() {
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
	return a.IsWorkspaceMember() && a.Member != nil && a.Member.Role == RoleAdmin
}

func (a *AuthInfo) CanWrite() bool {
	if a == nil {
		return false
	}
	if a.IsClusterAdmin() {
		return true
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
