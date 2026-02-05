package auth

import (
	"context"
	"errors"

	"github.com/beam-cloud/airstore/pkg/types"
)

type ctxKey int

const authInfoKey ctxKey = iota

var (
	ErrAuthRequired     = errors.New("authentication required")
	ErrAdminRequired    = errors.New("admin access required")
	ErrForbidden        = errors.New("access denied")
	ErrWorkerRequired   = errors.New("worker token required")
)

// Context operations

func WithAuthInfo(ctx context.Context, info *types.AuthInfo) context.Context {
	return context.WithValue(ctx, authInfoKey, info)
}

func AuthInfoFromContext(ctx context.Context) *types.AuthInfo {
	info, _ := ctx.Value(authInfoKey).(*types.AuthInfo)
	return info
}

// Authorization checks (return error if not authorized)

func RequireAuth(ctx context.Context) error {
	if AuthInfoFromContext(ctx) == nil {
		return ErrAuthRequired
	}
	return nil
}

func RequireClusterAdmin(ctx context.Context) error {
	info := AuthInfoFromContext(ctx)
	if info == nil || !info.IsClusterAdmin() {
		return ErrAdminRequired
	}
	return nil
}

func RequireAdmin(ctx context.Context) error {
	info := AuthInfoFromContext(ctx)
	if info == nil || !info.IsAdmin() {
		return ErrAdminRequired
	}
	return nil
}

func RequireWorkspaceAccess(ctx context.Context, workspaceExtId string) error {
	info := AuthInfoFromContext(ctx)
	if info == nil {
		return ErrAuthRequired
	}
	if !info.HasWorkspaceAccess(workspaceExtId) {
		return ErrForbidden
	}
	return nil
}

func RequireWorker(ctx context.Context) error {
	info := AuthInfoFromContext(ctx)
	if info == nil || !info.IsWorker() {
		return ErrWorkerRequired
	}
	return nil
}

func RequireWorkerForPool(ctx context.Context, poolName string) error {
	info := AuthInfoFromContext(ctx)
	if info == nil || !info.IsWorker() {
		return ErrWorkerRequired
	}
	if !info.CanAccessPool(poolName) {
		return ErrForbidden
	}
	return nil
}

// Boolean checks

func IsAuthenticated(ctx context.Context) bool { return AuthInfoFromContext(ctx) != nil }
func IsClusterAdmin(ctx context.Context) bool  { i := AuthInfoFromContext(ctx); return i != nil && i.IsClusterAdmin() }
func IsAdmin(ctx context.Context) bool         { i := AuthInfoFromContext(ctx); return i != nil && i.IsAdmin() }
func IsWorker(ctx context.Context) bool        { i := AuthInfoFromContext(ctx); return i != nil && i.IsWorker() }
func CanWrite(ctx context.Context) bool        { i := AuthInfoFromContext(ctx); return i != nil && i.CanWrite() }

// Field accessors

func WorkspaceId(ctx context.Context) uint {
	if i := AuthInfoFromContext(ctx); i != nil && i.Workspace != nil {
		return i.Workspace.Id
	}
	return 0
}

func WorkspaceExtId(ctx context.Context) string {
	if i := AuthInfoFromContext(ctx); i != nil && i.Workspace != nil {
		return i.Workspace.ExternalId
	}
	return ""
}

func WorkspaceName(ctx context.Context) string {
	if i := AuthInfoFromContext(ctx); i != nil && i.Workspace != nil {
		return i.Workspace.Name
	}
	return ""
}

func MemberId(ctx context.Context) uint {
	if i := AuthInfoFromContext(ctx); i != nil && i.Member != nil {
		return i.Member.Id
	}
	return 0
}

func TokenId(ctx context.Context) uint {
	if i := AuthInfoFromContext(ctx); i != nil {
		return i.TokenId
	}
	return 0
}

func MemberEmail(ctx context.Context) string {
	if i := AuthInfoFromContext(ctx); i != nil && i.Member != nil {
		return i.Member.Email
	}
	return ""
}

func MemberRole(ctx context.Context) types.MemberRole {
	if i := AuthInfoFromContext(ctx); i != nil && i.Member != nil {
		return i.Member.Role
	}
	return ""
}

func PoolName(ctx context.Context) string {
	if i := AuthInfoFromContext(ctx); i != nil && i.Worker != nil {
		return i.Worker.PoolName
	}
	return ""
}
