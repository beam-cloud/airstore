package auth

import (
	"context"
	"errors"

	"github.com/beam-cloud/airstore/pkg/types"
)

type ctxKey int

const authInfoKey ctxKey = iota

var (
	ErrAuthRequired  = errors.New("authentication required")
	ErrAdminRequired = errors.New("admin access required")
	ErrForbidden     = errors.New("access denied")
	ErrWorkerRequired = errors.New("worker token required")
)

// --- Context get/set ---

func WithAuthInfo(ctx context.Context, info *types.AuthInfo) context.Context {
	return context.WithValue(ctx, authInfoKey, info)
}

func AuthInfoFromContext(ctx context.Context) *types.AuthInfo {
	info, _ := ctx.Value(authInfoKey).(*types.AuthInfo)
	return info
}

// --- Authorization checks ---

func RequireAuth(ctx context.Context) error {
	if AuthInfoFromContext(ctx) == nil {
		return ErrAuthRequired
	}
	return nil
}

func RequireClusterAdmin(ctx context.Context) error {
	if i := AuthInfoFromContext(ctx); i == nil || !i.IsClusterAdmin() {
		return ErrAdminRequired
	}
	return nil
}

func RequireAdmin(ctx context.Context) error {
	if i := AuthInfoFromContext(ctx); i == nil || !i.IsAdmin() {
		return ErrAdminRequired
	}
	return nil
}

func RequireWorkspaceAccess(ctx context.Context, workspaceExtId string) error {
	i := AuthInfoFromContext(ctx)
	if i == nil {
		return ErrAuthRequired
	}
	if !i.HasWorkspaceAccess(workspaceExtId) {
		return ErrForbidden
	}
	return nil
}

func RequireWorker(ctx context.Context) error {
	if i := AuthInfoFromContext(ctx); i == nil || !i.IsWorker() {
		return ErrWorkerRequired
	}
	return nil
}

func RequireWorkerForPool(ctx context.Context, poolName string) error {
	i := AuthInfoFromContext(ctx)
	if i == nil || !i.IsWorker() {
		return ErrWorkerRequired
	}
	if !i.CanAccessPool(poolName) {
		return ErrForbidden
	}
	return nil
}

// ResolveWorkspaceExtId returns the effective workspace external ID.
// Uses explicit if non-empty, otherwise falls back to the token's workspace.
// Single context lookup. Validates access.
func ResolveWorkspaceExtId(ctx context.Context, explicit string) (string, error) {
	info := AuthInfoFromContext(ctx)
	if info == nil {
		return "", ErrAuthRequired
	}
	wsExtId := explicit
	if wsExtId == "" && info.Workspace != nil {
		wsExtId = info.Workspace.ExternalId
	}
	if wsExtId == "" {
		return "", ErrAuthRequired
	}
	if !info.HasWorkspaceAccess(wsExtId) {
		return "", ErrForbidden
	}
	return wsExtId, nil
}

// --- Boolean checks ---

func IsAuthenticated(ctx context.Context) bool { i := AuthInfoFromContext(ctx); return i != nil }
func IsClusterAdmin(ctx context.Context) bool  { i := AuthInfoFromContext(ctx); return i != nil && i.IsClusterAdmin() }
func IsAdmin(ctx context.Context) bool         { i := AuthInfoFromContext(ctx); return i != nil && i.IsAdmin() }
func IsWorker(ctx context.Context) bool        { i := AuthInfoFromContext(ctx); return i != nil && i.IsWorker() }
func CanWrite(ctx context.Context) bool        { i := AuthInfoFromContext(ctx); return i != nil && i.CanWrite() }

// --- Field accessors ---

func TokenId(ctx context.Context) uint         { i := AuthInfoFromContext(ctx); if i != nil { return i.TokenId }; return 0 }
func WorkspaceId(ctx context.Context) uint     { i := AuthInfoFromContext(ctx); if i != nil && i.Workspace != nil { return i.Workspace.Id }; return 0 }
func WorkspaceExtId(ctx context.Context) string { i := AuthInfoFromContext(ctx); if i != nil && i.Workspace != nil { return i.Workspace.ExternalId }; return "" }
func WorkspaceName(ctx context.Context) string  { i := AuthInfoFromContext(ctx); if i != nil && i.Workspace != nil { return i.Workspace.Name }; return "" }
func MemberId(ctx context.Context) uint         { i := AuthInfoFromContext(ctx); if i != nil && i.Member != nil { return i.Member.Id }; return 0 }
func MemberEmail(ctx context.Context) string    { i := AuthInfoFromContext(ctx); if i != nil && i.Member != nil { return i.Member.Email }; return "" }
func MemberRole(ctx context.Context) types.MemberRole { i := AuthInfoFromContext(ctx); if i != nil && i.Member != nil { return i.Member.Role }; return "" }
func PoolName(ctx context.Context) string       { i := AuthInfoFromContext(ctx); if i != nil && i.Worker != nil { return i.Worker.PoolName }; return "" }
