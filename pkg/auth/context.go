package auth

import (
	"context"

	"github.com/beam-cloud/airstore/pkg/types"
)

type ctxKey int

const requestCtxKey ctxKey = iota

// RequestContext contains identity information for authenticated requests
type RequestContext struct {
	WorkspaceId   uint
	WorkspaceExt  string
	WorkspaceName string
	MemberId      uint
	MemberExt     string
	MemberEmail   string
	MemberRole    types.MemberRole
	IsGatewayAuth bool // True if authenticated with gateway token (admin access)
}

// WithContext adds a RequestContext to the context
func WithContext(ctx context.Context, rc *RequestContext) context.Context {
	return context.WithValue(ctx, requestCtxKey, rc)
}

// FromContext extracts the RequestContext from the context
func FromContext(ctx context.Context) *RequestContext {
	rc, _ := ctx.Value(requestCtxKey).(*RequestContext)
	return rc
}

// IsAuthenticated returns true if the context has valid auth
func IsAuthenticated(ctx context.Context) bool {
	rc := FromContext(ctx)
	return rc != nil && (rc.WorkspaceId > 0 || rc.IsGatewayAuth)
}

// HasWorkspace returns true if the context has workspace identity
func HasWorkspace(ctx context.Context) bool {
	rc := FromContext(ctx)
	return rc != nil && rc.WorkspaceId > 0
}

// WorkspaceId returns the workspace ID from context
func WorkspaceId(ctx context.Context) uint {
	if rc := FromContext(ctx); rc != nil {
		return rc.WorkspaceId
	}
	return 0
}

// MemberId returns the member ID from context
func MemberId(ctx context.Context) uint {
	if rc := FromContext(ctx); rc != nil {
		return rc.MemberId
	}
	return 0
}

// MemberRole returns the member role from context
func MemberRole(ctx context.Context) types.MemberRole {
	if rc := FromContext(ctx); rc != nil {
		return rc.MemberRole
	}
	return ""
}

// IsAdmin returns true if the member has admin role
func IsAdmin(ctx context.Context) bool {
	return MemberRole(ctx) == types.RoleAdmin || FromContext(ctx).IsGatewayAuth
}

// CanWrite returns true if the member can write
func CanWrite(ctx context.Context) bool {
	role := MemberRole(ctx)
	return role == types.RoleAdmin || role == types.RoleMember || FromContext(ctx).IsGatewayAuth
}
