package apiv1

import (
	"context"
	"crypto/subtle"
	"fmt"
	"net/http"
	"strings"

	"github.com/beam-cloud/airstore/pkg/auth"
	"github.com/beam-cloud/airstore/pkg/repository"
	"github.com/beam-cloud/airstore/pkg/types"
	"github.com/labstack/echo/v4"
)

// WorkspaceAuthConfig for workspace-scoped API routes.
type WorkspaceAuthConfig struct {
	AdminToken string
	Backend    repository.BackendRepository
}

// resolveCallerWorkspace authenticates a bearer token and resolves the target
// workspace. This is the single source of truth for "who is calling and which
// workspace are they targeting?" used by both the workspace auth middleware
// and the OAuth session endpoint.
//
// For admin/org tokens the caller must supply workspaceID explicitly.
// For member/service tokens the workspace is embedded in the token.
//
// On success the returned AuthInfo always has Workspace populated.
func resolveCallerWorkspace(
	ctx context.Context,
	token string,
	adminToken string,
	workspaceID string,
	backend repository.BackendRepository,
) (*types.AuthInfo, error) {
	// 1. Cluster admin token — workspace_id is required.
	if adminToken != "" && subtle.ConstantTimeCompare([]byte(token), []byte(adminToken)) == 1 {
		if workspaceID == "" {
			return nil, fmt.Errorf("workspace_id required for admin token")
		}
		ws, err := backend.GetWorkspaceByExternalId(ctx, workspaceID)
		if err != nil || ws == nil {
			return nil, fmt.Errorf("workspace not found")
		}
		return &types.AuthInfo{
			TokenType: types.TokenTypeClusterAdmin,
			Workspace: &types.WorkspaceInfo{Id: ws.Id, ExternalId: ws.ExternalId, Name: ws.Name},
		}, nil
	}

	// 2. Validate the bearer token.
	if token == "" || backend == nil {
		return nil, fmt.Errorf("authorization required")
	}
	info, err := backend.AuthorizeToken(ctx, token)
	if err != nil || info == nil {
		return nil, fmt.Errorf("invalid token")
	}

	// 3. Branch on token type.
	switch {
	case info.IsOrganization():
		// Org tokens don't carry a workspace — require explicit workspace_id
		// and verify it belongs to the token's tenant.
		if workspaceID == "" {
			return nil, fmt.Errorf("workspace_id required for organization token")
		}
		ws, err := backend.GetWorkspaceByExternalId(ctx, workspaceID)
		if err != nil || ws == nil || ws.TenantId == nil || *ws.TenantId != info.TenantId {
			// Same error for "not found" and "wrong tenant" to prevent enumeration.
			return nil, fmt.Errorf("workspace not found")
		}
		info.Workspace = &types.WorkspaceInfo{Id: ws.Id, ExternalId: ws.ExternalId, Name: ws.Name}

	default:
		// Member / service tokens carry their workspace in the token.
		if info.Workspace == nil || (workspaceID != "" && info.Workspace.ExternalId != workspaceID) {
			return nil, fmt.Errorf("token does not have access to this workspace")
		}
	}

	return info, nil
}

// NewWorkspaceAuthMiddleware validates workspace access for admin and member tokens.
func NewWorkspaceAuthMiddleware(cfg WorkspaceAuthConfig) echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			workspaceID := c.Param("workspace_id")
			if workspaceID == "" {
				return ErrorResponse(c, http.StatusBadRequest, "workspace_id required")
			}

			ctx := c.Request().Context()
			token := strings.TrimPrefix(c.Request().Header.Get("Authorization"), "Bearer ")
			if token == "" {
				token = c.QueryParam("token")
			}

			// Fast path: no admin token configured — treat as open access.
			if cfg.AdminToken == "" && token == "" {
				ws, err := cfg.Backend.GetWorkspaceByExternalId(ctx, workspaceID)
				if err != nil || ws == nil {
					return ErrorResponse(c, http.StatusNotFound, "workspace not found")
				}
				info := &types.AuthInfo{
					TokenType: types.TokenTypeClusterAdmin,
					Workspace: &types.WorkspaceInfo{Id: ws.Id, ExternalId: ws.ExternalId, Name: ws.Name},
				}
				ctx = auth.WithAuthInfo(ctx, info)
				c.SetRequest(c.Request().WithContext(ctx))
				return next(c)
			}

			info, err := resolveCallerWorkspace(ctx, token, cfg.AdminToken, workspaceID, cfg.Backend)
			if err != nil {
				// Map error messages to appropriate HTTP status codes.
				msg := err.Error()
				switch msg {
				case "authorization required", "invalid token":
					return ErrorResponse(c, http.StatusUnauthorized, msg)
				case "workspace not found":
					return ErrorResponse(c, http.StatusNotFound, msg)
				case "token does not have access to this workspace":
					return ErrorResponse(c, http.StatusForbidden, msg)
				default:
					return ErrorResponse(c, http.StatusBadRequest, msg)
				}
			}

			ctx = auth.WithAuthInfo(ctx, info)
			c.SetRequest(c.Request().WithContext(ctx))
			return next(c)
		}
	}
}

// RequireAdmin middleware requires admin role or cluster admin.
func RequireAdmin() echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			if !auth.IsAdmin(c.Request().Context()) {
				return ErrorResponse(c, http.StatusForbidden, "admin access required")
			}
			return next(c)
		}
	}
}
