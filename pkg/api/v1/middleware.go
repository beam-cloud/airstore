package apiv1

import (
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

			var info *types.AuthInfo

			switch {
			case cfg.AdminToken != "" && token == cfg.AdminToken:
				ws, err := cfg.Backend.GetWorkspaceByExternalId(ctx, workspaceID)
				if err != nil || ws == nil {
					return ErrorResponse(c, http.StatusNotFound, "workspace not found")
				}
				info = &types.AuthInfo{
					TokenType: types.TokenTypeClusterAdmin,
					Workspace: &types.WorkspaceInfo{Id: ws.Id, ExternalId: ws.ExternalId, Name: ws.Name},
				}

			case token != "" && cfg.Backend != nil:
				var err error
				info, err = cfg.Backend.AuthorizeToken(ctx, token)
				if err != nil || info == nil {
					return ErrorResponse(c, http.StatusUnauthorized, "invalid token")
				}
				if info.Workspace == nil || info.Workspace.ExternalId != workspaceID {
					return ErrorResponse(c, http.StatusForbidden, "token does not have access to this workspace")
				}

			case cfg.AdminToken == "":
				ws, err := cfg.Backend.GetWorkspaceByExternalId(ctx, workspaceID)
				if err != nil || ws == nil {
					return ErrorResponse(c, http.StatusNotFound, "workspace not found")
				}
				info = &types.AuthInfo{
					TokenType: types.TokenTypeClusterAdmin,
					Workspace: &types.WorkspaceInfo{Id: ws.Id, ExternalId: ws.ExternalId, Name: ws.Name},
				}

			default:
				return ErrorResponse(c, http.StatusUnauthorized, "authorization required")
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
