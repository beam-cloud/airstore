package apiv1

import (
	"net/http"
	"strings"

	"github.com/beam-cloud/airstore/pkg/auth"
	"github.com/beam-cloud/airstore/pkg/repository"
	"github.com/labstack/echo/v4"
)

// WorkspaceAuthConfig holds auth configuration for workspace-scoped APIs
type WorkspaceAuthConfig struct {
	AdminToken string
	Backend    repository.BackendRepository
}

// NewWorkspaceAuthMiddleware creates middleware that validates workspace access.
// It accepts both admin tokens (gateway token) and workspace member tokens.
// The auth context is stored in the request context for downstream handlers.
func NewWorkspaceAuthMiddleware(cfg WorkspaceAuthConfig) echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			workspaceID := c.Param("workspace_id")
			if workspaceID == "" {
				return ErrorResponse(c, http.StatusBadRequest, "workspace_id required")
			}

			// Extract token from Authorization header
			authHeader := c.Request().Header.Get("Authorization")
			token := strings.TrimPrefix(authHeader, "Bearer ")

			var rc *auth.RequestContext

			// Check if it's an admin token
			if cfg.AdminToken != "" && token == cfg.AdminToken {
				// Admin token - get workspace from URL
				workspace, err := cfg.Backend.GetWorkspaceByExternalId(c.Request().Context(), workspaceID)
				if err != nil || workspace == nil {
					return ErrorResponse(c, http.StatusNotFound, "workspace not found")
				}

				rc = &auth.RequestContext{
					WorkspaceId:   workspace.Id,
					WorkspaceExt:  workspace.ExternalId,
					WorkspaceName: workspace.Name,
					IsGatewayAuth: true,
				}
			} else if token != "" && cfg.Backend != nil {
				// Try as workspace token
				result, err := cfg.Backend.ValidateToken(c.Request().Context(), token)
				if err != nil || result == nil {
					return ErrorResponse(c, http.StatusUnauthorized, "invalid token")
				}

				// Verify the token belongs to the requested workspace
				if result.WorkspaceExt != workspaceID {
					return ErrorResponse(c, http.StatusForbidden, "token does not have access to this workspace")
				}

				rc = &auth.RequestContext{
					WorkspaceId:   result.WorkspaceId,
					WorkspaceExt:  result.WorkspaceExt,
					WorkspaceName: result.WorkspaceName,
					MemberId:      result.MemberId,
					MemberExt:     result.MemberExt,
					MemberEmail:   result.MemberEmail,
					MemberRole:    result.MemberRole,
					IsGatewayAuth: false,
				}
			} else if cfg.AdminToken == "" {
				// No admin token configured - allow unauthenticated access (local mode)
				workspace, err := cfg.Backend.GetWorkspaceByExternalId(c.Request().Context(), workspaceID)
				if err != nil || workspace == nil {
					return ErrorResponse(c, http.StatusNotFound, "workspace not found")
				}

				rc = &auth.RequestContext{
					WorkspaceId:   workspace.Id,
					WorkspaceExt:  workspace.ExternalId,
					WorkspaceName: workspace.Name,
					IsGatewayAuth: true,
				}
			} else {
				return ErrorResponse(c, http.StatusUnauthorized, "authorization required")
			}

			// Add auth context to request
			ctx := auth.WithContext(c.Request().Context(), rc)
			c.SetRequest(c.Request().WithContext(ctx))

			return next(c)
		}
	}
}

// RequireAdmin returns middleware that requires admin role or gateway auth.
// Must be used after WorkspaceAuthMiddleware.
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
