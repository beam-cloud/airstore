package apiv1

import (
	"net/http"
	"strings"

	"github.com/beam-cloud/airstore/pkg/repository"
	"github.com/beam-cloud/airstore/pkg/types"
	"github.com/labstack/echo/v4"
)

type AuthGroup struct {
	routerGroup *echo.Group
	backend     repository.BackendRepository
}

func NewAuthGroup(routerGroup *echo.Group, backend repository.BackendRepository) *AuthGroup {
	g := &AuthGroup{routerGroup: routerGroup, backend: backend}
	g.routerGroup.GET("/whoami", g.Whoami)
	return g
}

// Whoami resolves the caller's identity from its Bearer token.
// Works with workspace, org, and service tokens alike.
func (g *AuthGroup) Whoami(c echo.Context) error {
	token := strings.TrimPrefix(c.Request().Header.Get("Authorization"), "Bearer ")
	if token == "" {
		return ErrorResponse(c, http.StatusUnauthorized, "authorization required")
	}

	info, err := g.backend.AuthorizeToken(c.Request().Context(), token)
	if err != nil || info == nil {
		return ErrorResponse(c, http.StatusUnauthorized, "invalid token")
	}

	resp := map[string]interface{}{
		"token_type": string(info.TokenType),
	}

	if info.Workspace != nil {
		resp["workspace_id"] = info.Workspace.ExternalId
		resp["workspace_name"] = info.Workspace.Name
	}

	if info.Member != nil {
		resp["member_id"] = info.Member.ExternalId
		resp["email"] = info.Member.Email
		resp["role"] = string(info.Member.Role)
	}

	if info.TenantId != "" {
		resp["tenant_id"] = info.TenantId
	}

	if info.TokenType == types.TokenTypeWorker && info.Worker != nil {
		resp["pool_name"] = info.Worker.PoolName
	}

	return SuccessResponse(c, resp)
}
