package apiv1

import (
	"net/http"

	"github.com/beam-cloud/airstore/pkg/auth"
	"github.com/beam-cloud/airstore/pkg/repository"
	"github.com/beam-cloud/airstore/pkg/types"
	"github.com/labstack/echo/v4"
)

type ConnectionsGroup struct {
	g       *echo.Group
	backend repository.BackendRepository
}

func NewConnectionsGroup(g *echo.Group, backend repository.BackendRepository) *ConnectionsGroup {
	cg := &ConnectionsGroup{g: g, backend: backend}
	cg.g.POST("", cg.Create)
	cg.g.GET("", cg.List)
	cg.g.DELETE("/:connection_id", cg.Delete)
	return cg
}

type CreateConnectionRequest struct {
	MemberId        string            `json:"member_id,omitempty"` // Empty = shared
	IntegrationType string            `json:"integration_type"`
	Scope           string            `json:"scope,omitempty"`
	AccessToken     string            `json:"access_token,omitempty"`
	RefreshToken    string            `json:"refresh_token,omitempty"`
	APIKey          string            `json:"api_key,omitempty"`
	Extra           map[string]string `json:"extra,omitempty"`
}

func (cg *ConnectionsGroup) Create(c echo.Context) error {
	ctx := c.Request().Context()
	workspaceId := c.Param("workspace_id")

	var req CreateConnectionRequest
	if err := c.Bind(&req); err != nil {
		return ErrorResponse(c, http.StatusBadRequest, "invalid request")
	}
	if req.IntegrationType == "" {
		return ErrorResponse(c, http.StatusBadRequest, "integration_type required")
	}
	if req.AccessToken == "" && req.APIKey == "" {
		return ErrorResponse(c, http.StatusBadRequest, "access_token or api_key required")
	}

	ws, err := cg.backend.GetWorkspaceByExternalId(ctx, workspaceId)
	if err != nil || ws == nil {
		return ErrorResponse(c, http.StatusNotFound, "workspace not found")
	}

	var memberId *uint
	if req.MemberId != "" {
		// Personal connection - require admin or self
		member, err := cg.backend.GetMember(ctx, req.MemberId)
		if err != nil {
			return ErrorResponse(c, http.StatusInternalServerError, err.Error())
		}
		if member == nil {
			return ErrorResponse(c, http.StatusNotFound, "member not found")
		}
		if member.WorkspaceId != ws.Id {
			return ErrorResponse(c, http.StatusBadRequest, "member not in workspace")
		}

		// Check authorization: admin can create for anyone, members only for themselves
		if !auth.IsAdmin(ctx) && member.Id != auth.MemberId(ctx) {
			return ErrorResponse(c, http.StatusForbidden, "cannot create connection for another member")
		}

		memberId = &member.Id
	} else {
		// Shared connection - require admin
		if !auth.IsAdmin(ctx) {
			return ErrorResponse(c, http.StatusForbidden, "admin access required for shared connections")
		}
	}

	creds := &types.IntegrationCredentials{
		AccessToken:  req.AccessToken,
		RefreshToken: req.RefreshToken,
		APIKey:       req.APIKey,
		Extra:        req.Extra,
	}

	conn, err := cg.backend.SaveConnection(ctx, ws.Id, memberId, req.IntegrationType, creds, req.Scope)
	if err != nil {
		return ErrorResponse(c, http.StatusInternalServerError, err.Error())
	}

	return c.JSON(http.StatusCreated, Response{Success: true, Data: conn})
}

func (cg *ConnectionsGroup) List(c echo.Context) error {
	workspaceId := c.Param("workspace_id")

	ws, err := cg.backend.GetWorkspaceByExternalId(c.Request().Context(), workspaceId)
	if err != nil || ws == nil {
		return ErrorResponse(c, http.StatusNotFound, "workspace not found")
	}

	conns, err := cg.backend.ListConnections(c.Request().Context(), ws.Id)
	if err != nil {
		return ErrorResponse(c, http.StatusInternalServerError, err.Error())
	}

	return c.JSON(http.StatusOK, Response{Success: true, Data: conns})
}

func (cg *ConnectionsGroup) Delete(c echo.Context) error {
	ctx := c.Request().Context()
	connId := c.Param("connection_id")

	// Fetch connection to check permissions
	conn, err := cg.backend.GetConnectionByExternalId(ctx, connId)
	if err != nil {
		return ErrorResponse(c, http.StatusInternalServerError, err.Error())
	}
	if conn == nil {
		return ErrorResponse(c, http.StatusNotFound, "connection not found")
	}

	// Check authorization: shared connections require admin, personal require admin or owner
	if conn.IsShared() {
		if !auth.IsAdmin(ctx) {
			return ErrorResponse(c, http.StatusForbidden, "admin access required for shared connections")
		}
	} else {
		// Personal connection
		if !auth.IsAdmin(ctx) && *conn.MemberId != auth.MemberId(ctx) {
			return ErrorResponse(c, http.StatusForbidden, "cannot delete another member's connection")
		}
	}

	if err := cg.backend.DeleteConnection(ctx, connId); err != nil {
		return ErrorResponse(c, http.StatusInternalServerError, err.Error())
	}

	return c.JSON(http.StatusOK, Response{Success: true})
}
