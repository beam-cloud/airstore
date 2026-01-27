package apiv1

import (
	"net/http"

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

	ws, err := cg.backend.GetWorkspaceByExternalId(c.Request().Context(), workspaceId)
	if err != nil || ws == nil {
		return ErrorResponse(c, http.StatusNotFound, "workspace not found")
	}

	var memberId *uint
	if req.MemberId != "" {
		member, err := cg.backend.GetMember(c.Request().Context(), req.MemberId)
		if err != nil {
			return ErrorResponse(c, http.StatusInternalServerError, err.Error())
		}
		if member == nil {
			return ErrorResponse(c, http.StatusNotFound, "member not found")
		}
		if member.WorkspaceId != ws.Id {
			return ErrorResponse(c, http.StatusBadRequest, "member not in workspace")
		}
		memberId = &member.Id
	}

	creds := &types.IntegrationCredentials{
		AccessToken:  req.AccessToken,
		RefreshToken: req.RefreshToken,
		APIKey:       req.APIKey,
		Extra:        req.Extra,
	}

	conn, err := cg.backend.SaveConnection(c.Request().Context(), ws.Id, memberId, req.IntegrationType, creds, req.Scope)
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
	connId := c.Param("connection_id")

	if err := cg.backend.DeleteConnection(c.Request().Context(), connId); err != nil {
		return ErrorResponse(c, http.StatusInternalServerError, err.Error())
	}

	return c.JSON(http.StatusOK, Response{Success: true})
}
