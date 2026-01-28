package admin

import (
	"net/http"

	"github.com/beam-cloud/airstore/pkg/repository"
	"github.com/beam-cloud/airstore/pkg/types"
	"github.com/labstack/echo/v4"
)

// APIGroup handles admin API routes
type APIGroup struct {
	backend repository.BackendRepository
	session *SessionManager
}

// NewAPIGroup creates and registers all API routes
func NewAPIGroup(g *echo.Group, backend repository.BackendRepository, session *SessionManager) *APIGroup {
	api := &APIGroup{backend: backend, session: session}

	g.GET("/user", api.GetCurrentUser)

	// Workspaces
	g.GET("/workspaces", api.ListWorkspaces)
	g.POST("/workspaces", api.CreateWorkspace)
	g.GET("/workspaces/:id", api.GetWorkspace)
	g.DELETE("/workspaces/:id", api.DeleteWorkspace)

	// Members (nested under workspace)
	g.GET("/workspaces/:id/members", api.ListMembers)
	g.POST("/workspaces/:id/members", api.CreateMember)

	// Tokens
	g.GET("/workspaces/:id/tokens", api.ListTokens)
	g.POST("/workspaces/:id/tokens", api.CreateToken)

	// Connections
	g.GET("/workspaces/:id/connections", api.ListConnections)
	g.POST("/workspaces/:id/connections", api.CreateConnection)
	g.DELETE("/workspaces/:id/connections/:connId", api.DeleteConnection)

	return api
}

// GetCurrentUser returns the logged-in user info
func (a *APIGroup) GetCurrentUser(c echo.Context) error {
	session := c.Get("session").(*Claims)
	return c.JSON(http.StatusOK, map[string]string{
		"email":   session.Email,
		"name":    session.Name,
		"picture": session.Picture,
	})
}

// --- Workspaces ---

func (a *APIGroup) ListWorkspaces(c echo.Context) error {
	workspaces, err := a.backend.ListWorkspaces(c.Request().Context())
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}
	return c.JSON(http.StatusOK, workspaces)
}

func (a *APIGroup) CreateWorkspace(c echo.Context) error {
	var req struct {
		Name string `json:"name"`
	}
	if err := c.Bind(&req); err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "invalid request")
	}
	if req.Name == "" {
		return echo.NewHTTPError(http.StatusBadRequest, "name required")
	}

	workspace, err := a.backend.CreateWorkspace(c.Request().Context(), req.Name)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}
	return c.JSON(http.StatusCreated, workspace)
}

func (a *APIGroup) GetWorkspace(c echo.Context) error {
	workspace, err := a.backend.GetWorkspaceByExternalId(c.Request().Context(), c.Param("id"))
	if err != nil {
		return echo.NewHTTPError(http.StatusNotFound, "workspace not found")
	}
	return c.JSON(http.StatusOK, workspace)
}

func (a *APIGroup) DeleteWorkspace(c echo.Context) error {
	workspace, err := a.backend.GetWorkspaceByExternalId(c.Request().Context(), c.Param("id"))
	if err != nil {
		return echo.NewHTTPError(http.StatusNotFound, "workspace not found")
	}
	if err := a.backend.DeleteWorkspace(c.Request().Context(), workspace.Id); err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}
	return c.JSON(http.StatusOK, map[string]string{"status": "deleted"})
}

// --- Members ---

func (a *APIGroup) ListMembers(c echo.Context) error {
	workspace, err := a.backend.GetWorkspaceByExternalId(c.Request().Context(), c.Param("id"))
	if err != nil {
		return echo.NewHTTPError(http.StatusNotFound, "workspace not found")
	}
	members, err := a.backend.ListMembers(c.Request().Context(), workspace.Id)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}
	return c.JSON(http.StatusOK, members)
}

func (a *APIGroup) CreateMember(c echo.Context) error {
	workspace, err := a.backend.GetWorkspaceByExternalId(c.Request().Context(), c.Param("id"))
	if err != nil {
		return echo.NewHTTPError(http.StatusNotFound, "workspace not found")
	}

	var req struct {
		Email string `json:"email"`
		Name  string `json:"name"`
		Role  string `json:"role"`
	}
	if err := c.Bind(&req); err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "invalid request")
	}
	if req.Email == "" {
		return echo.NewHTTPError(http.StatusBadRequest, "email required")
	}

	role := types.MemberRole(req.Role)
	if role == "" {
		role = types.RoleAdmin
	}

	member, err := a.backend.CreateMember(c.Request().Context(), workspace.Id, req.Email, req.Name, role)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}
	return c.JSON(http.StatusCreated, member)
}

// --- Tokens ---

func (a *APIGroup) ListTokens(c echo.Context) error {
	workspace, err := a.backend.GetWorkspaceByExternalId(c.Request().Context(), c.Param("id"))
	if err != nil {
		return echo.NewHTTPError(http.StatusNotFound, "workspace not found")
	}
	tokens, err := a.backend.ListTokens(c.Request().Context(), workspace.Id)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}
	return c.JSON(http.StatusOK, tokens)
}

func (a *APIGroup) CreateToken(c echo.Context) error {
	workspace, err := a.backend.GetWorkspaceByExternalId(c.Request().Context(), c.Param("id"))
	if err != nil {
		return echo.NewHTTPError(http.StatusNotFound, "workspace not found")
	}

	var req struct {
		MemberID string `json:"member_id"`
		Name     string `json:"name"`
	}
	if err := c.Bind(&req); err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "invalid request")
	}
	if req.MemberID == "" {
		return echo.NewHTTPError(http.StatusBadRequest, "member_id required")
	}

	member, err := a.backend.GetMember(c.Request().Context(), req.MemberID)
	if err != nil || member == nil {
		return echo.NewHTTPError(http.StatusNotFound, "member not found")
	}
	if member.WorkspaceId != workspace.Id {
		return echo.NewHTTPError(http.StatusBadRequest, "member not in workspace")
	}

	name := req.Name
	if name == "" {
		name = "Admin Token"
	}

	token, rawToken, err := a.backend.CreateToken(c.Request().Context(), workspace.Id, member.Id, name, nil)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}
	return c.JSON(http.StatusCreated, map[string]any{
		"token": rawToken,
		"info":  token,
	})
}

// --- Connections ---

func (a *APIGroup) ListConnections(c echo.Context) error {
	workspace, err := a.backend.GetWorkspaceByExternalId(c.Request().Context(), c.Param("id"))
	if err != nil {
		return echo.NewHTTPError(http.StatusNotFound, "workspace not found")
	}
	connections, err := a.backend.ListConnections(c.Request().Context(), workspace.Id)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}
	return c.JSON(http.StatusOK, connections)
}

func (a *APIGroup) CreateConnection(c echo.Context) error {
	workspace, err := a.backend.GetWorkspaceByExternalId(c.Request().Context(), c.Param("id"))
	if err != nil {
		return echo.NewHTTPError(http.StatusNotFound, "workspace not found")
	}

	var req struct {
		IntegrationType string `json:"integration_type"`
		AccessToken     string `json:"access_token"`
		APIKey          string `json:"api_key"`
	}
	if err := c.Bind(&req); err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "invalid request")
	}
	if req.IntegrationType == "" {
		return echo.NewHTTPError(http.StatusBadRequest, "integration_type required")
	}
	if req.AccessToken == "" && req.APIKey == "" {
		return echo.NewHTTPError(http.StatusBadRequest, "access_token or api_key required")
	}

	creds := &types.IntegrationCredentials{
		AccessToken: req.AccessToken,
		APIKey:      req.APIKey,
	}

	conn, err := a.backend.SaveConnection(c.Request().Context(), workspace.Id, nil, req.IntegrationType, creds, "")
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}
	return c.JSON(http.StatusCreated, conn)
}

func (a *APIGroup) DeleteConnection(c echo.Context) error {
	if err := a.backend.DeleteConnection(c.Request().Context(), c.Param("connId")); err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}
	return c.JSON(http.StatusOK, map[string]string{"status": "deleted"})
}
