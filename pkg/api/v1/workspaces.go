package apiv1

import (
	"net/http"

	"github.com/beam-cloud/airstore/pkg/repository"
	"github.com/beam-cloud/airstore/pkg/types"
	"github.com/labstack/echo/v4"
)

type WorkspacesGroup struct {
	routerGroup *echo.Group
	backend     repository.BackendRepository
}

type CreateWorkspaceRequest struct {
	Name string `json:"name" validate:"required"`
}

type WorkspaceResponse struct {
	ID        string `json:"id"` // External ID for API
	Name      string `json:"name"`
	CreatedAt string `json:"created_at"`
	UpdatedAt string `json:"updated_at"`
}

func NewWorkspacesGroup(routerGroup *echo.Group, backend repository.BackendRepository) *WorkspacesGroup {
	g := &WorkspacesGroup{
		routerGroup: routerGroup,
		backend:     backend,
	}
	g.registerRoutes()
	return g
}

func (g *WorkspacesGroup) registerRoutes() {
	g.routerGroup.POST("", g.CreateWorkspace)
	g.routerGroup.GET("", g.ListWorkspaces)
	g.routerGroup.GET("/:id", g.GetWorkspace)
	g.routerGroup.DELETE("/:id", g.DeleteWorkspace)
}

// CreateWorkspace creates a new workspace
func (g *WorkspacesGroup) CreateWorkspace(c echo.Context) error {
	var req CreateWorkspaceRequest
	if err := c.Bind(&req); err != nil {
		return ErrorResponse(c, http.StatusBadRequest, "invalid request body")
	}

	if req.Name == "" {
		return ErrorResponse(c, http.StatusBadRequest, "name is required")
	}

	workspace, err := g.backend.CreateWorkspace(c.Request().Context(), req.Name)
	if err != nil {
		return ErrorResponse(c, http.StatusInternalServerError, err.Error())
	}

	return c.JSON(http.StatusCreated, Response{
		Success: true,
		Data:    workspaceToResponse(workspace),
	})
}

// ListWorkspaces returns all workspaces
func (g *WorkspacesGroup) ListWorkspaces(c echo.Context) error {
	workspaces, err := g.backend.ListWorkspaces(c.Request().Context())
	if err != nil {
		return ErrorResponse(c, http.StatusInternalServerError, err.Error())
	}

	var response []WorkspaceResponse
	for _, w := range workspaces {
		response = append(response, workspaceToResponse(w))
	}

	return SuccessResponse(c, response)
}

// GetWorkspace returns a workspace by external ID
func (g *WorkspacesGroup) GetWorkspace(c echo.Context) error {
	externalId := c.Param("id")

	workspace, err := g.backend.GetWorkspaceByExternalId(c.Request().Context(), externalId)
	if err != nil {
		if _, ok := err.(*types.ErrWorkspaceNotFound); ok {
			return ErrorResponse(c, http.StatusNotFound, "workspace not found")
		}
		return ErrorResponse(c, http.StatusInternalServerError, err.Error())
	}

	return SuccessResponse(c, workspaceToResponse(workspace))
}

// DeleteWorkspace deletes a workspace by external ID
func (g *WorkspacesGroup) DeleteWorkspace(c echo.Context) error {
	externalId := c.Param("id")

	// Get workspace first to get internal ID
	workspace, err := g.backend.GetWorkspaceByExternalId(c.Request().Context(), externalId)
	if err != nil {
		if _, ok := err.(*types.ErrWorkspaceNotFound); ok {
			return ErrorResponse(c, http.StatusNotFound, "workspace not found")
		}
		return ErrorResponse(c, http.StatusInternalServerError, err.Error())
	}

	if err := g.backend.DeleteWorkspace(c.Request().Context(), workspace.Id); err != nil {
		return ErrorResponse(c, http.StatusInternalServerError, err.Error())
	}

	return SuccessResponse(c, nil)
}

func workspaceToResponse(w *types.Workspace) WorkspaceResponse {
	return WorkspaceResponse{
		ID:        w.ExternalId,
		Name:      w.Name,
		CreatedAt: w.CreatedAt.Format("2006-01-02T15:04:05Z07:00"),
		UpdatedAt: w.UpdatedAt.Format("2006-01-02T15:04:05Z07:00"),
	}
}
