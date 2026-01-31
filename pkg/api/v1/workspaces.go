package apiv1

import (
	"net/http"

	"github.com/beam-cloud/airstore/pkg/clients"
	"github.com/beam-cloud/airstore/pkg/repository"
	"github.com/beam-cloud/airstore/pkg/types"
	"github.com/labstack/echo/v4"
	"github.com/rs/zerolog/log"
)

type WorkspacesGroup struct {
	routerGroup   *echo.Group
	backend       repository.BackendRepository
	storageClient *clients.StorageClient
}

type CreateWorkspaceRequest struct {
	Name string `json:"name" validate:"required"`
}

type WorkspaceResponse struct {
	ExternalID string `json:"external_id"`
	Name       string `json:"name"`
	CreatedAt  string `json:"created_at"`
	UpdatedAt  string `json:"updated_at"`
}

// NewWorkspacesGroup creates a new workspaces API group.
// storageClient can be nil if workspace storage is not configured.
func NewWorkspacesGroup(routerGroup *echo.Group, backend repository.BackendRepository, storageClient *clients.StorageClient) *WorkspacesGroup {
	g := &WorkspacesGroup{
		routerGroup:   routerGroup,
		backend:       backend,
		storageClient: storageClient,
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

// CreateWorkspace creates a new workspace and its S3 storage bucket
func (g *WorkspacesGroup) CreateWorkspace(c echo.Context) error {
	ctx := c.Request().Context()

	var req CreateWorkspaceRequest
	if err := c.Bind(&req); err != nil {
		return ErrorResponse(c, http.StatusBadRequest, "invalid request body")
	}

	if req.Name == "" {
		return ErrorResponse(c, http.StatusBadRequest, "name is required")
	}

	// Create workspace in database
	workspace, err := g.backend.CreateWorkspace(ctx, req.Name)
	if err != nil {
		return ErrorResponse(c, http.StatusInternalServerError, err.Error())
	}

	// Create S3 bucket for workspace storage
	if g.storageClient != nil {
		bucketName, err := g.storageClient.CreateWorkspaceBucket(ctx, workspace.ExternalId)
		if err != nil {
			// Log error but don't fail workspace creation - bucket can be created later
			log.Error().
				Err(err).
				Str("workspace", workspace.ExternalId).
				Str("bucket", bucketName).
				Msg("failed to create workspace storage bucket")
		} else {
			log.Info().
				Str("workspace", workspace.ExternalId).
				Str("bucket", bucketName).
				Msg("created workspace storage bucket")
		}
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
		ExternalID: w.ExternalId,
		Name:       w.Name,
		CreatedAt:  w.CreatedAt.Format("2006-01-02T15:04:05Z07:00"),
		UpdatedAt:  w.UpdatedAt.Format("2006-01-02T15:04:05Z07:00"),
	}
}
