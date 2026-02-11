package apiv1

import (
	"net/http"
	"time"

	"github.com/beam-cloud/airstore/pkg/auth"
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
	Name     string `json:"name" validate:"required"`
	TenantId string `json:"tenant_id,omitempty"`
}

type WorkspaceResponse struct {
	ExternalID string  `json:"external_id"`
	Name       string  `json:"name"`
	TenantId   *string `json:"tenant_id,omitempty"`
	CreatedAt  string  `json:"created_at"`
	UpdatedAt  string  `json:"updated_at"`
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

// CreateWorkspace creates a new workspace and its S3 storage bucket.
// When called by an org token, the workspace is auto-tagged with the token's tenant_id.
func (g *WorkspacesGroup) CreateWorkspace(c echo.Context) error {
	ctx := c.Request().Context()

	var req CreateWorkspaceRequest
	if err := c.Bind(&req); err != nil {
		return ErrorResponse(c, http.StatusBadRequest, "invalid request body")
	}

	if req.Name == "" {
		return ErrorResponse(c, http.StatusBadRequest, "name is required")
	}

	// Resolve tenant_id: org tokens auto-set it from the token; admin callers
	// can pass it explicitly in the request body.
	var tenantId *string
	if info := auth.AuthInfoFromContext(ctx); info != nil && info.IsOrganization() {
		tenantId = &info.TenantId
	} else if req.TenantId != "" {
		tenantId = &req.TenantId
	}

	// Create workspace in database
	workspace, err := g.backend.CreateWorkspace(ctx, req.Name, tenantId)
	if err != nil {
		log.Error().Err(err).Str("name", req.Name).Msg("failed to create workspace")
		return ErrorResponse(c, http.StatusInternalServerError, "failed to create workspace")
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

// ListWorkspaces returns all workspaces. Org tokens only see their tenant's workspaces.
func (g *WorkspacesGroup) ListWorkspaces(c echo.Context) error {
	ctx := c.Request().Context()

	var workspaces []*types.Workspace
	var err error

	// Org tokens auto-filter by tenant_id
	if info := auth.AuthInfoFromContext(ctx); info != nil && info.IsOrganization() {
		workspaces, err = g.backend.ListWorkspacesByTenantId(ctx, info.TenantId)
	} else {
		workspaces, err = g.backend.ListWorkspaces(ctx)
	}

	if err != nil {
		log.Error().Err(err).Msg("failed to list workspaces")
		return ErrorResponse(c, http.StatusInternalServerError, "failed to list workspaces")
	}

	// Always return an array, never null
	response := make([]WorkspaceResponse, 0, len(workspaces))
	for _, w := range workspaces {
		response = append(response, workspaceToResponse(w))
	}

	return SuccessResponse(c, response)
}

// GetWorkspace returns a workspace by external ID.
func (g *WorkspacesGroup) GetWorkspace(c echo.Context) error {
	externalId := c.Param("id")

	workspace, err := g.backend.GetWorkspaceByExternalId(c.Request().Context(), externalId)
	if err != nil {
		if _, ok := err.(*types.ErrWorkspaceNotFound); ok {
			return ErrorResponse(c, http.StatusNotFound, "workspace not found")
		}
		log.Error().Err(err).Str("workspace", externalId).Msg("failed to get workspace")
		return ErrorResponse(c, http.StatusInternalServerError, "failed to get workspace")
	}

	return SuccessResponse(c, workspaceToResponse(workspace))
}

// DeleteWorkspace deletes a workspace by external ID.
func (g *WorkspacesGroup) DeleteWorkspace(c echo.Context) error {
	externalId := c.Param("id")

	// Get workspace first to get internal ID
	workspace, err := g.backend.GetWorkspaceByExternalId(c.Request().Context(), externalId)
	if err != nil {
		if _, ok := err.(*types.ErrWorkspaceNotFound); ok {
			return ErrorResponse(c, http.StatusNotFound, "workspace not found")
		}
		log.Error().Err(err).Str("workspace", externalId).Msg("failed to get workspace for deletion")
		return ErrorResponse(c, http.StatusInternalServerError, "failed to delete workspace")
	}

	if err := g.backend.DeleteWorkspace(c.Request().Context(), workspace.Id); err != nil {
		log.Error().Err(err).Str("workspace", externalId).Msg("failed to delete workspace")
		return ErrorResponse(c, http.StatusInternalServerError, "failed to delete workspace")
	}

	return SuccessResponse(c, nil)
}

func workspaceToResponse(w *types.Workspace) WorkspaceResponse {
	return WorkspaceResponse{
		ExternalID: w.ExternalId,
		Name:       w.Name,
		TenantId:   w.TenantId,
		CreatedAt:  w.CreatedAt.Format(time.RFC3339),
		UpdatedAt:  w.UpdatedAt.Format(time.RFC3339),
	}
}
