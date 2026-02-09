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
	g.routerGroup.PUT("/:id/visibility", g.SetVisibility)
	g.routerGroup.PUT("/:id/slug", g.SetSlug)
	g.routerGroup.POST("/:id/fork", g.ForkWorkspace)
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

// SetVisibility sets workspace visibility (public/private)
func (g *WorkspacesGroup) SetVisibility(c echo.Context) error {
	externalId := c.Param("id")

	var req struct {
		Visibility string `json:"visibility"`
	}
	if err := c.Bind(&req); err != nil {
		return ErrorResponse(c, http.StatusBadRequest, "invalid request body")
	}

	visibility := types.WorkspaceVisibility(req.Visibility)
	if visibility != types.VisibilityPublic && visibility != types.VisibilityPrivate {
		return ErrorResponse(c, http.StatusBadRequest, "visibility must be 'public' or 'private'")
	}

	ctx := c.Request().Context()
	workspace, err := g.backend.GetWorkspaceByExternalId(ctx, externalId)
	if err != nil {
		if _, ok := err.(*types.ErrWorkspaceNotFound); ok {
			return ErrorResponse(c, http.StatusNotFound, "workspace not found")
		}
		return ErrorResponse(c, http.StatusInternalServerError, err.Error())
	}

	if err := g.backend.SetWorkspaceVisibility(ctx, workspace.Id, visibility); err != nil {
		return ErrorResponse(c, http.StatusInternalServerError, err.Error())
	}

	return SuccessResponse(c, map[string]string{"visibility": string(visibility)})
}

// SetSlug sets workspace vanity slug for public access
func (g *WorkspacesGroup) SetSlug(c echo.Context) error {
	externalId := c.Param("id")

	var req struct {
		Slug string `json:"slug"`
	}
	if err := c.Bind(&req); err != nil {
		return ErrorResponse(c, http.StatusBadRequest, "invalid request body")
	}

	if req.Slug == "" {
		return ErrorResponse(c, http.StatusBadRequest, "slug is required")
	}

	ctx := c.Request().Context()
	workspace, err := g.backend.GetWorkspaceByExternalId(ctx, externalId)
	if err != nil {
		if _, ok := err.(*types.ErrWorkspaceNotFound); ok {
			return ErrorResponse(c, http.StatusNotFound, "workspace not found")
		}
		return ErrorResponse(c, http.StatusInternalServerError, err.Error())
	}

	// Check slug is not already taken
	existing, err := g.backend.GetWorkspaceBySlug(ctx, req.Slug)
	if err != nil {
		return ErrorResponse(c, http.StatusInternalServerError, err.Error())
	}
	if existing != nil && existing.Id != workspace.Id {
		return ErrorResponse(c, http.StatusConflict, "slug already taken")
	}

	if err := g.backend.SetWorkspaceSlug(ctx, workspace.Id, req.Slug); err != nil {
		return ErrorResponse(c, http.StatusInternalServerError, err.Error())
	}

	return SuccessResponse(c, map[string]string{"slug": req.Slug})
}

// ForkWorkspace copies skills and directory structure from a public workspace.
// POST /workspaces/:id/fork
func (g *WorkspacesGroup) ForkWorkspace(c echo.Context) error {
	ctx := c.Request().Context()

	var req struct {
		SourceSlug string `json:"source_slug"`
	}
	if err := c.Bind(&req); err != nil {
		return ErrorResponse(c, http.StatusBadRequest, "invalid request body")
	}

	if req.SourceSlug == "" {
		return ErrorResponse(c, http.StatusBadRequest, "source_slug is required")
	}

	// Resolve source workspace
	sourceWs, err := g.backend.GetWorkspaceBySlug(ctx, req.SourceSlug)
	if err != nil || sourceWs == nil {
		return ErrorResponse(c, http.StatusNotFound, "source workspace not found")
	}
	if sourceWs.Visibility != types.VisibilityPublic {
		return ErrorResponse(c, http.StatusNotFound, "source workspace not found")
	}

	// Resolve target workspace
	targetId := c.Param("id")
	targetWs, err := g.backend.GetWorkspaceByExternalId(ctx, targetId)
	if err != nil {
		if _, ok := err.(*types.ErrWorkspaceNotFound); ok {
			return ErrorResponse(c, http.StatusNotFound, "target workspace not found")
		}
		return ErrorResponse(c, http.StatusInternalServerError, err.Error())
	}

	if g.storageClient == nil {
		return ErrorResponse(c, http.StatusInternalServerError, "storage not configured")
	}

	// Copy skills directory from source to target
	srcBucket := types.WorkspaceBucketName(g.storageClient.BucketPrefix(), sourceWs.ExternalId)
	dstBucket := types.WorkspaceBucketName(g.storageClient.BucketPrefix(), targetWs.ExternalId)

	// List all objects under /skills/ in source
	result, err := g.storageClient.ListObjects(ctx, srcBucket, "skills/", 10000)
	if err != nil {
		return ErrorResponse(c, http.StatusInternalServerError, "failed to list source skills")
	}

	copied := 0
	for _, obj := range result.Contents {
		key := *obj.Key
		if err := g.storageClient.CopyObject(ctx, srcBucket, key, dstBucket, key); err != nil {
			log.Error().Err(err).Str("key", key).Msg("fork: failed to copy object")
			continue
		}
		copied++
	}

	return SuccessResponse(c, map[string]interface{}{
		"source_slug":  req.SourceSlug,
		"files_copied": copied,
	})
}

func workspaceToResponse(w *types.Workspace) WorkspaceResponse {
	return WorkspaceResponse{
		ExternalID: w.ExternalId,
		Name:       w.Name,
		CreatedAt:  w.CreatedAt.Format("2006-01-02T15:04:05Z07:00"),
		UpdatedAt:  w.UpdatedAt.Format("2006-01-02T15:04:05Z07:00"),
	}
}
