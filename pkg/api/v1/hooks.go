package apiv1

import (
	"net/http"
	"strings"
	"time"

	"github.com/beam-cloud/airstore/pkg/auth"
	"github.com/beam-cloud/airstore/pkg/common"
	"github.com/beam-cloud/airstore/pkg/hooks"
	"github.com/beam-cloud/airstore/pkg/repository"
	"github.com/beam-cloud/airstore/pkg/types"
	"github.com/labstack/echo/v4"
)

// HooksGroup handles CRUD for filesystem hooks.
type HooksGroup struct {
	g        *echo.Group
	backend  repository.BackendRepository
	fsStore  repository.FilesystemStore
	eventBus *common.EventBus
}

// NewHooksGroup registers hook API routes.
func NewHooksGroup(g *echo.Group, backend repository.BackendRepository, fsStore repository.FilesystemStore, eventBus *common.EventBus) *HooksGroup {
	hg := &HooksGroup{g: g, backend: backend, fsStore: fsStore, eventBus: eventBus}
	hg.g.POST("", hg.Create)
	hg.g.GET("", hg.List)
	hg.g.GET("/:id", hg.Get)
	hg.g.PATCH("/:id", hg.Update)
	hg.g.DELETE("/:id", hg.Delete)
	return hg
}

type CreateHookRequest struct {
	Path   string `json:"path"`
	Prompt string `json:"prompt"`
}

type UpdateHookRequest struct {
	Prompt *string `json:"prompt,omitempty"`
	Active *bool   `json:"active,omitempty"`
}

type HookResponse struct {
	ExternalID  string `json:"external_id"`
	WorkspaceID string `json:"workspace_id"`
	Path        string `json:"path"`
	Prompt      string `json:"prompt"`
	Active      bool   `json:"active"`
	CreatedAt   string `json:"created_at"`
	UpdatedAt   string `json:"updated_at"`
}

// Create creates a new hook with a dedicated token.
func (hg *HooksGroup) Create(c echo.Context) error {
	ctx := c.Request().Context()
	workspaceId := c.Param("workspace_id")

	var req CreateHookRequest
	if err := c.Bind(&req); err != nil {
		return ErrorResponse(c, http.StatusBadRequest, "invalid request")
	}
	if req.Path == "" {
		return ErrorResponse(c, http.StatusBadRequest, "path required")
	}

	ws, err := hg.backend.GetWorkspaceByExternalId(ctx, workspaceId)
	if err != nil || ws == nil {
		return ErrorResponse(c, http.StatusNotFound, "workspace not found")
	}

	// Capture the caller's raw token from the Authorization header
	memberId := auth.MemberId(ctx)
	var createdByMemberId *uint
	if memberId > 0 {
		createdByMemberId = &memberId
	}

	rawToken := strings.TrimPrefix(c.Request().Header.Get("Authorization"), "Bearer ")
	if rawToken == "" {
		return ErrorResponse(c, http.StatusBadRequest, "authentication token required")
	}

	encryptedToken, err := hooks.EncodeToken(rawToken)
	if err != nil {
		return ErrorResponse(c, http.StatusInternalServerError, "failed to store token")
	}

	var tokenId *uint
	if tid := auth.TokenId(ctx); tid > 0 {
		tokenId = &tid
	}

	hook := &types.Hook{
		WorkspaceId:       ws.Id,
		Path:              hooks.NormalizePath(req.Path),
		Prompt:            req.Prompt,
		Active:            true,
		CreatedByMemberId: createdByMemberId,
		TokenId:           tokenId,
		EncryptedToken:    encryptedToken,
	}

	created, err := hg.fsStore.CreateHook(ctx, hook)
	if err != nil {
		if strings.Contains(err.Error(), "duplicate") || strings.Contains(err.Error(), "unique") {
			return ErrorResponse(c, http.StatusConflict, "hook already exists for this path")
		}
		return ErrorResponse(c, http.StatusInternalServerError, err.Error())
	}

	// Invalidate hook cache across replicas
	hg.invalidateHookCache(ws.Id)

	return c.JSON(http.StatusCreated, Response{
		Success: true,
		Data:    hookToResponse(created, ws.ExternalId),
	})
}

// List returns all hooks for a workspace.
func (hg *HooksGroup) List(c echo.Context) error {
	ctx := c.Request().Context()
	workspaceId := c.Param("workspace_id")

	ws, err := hg.backend.GetWorkspaceByExternalId(ctx, workspaceId)
	if err != nil || ws == nil {
		return ErrorResponse(c, http.StatusNotFound, "workspace not found")
	}

	hooks, err := hg.fsStore.ListHooks(ctx, ws.Id)
	if err != nil {
		return ErrorResponse(c, http.StatusInternalServerError, err.Error())
	}

	var resp []HookResponse
	for _, h := range hooks {
		resp = append(resp, hookToResponse(h, ws.ExternalId))
	}
	return SuccessResponse(c, resp)
}

// Get returns a single hook.
func (hg *HooksGroup) Get(c echo.Context) error {
	ctx := c.Request().Context()
	hookId := c.Param("id")

	hook, err := hg.fsStore.GetHook(ctx, hookId)
	if err != nil {
		return ErrorResponse(c, http.StatusInternalServerError, err.Error())
	}
	if hook == nil {
		return ErrorResponse(c, http.StatusNotFound, "hook not found")
	}

	ws, _ := hg.backend.GetWorkspace(ctx, hook.WorkspaceId)
	wsExt := ""
	if ws != nil {
		wsExt = ws.ExternalId
	}
	return SuccessResponse(c, hookToResponse(hook, wsExt))
}

// Update modifies a hook's prompt or active status.
func (hg *HooksGroup) Update(c echo.Context) error {
	ctx := c.Request().Context()
	hookId := c.Param("id")

	var req UpdateHookRequest
	if err := c.Bind(&req); err != nil {
		return ErrorResponse(c, http.StatusBadRequest, "invalid request")
	}

	hook, err := hg.fsStore.GetHook(ctx, hookId)
	if err != nil {
		return ErrorResponse(c, http.StatusInternalServerError, err.Error())
	}
	if hook == nil {
		return ErrorResponse(c, http.StatusNotFound, "hook not found")
	}

	if req.Prompt != nil {
		hook.Prompt = *req.Prompt
	}
	if req.Active != nil {
		hook.Active = *req.Active
	}

	if err := hg.fsStore.UpdateHook(ctx, hook); err != nil {
		return ErrorResponse(c, http.StatusInternalServerError, err.Error())
	}

	hg.invalidateHookCache(hook.WorkspaceId)

	ws, _ := hg.backend.GetWorkspace(ctx, hook.WorkspaceId)
	wsExt := ""
	if ws != nil {
		wsExt = ws.ExternalId
	}
	return SuccessResponse(c, hookToResponse(hook, wsExt))
}

// Delete removes a hook and revokes its dedicated token.
func (hg *HooksGroup) Delete(c echo.Context) error {
	ctx := c.Request().Context()
	hookId := c.Param("id")

	hook, err := hg.fsStore.GetHook(ctx, hookId)
	if err != nil {
		return ErrorResponse(c, http.StatusInternalServerError, err.Error())
	}
	if hook == nil {
		return ErrorResponse(c, http.StatusNotFound, "hook not found")
	}

	if err := hg.fsStore.DeleteHook(ctx, hookId); err != nil {
		return ErrorResponse(c, http.StatusInternalServerError, err.Error())
	}

	hg.invalidateHookCache(hook.WorkspaceId)

	return SuccessResponse(c, nil)
}

// invalidateHookCache broadcasts cache invalidation to all gateway replicas.
func (hg *HooksGroup) invalidateHookCache(workspaceId uint) {
	if hg.eventBus == nil {
		return
	}
	hg.eventBus.Emit(common.Event{
		Type: common.EventCacheInvalidate,
		Data: map[string]any{
			"scope":        "hooks",
			"workspace_id": workspaceId,
		},
	})
}

func hookToResponse(h *types.Hook, workspaceExternalId string) HookResponse {
	return HookResponse{
		ExternalID:  h.ExternalId,
		WorkspaceID: workspaceExternalId,
		Path:        h.Path,
		Prompt:      h.Prompt,
		Active:      h.Active,
		CreatedAt:   h.CreatedAt.Format(time.RFC3339),
		UpdatedAt:   h.UpdatedAt.Format(time.RFC3339),
	}
}
