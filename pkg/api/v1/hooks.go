package apiv1

import (
	"context"
	"net/http"
	"strings"
	"time"

	"github.com/beam-cloud/airstore/pkg/auth"
	"github.com/beam-cloud/airstore/pkg/hooks"
	"github.com/beam-cloud/airstore/pkg/repository"
	"github.com/beam-cloud/airstore/pkg/types"
	"github.com/labstack/echo/v4"
)

type HooksGroup struct {
	g       *echo.Group
	backend repository.BackendRepository
	svc     *hooks.Service
}

func NewHooksGroup(g *echo.Group, backend repository.BackendRepository, svc *hooks.Service) *HooksGroup {
	hg := &HooksGroup{g: g, backend: backend, svc: svc}
	hg.g.POST("", hg.Create)
	hg.g.GET("", hg.List)
	hg.g.GET("/:id", hg.Get)
	hg.g.PATCH("/:id", hg.Update)
	hg.g.DELETE("/:id", hg.Delete)
	return hg
}

func (hg *HooksGroup) Create(c echo.Context) error {
	ctx := c.Request().Context()

	var req struct {
		Path   string `json:"path"`
		Prompt string `json:"prompt"`
	}
	if err := c.Bind(&req); err != nil || req.Path == "" {
		return ErrorResponse(c, http.StatusBadRequest, "path required")
	}

	ws, err := hg.backend.GetWorkspaceByExternalId(ctx, c.Param("workspace_id"))
	if err != nil || ws == nil {
		return ErrorResponse(c, http.StatusNotFound, "workspace not found")
	}

	rawToken := strings.TrimPrefix(c.Request().Header.Get("Authorization"), "Bearer ")
	if rawToken == "" {
		return ErrorResponse(c, http.StatusBadRequest, "authentication token required")
	}

	hook, err := hg.svc.Create(ctx, ws.Id,
		ptrUint(auth.MemberId(ctx)),
		ptrUint(auth.TokenId(ctx)),
		rawToken, req.Path, req.Prompt)
	if err != nil {
		return ErrorResponse(c, http.StatusInternalServerError, err.Error())
	}

	return c.JSON(http.StatusCreated, Response{Success: true, Data: hookJSON(hook, ws.ExternalId)})
}

func (hg *HooksGroup) List(c echo.Context) error {
	ctx := c.Request().Context()

	ws, err := hg.backend.GetWorkspaceByExternalId(ctx, c.Param("workspace_id"))
	if err != nil || ws == nil {
		return ErrorResponse(c, http.StatusNotFound, "workspace not found")
	}

	list, err := hg.svc.List(ctx, ws.Id)
	if err != nil {
		return ErrorResponse(c, http.StatusInternalServerError, err.Error())
	}

	resp := make([]hookResp, 0, len(list))
	for _, h := range list {
		resp = append(resp, hookJSON(h, ws.ExternalId))
	}
	return SuccessResponse(c, resp)
}

func (hg *HooksGroup) Get(c echo.Context) error {
	hook, err := hg.svc.Get(c.Request().Context(), c.Param("id"))
	if err != nil {
		return ErrorResponse(c, http.StatusNotFound, err.Error())
	}

	wsExt := hg.workspaceExt(c.Request().Context(), hook.WorkspaceId)
	return SuccessResponse(c, hookJSON(hook, wsExt))
}

func (hg *HooksGroup) Update(c echo.Context) error {
	var req struct {
		Prompt *string `json:"prompt,omitempty"`
		Active *bool   `json:"active,omitempty"`
	}
	if err := c.Bind(&req); err != nil {
		return ErrorResponse(c, http.StatusBadRequest, "invalid request")
	}

	hook, err := hg.svc.Update(c.Request().Context(), c.Param("id"), req.Prompt, req.Active)
	if err != nil {
		return ErrorResponse(c, http.StatusInternalServerError, err.Error())
	}

	wsExt := hg.workspaceExt(c.Request().Context(), hook.WorkspaceId)
	return SuccessResponse(c, hookJSON(hook, wsExt))
}

func (hg *HooksGroup) Delete(c echo.Context) error {
	if err := hg.svc.Delete(c.Request().Context(), c.Param("id")); err != nil {
		return ErrorResponse(c, http.StatusInternalServerError, err.Error())
	}
	return SuccessResponse(c, nil)
}

func (hg *HooksGroup) workspaceExt(ctx context.Context, wsId uint) string {
	ws, _ := hg.backend.GetWorkspace(ctx, wsId)
	if ws != nil {
		return ws.ExternalId
	}
	return ""
}

type hookResp struct {
	ExternalID  string `json:"external_id"`
	WorkspaceID string `json:"workspace_id"`
	Path        string `json:"path"`
	Prompt      string `json:"prompt"`
	Active      bool   `json:"active"`
	CreatedAt   string `json:"created_at"`
	UpdatedAt   string `json:"updated_at"`
}

func hookJSON(h *types.Hook, wsExt string) hookResp {
	return hookResp{
		ExternalID:  h.ExternalId,
		WorkspaceID: wsExt,
		Path:        h.Path,
		Prompt:      h.Prompt,
		Active:      h.Active,
		CreatedAt:   h.CreatedAt.Format(time.RFC3339),
		UpdatedAt:   h.UpdatedAt.Format(time.RFC3339),
	}
}

func ptrUint(v uint) *uint {
	if v == 0 {
		return nil
	}
	return &v
}
