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
	"github.com/rs/zerolog/log"
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
		Path       string             `json:"path"`
		Prompt     string             `json:"prompt"`
		SkillPath  string             `json:"skill_path"`
		SkillPaths []string           `json:"skill_paths"`
		AgentName  *string            `json:"agent_name,omitempty"`
		AgentCfg   *hookAgentConfigIn `json:"agent_config,omitempty"`
	}
	if err := c.Bind(&req); err != nil || strings.TrimSpace(req.Path) == "" {
		return ErrorResponse(c, http.StatusBadRequest, "path required")
	}
	if strings.TrimSpace(req.Prompt) == "" {
		return ErrorResponse(c, http.StatusBadRequest, "prompt required")
	}

	ws, err := hg.backend.GetWorkspaceByExternalId(ctx, c.Param("workspace_id"))
	if err != nil || ws == nil {
		return ErrorResponse(c, http.StatusNotFound, "workspace not found")
	}

	// Resolve the token to store on the hook.
	// For member auth: use the caller's token directly.
	// For admin auth: the cluster admin token is a static secret that
	// workers can't use for filesystem mounts, so we auto-provision a
	// workspace service token instead.
	tokenId := ptrUint(auth.TokenId(ctx))
	memberId := ptrUint(auth.MemberId(ctx))
	rawToken := strings.TrimPrefix(c.Request().Header.Get("Authorization"), "Bearer ")

	if tokenId == nil {
		// Admin auth — provision a workspace service token
		svcToken, svcRaw, err := hg.backend.EnsureWorkspaceServiceToken(ctx, ws.Id)
		if err != nil {
			return ErrorResponse(c, http.StatusInternalServerError, "failed to provision workspace token: "+err.Error())
		}
		tokenId = &svcToken.Id
		rawToken = svcRaw
	}

	if rawToken == "" {
		return ErrorResponse(c, http.StatusBadRequest, "authentication token required")
	}

	skillPaths := req.SkillPaths
	if len(skillPaths) == 0 && strings.TrimSpace(req.SkillPath) != "" {
		skillPaths = []string{req.SkillPath}
	}

	hook, err := hg.svc.Create(
		ctx,
		ws.Id,
		memberId,
		tokenId,
		rawToken,
		req.Path,
		req.Prompt,
		skillPaths,
		buildAgentPatch(req.AgentName, req.AgentCfg),
	)
	if err != nil {
		log.Error().Err(err).Str("workspace", ws.ExternalId).Str("path", req.Path).Msg("hook create failed")
		status := http.StatusInternalServerError
		if isHookConflictErr(err) {
			status = http.StatusConflict
		}
		return ErrorResponse(c, status, err.Error())
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
		Prompt     *string            `json:"prompt,omitempty"`
		Active     *bool              `json:"active,omitempty"`
		SkillPath  *string            `json:"skill_path,omitempty"`
		SkillPaths *[]string          `json:"skill_paths,omitempty"`
		AgentName  *string            `json:"agent_name,omitempty"`
		AgentCfg   *hookAgentConfigIn `json:"agent_config,omitempty"`
	}
	if err := c.Bind(&req); err != nil {
		return ErrorResponse(c, http.StatusBadRequest, "invalid request")
	}

	var skillPaths *[]string
	switch {
	case req.SkillPaths != nil:
		values := append([]string(nil), (*req.SkillPaths)...)
		if len(values) == 0 && req.SkillPath != nil && strings.TrimSpace(*req.SkillPath) != "" {
			values = []string{*req.SkillPath}
		}
		skillPaths = &values
	case req.SkillPath != nil:
		trimmed := strings.TrimSpace(*req.SkillPath)
		values := []string{}
		if trimmed != "" {
			values = []string{trimmed}
		}
		skillPaths = &values
	}

	hook, err := hg.svc.Update(
		c.Request().Context(),
		c.Param("id"),
		req.Prompt,
		req.Active,
		skillPaths,
		buildAgentPatch(req.AgentName, req.AgentCfg),
	)
	if err != nil {
		status := http.StatusInternalServerError
		if isHookConflictErr(err) {
			status = http.StatusConflict
		}
		return ErrorResponse(c, status, err.Error())
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
	ExternalID  string         `json:"external_id"`
	WorkspaceID string         `json:"workspace_id"`
	Path        string         `json:"path"`
	Prompt      string         `json:"prompt"`
	SkillPath   string         `json:"skill_path"`
	SkillPaths  []string       `json:"skill_paths"`
	AgentID     string         `json:"agent_id,omitempty"`
	AgentKey    string         `json:"agent_key,omitempty"`
	AgentName   string         `json:"agent_name,omitempty"`
	AgentConfig map[string]any `json:"agent_config,omitempty"`
	Active      bool           `json:"active"`
	CreatedAt   string         `json:"created_at"`
	UpdatedAt   string         `json:"updated_at"`
}

func hookJSON(h *types.Hook, wsExt string) hookResp {
	agentID := ""
	if h.AgentId != nil {
		agentID = strings.TrimSpace(*h.AgentId)
	}
	skillPaths := types.NormalizeSkillPaths(h.SkillPaths, h.SkillPath)
	legacySkillPath := ""
	if len(skillPaths) > 0 {
		legacySkillPath = skillPaths[0]
	}
	return hookResp{
		ExternalID:  h.ExternalId,
		WorkspaceID: wsExt,
		Path:        h.Path,
		Prompt:      h.Prompt,
		SkillPath:   legacySkillPath,
		SkillPaths:  skillPaths,
		AgentID:     agentID,
		AgentKey:    h.AgentKey,
		AgentName:   h.AgentName,
		AgentConfig: h.AgentConfig,
		Active:      h.Active,
		CreatedAt:   h.CreatedAt.Format(time.RFC3339),
		UpdatedAt:   h.UpdatedAt.Format(time.RFC3339),
	}
}

type hookAgentConfigIn struct {
	Runner           *string `json:"runner,omitempty"`
	Model            *string `json:"model,omitempty"`
	SystemPrompt     *string `json:"system_prompt,omitempty"`
	SystemPromptMode *string `json:"system_prompt_mode,omitempty"`
	WorkspaceDir     *string `json:"workspace_dir,omitempty"`
}

func buildAgentPatch(name *string, cfg *hookAgentConfigIn) *hooks.AgentConfigPatch {
	patch := &hooks.AgentConfigPatch{}
	has := false
	if name != nil {
		patch.Name = name
		has = true
	}
	if cfg != nil {
		if cfg.Runner != nil {
			patch.Runner = cfg.Runner
			has = true
		}
		if cfg.Model != nil {
			patch.Model = cfg.Model
			has = true
		}
		if cfg.SystemPrompt != nil {
			patch.SystemPrompt = cfg.SystemPrompt
			has = true
		}
		if cfg.SystemPromptMode != nil {
			patch.SystemPromptMode = cfg.SystemPromptMode
			has = true
		}
		if cfg.WorkspaceDir != nil {
			patch.WorkspaceDir = cfg.WorkspaceDir
			has = true
		}
	}
	if !has {
		return nil
	}
	return patch
}

func ptrUint(v uint) *uint {
	if v == 0 {
		return nil
	}
	return &v
}

func isHookConflictErr(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "already exists") ||
		strings.Contains(msg, "already in use") ||
		strings.Contains(msg, "duplicate key") ||
		strings.Contains(msg, "unique constraint") ||
		strings.Contains(msg, "violates unique") ||
		strings.Contains(msg, "conflicts with an existing")
}
