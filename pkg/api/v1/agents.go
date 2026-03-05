package apiv1

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/beam-cloud/airstore/pkg/auth"
	"github.com/beam-cloud/airstore/pkg/hooks"
	"github.com/beam-cloud/airstore/pkg/orchestration"
	"github.com/beam-cloud/airstore/pkg/types"
	"github.com/labstack/echo/v4"
)

type AgentsGroup struct {
	routerGroup *echo.Group
	agents      *orchestration.AgentAPI
	hooks       *hooks.Service
}

type createAgentAPIRequest struct {
	AgentKey string         `json:"agent_key"`
	Name     string         `json:"name"`
	Config   map[string]any `json:"config,omitempty"`
	Active   *bool          `json:"active,omitempty"`
}

type updateAgentAPIRequest struct {
	Name   *string        `json:"name,omitempty"`
	Config map[string]any `json:"config,omitempty"`
	Active *bool          `json:"active,omitempty"`
}

func NewAgentsGroup(routerGroup *echo.Group, agents *orchestration.AgentAPI, hooksSvc *hooks.Service) *AgentsGroup {
	g := &AgentsGroup{
		routerGroup: routerGroup,
		agents:      agents,
		hooks:       hooksSvc,
	}
	g.registerRoutes()
	return g
}

func (g *AgentsGroup) registerRoutes() {
	g.routerGroup.GET("/defaults", g.GetDefaults)
	g.routerGroup.POST("", g.CreateAgent)
	g.routerGroup.GET("", g.ListAgents)
	g.routerGroup.GET("/:agent_id", g.GetAgent)
	g.routerGroup.PATCH("/:agent_id", g.UpdateAgent)
	g.routerGroup.DELETE("/:agent_id", g.DeleteAgent)
}

func (g *AgentsGroup) CreateAgent(c echo.Context) error {
	if g.agents == nil {
		return ErrorResponse(c, http.StatusServiceUnavailable, "agent service unavailable")
	}

	var req createAgentAPIRequest
	if err := decodeStrictBody(c, &req); err != nil {
		return ErrorResponse(c, http.StatusBadRequest, "invalid request body")
	}
	if strings.TrimSpace(req.AgentKey) == "" {
		return ErrorResponse(c, http.StatusBadRequest, "agent_key is required")
	}
	if strings.TrimSpace(req.Name) == "" {
		return ErrorResponse(c, http.StatusBadRequest, "name is required")
	}

	workspaceID, err := requireWorkspaceID(c)
	if err != nil {
		return err
	}

	profile, err := g.agents.CreateAgent(c.Request().Context(), workspaceID, req.AgentKey, req.Name, req.Config, req.Active)
	if err != nil {
		return ErrorResponse(c, http.StatusBadRequest, err.Error())
	}
	return c.JSON(http.StatusCreated, Response{Success: true, Data: profile})
}

func (g *AgentsGroup) ListAgents(c echo.Context) error {
	if g.agents == nil {
		return ErrorResponse(c, http.StatusServiceUnavailable, "agent service unavailable")
	}
	workspaceID, err := requireWorkspaceID(c)
	if err != nil {
		return err
	}
	profiles, err := g.agents.ListAgents(c.Request().Context(), workspaceID)
	if err != nil {
		return ErrorResponse(c, http.StatusInternalServerError, err.Error())
	}
	return SuccessResponse(c, profiles)
}

func (g *AgentsGroup) UpdateAgent(c echo.Context) error {
	if g.agents == nil {
		return ErrorResponse(c, http.StatusServiceUnavailable, "agent service unavailable")
	}

	var req updateAgentAPIRequest
	if err := decodeStrictBody(c, &req); err != nil {
		return ErrorResponse(c, http.StatusBadRequest, "invalid request body")
	}

	workspaceID, err := requireWorkspaceID(c)
	if err != nil {
		return err
	}
	agentID := c.Param("agent_id")

	profile, err := g.agents.UpdateAgent(c.Request().Context(), workspaceID, agentID, req.Name, req.Config, req.Active)
	if err != nil {
		if _, ok := err.(*types.ErrAgentProfileNotFound); ok {
			return ErrorResponse(c, http.StatusNotFound, "agent not found")
		}
		return ErrorResponse(c, http.StatusBadRequest, err.Error())
	}
	return c.JSON(http.StatusOK, Response{Success: true, Data: profile})
}

func (g *AgentsGroup) DeleteAgent(c echo.Context) error {
	if g.agents == nil {
		return ErrorResponse(c, http.StatusServiceUnavailable, "agent service unavailable")
	}

	workspaceID, err := requireWorkspaceID(c)
	if err != nil {
		return err
	}
	agentID := strings.TrimSpace(c.Param("agent_id"))
	if agentID == "" {
		return ErrorResponse(c, http.StatusBadRequest, "agent_id is required")
	}

	deletedHooks := 0
	if g.hooks != nil {
		hooksList, err := g.hooks.List(c.Request().Context(), workspaceID)
		if err != nil {
			return ErrorResponse(c, http.StatusInternalServerError, err.Error())
		}
		for _, hook := range hooksList {
			if hook == nil || hook.AgentId == nil {
				continue
			}
			if strings.TrimSpace(*hook.AgentId) != agentID {
				continue
			}
			if err := g.hooks.Delete(c.Request().Context(), hook.ExternalId); err != nil {
				return ErrorResponse(c, http.StatusBadRequest, err.Error())
			}
			deletedHooks++
		}
	}

	if err := g.agents.DeleteAgent(c.Request().Context(), workspaceID, agentID); err != nil {
		if _, ok := err.(*types.ErrAgentProfileNotFound); ok && deletedHooks > 0 {
			return SuccessResponse(c, map[string]any{
				"deleted_hooks": deletedHooks,
			})
		}
		if _, ok := err.(*types.ErrAgentProfileNotFound); ok {
			return ErrorResponse(c, http.StatusNotFound, "agent not found")
		}
		return ErrorResponse(c, http.StatusBadRequest, err.Error())
	}

	return SuccessResponse(c, map[string]any{
		"deleted_hooks": deletedHooks,
	})
}

func (g *AgentsGroup) GetDefaults(c echo.Context) error {
	if g.agents == nil {
		return ErrorResponse(c, http.StatusServiceUnavailable, "agent service unavailable")
	}
	agentKey := strings.TrimSpace(c.QueryParam("agent_key"))
	config := g.agents.GetDefaultConfig(agentKey)
	return SuccessResponse(c, config)
}

func (g *AgentsGroup) GetAgent(c echo.Context) error {
	if g.agents == nil {
		return ErrorResponse(c, http.StatusServiceUnavailable, "agent service unavailable")
	}
	workspaceID, err := requireWorkspaceID(c)
	if err != nil {
		return err
	}
	agentID := c.Param("agent_id")
	profile, err := g.agents.GetAgent(c.Request().Context(), workspaceID, agentID)
	if err != nil {
		if _, ok := err.(*types.ErrAgentProfileNotFound); ok {
			return ErrorResponse(c, http.StatusNotFound, "agent not found")
		}
		return ErrorResponse(c, http.StatusInternalServerError, err.Error())
	}
	return SuccessResponse(c, profile)
}

func decodeStrictBody(c echo.Context, dst any) error {
	return decodeStrict(c.Request().Body, dst)
}

// decodeStrictJSON is used by tests that need strict unknown-field validation on raw body blobs.
func decodeStrictJSON(data []byte, dst any) error {
	return decodeStrict(bytes.NewReader(data), dst)
}

func decodeStrict(r io.Reader, dst any) error {
	dec := json.NewDecoder(r)
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return err
	}
	if err := dec.Decode(&struct{}{}); err != io.EOF {
		return errors.New("request body must contain exactly one JSON object")
	}
	return nil
}

func requireWorkspaceID(c echo.Context) (uint, error) {
	workspaceID := auth.WorkspaceId(c.Request().Context())
	if workspaceID == 0 {
		return 0, ErrorResponse(c, http.StatusUnauthorized, "workspace auth required")
	}
	return workspaceID, nil
}
