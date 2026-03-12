package apiv1

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/beam-cloud/airstore/pkg/auth"
	"github.com/beam-cloud/airstore/pkg/channels"
	"github.com/beam-cloud/airstore/pkg/hooks"
	"github.com/beam-cloud/airstore/pkg/orchestration"
	"github.com/beam-cloud/airstore/pkg/types"
	"github.com/rs/zerolog/log"
	"github.com/labstack/echo/v4"
)

type AgentsGroup struct {
	routerGroup  *echo.Group
	agents       *orchestration.AgentAPI
	hooks        *hooks.Service
	emailChannel *channels.Email
}

type createAgentAPIRequest struct {
	AgentKey string         `json:"agent_key"`
	Name     string         `json:"name"`
	Config   map[string]any `json:"config,omitempty"`
	Active   *bool          `json:"active,omitempty"`
}

type updateAgentAPIRequest struct {
	Name          *string        `json:"name,omitempty"`
	Role          *string        `json:"role,omitempty"`
	MemoryScope   *string        `json:"memory_scope,omitempty"`
	QualityScore  *float64       `json:"quality_score,omitempty"`
	CostBudgetUSD *float64       `json:"cost_budget_usd,omitempty"`
	Config        map[string]any `json:"config,omitempty"`
	Active        *bool          `json:"active,omitempty"`
}

func NewAgentsGroup(routerGroup *echo.Group, agents *orchestration.AgentAPI, hooksSvc *hooks.Service, emailCh *channels.Email) *AgentsGroup {
	g := &AgentsGroup{
		routerGroup:  routerGroup,
		agents:       agents,
		hooks:        hooksSvc,
		emailChannel: emailCh,
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
	g.routerGroup.GET("/:agent_id/stats", g.GetAgentStats)
	g.routerGroup.GET("/:agent_id/channels", g.ListChannels)
	g.routerGroup.PUT("/:agent_id/channels", g.UpsertChannels)
	g.routerGroup.DELETE("/:agent_id/channels/:channel_type", g.DeleteChannel)
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

	profile, err := g.agents.CreateAgent(
		c.Request().Context(),
		workspaceID,
		req.AgentKey,
		req.Name,
		req.Config,
		req.Active,
	)
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

	profile, err := g.agents.UpdateAgent(
		c.Request().Context(),
		workspaceID,
		agentID,
		req.Name,
		req.Role,
		req.MemoryScope,
		req.QualityScore,
		req.CostBudgetUSD,
		req.Config,
		req.Active,
	)
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

func (g *AgentsGroup) GetAgentStats(c echo.Context) error {
	if g.agents == nil {
		return ErrorResponse(c, http.StatusServiceUnavailable, "stats service unavailable")
	}
	workspaceID, err := requireWorkspaceID(c)
	if err != nil {
		return err
	}
	agentID := strings.TrimSpace(c.Param("agent_id"))
	if agentID == "" {
		return ErrorResponse(c, http.StatusBadRequest, "agent_id is required")
	}
	stats, err := g.agents.GetAgentStats(c.Request().Context(), workspaceID, agentID)
	if err != nil {
		return ErrorResponse(c, http.StatusInternalServerError, err.Error())
	}
	return SuccessResponse(c, stats)
}

// --- Agent Channel Binding Handlers ---

type upsertChannelsRequest struct {
	Channels []channelBindingEntry `json:"channels"`
}

type channelBindingEntry struct {
	ChannelType string         `json:"channel_type"`
	Address     string         `json:"address"`
	Active      *bool          `json:"active,omitempty"`
	ConfigJSON  map[string]any `json:"config_json,omitempty"`
}

func (g *AgentsGroup) ListChannels(c echo.Context) error {
	if g.agents == nil {
		return ErrorResponse(c, http.StatusServiceUnavailable, "channel service unavailable")
	}
	workspaceID, err := requireWorkspaceID(c)
	if err != nil {
		return err
	}
	agentID := strings.TrimSpace(c.Param("agent_id"))
	if agentID == "" {
		return ErrorResponse(c, http.StatusBadRequest, "agent_id is required")
	}
	bindings, err := g.agents.ListChannelBindings(c.Request().Context(), workspaceID, &agentID)
	if err != nil {
		return ErrorResponse(c, http.StatusInternalServerError, err.Error())
	}
	return SuccessResponse(c, bindings)
}

func (g *AgentsGroup) UpsertChannels(c echo.Context) error {
	if g.agents == nil {
		return ErrorResponse(c, http.StatusServiceUnavailable, "channel service unavailable")
	}
	workspaceID, err := requireWorkspaceID(c)
	if err != nil {
		return err
	}
	agentID := strings.TrimSpace(c.Param("agent_id"))
	if agentID == "" {
		return ErrorResponse(c, http.StatusBadRequest, "agent_id is required")
	}

	var req upsertChannelsRequest
	if err := decodeStrictBody(c, &req); err != nil {
		return ErrorResponse(c, http.StatusBadRequest, "invalid request body")
	}

	results := make([]*types.ChannelBinding, 0, len(req.Channels))
	for _, ch := range req.Channels {
		if strings.TrimSpace(ch.ChannelType) == "" {
			return ErrorResponse(c, http.StatusBadRequest, "channel_type is required")
		}

		active := true
		if ch.Active != nil {
			active = *ch.Active
		}
		address := strings.TrimSpace(ch.Address)

		if ch.ChannelType == string(channels.ChannelTypeEmail) && address == "" && g.emailChannel != nil && g.emailChannel.Mail() != nil {
			agent, err := g.agents.GetAgent(c.Request().Context(), workspaceID, agentID)
			if err != nil {
				return ErrorResponse(c, http.StatusBadRequest, "failed to look up agent: "+err.Error())
			}
			emailAddr, err := g.emailChannel.ProvisionInbox(c.Request().Context(), agent.AgentKey, agent.Name)
			if err != nil {
				return ErrorResponse(c, http.StatusBadGateway, "failed to provision inbox: "+err.Error())
			}
			address = emailAddr
		}

		if address == "" {
			return ErrorResponse(c, http.StatusBadRequest, "address is required (or enable agentmail for auto-provisioning)")
		}

		binding := &types.ChannelBinding{
			WorkspaceID: workspaceID,
			AgentID:     &agentID,
			ChannelType: ch.ChannelType,
			Address:     address,
			ConfigJSON:  ch.ConfigJSON,
			Active:      active,
		}
		if err := g.agents.UpsertChannelBinding(c.Request().Context(), binding); err != nil {
			return ErrorResponse(c, http.StatusInternalServerError, err.Error())
		}
		results = append(results, binding)
	}
	return c.JSON(http.StatusOK, Response{Success: true, Data: results})
}

func (g *AgentsGroup) DeleteChannel(c echo.Context) error {
	if g.agents == nil {
		return ErrorResponse(c, http.StatusServiceUnavailable, "channel service unavailable")
	}
	workspaceID, err := requireWorkspaceID(c)
	if err != nil {
		return err
	}
	agentID := strings.TrimSpace(c.Param("agent_id"))
	channelType := strings.TrimSpace(c.Param("channel_type"))
	if agentID == "" || channelType == "" {
		return ErrorResponse(c, http.StatusBadRequest, "agent_id and channel_type are required")
	}

	if channelType == string(channels.ChannelTypeEmail) && g.emailChannel != nil && g.emailChannel.Mail() != nil {
		bindings, err := g.agents.ListChannelBindings(c.Request().Context(), workspaceID, &agentID)
		if err != nil {
			return ErrorResponse(c, http.StatusInternalServerError, "failed to look up existing bindings: "+err.Error())
		}
		for _, b := range bindings {
			if b.ChannelType == string(channels.ChannelTypeEmail) && b.Address != "" {
				if depErr := g.emailChannel.DeprovisionInbox(c.Request().Context(), b.Address); depErr != nil {
					log.Warn().Err(depErr).Str("address", b.Address).Msg("failed to deprovision inbox")
				}
			}
		}
	}

	if err := g.agents.DeleteChannelBinding(c.Request().Context(), workspaceID, &agentID, channelType); err != nil {
		return ErrorResponse(c, http.StatusNotFound, err.Error())
	}
	return SuccessResponse(c, map[string]bool{"deleted": true})
}
