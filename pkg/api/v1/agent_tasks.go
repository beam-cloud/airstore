package apiv1

import (
	"net/http"

	"github.com/beam-cloud/airstore/pkg/orchestration"
	"github.com/beam-cloud/airstore/pkg/types"
	"github.com/labstack/echo/v4"
)

type AgentTasksGroup struct {
	routerGroup *echo.Group
	agents      *orchestration.AgentAPI
}

func NewAgentTasksGroup(routerGroup *echo.Group, agents *orchestration.AgentAPI) *AgentTasksGroup {
	g := &AgentTasksGroup{
		routerGroup: routerGroup,
		agents:      agents,
	}
	g.registerRoutes()
	return g
}

func (g *AgentTasksGroup) registerRoutes() {
	g.routerGroup.POST("", g.CreateTaskEnvelope)
	g.routerGroup.GET("/:envelope_id", g.GetTaskEnvelope)
}

func (g *AgentTasksGroup) CreateTaskEnvelope(c echo.Context) error {
	if g.agents == nil {
		return ErrorResponse(c, http.StatusServiceUnavailable, "orchestration unavailable")
	}

	var req orchestration.AgentCommandParams
	if err := decodeStrictBody(c, &req); err != nil {
		return ErrorResponse(c, http.StatusBadRequest, "invalid request body")
	}

	workspaceID, err := requireWorkspaceID(c)
	if err != nil {
		return err
	}

	envelope, deduped, err := g.agents.AcceptAgentCommand(c.Request().Context(), workspaceID, req)
	if err != nil {
		return ErrorResponse(c, http.StatusBadRequest, err.Error())
	}
	statusCode := http.StatusAccepted
	if deduped {
		statusCode = http.StatusOK
	}
	return c.JSON(statusCode, Response{
		Success: true,
		Data: map[string]any{
			"accepted":       true,
			"idempotent_hit": deduped,
			"envelope":       envelope,
			"run_id":         envelope.TargetRunID,
		},
	})
}

func (g *AgentTasksGroup) GetTaskEnvelope(c echo.Context) error {
	if g.agents == nil {
		return ErrorResponse(c, http.StatusServiceUnavailable, "orchestration unavailable")
	}
	workspaceID, err := requireWorkspaceID(c)
	if err != nil {
		return err
	}
	envelopeID := c.Param("envelope_id")
	envelope, err := g.agents.GetEnvelope(c.Request().Context(), workspaceID, envelopeID)
	if err != nil {
		if _, ok := err.(*types.ErrAgentTaskEnvelopeNotFound); ok {
			return ErrorResponse(c, http.StatusNotFound, "envelope not found")
		}
		return ErrorResponse(c, http.StatusInternalServerError, err.Error())
	}
	return SuccessResponse(c, envelope)
}
