package apiv1

import (
	"bytes"
	"encoding/json"
	"net/http"
	"strings"

	"github.com/beam-cloud/airstore/pkg/auth"
	"github.com/beam-cloud/airstore/pkg/orchestration"
	"github.com/beam-cloud/airstore/pkg/types"
	"github.com/labstack/echo/v4"
)

type OrchestrationGroup struct {
	routerGroup *echo.Group
	agents      *orchestration.AgentAPI
}

func NewOrchestrationGroup(
	routerGroup *echo.Group,
	agents *orchestration.AgentAPI,
) *OrchestrationGroup {
	g := &OrchestrationGroup{
		routerGroup: routerGroup,
		agents:      agents,
	}
	g.registerRoutes()
	return g
}

func (g *OrchestrationGroup) registerRoutes() {
	g.routerGroup.POST("/agents", g.CreateAgent)
	g.routerGroup.GET("/agents", g.ListAgents)
	g.routerGroup.GET("/agents/:agent_id", g.GetAgent)

	g.routerGroup.POST("/tasks", g.AcceptAgentCommand)
	g.routerGroup.GET("/tasks/:envelope_id", g.GetEnvelope)

	g.routerGroup.GET("/runs", g.ListRuns)
	g.routerGroup.GET("/runs/:run_id", g.GetRun)
	g.routerGroup.GET("/runs/:run_id/attempts", g.ListRunAttempts)
	g.routerGroup.GET("/runs/:run_id/snapshots", g.ListRunSnapshots)
	g.routerGroup.GET("/runs/:run_id/events", g.ListRunEvents)
	g.routerGroup.POST("/runs/:run_id/input", g.EnqueueRunInput)
	g.routerGroup.POST("/runs/:run_id/cancel", g.CancelRun)
}

type createAgentRequest struct {
	AgentKey string         `json:"agent_key"`
	Name     string         `json:"name"`
	Config   map[string]any `json:"config,omitempty"`
	Active   *bool          `json:"active,omitempty"`
}

func decodeStrict(c echo.Context, dst any) error {
	dec := json.NewDecoder(c.Request().Body)
	dec.DisallowUnknownFields()
	return dec.Decode(dst)
}

func (g *OrchestrationGroup) CreateAgent(c echo.Context) error {
	var req createAgentRequest
	if err := decodeStrict(c, &req); err != nil {
		return ErrorResponse(c, http.StatusBadRequest, "invalid request body")
	}
	if strings.TrimSpace(req.AgentKey) == "" {
		return ErrorResponse(c, http.StatusBadRequest, "agent_key is required")
	}
	if strings.TrimSpace(req.Name) == "" {
		return ErrorResponse(c, http.StatusBadRequest, "name is required")
	}

	workspaceID := auth.WorkspaceId(c.Request().Context())
	if workspaceID == 0 {
		return ErrorResponse(c, http.StatusUnauthorized, "workspace auth required")
	}

	profile, err := g.agents.CreateAgent(c.Request().Context(), workspaceID, req.AgentKey, req.Name, req.Config, req.Active)
	if err != nil {
		return ErrorResponse(c, http.StatusBadRequest, err.Error())
	}
	return c.JSON(http.StatusCreated, Response{Success: true, Data: profile})
}

func (g *OrchestrationGroup) ListAgents(c echo.Context) error {
	workspaceID := auth.WorkspaceId(c.Request().Context())
	if workspaceID == 0 {
		return ErrorResponse(c, http.StatusUnauthorized, "workspace auth required")
	}
	profiles, err := g.agents.ListAgents(c.Request().Context(), workspaceID)
	if err != nil {
		return ErrorResponse(c, http.StatusInternalServerError, err.Error())
	}
	return SuccessResponse(c, profiles)
}

func (g *OrchestrationGroup) GetAgent(c echo.Context) error {
	workspaceID := auth.WorkspaceId(c.Request().Context())
	if workspaceID == 0 {
		return ErrorResponse(c, http.StatusUnauthorized, "workspace auth required")
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

func (g *OrchestrationGroup) AcceptAgentCommand(c echo.Context) error {
	if g.agents == nil {
		return ErrorResponse(c, http.StatusServiceUnavailable, "orchestration unavailable")
	}

	var req orchestration.AgentCommandParams
	if err := decodeStrict(c, &req); err != nil {
		return ErrorResponse(c, http.StatusBadRequest, "invalid request body")
	}

	workspaceID := auth.WorkspaceId(c.Request().Context())
	if workspaceID == 0 {
		return ErrorResponse(c, http.StatusUnauthorized, "workspace auth required")
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

func (g *OrchestrationGroup) GetEnvelope(c echo.Context) error {
	workspaceID := auth.WorkspaceId(c.Request().Context())
	if workspaceID == 0 {
		return ErrorResponse(c, http.StatusUnauthorized, "workspace auth required")
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

type enqueueRunInputRequest struct {
	Message        string               `json:"message"`
	QueueMode      types.AgentQueueMode `json:"queue_mode"`
	IdempotencyKey string               `json:"idempotency_key"`
}

func (g *OrchestrationGroup) EnqueueRunInput(c echo.Context) error {
	if g.agents == nil {
		return ErrorResponse(c, http.StatusServiceUnavailable, "orchestration unavailable")
	}

	var req enqueueRunInputRequest
	if err := decodeStrict(c, &req); err != nil {
		return ErrorResponse(c, http.StatusBadRequest, "invalid request body")
	}

	workspaceID := auth.WorkspaceId(c.Request().Context())
	if workspaceID == 0 {
		return ErrorResponse(c, http.StatusUnauthorized, "workspace auth required")
	}
	runID := c.Param("run_id")

	envelope, deduped, err := g.agents.EnqueueRunInputEnvelope(
		c.Request().Context(),
		workspaceID,
		runID,
		req.QueueMode,
		req.Message,
		req.IdempotencyKey,
	)
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
		},
	})
}

func (g *OrchestrationGroup) ListRuns(c echo.Context) error {
	workspaceID := auth.WorkspaceId(c.Request().Context())
	if workspaceID == 0 {
		return ErrorResponse(c, http.StatusUnauthorized, "workspace auth required")
	}
	runs, err := g.agents.ListRuns(c.Request().Context(), workspaceID, 100)
	if err != nil {
		return ErrorResponse(c, http.StatusInternalServerError, err.Error())
	}
	return SuccessResponse(c, runs)
}

func (g *OrchestrationGroup) GetRun(c echo.Context) error {
	workspaceID := auth.WorkspaceId(c.Request().Context())
	if workspaceID == 0 {
		return ErrorResponse(c, http.StatusUnauthorized, "workspace auth required")
	}
	runID := c.Param("run_id")
	run, err := g.agents.GetRun(c.Request().Context(), workspaceID, runID)
	if err != nil {
		if _, ok := err.(*types.ErrAgentRunNotFound); ok {
			return ErrorResponse(c, http.StatusNotFound, "run not found")
		}
		return ErrorResponse(c, http.StatusInternalServerError, err.Error())
	}
	return SuccessResponse(c, run)
}

func (g *OrchestrationGroup) ListRunAttempts(c echo.Context) error {
	workspaceID := auth.WorkspaceId(c.Request().Context())
	if workspaceID == 0 {
		return ErrorResponse(c, http.StatusUnauthorized, "workspace auth required")
	}
	runID := c.Param("run_id")
	attempts, err := g.agents.ListRunAttempts(c.Request().Context(), workspaceID, runID)
	if err != nil {
		return ErrorResponse(c, http.StatusInternalServerError, err.Error())
	}
	return SuccessResponse(c, attempts)
}

func (g *OrchestrationGroup) ListRunSnapshots(c echo.Context) error {
	workspaceID := auth.WorkspaceId(c.Request().Context())
	if workspaceID == 0 {
		return ErrorResponse(c, http.StatusUnauthorized, "workspace auth required")
	}
	runID := c.Param("run_id")
	snaps, err := g.agents.ListRunSnapshots(c.Request().Context(), workspaceID, runID, 500)
	if err != nil {
		return ErrorResponse(c, http.StatusInternalServerError, err.Error())
	}
	return SuccessResponse(c, snaps)
}

func (g *OrchestrationGroup) ListRunEvents(c echo.Context) error {
	workspaceID := auth.WorkspaceId(c.Request().Context())
	if workspaceID == 0 {
		return ErrorResponse(c, http.StatusUnauthorized, "workspace auth required")
	}
	runID := c.Param("run_id")
	events, err := g.agents.ListRunEvents(c.Request().Context(), workspaceID, runID)
	if err != nil {
		return ErrorResponse(c, http.StatusInternalServerError, err.Error())
	}
	return SuccessResponse(c, events)
}

func (g *OrchestrationGroup) CancelRun(c echo.Context) error {
	workspaceID := auth.WorkspaceId(c.Request().Context())
	if workspaceID == 0 {
		return ErrorResponse(c, http.StatusUnauthorized, "workspace auth required")
	}
	runID := c.Param("run_id")
	if err := g.agents.CancelRun(c.Request().Context(), workspaceID, runID); err != nil {
		if _, ok := err.(*types.ErrAgentRunNotFound); ok {
			return ErrorResponse(c, http.StatusNotFound, "run not found")
		}
		return ErrorResponse(c, http.StatusInternalServerError, err.Error())
	}
	return SuccessResponse(c, map[string]any{"status": "cancelled"})
}

// decodeStrictJSON is used by tests that need strict unknown-field validation on raw body blobs.
func decodeStrictJSON(data []byte, dst any) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	return dec.Decode(dst)
}
