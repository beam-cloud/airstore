package apiv1

import (
	"errors"
	"net/http"
	"strings"

	"github.com/beam-cloud/airstore/pkg/orchestration"
	"github.com/beam-cloud/airstore/pkg/types"
	"github.com/labstack/echo/v4"
)

type RunsGroup struct {
	routerGroup *echo.Group
	agents      *orchestration.AgentAPI
}

type listRunsResponse struct {
	Runs       []*types.AgentRun `json:"runs"`
	NextCursor string            `json:"next_cursor"`
	HasMore    bool              `json:"has_more"`
}

func NewRunsGroup(routerGroup *echo.Group, agents *orchestration.AgentAPI) *RunsGroup {
	g := &RunsGroup{
		routerGroup: routerGroup,
		agents:      agents,
	}
	g.registerRoutes()
	return g
}

func (g *RunsGroup) registerRoutes() {
	g.routerGroup.GET("", g.ListRuns)
	g.routerGroup.GET("/:run_id", g.GetRun)
	g.routerGroup.GET("/:run_id/snapshots", g.ListRunSnapshots)
	g.routerGroup.GET("/:run_id/events", g.ListRunEvents)
	g.routerGroup.POST("/:run_id/cancel", g.CancelRun)
	g.routerGroup.POST("/:run_id/input", g.EnqueueInput)
}

func (g *RunsGroup) ListRuns(c echo.Context) error {
	if g.agents == nil {
		return ErrorResponse(c, http.StatusServiceUnavailable, "run service unavailable")
	}
	workspaceID, err := requireWorkspaceID(c)
	if err != nil {
		return err
	}
	limitQuery := strings.TrimSpace(c.QueryParam("limit"))
	cursorQuery := strings.TrimSpace(c.QueryParam("cursor"))
	statusQuery := strings.TrimSpace(c.QueryParam("status"))
	agentQuery := strings.TrimSpace(c.QueryParam("agent_id"))
	sessionQuery := strings.TrimSpace(c.QueryParam("session_id"))
	createdAfterQuery := strings.TrimSpace(c.QueryParam("created_after"))
	createdBeforeQuery := strings.TrimSpace(c.QueryParam("created_before"))
	if limitQuery == "" &&
		cursorQuery == "" &&
		statusQuery == "" &&
		agentQuery == "" &&
		sessionQuery == "" &&
		createdAfterQuery == "" &&
		createdBeforeQuery == "" {
		runs, err := g.agents.ListRuns(c.Request().Context(), workspaceID, 100)
		if err != nil {
			return ErrorResponse(c, http.StatusInternalServerError, err.Error())
		}
		return SuccessResponse(c, runs)
	}

	limit := parseLimitParam(limitQuery, 50, 200)
	offset, err := parseOffsetCursor(cursorQuery)
	if err != nil {
		return ErrorResponse(c, http.StatusBadRequest, "invalid cursor")
	}
	statuses, err := parseRunStatuses(statusQuery)
	if err != nil {
		return ErrorResponse(c, http.StatusBadRequest, err.Error())
	}
	createdAfter, err := parseOptionalRFC3339(createdAfterQuery)
	if err != nil {
		return ErrorResponse(c, http.StatusBadRequest, "invalid created_after timestamp")
	}
	createdBefore, err := parseOptionalRFC3339(createdBeforeQuery)
	if err != nil {
		return ErrorResponse(c, http.StatusBadRequest, "invalid created_before timestamp")
	}

	filter := types.AgentRunListFilter{
		AgentID:       strPtrMaybeQuery(agentQuery),
		Statuses:      statuses,
		SessionID:     strPtrMaybeQuery(sessionQuery),
		CreatedAfter:  createdAfter,
		CreatedBefore: createdBefore,
		Limit:         limit,
		Offset:        offset,
	}
	runs, nextCursor, hasMore, err := g.agents.ListRunsFiltered(c.Request().Context(), workspaceID, filter)
	if err != nil {
		return ErrorResponse(c, http.StatusInternalServerError, err.Error())
	}
	return SuccessResponse(c, listRunsResponse{
		Runs:       runs,
		NextCursor: nextCursor,
		HasMore:    hasMore,
	})
}

func (g *RunsGroup) GetRun(c echo.Context) error {
	if g.agents == nil {
		return ErrorResponse(c, http.StatusServiceUnavailable, "run service unavailable")
	}
	workspaceID, err := requireWorkspaceID(c)
	if err != nil {
		return err
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

func (g *RunsGroup) ListRunSnapshots(c echo.Context) error {
	if g.agents == nil {
		return ErrorResponse(c, http.StatusServiceUnavailable, "run service unavailable")
	}
	workspaceID, err := requireWorkspaceID(c)
	if err != nil {
		return err
	}
	runID := c.Param("run_id")
	snaps, err := g.agents.ListRunSnapshots(c.Request().Context(), workspaceID, runID, 500)
	if err != nil {
		if _, ok := err.(*types.ErrAgentRunNotFound); ok {
			return ErrorResponse(c, http.StatusNotFound, "run not found")
		}
		return ErrorResponse(c, http.StatusInternalServerError, err.Error())
	}
	return SuccessResponse(c, snaps)
}

func (g *RunsGroup) ListRunEvents(c echo.Context) error {
	if g.agents == nil {
		return ErrorResponse(c, http.StatusServiceUnavailable, "run service unavailable")
	}
	workspaceID, err := requireWorkspaceID(c)
	if err != nil {
		return err
	}
	runID := c.Param("run_id")
	events, err := g.agents.ListRunEvents(c.Request().Context(), workspaceID, runID)
	if err != nil {
		if _, ok := err.(*types.ErrAgentRunNotFound); ok {
			return ErrorResponse(c, http.StatusNotFound, "run not found")
		}
		return ErrorResponse(c, http.StatusInternalServerError, err.Error())
	}
	return SuccessResponse(c, events)
}

func (g *RunsGroup) CancelRun(c echo.Context) error {
	if g.agents == nil {
		return ErrorResponse(c, http.StatusServiceUnavailable, "run service unavailable")
	}
	workspaceID, err := requireWorkspaceID(c)
	if err != nil {
		return err
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

type enqueueRunInputRequest struct {
	Message        string               `json:"message"`
	QueueMode      types.AgentQueueMode `json:"queue_mode,omitempty"`
	IdempotencyKey string               `json:"idempotency_key,omitempty"`
}

func (g *RunsGroup) EnqueueInput(c echo.Context) error {
	if g.agents == nil {
		return ErrorResponse(c, http.StatusServiceUnavailable, "run service unavailable")
	}
	workspaceID, err := requireWorkspaceID(c)
	if err != nil {
		return err
	}
	var req enqueueRunInputRequest
	if err := decodeStrictBody(c, &req); err != nil {
		return ErrorResponse(c, http.StatusBadRequest, "invalid request body")
	}
	if strings.TrimSpace(req.Message) == "" {
		return ErrorResponse(c, http.StatusBadRequest, "message is required")
	}
	runID := c.Param("run_id")
	task, deduped, outcome, err := g.agents.EnqueueRunInput(
		c.Request().Context(), workspaceID, runID, req.QueueMode, req.Message, req.IdempotencyKey,
	)
	if err != nil {
		if _, ok := err.(*types.ErrAgentRunNotFound); ok {
			return ErrorResponse(c, http.StatusNotFound, "run not found")
		}
		if isAgentCommandValidationError(err) {
			return ErrorResponse(c, http.StatusBadRequest, err.Error())
		}
		return ErrorResponse(c, http.StatusInternalServerError, err.Error())
	}
	statusCode := http.StatusAccepted
	if deduped {
		statusCode = http.StatusOK
	}
	return c.JSON(statusCode, Response{
		Success: true,
		Data: map[string]any{
			"accepted":         true,
			"idempotent_hit":   deduped,
			"task":             task,
			"delivery_outcome": outcome,
		},
	})
}

func parseRunStatuses(raw string) ([]types.AgentRunStatus, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return nil, nil
	}
	parts := strings.Split(trimmed, ",")
	statuses := make([]types.AgentRunStatus, 0, len(parts))
	for _, part := range parts {
		status := types.AgentRunStatus(strings.TrimSpace(part))
		switch status {
		case types.AgentRunStatusAccepted,
			types.AgentRunStatusRunning,
			types.AgentRunStatusOK,
			types.AgentRunStatusError,
			types.AgentRunStatusTimeout,
			types.AgentRunStatusCancelled:
			statuses = append(statuses, status)
		default:
			return nil, errors.New("invalid status filter")
		}
	}
	return statuses, nil
}
