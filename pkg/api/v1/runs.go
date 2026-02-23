package apiv1

import (
	"net/http"

	"github.com/beam-cloud/airstore/pkg/orchestration"
	"github.com/beam-cloud/airstore/pkg/types"
	"github.com/labstack/echo/v4"
)

type RunsGroup struct {
	routerGroup *echo.Group
	agents      *orchestration.AgentAPI
}

type enqueueRunInputAPIRequest struct {
	Message        string               `json:"message"`
	QueueMode      types.AgentQueueMode `json:"queue_mode"`
	IdempotencyKey string               `json:"idempotency_key"`
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
	g.routerGroup.POST("/:run_id/input", g.EnqueueRunInput)
	g.routerGroup.POST("/:run_id/cancel", g.CancelRun)
}

func (g *RunsGroup) ListRuns(c echo.Context) error {
	if g.agents == nil {
		return ErrorResponse(c, http.StatusServiceUnavailable, "run service unavailable")
	}
	workspaceID, err := requireWorkspaceID(c)
	if err != nil {
		return err
	}
	runs, err := g.agents.ListRuns(c.Request().Context(), workspaceID, 100)
	if err != nil {
		return ErrorResponse(c, http.StatusInternalServerError, err.Error())
	}
	return SuccessResponse(c, runs)
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

func (g *RunsGroup) EnqueueRunInput(c echo.Context) error {
	if g.agents == nil {
		return ErrorResponse(c, http.StatusServiceUnavailable, "run service unavailable")
	}

	var req enqueueRunInputAPIRequest
	if err := decodeStrictBody(c, &req); err != nil {
		return ErrorResponse(c, http.StatusBadRequest, "invalid request body")
	}

	workspaceID, err := requireWorkspaceID(c)
	if err != nil {
		return err
	}
	runID := c.Param("run_id")

	task, deduped, err := g.agents.EnqueueRunInput(
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
			"task":           task,
			"run_id":         task.TargetRunID,
		},
	})
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
