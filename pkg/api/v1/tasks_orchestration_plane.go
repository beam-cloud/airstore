package apiv1

import (
	"net/http"
	"strings"

	"github.com/beam-cloud/airstore/pkg/types"
	"github.com/labstack/echo/v4"
)

func (g *WorkspaceTasksGroup) UpdateTask(c echo.Context) error {
	if g.agents == nil {
		return ErrorResponse(c, http.StatusServiceUnavailable, "task service unavailable")
	}
	workspaceID, err := requireWorkspaceID(c)
	if err != nil {
		return err
	}
	taskID := strings.TrimSpace(c.Param("task_id"))
	task, err := g.agents.GetTask(c.Request().Context(), workspaceID, taskID)
	if err != nil {
		if _, ok := err.(*types.ErrAgentTaskNotFound); ok {
			return ErrorResponse(c, http.StatusNotFound, "task not found")
		}
		return ErrorResponse(c, http.StatusInternalServerError, err.Error())
	}

	var req struct {
		Priority    *string        `json:"priority,omitempty"`
		BudgetUSD   *float64       `json:"budget_usd,omitempty"`
		PayloadJSON map[string]any `json:"payload_json,omitempty"`
		RoutingJSON map[string]any `json:"routing_json,omitempty"`
	}
	if err := decodeStrictBody(c, &req); err != nil {
		return ErrorResponse(c, http.StatusBadRequest, "invalid request body")
	}
	if req.Priority != nil {
		task.Priority = strings.TrimSpace(*req.Priority)
	}
	if req.BudgetUSD != nil {
		task.BudgetUSD = req.BudgetUSD
	}
	if req.PayloadJSON != nil {
		task.PayloadJSON = req.PayloadJSON
	}
	if req.RoutingJSON != nil {
		task.RoutingJSON = req.RoutingJSON
	}
	if err := g.backend.UpdateTask(c.Request().Context(), task); err != nil {
		if _, ok := err.(*types.ErrAgentTaskNotFound); ok {
			return ErrorResponse(c, http.StatusNotFound, "task not found")
		}
		return ErrorResponse(c, http.StatusBadRequest, err.Error())
	}
	updated, err := g.agents.GetTask(c.Request().Context(), workspaceID, taskID)
	if err != nil {
		return ErrorResponse(c, http.StatusInternalServerError, err.Error())
	}
	return SuccessResponse(c, updated)
}
