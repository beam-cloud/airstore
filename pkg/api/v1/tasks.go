package apiv1

import (
	"errors"
	"net/http"
	"strings"

	"github.com/beam-cloud/airstore/pkg/orchestration"
	"github.com/beam-cloud/airstore/pkg/types"
	"github.com/labstack/echo/v4"
)

type WorkspaceTasksGroup struct {
	routerGroup *echo.Group
	agents      *orchestration.AgentAPI
}

func NewWorkspaceTasksGroup(routerGroup *echo.Group, agents *orchestration.AgentAPI) *WorkspaceTasksGroup {
	g := &WorkspaceTasksGroup{
		routerGroup: routerGroup,
		agents:      agents,
	}
	g.registerRoutes()
	return g
}

func (g *WorkspaceTasksGroup) registerRoutes() {
	g.routerGroup.POST("", g.CreateTask)
	g.routerGroup.GET("/:task_id", g.GetTask)
}

func (g *WorkspaceTasksGroup) CreateTask(c echo.Context) error {
	if g.agents == nil {
		return ErrorResponse(c, http.StatusServiceUnavailable, "task service unavailable")
	}

	var req orchestration.AgentCommandParams
	if err := decodeStrictBody(c, &req); err != nil {
		return ErrorResponse(c, http.StatusBadRequest, "invalid request body")
	}

	workspaceID, err := requireWorkspaceID(c)
	if err != nil {
		return err
	}

	task, deduped, err := g.agents.AcceptAgentCommand(c.Request().Context(), workspaceID, req)
	if err != nil {
		return ErrorResponse(c, statusForAcceptAgentCommandError(err), err.Error())
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

func (g *WorkspaceTasksGroup) GetTask(c echo.Context) error {
	if g.agents == nil {
		return ErrorResponse(c, http.StatusServiceUnavailable, "task service unavailable")
	}
	workspaceID, err := requireWorkspaceID(c)
	if err != nil {
		return err
	}
	taskID := c.Param("task_id")
	task, err := g.agents.GetTask(c.Request().Context(), workspaceID, taskID)
	if err != nil {
		if _, ok := err.(*types.ErrAgentTaskNotFound); ok {
			return ErrorResponse(c, http.StatusNotFound, "task not found")
		}
		return ErrorResponse(c, http.StatusInternalServerError, err.Error())
	}
	return SuccessResponse(c, task)
}

func statusForAcceptAgentCommandError(err error) int {
	if err == nil {
		return http.StatusOK
	}

	if strings.Contains(strings.ToLower(err.Error()), "task service unavailable") {
		return http.StatusServiceUnavailable
	}

	var profileErr *types.ErrAgentProfileNotFound
	if errors.As(err, &profileErr) {
		return http.StatusBadRequest
	}

	if isAgentCommandValidationError(err) {
		return http.StatusBadRequest
	}

	return http.StatusInternalServerError
}

func isAgentCommandValidationError(err error) bool {
	msg := strings.ToLower(strings.TrimSpace(err.Error()))

	return strings.Contains(msg, " is required") ||
		strings.Contains(msg, " must be ") ||
		strings.Contains(msg, " must not ") ||
		strings.HasPrefix(msg, "invalid ") ||
		strings.Contains(msg, "not supported")
}
