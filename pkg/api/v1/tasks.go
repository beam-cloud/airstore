package apiv1

import (
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/beam-cloud/airstore/pkg/common"
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
	g.routerGroup.GET("", g.ListTasks)
	g.routerGroup.GET("/:task_id", g.GetTask)
	g.routerGroup.GET("/:task_id/logs", g.ListTaskLogs)
	g.routerGroup.GET("/:task_id/stream", g.StreamTaskEvents)
	g.routerGroup.POST("/:task_id/cancel", g.CancelTask)
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

type listTasksResponse struct {
	Tasks      []*types.AgentTask `json:"tasks"`
	NextCursor string             `json:"next_cursor"`
	HasMore    bool               `json:"has_more"`
}

type listTaskLogsResponse struct {
	Logs       []common.TaskLogEntry `json:"logs"`
	NextCursor int64                 `json:"next_cursor"`
}

func (g *WorkspaceTasksGroup) ListTasks(c echo.Context) error {
	if g.agents == nil {
		return ErrorResponse(c, http.StatusServiceUnavailable, "task service unavailable")
	}
	workspaceID, err := requireWorkspaceID(c)
	if err != nil {
		return err
	}

	limit := parseLimitParam(c.QueryParam("limit"), 50, 200)
	offset, err := parseOffsetCursor(c.QueryParam("cursor"))
	if err != nil {
		return ErrorResponse(c, http.StatusBadRequest, "invalid cursor")
	}

	states, err := parseTaskStates(c.QueryParam("state"))
	if err != nil {
		return ErrorResponse(c, http.StatusBadRequest, err.Error())
	}

	createdAfter, err := parseOptionalRFC3339(c.QueryParam("created_after"))
	if err != nil {
		return ErrorResponse(c, http.StatusBadRequest, "invalid created_after timestamp")
	}
	createdBefore, err := parseOptionalRFC3339(c.QueryParam("created_before"))
	if err != nil {
		return ErrorResponse(c, http.StatusBadRequest, "invalid created_before timestamp")
	}

	filter := types.AgentTaskListFilter{
		AgentID:       strPtrMaybeQuery(c.QueryParam("agent_id")),
		States:        states,
		CreatedAfter:  createdAfter,
		CreatedBefore: createdBefore,
		Limit:         limit,
		Offset:        offset,
	}
	tasks, nextCursor, hasMore, err := g.agents.ListTasksFiltered(c.Request().Context(), workspaceID, filter)
	if err != nil {
		return ErrorResponse(c, http.StatusInternalServerError, err.Error())
	}

	return SuccessResponse(c, listTasksResponse{
		Tasks:      tasks,
		NextCursor: nextCursor,
		HasMore:    hasMore,
	})
}

func (g *WorkspaceTasksGroup) ListTaskLogs(c echo.Context) error {
	if g.agents == nil {
		return ErrorResponse(c, http.StatusServiceUnavailable, "task service unavailable")
	}
	workspaceID, err := requireWorkspaceID(c)
	if err != nil {
		return err
	}

	taskID := c.Param("task_id")
	cursor, err := parseInt64Query(c.QueryParam("cursor"))
	if err != nil {
		return ErrorResponse(c, http.StatusBadRequest, "invalid cursor")
	}

	logs, nextCursor, err := g.agents.ListTaskLogs(c.Request().Context(), workspaceID, taskID, cursor)
	if err != nil {
		if _, ok := err.(*types.ErrAgentTaskNotFound); ok {
			return ErrorResponse(c, http.StatusNotFound, "task not found")
		}
		return ErrorResponse(c, http.StatusInternalServerError, err.Error())
	}

	return SuccessResponse(c, listTaskLogsResponse{
		Logs:       logs,
		NextCursor: nextCursor,
	})
}

func (g *WorkspaceTasksGroup) StreamTaskEvents(c echo.Context) error {
	if g.agents == nil {
		return ErrorResponse(c, http.StatusServiceUnavailable, "task service unavailable")
	}
	workspaceID, err := requireWorkspaceID(c)
	if err != nil {
		return err
	}

	taskID := c.Param("task_id")
	logCursor, err := parseInt64Query(c.QueryParam("log_cursor"))
	if err != nil {
		return ErrorResponse(c, http.StatusBadRequest, "invalid log_cursor")
	}
	runEventCursor, err := parseOffsetCursor(c.QueryParam("run_event_cursor"))
	if err != nil {
		return ErrorResponse(c, http.StatusBadRequest, "invalid run_event_cursor")
	}
	cursorRunID := strings.TrimSpace(c.QueryParam("cursor_run_id"))

	batch, err := g.agents.StreamTaskEvents(
		c.Request().Context(),
		workspaceID,
		taskID,
		logCursor,
		runEventCursor,
		cursorRunID,
	)
	if err != nil {
		if _, ok := err.(*types.ErrAgentTaskNotFound); ok {
			return ErrorResponse(c, http.StatusNotFound, "task not found")
		}
		if _, ok := err.(*types.ErrAgentRunNotFound); ok {
			return ErrorResponse(c, http.StatusNotFound, "run not found")
		}
		return ErrorResponse(c, http.StatusInternalServerError, err.Error())
	}
	return SuccessResponse(c, batch)
}

func (g *WorkspaceTasksGroup) CancelTask(c echo.Context) error {
	if g.agents == nil {
		return ErrorResponse(c, http.StatusServiceUnavailable, "task service unavailable")
	}
	workspaceID, err := requireWorkspaceID(c)
	if err != nil {
		return err
	}
	taskID := c.Param("task_id")
	if err := g.agents.CancelTask(c.Request().Context(), workspaceID, taskID); err != nil {
		if _, ok := err.(*types.ErrAgentTaskNotFound); ok {
			return ErrorResponse(c, http.StatusNotFound, "task not found")
		}
		return ErrorResponse(c, http.StatusInternalServerError, err.Error())
	}
	return SuccessResponse(c, map[string]any{"status": "cancelled"})
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

func parseTaskStates(raw string) ([]types.AgentTaskState, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return nil, nil
	}
	parts := strings.Split(trimmed, ",")
	states := make([]types.AgentTaskState, 0, len(parts))
	for _, part := range parts {
		state := types.AgentTaskState(strings.TrimSpace(part))
		switch state {
		case types.AgentTaskStateQueued,
			types.AgentTaskStateRunning,
			types.AgentTaskStateIdle,
			types.AgentTaskStateDone,
			types.AgentTaskStateDropped,
			types.AgentTaskStateCancelled:
			states = append(states, state)
		default:
			return nil, errors.New("invalid state filter")
		}
	}
	return states, nil
}

func parseLimitParam(raw string, fallback int, max int) int {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return fallback
	}
	value, err := strconv.Atoi(trimmed)
	if err != nil || value <= 0 {
		return fallback
	}
	if value > max {
		return max
	}
	return value
}

func parseOffsetCursor(raw string) (int, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return 0, nil
	}
	value, err := strconv.Atoi(trimmed)
	if err != nil || value < 0 {
		return 0, errors.New("invalid cursor")
	}
	return value, nil
}

func parseInt64Query(raw string) (int64, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return 0, nil
	}
	value, err := strconv.ParseInt(trimmed, 10, 64)
	if err != nil || value < 0 {
		return 0, errors.New("invalid cursor")
	}
	return value, nil
}

func parseOptionalRFC3339(raw string) (*time.Time, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return nil, nil
	}
	parsed, err := time.Parse(time.RFC3339, trimmed)
	if err != nil {
		return nil, err
	}
	return &parsed, nil
}

func strPtrMaybeQuery(raw string) *string {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return nil
	}
	return &trimmed
}
