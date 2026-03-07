package apiv1

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/beam-cloud/airstore/pkg/auth"
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
	g.routerGroup.PATCH("/:task_id", g.UpdateTask)
	g.routerGroup.GET("/:task_id/logs", g.ListTaskLogs)
	g.routerGroup.GET("/:task_id/stream", g.StreamTaskEvents)
	g.routerGroup.POST("/:task_id/cancel", g.CancelTask)
	g.routerGroup.POST("/:task_id/archive", g.ArchiveTask)

	sched := g.routerGroup.Group("/schedules")
	sched.POST("", g.CreateSchedule)
	sched.GET("", g.ListSchedules)
	sched.GET("/:id", g.GetSchedule)
	sched.PATCH("/:id", g.UpdateSchedule)
	sched.DELETE("/:id", g.DeleteSchedule)
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
		AgentID: strPtrMaybeQuery(c.QueryParam("agent_id")),
		States:  states,
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
		if _, ok := err.(*types.ErrTaskNotCancellable); ok {
			return ErrorResponse(c, http.StatusBadRequest, err.Error())
		}
		return ErrorResponse(c, http.StatusInternalServerError, err.Error())
	}
	return SuccessResponse(c, map[string]any{"status": "cancelled"})
}

func (g *WorkspaceTasksGroup) ArchiveTask(c echo.Context) error {
	if g.agents == nil {
		return ErrorResponse(c, http.StatusServiceUnavailable, "task service unavailable")
	}
	workspaceID, err := requireWorkspaceID(c)
	if err != nil {
		return err
	}
	taskID := c.Param("task_id")
	if err := g.agents.ArchiveTask(c.Request().Context(), workspaceID, taskID); err != nil {
		if _, ok := err.(*types.ErrAgentTaskNotFound); ok {
			return ErrorResponse(c, http.StatusNotFound, "task not found")
		}
		if _, ok := err.(*types.ErrTaskNotArchivable); ok {
			return ErrorResponse(c, http.StatusBadRequest, err.Error())
		}
		return ErrorResponse(c, http.StatusInternalServerError, err.Error())
	}
	return SuccessResponse(c, map[string]any{"status": "archived"})
}

func (g *WorkspaceTasksGroup) UpdateTask(c echo.Context) error {
	if g.agents == nil {
		return ErrorResponse(c, http.StatusServiceUnavailable, "task service unavailable")
	}
	workspaceID, err := requireWorkspaceID(c)
	if err != nil {
		return err
	}

	var params orchestration.TaskUpdateParams
	if err := decodeStrictBody(c, &params); err != nil {
		return ErrorResponse(c, http.StatusBadRequest, "invalid request body")
	}

	task, err := g.agents.UpdateTask(c.Request().Context(), workspaceID, c.Param("task_id"), params)
	if err != nil {
		if _, ok := err.(*types.ErrAgentTaskNotFound); ok {
			return ErrorResponse(c, http.StatusNotFound, "task not found")
		}
		return ErrorResponse(c, http.StatusBadRequest, err.Error())
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
			types.AgentTaskStateWaiting,
			types.AgentTaskStateSleeping,
			types.AgentTaskStateRunning,
			types.AgentTaskStateDone,
			types.AgentTaskStateError,
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

// Scheduled task handlers

func (g *WorkspaceTasksGroup) CreateSchedule(c echo.Context) error {
	if g.agents == nil {
		return ErrorResponse(c, http.StatusServiceUnavailable, "task service unavailable")
	}
	ctx := c.Request().Context()
	workspaceID, err := requireWorkspaceID(c)
	if err != nil {
		return err
	}

	var req struct {
		AgentID    string   `json:"agent_id"`
		CronExpr   string   `json:"cron_expr"`
		Timezone   string   `json:"timezone"`
		Prompt     string   `json:"prompt"`
		SkillPaths []string `json:"skill_paths"`
	}
	if err := c.Bind(&req); err != nil {
		return ErrorResponse(c, http.StatusBadRequest, "invalid request")
	}
	for _, pair := range [][2]string{{"cron_expr", req.CronExpr}, {"prompt", req.Prompt}, {"agent_id", req.AgentID}} {
		if strings.TrimSpace(pair[1]) == "" {
			return ErrorResponse(c, http.StatusBadRequest, pair[0]+" is required")
		}
	}

	agent, err := g.agents.GetAgent(ctx, workspaceID, req.AgentID)
	if err != nil {
		return ErrorResponse(c, http.StatusNotFound, "agent not found")
	}

	st, err := g.agents.CreateSchedule(
		ctx, workspaceID, agent.ID, req.CronExpr, req.Timezone, req.Prompt,
		req.SkillPaths, ptrUint(auth.MemberId(ctx)), ptrUint(auth.TokenId(ctx)), nil,
	)
	if err != nil {
		return ErrorResponse(c, http.StatusInternalServerError, err.Error())
	}
	return c.JSON(http.StatusCreated, Response{Success: true, Data: g.scheduleResp(ctx, st)})
}

func (g *WorkspaceTasksGroup) ListSchedules(c echo.Context) error {
	if g.agents == nil {
		return ErrorResponse(c, http.StatusServiceUnavailable, "task service unavailable")
	}
	ctx := c.Request().Context()
	workspaceID, err := requireWorkspaceID(c)
	if err != nil {
		return err
	}
	list, err := g.agents.ListSchedules(ctx, workspaceID)
	if err != nil {
		return ErrorResponse(c, http.StatusInternalServerError, err.Error())
	}
	resp := make([]map[string]any, 0, len(list))
	for _, s := range list {
		resp = append(resp, g.scheduleResp(ctx, s))
	}
	return SuccessResponse(c, resp)
}

func (g *WorkspaceTasksGroup) GetSchedule(c echo.Context) error {
	if g.agents == nil {
		return ErrorResponse(c, http.StatusServiceUnavailable, "task service unavailable")
	}
	ctx := c.Request().Context()
	workspaceID, err := requireWorkspaceID(c)
	if err != nil {
		return err
	}
	st, err := g.agents.GetSchedule(ctx, workspaceID, c.Param("id"))
	if err != nil {
		return ErrorResponse(c, http.StatusNotFound, err.Error())
	}
	return SuccessResponse(c, g.scheduleResp(ctx, st))
}

func (g *WorkspaceTasksGroup) UpdateSchedule(c echo.Context) error {
	if g.agents == nil {
		return ErrorResponse(c, http.StatusServiceUnavailable, "task service unavailable")
	}
	var req struct {
		CronExpr   *string   `json:"cron_expr,omitempty"`
		Timezone   *string   `json:"timezone,omitempty"`
		Prompt     *string   `json:"prompt,omitempty"`
		SkillPaths *[]string `json:"skill_paths,omitempty"`
		Active     *bool     `json:"active,omitempty"`
	}
	if err := c.Bind(&req); err != nil {
		return ErrorResponse(c, http.StatusBadRequest, "invalid request")
	}
	ctx := c.Request().Context()
	workspaceID, err := requireWorkspaceID(c)
	if err != nil {
		return err
	}
	st, err := g.agents.UpdateSchedule(ctx, workspaceID, c.Param("id"), req.CronExpr, req.Timezone, req.Prompt, req.SkillPaths, req.Active)
	if err != nil {
		return g.scheduleError(c, err)
	}
	return SuccessResponse(c, g.scheduleResp(ctx, st))
}

func (g *WorkspaceTasksGroup) DeleteSchedule(c echo.Context) error {
	if g.agents == nil {
		return ErrorResponse(c, http.StatusServiceUnavailable, "task service unavailable")
	}
	ctx := c.Request().Context()
	workspaceID, err := requireWorkspaceID(c)
	if err != nil {
		return err
	}
	if err := g.agents.DeleteSchedule(ctx, workspaceID, c.Param("id")); err != nil {
		return g.scheduleError(c, err)
	}
	return SuccessResponse(c, nil)
}

func (g *WorkspaceTasksGroup) scheduleError(c echo.Context, err error) error {
	if _, ok := err.(*types.ErrScheduledTaskNotFound); ok {
		return ErrorResponse(c, http.StatusNotFound, err.Error())
	}
	return ErrorResponse(c, http.StatusInternalServerError, err.Error())
}

func (g *WorkspaceTasksGroup) scheduleResp(ctx context.Context, st *types.ScheduledTask) map[string]any {
	agentName := ""
	if agent, _ := g.agents.GetAgent(ctx, st.WorkspaceID, st.AgentID); agent != nil {
		agentName = agent.Name
	}
	skillPaths := st.SkillPaths
	if skillPaths == nil {
		skillPaths = []string{}
	}
	resp := map[string]any{
		"external_id":  st.ExternalID,
		"agent_id":     st.AgentID,
		"agent_name":   agentName,
		"cron_expr":    st.CronExpr,
		"timezone":     st.Timezone,
		"prompt":       st.Prompt,
		"skill_paths":  skillPaths,
		"active":       st.Active,
		"next_run_at":  st.NextRunAt.Format(time.RFC3339),
		"created_at":   st.CreatedAt.Format(time.RFC3339),
		"updated_at":   st.UpdatedAt.Format(time.RFC3339),
	}
	if st.LastRunAt != nil {
		resp["last_run_at"] = st.LastRunAt.Format(time.RFC3339)
	}
	return resp
}
