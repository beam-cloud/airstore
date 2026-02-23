package apiv1

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/beam-cloud/airstore/pkg/common"
	"github.com/beam-cloud/airstore/pkg/orchestration"
	"github.com/beam-cloud/airstore/pkg/repository"
	"github.com/beam-cloud/airstore/pkg/types"
	"github.com/labstack/echo/v4"
)

type TasksGroup struct {
	routerGroup *echo.Group
	backend     repository.BackendRepository
	agents      *orchestration.AgentAPI
	terminalIO  repository.TerminalIORepository
	s2Client    *common.S2Client
}

type WorkspaceTasksGroup struct {
	routerGroup *echo.Group
	agents      *orchestration.AgentAPI
}

// TasksGroup is the task API surface.
//
// Task creation is unified and delegates to the same
// task acceptance path as `/workspaces/:workspace_id/tasks`.

type CreateTaskRequest struct {
	WorkspaceID       string                            `json:"workspace_id"`   // External workspace ID
	WorkspaceName     string                            `json:"workspace_name"` // Or workspace name
	Message           string                            `json:"message"`
	AgentID           string                            `json:"agent_id"`
	SessionID         string                            `json:"session_id,omitempty"`
	SessionKey        string                            `json:"session_key,omitempty"`
	Deliver           *bool                             `json:"deliver,omitempty"`
	TimeoutMs         *int                              `json:"timeout_ms,omitempty"`
	Policy            *orchestration.RunExecutionPolicy `json:"policy,omitempty"`
	Lane              string                            `json:"lane,omitempty"`
	ExtraSystemPrompt string                            `json:"extra_system_prompt,omitempty"`
	InputProvenance   *orchestration.InputProvenance    `json:"input_provenance,omitempty"`
	Routing           orchestration.RoutingContext      `json:"routing"`
	Attachments       []map[string]any                  `json:"attachments,omitempty"`
	IdempotencyKey    string                            `json:"idempotency_key,omitempty"`
	Label             string                            `json:"label,omitempty"`
	SpawnedBy         string                            `json:"spawned_by,omitempty"`
}

type RunExecutionResponse struct {
	ExternalID  string            `json:"external_id"`
	WorkspaceID string            `json:"workspace_id"`
	Status      string            `json:"status"`
	Type        string            `json:"type"`
	Prompt      string            `json:"prompt,omitempty"`
	Image       string            `json:"image"`
	Entrypoint  []string          `json:"entrypoint"`
	Env         map[string]string `json:"env"`
	ExitCode    *int              `json:"exit_code,omitempty"`
	Error       string            `json:"error,omitempty"`
	CreatedAt   string            `json:"created_at"`
	StartedAt   string            `json:"started_at,omitempty"`
	FinishedAt  string            `json:"finished_at,omitempty"`
}

type SetTaskResultRequest struct {
	ExitCode int    `json:"exit_code"`
	Error    string `json:"error"`
}

type TerminalInputRequest struct {
	Data    string `json:"data,omitempty"`
	DataB64 string `json:"data_b64,omitempty"`
}

func NewTasksGroup(
	routerGroup *echo.Group,
	backend repository.BackendRepository,
	agents *orchestration.AgentAPI,
	terminalIO repository.TerminalIORepository,
	s2Client *common.S2Client,
) *TasksGroup {
	g := &TasksGroup{
		routerGroup: routerGroup,
		backend:     backend,
		agents:      agents,
		terminalIO:  terminalIO,
		s2Client:    s2Client,
	}
	g.registerRoutes()
	return g
}

func NewWorkspaceTasksGroup(routerGroup *echo.Group, agents *orchestration.AgentAPI) *WorkspaceTasksGroup {
	g := &WorkspaceTasksGroup{
		routerGroup: routerGroup,
		agents:      agents,
	}
	g.registerRoutes()
	return g
}

func (g *TasksGroup) registerRoutes() {
	g.routerGroup.POST("", g.CreateTask)
	g.routerGroup.GET("", g.ListTasks)
	g.routerGroup.GET("/:id", g.GetTask)
	g.routerGroup.DELETE("/:id", g.DeleteTask)
	g.routerGroup.POST("/:id/cancel", g.CancelTask)
	g.routerGroup.PATCH("/:id/result", g.SetTaskResult)
	g.routerGroup.GET("/:id/logs/stream", g.StreamLogs)
	g.routerGroup.GET("/:id/terminal/connect", g.ConnectTerminal)
	g.routerGroup.POST("/:id/terminal/input", g.SendTerminalInput)
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

// CreateTask accepts a task and queues it through the agent runtime.
func (g *TasksGroup) CreateTask(c echo.Context) error {
	ctx := c.Request().Context()
	if g.agents == nil {
		return ErrorResponse(c, http.StatusServiceUnavailable, "task service unavailable")
	}

	var req CreateTaskRequest
	if err := decodeStrictBody(c, &req); err != nil {
		return ErrorResponse(c, http.StatusBadRequest, "invalid request body")
	}
	agentID := strings.TrimSpace(req.AgentID)
	if agentID == "" {
		return ErrorResponse(c, http.StatusBadRequest, "agent_id is required")
	}

	// Resolve workspace
	var workspace *types.Workspace
	var err error

	if req.WorkspaceID != "" {
		workspace, err = g.backend.GetWorkspaceByExternalId(ctx, req.WorkspaceID)
	} else if req.WorkspaceName != "" {
		workspace, err = g.backend.GetWorkspaceByName(ctx, req.WorkspaceName)
	} else {
		return ErrorResponse(c, http.StatusBadRequest, "workspace_id or workspace_name is required")
	}

	if err != nil {
		if _, ok := err.(*types.ErrWorkspaceNotFound); ok {
			return ErrorResponse(c, http.StatusBadRequest, "workspace not found")
		}
		return ErrorResponse(c, http.StatusInternalServerError, err.Error())
	}

	var sessionKey *string
	if value := strings.TrimSpace(req.SessionKey); value != "" {
		sessionKey = &value
	}
	var lane *string
	if value := strings.TrimSpace(req.Lane); value != "" {
		lane = &value
	}
	var extraSystemPrompt *string
	if value := strings.TrimSpace(req.ExtraSystemPrompt); value != "" {
		extraSystemPrompt = &value
	}
	var label *string
	if value := strings.TrimSpace(req.Label); value != "" {
		label = &value
	}
	var spawnedBy *string
	if value := strings.TrimSpace(req.SpawnedBy); value != "" {
		spawnedBy = &value
	}

	task, deduped, err := g.agents.AcceptAgentCommand(ctx, workspace.Id, orchestration.AgentCommandParams{
		Message:           req.Message,
		AgentID:           &agentID,
		SessionID:         req.SessionID,
		SessionKey:        sessionKey,
		Deliver:           req.Deliver,
		TimeoutMs:         req.TimeoutMs,
		Policy:            req.Policy,
		Lane:              lane,
		ExtraSystemPrompt: extraSystemPrompt,
		InputProvenance:   req.InputProvenance,
		Routing:           req.Routing,
		Attachments:       req.Attachments,
		IdempotencyKey:    req.IdempotencyKey,
		Label:             label,
		SpawnedBy:         spawnedBy,
	})
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

// ListTasks returns tasks, optionally filtered by workspace
func (g *TasksGroup) ListTasks(c echo.Context) error {
	workspaceExternalId := c.QueryParam("workspace_id")

	var workspaceId uint = 0
	if workspaceExternalId != "" {
		workspace, err := g.backend.GetWorkspaceByExternalId(c.Request().Context(), workspaceExternalId)
		if err != nil {
			if _, ok := err.(*types.ErrWorkspaceNotFound); ok {
				return ErrorResponse(c, http.StatusBadRequest, "workspace not found")
			}
			return ErrorResponse(c, http.StatusInternalServerError, err.Error())
		}
		workspaceId = workspace.Id
	}

	tasks, err := g.backend.ListRunExecutions(c.Request().Context(), workspaceId)
	if err != nil {
		return ErrorResponse(c, http.StatusInternalServerError, err.Error())
	}

	var response []RunExecutionResponse
	for _, t := range tasks {
		// Get workspace external ID for each task
		ws, _ := g.backend.GetWorkspace(c.Request().Context(), t.WorkspaceId)
		wsExternalId := ""
		if ws != nil {
			wsExternalId = ws.ExternalId
		}
		response = append(response, runExecutionToResponse(t, wsExternalId))
	}

	return SuccessResponse(c, response)
}

// GetTask returns a task by external ID
func (g *TasksGroup) GetTask(c echo.Context) error {
	externalId := c.Param("id")

	task, err := g.backend.GetRunExecution(c.Request().Context(), externalId)
	if err != nil {
		if _, ok := err.(*types.ErrRunExecutionNotFound); ok {
			return ErrorResponse(c, http.StatusNotFound, "task not found")
		}
		return ErrorResponse(c, http.StatusInternalServerError, err.Error())
	}

	// Get workspace external ID
	ws, _ := g.backend.GetWorkspace(c.Request().Context(), task.WorkspaceId)
	wsExternalId := ""
	if ws != nil {
		wsExternalId = ws.ExternalId
	}

	return SuccessResponse(c, runExecutionToResponse(task, wsExternalId))
}

// DeleteTask deletes a task by external ID
func (g *TasksGroup) DeleteTask(c echo.Context) error {
	externalId := c.Param("id")

	if err := g.backend.DeleteRunExecution(c.Request().Context(), externalId); err != nil {
		if _, ok := err.(*types.ErrRunExecutionNotFound); ok {
			return ErrorResponse(c, http.StatusNotFound, "task not found")
		}
		return ErrorResponse(c, http.StatusInternalServerError, err.Error())
	}

	return SuccessResponse(c, nil)
}

// CancelTask cancels a pending or running task
func (g *TasksGroup) CancelTask(c echo.Context) error {
	externalId := c.Param("id")

	if err := g.backend.CancelRunExecution(c.Request().Context(), externalId); err != nil {
		if _, ok := err.(*types.ErrRunExecutionNotFound); ok {
			return ErrorResponse(c, http.StatusNotFound, "task not found")
		}
		return ErrorResponse(c, http.StatusBadRequest, err.Error())
	}

	// Signal the worker to stop the sandbox immediately (best-effort).
	if g.terminalIO != nil {
		_ = g.terminalIO.PublishCancel(c.Request().Context(), externalId)
	}

	return SuccessResponse(c, map[string]string{"status": "cancelled"})
}

// SetTaskResult is called by workers to report task completion.
func (g *TasksGroup) SetTaskResult(c echo.Context) error {
	externalId := c.Param("id")

	var req SetTaskResultRequest
	if err := c.Bind(&req); err != nil {
		return ErrorResponse(c, http.StatusBadRequest, "invalid request body")
	}

	if err := g.backend.SetRunExecutionResult(c.Request().Context(), externalId, req.ExitCode, req.Error); err != nil {
		if _, ok := err.(*types.ErrRunExecutionNotFound); ok {
			return ErrorResponse(c, http.StatusNotFound, "task not found")
		}
		return ErrorResponse(c, http.StatusInternalServerError, err.Error())
	}

	return SuccessResponse(c, nil)
}

// ConnectTerminal streams interactive terminal output via SSE.
func (g *TasksGroup) ConnectTerminal(c echo.Context) error {
	ctx := c.Request().Context()
	taskID := c.Param("id")

	if _, err := g.requireInteractiveTerminalTask(c, taskID); err != nil {
		return err
	}

	outCh, cleanup, err := g.terminalIO.SubscribeOutput(ctx, taskID)
	if err != nil {
		return ErrorResponse(c, http.StatusInternalServerError, "failed to subscribe terminal output")
	}
	defer cleanup()

	w := &sseWriter{c: c}
	w.init()

	keepalive := time.NewTicker(20 * time.Second)
	defer keepalive.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case chunk, ok := <-outCh:
			if !ok {
				return nil
			}
			w.write(map[string]any{
				"type":     "output",
				"data_b64": base64.StdEncoding.EncodeToString(chunk),
			})
			w.flush()
		case <-keepalive.C:
			w.comment("keepalive")
		}
	}
}

// SendTerminalInput publishes input bytes for an interactive task terminal.
func (g *TasksGroup) SendTerminalInput(c echo.Context) error {
	ctx := c.Request().Context()
	taskID := c.Param("id")

	if _, err := g.requireInteractiveTerminalTask(c, taskID); err != nil {
		return err
	}

	var req TerminalInputRequest
	if err := c.Bind(&req); err != nil {
		return ErrorResponse(c, http.StatusBadRequest, "invalid request body")
	}

	data, err := decodeTerminalInput(req)
	if err != nil {
		return ErrorResponse(c, http.StatusBadRequest, err.Error())
	}

	if err := g.terminalIO.PublishInput(ctx, taskID, data); err != nil {
		return ErrorResponse(c, http.StatusInternalServerError, "failed to publish terminal input")
	}

	return SuccessResponse(c, map[string]bool{"ok": true})
}

// StreamLogs streams task logs via SSE from S2.
func (g *TasksGroup) StreamLogs(c echo.Context) error {
	taskID := c.Param("id")
	ctx := c.Request().Context()

	task, err := g.backend.GetRunExecution(ctx, taskID)
	if err != nil {
		if _, ok := err.(*types.ErrRunExecutionNotFound); ok {
			return ErrorResponse(c, http.StatusNotFound, "task not found")
		}
		return ErrorResponse(c, http.StatusInternalServerError, err.Error())
	}

	if g.s2Client == nil || !g.s2Client.Enabled() {
		return ErrorResponse(c, http.StatusServiceUnavailable, "log streaming unavailable")
	}

	w := &sseWriter{c: c}
	w.init()

	// Send buffered logs, track cursor for dedup and seqNum for pagination
	logs, seqNum, _ := g.s2Client.ReadLogs(ctx, taskID, 0)
	cursor := w.sendLogs(logs)

	if task.IsTerminal() {
		w.sendStatus(task)
		return nil
	}

	// Poll for new logs until done
	tick := time.NewTicker(500 * time.Millisecond)
	defer tick.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-tick.C:
			logs, seqNum, _ = g.s2Client.ReadLogs(ctx, taskID, seqNum)
			cursor = w.sendLogsAfter(logs, cursor)

			if task, err = g.backend.GetRunExecution(ctx, taskID); err == nil && task.IsTerminal() {
				w.sendStatus(task)
				return nil
			}
		}
	}
}

func (g *TasksGroup) requireInteractiveTask(c echo.Context, taskID string) (*types.RunExecution, error) {
	task, err := g.backend.GetRunExecution(c.Request().Context(), taskID)
	if err != nil {
		if _, ok := err.(*types.ErrRunExecutionNotFound); ok {
			return nil, ErrorResponse(c, http.StatusNotFound, "task not found")
		}
		return nil, ErrorResponse(c, http.StatusInternalServerError, err.Error())
	}

	if !task.IsInteractive() {
		return nil, ErrorResponse(c, http.StatusBadRequest, "task is not interactive")
	}

	return task, nil
}

func (g *TasksGroup) requireInteractiveTerminalTask(c echo.Context, taskID string) (*types.RunExecution, error) {
	task, err := g.requireInteractiveTask(c, taskID)
	if err != nil {
		return nil, err
	}
	if task.IsTerminal() {
		return nil, ErrorResponse(c, http.StatusConflict, "task has already finished")
	}
	if g.terminalIO == nil {
		return nil, ErrorResponse(c, http.StatusServiceUnavailable, "terminal transport unavailable")
	}
	return task, nil
}

func decodeTerminalInput(req TerminalInputRequest) ([]byte, error) {
	if req.DataB64 == "" {
		return []byte(req.Data), nil
	}
	decoded, err := base64.StdEncoding.DecodeString(req.DataB64)
	if err != nil {
		return nil, errors.New("invalid data_b64")
	}
	return decoded, nil
}

// sseWriter handles SSE output formatting.
type sseWriter struct {
	c echo.Context
}

func (w *sseWriter) init() {
	h := w.c.Response().Header()
	h.Set("Content-Type", "text/event-stream")
	h.Set("Cache-Control", "no-cache")
	h.Set("Connection", "keep-alive")
	h.Set("X-Accel-Buffering", "no")
	w.c.Response().WriteHeader(http.StatusOK)
}

func (w *sseWriter) write(v any) {
	data, _ := json.Marshal(v)
	r := w.c.Response()
	r.Write([]byte("data: "))
	r.Write(data)
	r.Write([]byte("\n\n"))
}

func (w *sseWriter) comment(text string) {
	r := w.c.Response()
	r.Write([]byte(": " + text + "\n\n"))
	r.Flush()
}

func (w *sseWriter) flush() {
	w.c.Response().Flush()
}

func (w *sseWriter) logEvent(e common.TaskLogEntry) map[string]any {
	return map[string]any{
		"type":      "log",
		"task_id":   e.TaskID,
		"timestamp": e.Timestamp,
		"stream":    e.Stream,
		"data":      e.Data,
	}
}

func (w *sseWriter) sendLogs(logs []common.TaskLogEntry) int64 {
	var cursor int64
	for _, e := range logs {
		w.write(w.logEvent(e))
		if e.Timestamp > cursor {
			cursor = e.Timestamp
		}
	}
	if len(logs) > 0 {
		w.flush()
	}
	return cursor
}

func (w *sseWriter) sendLogsAfter(logs []common.TaskLogEntry, cursor int64) int64 {
	dirty := false
	for _, e := range logs {
		if e.Timestamp > cursor {
			w.write(w.logEvent(e))
			cursor = e.Timestamp
			dirty = true
		}
	}
	if dirty {
		w.flush()
	}
	return cursor
}

func (w *sseWriter) sendStatus(task *types.RunExecution) {
	w.write(map[string]any{
		"type":      "status",
		"task_id":   task.ExternalId,
		"status":    task.Status,
		"exit_code": task.ExitCode,
		"error":     task.Error,
	})
	w.flush()
}

func runExecutionToResponse(t *types.RunExecution, workspaceExternalId string) RunExecutionResponse {
	t.NormalizeType()
	resp := RunExecutionResponse{
		ExternalID:  t.ExternalId,
		WorkspaceID: workspaceExternalId,
		Status:      string(t.Status),
		Type:        string(t.Type),
		Prompt:      t.Prompt,
		Image:       t.Image,
		Entrypoint:  t.Entrypoint,
		Env:         t.Env,
		ExitCode:    t.ExitCode,
		Error:       t.Error,
		CreatedAt:   t.CreatedAt.Format("2006-01-02T15:04:05Z07:00"),
	}
	if t.StartedAt != nil {
		resp.StartedAt = t.StartedAt.Format("2006-01-02T15:04:05Z07:00")
	}
	if t.FinishedAt != nil {
		resp.FinishedAt = t.FinishedAt.Format("2006-01-02T15:04:05Z07:00")
	}
	return resp
}
