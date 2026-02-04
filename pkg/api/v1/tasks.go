package apiv1

import (
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/beam-cloud/airstore/pkg/auth"
	"github.com/beam-cloud/airstore/pkg/common"
	"github.com/beam-cloud/airstore/pkg/repository"
	"github.com/beam-cloud/airstore/pkg/types"
	"github.com/labstack/echo/v4"
)

type TasksGroup struct {
	routerGroup  *echo.Group
	backend      repository.BackendRepository
	taskQueue    repository.TaskQueue
	s2Client     *common.S2Client
	defaultImage string
}

type CreateTaskRequest struct {
	WorkspaceID   string               `json:"workspace_id"`   // External workspace ID
	WorkspaceName string               `json:"workspace_name"` // Or workspace name
	Prompt        string               `json:"prompt"`         // Claude Code prompt (auto-sets image)
	Image         string               `json:"image"`          // Container image (optional if prompt provided)
	Entrypoint    []string             `json:"entrypoint"`
	Env           map[string]string    `json:"env"`
	Resources     *types.TaskResources `json:"resources,omitempty"` // CPU/Memory/GPU (uses defaults if nil)
}

type TaskResponse struct {
	ExternalID  string            `json:"external_id"`
	WorkspaceID string            `json:"workspace_id"`
	Status      string            `json:"status"`
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

func NewTasksGroup(
	routerGroup *echo.Group,
	backend repository.BackendRepository,
	taskQueue repository.TaskQueue,
	s2Client *common.S2Client,
	defaultImage string,
) *TasksGroup {
	g := &TasksGroup{
		routerGroup:  routerGroup,
		backend:      backend,
		taskQueue:    taskQueue,
		s2Client:     s2Client,
		defaultImage: defaultImage,
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
}

// CreateTask creates a new task and queues it for execution
func (g *TasksGroup) CreateTask(c echo.Context) error {
	ctx := c.Request().Context()

	var req CreateTaskRequest
	if err := c.Bind(&req); err != nil {
		return ErrorResponse(c, http.StatusBadRequest, "invalid request body")
	}

	// If prompt is provided, this is a Claude Code task - use default sandbox image
	if req.Prompt != "" {
		req.Image = g.defaultImage
	}

	if req.Image == "" {
		return ErrorResponse(c, http.StatusBadRequest, "image or prompt is required")
	}

	// Validate resource limits
	if err := req.Resources.Validate(); err != nil {
		return ErrorResponse(c, http.StatusBadRequest, err.Error())
	}

	// Get member info from auth context
	var createdByMemberId *uint
	memberId := auth.MemberId(ctx)
	if memberId > 0 {
		createdByMemberId = &memberId
	}

	// Extract auth token for passing to container (for filesystem mounting)
	var memberToken string
	authHeader := c.Request().Header.Get("Authorization")
	if strings.HasPrefix(authHeader, "Bearer ") {
		memberToken = strings.TrimPrefix(authHeader, "Bearer ")
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

	task := &types.Task{
		WorkspaceId:       workspace.Id,
		CreatedByMemberId: createdByMemberId,
		MemberToken:       memberToken,
		Status:            types.TaskStatusPending,
		Prompt:            req.Prompt,
		Image:             req.Image,
		Entrypoint:        req.Entrypoint,
		Env:               req.Env,
		Resources:         req.Resources,
	}

	if task.Env == nil {
		task.Env = make(map[string]string)
	}
	if task.Entrypoint == nil {
		task.Entrypoint = []string{}
	}

	// Save to Postgres
	if err := g.backend.CreateTask(ctx, task); err != nil {
		return ErrorResponse(c, http.StatusInternalServerError, err.Error())
	}

	// Push to Redis queue for worker to pick up
	if g.taskQueue != nil {
		if err := g.taskQueue.Push(ctx, task); err != nil {
			// Log but don't fail - task is saved, can be retried
			c.Logger().Errorf("failed to push task to queue: %v", err)
		}
	}

	return c.JSON(http.StatusCreated, Response{
		Success: true,
		Data:    taskToResponse(task, workspace.ExternalId),
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

	tasks, err := g.backend.ListTasks(c.Request().Context(), workspaceId)
	if err != nil {
		return ErrorResponse(c, http.StatusInternalServerError, err.Error())
	}

	var response []TaskResponse
	for _, t := range tasks {
		// Get workspace external ID for each task
		ws, _ := g.backend.GetWorkspace(c.Request().Context(), t.WorkspaceId)
		wsExternalId := ""
		if ws != nil {
			wsExternalId = ws.ExternalId
		}
		response = append(response, taskToResponse(t, wsExternalId))
	}

	return SuccessResponse(c, response)
}

// GetTask returns a task by external ID
func (g *TasksGroup) GetTask(c echo.Context) error {
	externalId := c.Param("id")

	task, err := g.backend.GetTask(c.Request().Context(), externalId)
	if err != nil {
		if _, ok := err.(*types.ErrTaskNotFound); ok {
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

	return SuccessResponse(c, taskToResponse(task, wsExternalId))
}

// DeleteTask deletes a task by external ID
func (g *TasksGroup) DeleteTask(c echo.Context) error {
	externalId := c.Param("id")

	if err := g.backend.DeleteTask(c.Request().Context(), externalId); err != nil {
		if _, ok := err.(*types.ErrTaskNotFound); ok {
			return ErrorResponse(c, http.StatusNotFound, "task not found")
		}
		return ErrorResponse(c, http.StatusInternalServerError, err.Error())
	}

	return SuccessResponse(c, nil)
}

// CancelTask cancels a pending or running task
func (g *TasksGroup) CancelTask(c echo.Context) error {
	externalId := c.Param("id")

	if err := g.backend.CancelTask(c.Request().Context(), externalId); err != nil {
		if _, ok := err.(*types.ErrTaskNotFound); ok {
			return ErrorResponse(c, http.StatusNotFound, "task not found")
		}
		return ErrorResponse(c, http.StatusBadRequest, err.Error())
	}

	return SuccessResponse(c, map[string]string{"status": "cancelled"})
}

// SetTaskResult is called by workers to report task completion
type SetTaskResultRequest struct {
	ExitCode int    `json:"exit_code"`
	Error    string `json:"error"`
}

func (g *TasksGroup) SetTaskResult(c echo.Context) error {
	externalId := c.Param("id")

	var req SetTaskResultRequest
	if err := c.Bind(&req); err != nil {
		return ErrorResponse(c, http.StatusBadRequest, "invalid request body")
	}

	if err := g.backend.SetTaskResult(c.Request().Context(), externalId, req.ExitCode, req.Error); err != nil {
		if _, ok := err.(*types.ErrTaskNotFound); ok {
			return ErrorResponse(c, http.StatusNotFound, "task not found")
		}
		return ErrorResponse(c, http.StatusInternalServerError, err.Error())
	}

	return SuccessResponse(c, nil)
}

// StreamLogs streams task logs via SSE from S2.
func (g *TasksGroup) StreamLogs(c echo.Context) error {
	taskID := c.Param("id")
	ctx := c.Request().Context()

	task, err := g.backend.GetTask(ctx, taskID)
	if err != nil {
		if _, ok := err.(*types.ErrTaskNotFound); ok {
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

			if task, err = g.backend.GetTask(ctx, taskID); err == nil && task.IsTerminal() {
				w.sendStatus(task)
				return nil
			}
		}
	}
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

func (w *sseWriter) flush() {
	w.c.Response().Flush()
}

func (w *sseWriter) sendLogs(logs []common.TaskLogEntry) int64 {
	var cursor int64
	for _, e := range logs {
		w.write(e)
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
			w.write(e)
			cursor = e.Timestamp
			dirty = true
		}
	}
	if dirty {
		w.flush()
	}
	return cursor
}

func (w *sseWriter) sendStatus(task *types.Task) {
	w.write(map[string]any{
		"type":      "status",
		"task_id":   task.ExternalId,
		"status":    task.Status,
		"exit_code": task.ExitCode,
		"error":     task.Error,
	})
	w.flush()
}

func taskToResponse(t *types.Task, workspaceExternalId string) TaskResponse {
	resp := TaskResponse{
		ExternalID:  t.ExternalId,
		WorkspaceID: workspaceExternalId,
		Status:      string(t.Status),
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
