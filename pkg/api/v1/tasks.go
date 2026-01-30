package apiv1

import (
	"net/http"

	"github.com/beam-cloud/airstore/pkg/repository"
	"github.com/beam-cloud/airstore/pkg/types"
	"github.com/labstack/echo/v4"
)

type TasksGroup struct {
	routerGroup *echo.Group
	backend     repository.BackendRepository
	taskQueue   repository.TaskQueue
}

type CreateTaskRequest struct {
	WorkspaceID   string            `json:"workspace_id"`   // External workspace ID
	WorkspaceName string            `json:"workspace_name"` // Or workspace name
	Image         string            `json:"image" validate:"required"`
	Entrypoint    []string          `json:"entrypoint"`
	Env           map[string]string `json:"env"`
}

type TaskResponse struct {
	ExternalID  string            `json:"external_id"`
	WorkspaceID string            `json:"workspace_id"`
	Status      string            `json:"status"`
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
) *TasksGroup {
	g := &TasksGroup{
		routerGroup: routerGroup,
		backend:     backend,
		taskQueue:   taskQueue,
	}
	g.registerRoutes()
	return g
}

func (g *TasksGroup) registerRoutes() {
	g.routerGroup.POST("", g.CreateTask)
	g.routerGroup.GET("", g.ListTasks)
	g.routerGroup.GET("/:id", g.GetTask)
	g.routerGroup.DELETE("/:id", g.DeleteTask)
	g.routerGroup.PATCH("/:id/result", g.SetTaskResult)
}

// CreateTask creates a new task and queues it for execution
func (g *TasksGroup) CreateTask(c echo.Context) error {
	var req CreateTaskRequest
	if err := c.Bind(&req); err != nil {
		return ErrorResponse(c, http.StatusBadRequest, "invalid request body")
	}

	if req.Image == "" {
		return ErrorResponse(c, http.StatusBadRequest, "image is required")
	}

	// Resolve workspace
	var workspace *types.Workspace
	var err error

	if req.WorkspaceID != "" {
		workspace, err = g.backend.GetWorkspaceByExternalId(c.Request().Context(), req.WorkspaceID)
	} else if req.WorkspaceName != "" {
		workspace, err = g.backend.GetWorkspaceByName(c.Request().Context(), req.WorkspaceName)
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
		WorkspaceId: workspace.Id,
		Status:      types.TaskStatusPending,
		Image:       req.Image,
		Entrypoint:  req.Entrypoint,
		Env:         req.Env,
	}

	if task.Env == nil {
		task.Env = make(map[string]string)
	}
	if task.Entrypoint == nil {
		task.Entrypoint = []string{}
	}

	// Save to Postgres
	if err := g.backend.CreateTask(c.Request().Context(), task); err != nil {
		return ErrorResponse(c, http.StatusInternalServerError, err.Error())
	}

	// Push to Redis queue for worker to pick up
	if g.taskQueue != nil {
		if err := g.taskQueue.Push(c.Request().Context(), task); err != nil {
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

func taskToResponse(t *types.Task, workspaceExternalId string) TaskResponse {
	resp := TaskResponse{
		ExternalID:  t.ExternalId,
		WorkspaceID: workspaceExternalId,
		Status:      string(t.Status),
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
