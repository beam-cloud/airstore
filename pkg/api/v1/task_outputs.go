package apiv1

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/beam-cloud/airstore/pkg/common"
	"github.com/beam-cloud/airstore/pkg/repository"
	"github.com/beam-cloud/airstore/pkg/types"
	"github.com/beam-cloud/airstore/pkg/views"
	"github.com/labstack/echo/v4"
	"github.com/rs/zerolog/log"
)

const ctxKeyTaskOutput = "task_output"

type TaskOutputsGroup struct {
	routerGroup *echo.Group
	backend     repository.BackendRepository
	live        *repository.OrchestrationStore
	viewSync    *views.ViewSync
}

type WorkspaceOutputsGroup struct {
	routerGroup *echo.Group
	backend     repository.BackendRepository
	live        *repository.OrchestrationStore
}

func publishWorkspaceLive(ctx echo.Context, live *repository.OrchestrationStore, workspaceID uint) {
	if live == nil {
		return
	}
	_ = live.PublishWorkspaceLive(ctx.Request().Context(), workspaceID)
}

func publishTaskLive(ctx echo.Context, live *repository.OrchestrationStore, workspaceID uint, taskID string) {
	if live == nil {
		return
	}
	_ = live.PublishTaskLive(ctx.Request().Context(), taskID)
	_ = live.PublishWorkspaceLive(ctx.Request().Context(), workspaceID)
}

func NewWorkspaceOutputsGroup(routerGroup *echo.Group, backend repository.BackendRepository, redis *common.RedisClient) *WorkspaceOutputsGroup {
	g := &WorkspaceOutputsGroup{
		routerGroup: routerGroup,
		backend:     backend,
		live:        repository.NewOrchestrationStore(backend, redis),
	}
	g.registerRoutes()
	return g
}

func NewTaskOutputsGroup(routerGroup *echo.Group, backend repository.BackendRepository, redis *common.RedisClient, viewSync *views.ViewSync) *TaskOutputsGroup {
	g := &TaskOutputsGroup{
		routerGroup: routerGroup,
		backend:     backend,
		live:        repository.NewOrchestrationStore(backend, redis),
		viewSync:    viewSync,
	}
	g.registerRoutes()
	return g
}

func (g *WorkspaceOutputsGroup) registerRoutes() {
	g.routerGroup.GET("", g.ListOutputs)
	g.routerGroup.POST("/:output_id/archive", g.ArchiveOutput)
	g.routerGroup.POST("/archive-all", g.ArchiveAllOutputs)
}

func (g *TaskOutputsGroup) registerRoutes() {
	g.routerGroup.GET("", g.ListOutputs)
	g.routerGroup.POST("", g.CreateOutput)

	g.routerGroup.GET("/:output_id", g.withOutputOwnership(g.GetOutput))
	g.routerGroup.POST("/:output_id/rows", g.withOutputOwnership(g.AppendRows))
	g.routerGroup.DELETE("/:output_id", g.withOutputOwnership(g.DeleteOutput))
}

// withOutputOwnership is a handler wrapper (like WithAuth) that verifies the
// output identified by :output_id belongs to the :task_id in the URL.
// The loaded output is stored in the echo context for the inner handler.
func (g *TaskOutputsGroup) withOutputOwnership(h echo.HandlerFunc) echo.HandlerFunc {
	return func(c echo.Context) error {
		workspaceID, err := requireWorkspaceID(c)
		if err != nil {
			return err
		}

		taskID := c.Param("task_id")
		outputID := c.Param("output_id")

		output, err := g.backend.GetTaskOutput(c.Request().Context(), workspaceID, outputID)
		if err != nil {
			if _, ok := err.(*types.ErrTaskOutputNotFound); ok {
				return ErrorResponse(c, http.StatusNotFound, "output not found")
			}
			return ErrorResponse(c, http.StatusInternalServerError, err.Error())
		}
		if output.TaskID != taskID {
			return ErrorResponse(c, http.StatusNotFound, "output not found")
		}

		c.Set(ctxKeyTaskOutput, output)
		return h(c)
	}
}

// ── Workspace-scoped handlers ───────────────────────────────────────────────

func (g *WorkspaceOutputsGroup) ListOutputs(c echo.Context) error {
	workspaceID, err := requireWorkspaceID(c)
	if err != nil {
		return err
	}

	excludeArchived := c.QueryParam("include_archived") != "true"

	outputs, err := g.backend.ListWorkspaceTaskOutputs(c.Request().Context(), workspaceID, types.TaskOutputListFilter{
		TaskID:          strPtrMaybeQuery(c.QueryParam("task_id")),
		AgentID:         strPtrMaybeQuery(c.QueryParam("agent_id")),
		OutputType:      strPtrMaybeQuery(c.QueryParam("output_type")),
		SourceViewID:    strPtrMaybeQuery(c.QueryParam("source_view_id")),
		ExcludeArchived: excludeArchived,
		Limit:           parseLimitParam(c.QueryParam("limit"), 60, 200),
	})
	if err != nil {
		return ErrorResponse(c, http.StatusInternalServerError, err.Error())
	}
	if outputs == nil {
		outputs = []*types.TaskOutput{}
	}
	return SuccessResponse(c, map[string]any{
		"outputs":     outputs,
		"next_cursor": "",
		"has_more":    false,
	})
}

func (g *WorkspaceOutputsGroup) ArchiveOutput(c echo.Context) error {
	workspaceID, err := requireWorkspaceID(c)
	if err != nil {
		return err
	}
	outputID := c.Param("output_id")
	output, err := g.backend.GetTaskOutput(c.Request().Context(), workspaceID, outputID)
	if err != nil {
		if _, ok := err.(*types.ErrTaskOutputNotFound); ok {
			return ErrorResponse(c, http.StatusNotFound, "output not found")
		}
		return ErrorResponse(c, http.StatusInternalServerError, err.Error())
	}
	if err := g.backend.ArchiveTaskOutput(c.Request().Context(), workspaceID, outputID); err != nil {
		if _, ok := err.(*types.ErrTaskOutputNotFound); ok {
			return ErrorResponse(c, http.StatusNotFound, "output not found")
		}
		return ErrorResponse(c, http.StatusInternalServerError, err.Error())
	}
	publishWorkspaceLive(c, g.live, workspaceID)
	if output != nil {
		publishTaskLive(c, g.live, workspaceID, output.TaskID)
	}
	return c.NoContent(http.StatusNoContent)
}

func (g *WorkspaceOutputsGroup) ArchiveAllOutputs(c echo.Context) error {
	workspaceID, err := requireWorkspaceID(c)
	if err != nil {
		return err
	}
	count, err := g.backend.ArchiveAllTaskOutputs(c.Request().Context(), workspaceID)
	if err != nil {
		return ErrorResponse(c, http.StatusInternalServerError, err.Error())
	}
	publishWorkspaceLive(c, g.live, workspaceID)
	return SuccessResponse(c, map[string]any{"archived": count})
}

// ── Task-scoped handlers ────────────────────────────────────────────────────

func (g *TaskOutputsGroup) ListOutputs(c echo.Context) error {
	workspaceID, err := requireWorkspaceID(c)
	if err != nil {
		return err
	}
	taskID := c.Param("task_id")
	outputs, err := g.backend.ListTaskOutputs(c.Request().Context(), workspaceID, taskID)
	if err != nil {
		return ErrorResponse(c, http.StatusInternalServerError, err.Error())
	}
	if outputs == nil {
		outputs = []*types.TaskOutput{}
	}
	return SuccessResponse(c, map[string]any{"outputs": outputs})
}

type createOutputRequest struct {
	OutputType string         `json:"output_type"`
	Title      string         `json:"title"`
	OutputID   string         `json:"output_id,omitempty"`
	Summary    *string        `json:"summary,omitempty"`
	URI        *string        `json:"uri,omitempty"`
	Data       map[string]any `json:"data"`
	Metadata   map[string]any `json:"metadata,omitempty"`
	RunID      *string        `json:"run_id,omitempty"`
	AgentID    *string        `json:"agent_id,omitempty"`
}

func (g *TaskOutputsGroup) CreateOutput(c echo.Context) error {
	workspaceID, err := requireWorkspaceID(c)
	if err != nil {
		return err
	}
	taskID := c.Param("task_id")

	var req createOutputRequest
	if err := decodeStrictBody(c, &req); err != nil {
		return ErrorResponse(c, http.StatusBadRequest, "invalid request body")
	}
	if req.OutputType == "" || req.Title == "" {
		return ErrorResponse(c, http.StatusBadRequest, "output_type and title are required")
	}

	output := &types.TaskOutput{
		WorkspaceID: workspaceID,
		TaskID:      taskID,
		RunID:       req.RunID,
		AgentID:     req.AgentID,
		OutputType:  req.OutputType,
		Title:       req.Title,
		Summary:     req.Summary,
		URI:         req.URI,
		Data:        req.Data,
		Metadata:    req.Metadata,
	}
	if req.OutputID != "" {
		output.ID = req.OutputID
	}

	if err := g.backend.CreateTaskOutput(c.Request().Context(), output); err != nil {
		var conflictErr *types.ErrTaskOutputConflict
		if errors.As(err, &conflictErr) {
			return ErrorResponse(c, http.StatusConflict, err.Error())
		}
		return ErrorResponse(c, http.StatusInternalServerError, err.Error())
	}

	if g.viewSync != nil {
		if result := g.viewSync.Sync(c.Request().Context(), output); result != nil && !result.Skipped {
			log.Info().
				Str("task_id", taskID).
				Int("updated", len(result.Updated)).
				Int("created", len(result.Created)).
				Msg("http CreateOutput: viewsync completed")
		}
	}

	publishTaskLive(c, g.live, workspaceID, taskID)
	return c.JSON(http.StatusCreated, output)
}

// GetOutput returns a single output. Ownership is enforced by withOutputOwnership.
func (g *TaskOutputsGroup) GetOutput(c echo.Context) error {
	output := c.Get(ctxKeyTaskOutput).(*types.TaskOutput)
	return SuccessResponse(c, output)
}

type appendRowsRequest struct {
	Rows []map[string]any `json:"rows"`
}

// AppendRows appends rows to an output. Ownership is enforced by withOutputOwnership.
func (g *TaskOutputsGroup) AppendRows(c echo.Context) error {
	workspaceID, err := requireWorkspaceID(c)
	if err != nil {
		return err
	}
	output := c.Get(ctxKeyTaskOutput).(*types.TaskOutput)

	var req appendRowsRequest
	if err := decodeStrictBody(c, &req); err != nil {
		return ErrorResponse(c, http.StatusBadRequest, "invalid request body")
	}
	if len(req.Rows) == 0 {
		return ErrorResponse(c, http.StatusBadRequest, "rows must not be empty")
	}

	rowsJSON, err := json.Marshal(req.Rows)
	if err != nil {
		return ErrorResponse(c, http.StatusBadRequest, "invalid row data")
	}

	if err := g.backend.AppendTaskOutputRows(c.Request().Context(), workspaceID, output.ID, rowsJSON); err != nil {
		if _, ok := err.(*types.ErrTaskOutputNotFound); ok {
			return ErrorResponse(c, http.StatusNotFound, "output not found")
		}
		return ErrorResponse(c, http.StatusInternalServerError, err.Error())
	}
	publishTaskLive(c, g.live, workspaceID, output.TaskID)
	return c.NoContent(http.StatusNoContent)
}

// DeleteOutput deletes an output. Ownership is enforced by withOutputOwnership.
func (g *TaskOutputsGroup) DeleteOutput(c echo.Context) error {
	workspaceID, err := requireWorkspaceID(c)
	if err != nil {
		return err
	}
	output := c.Get(ctxKeyTaskOutput).(*types.TaskOutput)

	if err := g.backend.DeleteTaskOutput(c.Request().Context(), workspaceID, output.ID); err != nil {
		if _, ok := err.(*types.ErrTaskOutputNotFound); ok {
			return ErrorResponse(c, http.StatusNotFound, "output not found")
		}
		return ErrorResponse(c, http.StatusInternalServerError, err.Error())
	}
	publishTaskLive(c, g.live, workspaceID, output.TaskID)
	return c.NoContent(http.StatusNoContent)
}
