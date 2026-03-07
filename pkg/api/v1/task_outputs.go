package apiv1

import (
	"encoding/json"
	"net/http"

	"github.com/beam-cloud/airstore/pkg/repository"
	"github.com/beam-cloud/airstore/pkg/types"
	"github.com/labstack/echo/v4"
)

type TaskOutputsGroup struct {
	routerGroup *echo.Group
	backend     repository.BackendRepository
}

type WorkspaceOutputsGroup struct {
	routerGroup *echo.Group
	backend     repository.BackendRepository
}

func NewWorkspaceOutputsGroup(routerGroup *echo.Group, backend repository.BackendRepository) *WorkspaceOutputsGroup {
	g := &WorkspaceOutputsGroup{routerGroup: routerGroup, backend: backend}
	g.registerRoutes()
	return g
}

func NewTaskOutputsGroup(routerGroup *echo.Group, backend repository.BackendRepository) *TaskOutputsGroup {
	g := &TaskOutputsGroup{routerGroup: routerGroup, backend: backend}
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
	g.routerGroup.GET("/:output_id", g.GetOutput)
	g.routerGroup.POST("/:output_id/rows", g.AppendRows)
	g.routerGroup.DELETE("/:output_id", g.DeleteOutput)
}

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
	if err := g.backend.ArchiveTaskOutput(c.Request().Context(), workspaceID, outputID); err != nil {
		if _, ok := err.(*types.ErrTaskOutputNotFound); ok {
			return ErrorResponse(c, http.StatusNotFound, "output not found")
		}
		return ErrorResponse(c, http.StatusInternalServerError, err.Error())
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
	return SuccessResponse(c, map[string]any{"archived": count})
}

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
	Schema     map[string]any `json:"schema,omitempty"`
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
		Schema:      req.Schema,
		Data:        req.Data,
		Metadata:    req.Metadata,
	}
	if req.OutputID != "" {
		output.ID = req.OutputID
	}

	if err := g.backend.CreateTaskOutput(c.Request().Context(), output); err != nil {
		return ErrorResponse(c, http.StatusInternalServerError, err.Error())
	}
	return c.JSON(http.StatusCreated, output)
}

func (g *TaskOutputsGroup) GetOutput(c echo.Context) error {
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
	return SuccessResponse(c, output)
}

type appendRowsRequest struct {
	Rows []map[string]any `json:"rows"`
}

func (g *TaskOutputsGroup) AppendRows(c echo.Context) error {
	workspaceID, err := requireWorkspaceID(c)
	if err != nil {
		return err
	}
	outputID := c.Param("output_id")

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

	if err := g.backend.AppendTaskOutputRows(c.Request().Context(), workspaceID, outputID, rowsJSON); err != nil {
		if _, ok := err.(*types.ErrTaskOutputNotFound); ok {
			return ErrorResponse(c, http.StatusNotFound, "output not found")
		}
		return ErrorResponse(c, http.StatusInternalServerError, err.Error())
	}
	return c.NoContent(http.StatusNoContent)
}

func (g *TaskOutputsGroup) DeleteOutput(c echo.Context) error {
	workspaceID, err := requireWorkspaceID(c)
	if err != nil {
		return err
	}
	outputID := c.Param("output_id")
	if err := g.backend.DeleteTaskOutput(c.Request().Context(), workspaceID, outputID); err != nil {
		if _, ok := err.(*types.ErrTaskOutputNotFound); ok {
			return ErrorResponse(c, http.StatusNotFound, "output not found")
		}
		return ErrorResponse(c, http.StatusInternalServerError, err.Error())
	}
	return c.NoContent(http.StatusNoContent)
}
