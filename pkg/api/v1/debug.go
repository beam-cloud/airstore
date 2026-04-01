package apiv1

import (
	"net/http"

	"github.com/beam-cloud/airstore/pkg/hooks"
	"github.com/beam-cloud/airstore/pkg/sources"
	"github.com/labstack/echo/v4"
)

type DebugGroup struct {
	injector *sources.MemoryEventInjector
	poller   *hooks.SourcePoller
}

func NewDebugGroup(g *echo.Group, sourceService interface{ SetEventInjector(sources.EventInjector) }, poller *hooks.SourcePoller) *DebugGroup {
	d := &DebugGroup{
		injector: sources.NewMemoryEventInjector(),
		poller:   poller,
	}
	sourceService.SetEventInjector(d.injector)

	g.POST("/inject-source-items", d.InjectSourceItems)
	g.POST("/poll-now", d.PollNow)

	return d
}

func (d *DebugGroup) InjectSourceItems(c echo.Context) error {
	var req struct {
		TaskID string   `json:"task_id"`
		Items  []string `json:"items"`
	}
	if err := c.Bind(&req); err != nil || req.TaskID == "" || len(req.Items) == 0 {
		return ErrorResponse(c, http.StatusBadRequest, "task_id and items required")
	}
	items := make([]sources.QueryResult, len(req.Items))
	for i, id := range req.Items {
		items[i] = sources.QueryResult{ID: id, Filename: id + ".txt", Metadata: map[string]string{"thread_id": id}}
	}
	d.injector.InjectItems(req.TaskID, items)
	return SuccessResponse(c, map[string]any{"task_id": req.TaskID, "items": len(req.Items)})
}

func (d *DebugGroup) PollNow(c echo.Context) error {
	if d.poller == nil {
		return ErrorResponse(c, http.StatusServiceUnavailable, "source poller not available")
	}
	d.poller.PollNow(c.Request().Context())
	return SuccessResponse(c, nil)
}
