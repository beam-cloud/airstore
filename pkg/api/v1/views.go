package apiv1

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/beam-cloud/airstore/pkg/common"
	"github.com/beam-cloud/airstore/pkg/repository"
	"github.com/beam-cloud/airstore/pkg/types"
	"github.com/beam-cloud/airstore/pkg/views"
	baml "github.com/beam-cloud/airstore/pkg/views/baml_client"
	"github.com/labstack/echo/v4"
	"github.com/rs/zerolog/log"
)

type ViewsGroup struct {
	g       *echo.Group
	backend repository.BackendRepository
	copilot *views.Copilot
	rdb     *common.RedisClient
}

func NewViewsGroup(g *echo.Group, backend repository.BackendRepository, copilot *views.Copilot, rdb *common.RedisClient) *ViewsGroup {
	vg := &ViewsGroup{g: g, backend: backend, copilot: copilot, rdb: rdb}
	vg.g.GET("", vg.List)
	vg.g.POST("", vg.Create)
	vg.g.GET("/:view_id", vg.Get)
	vg.g.PATCH("/:view_id", vg.Update)
	vg.g.DELETE("/:view_id", vg.Delete)
	vg.g.GET("/:view_id/data", vg.ResolveData)
	vg.g.GET("/:view_id/outputs/:output_id/detail", vg.FormattedDetail)
	vg.g.POST("/drafts", vg.CreateDraft)
	vg.g.GET("/drafts", vg.ListDrafts)
	vg.g.GET("/drafts/:draft_id", vg.GetDraft)
	vg.g.DELETE("/drafts/:draft_id", vg.DeleteDraft)
	vg.g.POST("/drafts/:draft_id/chat", vg.ChatDraft)
	vg.g.POST("/drafts/:draft_id/publish", vg.PublishDraft)
	return vg
}

// ---------------------------------------------------------------------------
// Published view CRUD
// ---------------------------------------------------------------------------

func (vg *ViewsGroup) List(c echo.Context) error {
	workspaceID, err := vg.workspaceID(c)
	if err != nil {
		return ErrorResponse(c, http.StatusBadRequest, err.Error())
	}
	result, err := vg.backend.ListViews(c.Request().Context(), workspaceID)
	if err != nil {
		return ErrorResponse(c, http.StatusInternalServerError, err.Error())
	}
	if result == nil {
		result = []*types.View{}
	}
	return SuccessResponse(c, result)
}

type createViewRequest struct {
	Name        string               `json:"name"`
	Description string               `json:"description"`
	Definition  types.ViewDefinition `json:"definition"`
}

func (vg *ViewsGroup) Create(c echo.Context) error {
	workspaceID, err := vg.workspaceID(c)
	if err != nil {
		return ErrorResponse(c, http.StatusBadRequest, err.Error())
	}
	var req createViewRequest
	if err := decodeStrictBody(c, &req); err != nil {
		return ErrorResponse(c, http.StatusBadRequest, "invalid request body")
	}
	v := &types.View{
		WorkspaceID: workspaceID,
		Name:        req.Name,
		Description: req.Description,
		Definition:  req.Definition,
	}
	if err := vg.backend.CreateView(c.Request().Context(), v); err != nil {
		return ErrorResponse(c, http.StatusInternalServerError, err.Error())
	}
	return SuccessResponse(c, v)
}

func (vg *ViewsGroup) Get(c echo.Context) error {
	workspaceID, err := vg.workspaceID(c)
	if err != nil {
		return ErrorResponse(c, http.StatusBadRequest, err.Error())
	}
	v, err := vg.backend.GetView(c.Request().Context(), workspaceID, c.Param("view_id"))
	if err != nil {
		return ErrorResponse(c, http.StatusNotFound, "view not found")
	}
	return SuccessResponse(c, v)
}

type updateViewRequest struct {
	Name        *string               `json:"name,omitempty"`
	Description *string               `json:"description,omitempty"`
	Definition  *types.ViewDefinition `json:"definition,omitempty"`
}

func (vg *ViewsGroup) Update(c echo.Context) error {
	workspaceID, err := vg.workspaceID(c)
	if err != nil {
		return ErrorResponse(c, http.StatusBadRequest, err.Error())
	}
	ctx := c.Request().Context()
	v, err := vg.backend.GetView(ctx, workspaceID, c.Param("view_id"))
	if err != nil {
		return ErrorResponse(c, http.StatusNotFound, "view not found")
	}

	var req updateViewRequest
	if err := decodeStrictBody(c, &req); err != nil {
		return ErrorResponse(c, http.StatusBadRequest, "invalid request body")
	}
	if req.Name != nil {
		v.Name = *req.Name
	}
	if req.Description != nil {
		v.Description = *req.Description
	}
	if req.Definition != nil {
		v.Definition = *req.Definition
	}
	if err := vg.backend.UpdateView(ctx, v); err != nil {
		return ErrorResponse(c, http.StatusInternalServerError, err.Error())
	}
	return SuccessResponse(c, v)
}

func (vg *ViewsGroup) Delete(c echo.Context) error {
	workspaceID, err := vg.workspaceID(c)
	if err != nil {
		return ErrorResponse(c, http.StatusBadRequest, err.Error())
	}
	if err := vg.backend.DeleteView(c.Request().Context(), workspaceID, c.Param("view_id")); err != nil {
		return ErrorResponse(c, http.StatusNotFound, "view not found")
	}
	return SuccessResponse(c, nil)
}

// ---------------------------------------------------------------------------
// DataResolver endpoint
// ---------------------------------------------------------------------------

func (vg *ViewsGroup) ResolveData(c echo.Context) error {
	workspaceID, err := vg.workspaceID(c)
	if err != nil {
		return ErrorResponse(c, http.StatusBadRequest, err.Error())
	}

	ctx := c.Request().Context()
	v, err := vg.backend.GetView(ctx, workspaceID, c.Param("view_id"))
	if err != nil {
		return ErrorResponse(c, http.StatusNotFound, "view not found")
	}
	componentID := c.QueryParam("component")
	if componentID == "" {
		return ErrorResponse(c, http.StatusBadRequest, "component query parameter is required")
	}

	var comp *types.ComponentSpec
	for i := range v.Definition.Components {
		if v.Definition.Components[i].ID == componentID {
			comp = &v.Definition.Components[i]
			break
		}
	}
	if comp == nil {
		return ErrorResponse(c, http.StatusNotFound, "component not found in view")
	}

	resolver := views.NewDataResolver(vg.backend, vg.rdb)
	data, err := resolver.Resolve(ctx, workspaceID, *comp)
	if err != nil {
		log.Error().Err(err).Str("view_id", v.ID).Str("component", componentID).Msg("data resolve failed")
		return ErrorResponse(c, http.StatusInternalServerError, "failed to resolve data")
	}

	return SuccessResponse(c, data)
}

// ---------------------------------------------------------------------------
// Formatted detail — BAML formatting with view-specific cache
// ---------------------------------------------------------------------------

const formattedOutputCacheKeyField = "__cache_key"

func (vg *ViewsGroup) FormattedDetail(c echo.Context) error {
	workspaceID, err := vg.workspaceID(c)
	if err != nil {
		return ErrorResponse(c, http.StatusBadRequest, err.Error())
	}

	viewID := c.Param("view_id")
	outputID := c.Param("output_id")
	ctx := c.Request().Context()

	v, err := vg.backend.GetView(ctx, workspaceID, viewID)
	if err != nil {
		return ErrorResponse(c, http.StatusNotFound, "view not found")
	}

	output, err := vg.backend.GetTaskOutput(ctx, workspaceID, outputID)
	if err != nil {
		return ErrorResponse(c, http.StatusNotFound, "output not found")
	}
	component := resolveFormattedDetailComponent(v, output, c.QueryParam("component"))

	dataJSON, _ := json.Marshal(output.Data)
	metaJSON, _ := json.Marshal(output.Metadata)
	summary := ""
	if output.Summary != nil {
		summary = *output.Summary
	}
	cacheKey := formattedOutputCacheKey(v, output, summary, component)

	if cached, err := vg.backend.GetFormattedOutput(ctx, viewID, outputID); err == nil {
		if formatted, ok := readFormattedCache(cached.Formatted, cacheKey); ok {
			return SuccessResponse(c, formatted)
		}
	} else if err != sql.ErrNoRows {
		log.Warn().Err(err).Str("view_id", viewID).Str("output_id", outputID).Msg("formatted output lookup error")
	}

	viewDesc := formattedDetailContext(v, component)

	result, err := baml.FormatOutputDetail(
		ctx,
		viewDesc,
		output.OutputType,
		output.Title,
		string(dataJSON),
		string(metaJSON),
		summary,
	)
	if err != nil {
		log.Error().Err(err).Str("view_id", viewID).Str("output_id", outputID).Msg("BAML format failed")
		return ErrorResponse(c, http.StatusInternalServerError, "failed to format output")
	}

	formatted := map[string]any{
		"title":    result.Title,
		"sections": result.Sections,
	}

	if err := vg.backend.UpsertFormattedOutput(ctx, viewID, outputID, writeFormattedCache(formatted, cacheKey)); err != nil {
		log.Warn().Err(err).Msg("failed to cache formatted output")
	}

	return SuccessResponse(c, formatted)
}

func formattedOutputCacheKey(view *types.View, output *types.TaskOutput, summary string, component *types.ComponentSpec) string {
	payload := map[string]any{
		"view_name":        view.Name,
		"view_description": view.Description,
		"view_definition":  view.Definition,
		"view_component":   component,
		"output_type":      output.OutputType,
		"output_title":     output.Title,
		"output_uri":       output.URI,
		"output_summary":   summary,
		"output_data":      output.Data,
		"output_metadata":  output.Metadata,
	}
	raw, _ := json.Marshal(payload)
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

func resolveFormattedDetailComponent(view *types.View, output *types.TaskOutput, componentID string) *types.ComponentSpec {
	if view == nil {
		return nil
	}
	if componentID != "" {
		for i := range view.Definition.Components {
			if view.Definition.Components[i].ID == componentID {
				return &view.Definition.Components[i]
			}
		}
	}
	for i := range view.Definition.Components {
		comp := &view.Definition.Components[i]
		if views.OutputMatchesDataSource(output, comp.DataSource) {
			return comp
		}
	}
	return nil
}

func formattedDetailContext(view *types.View, component *types.ComponentSpec) string {
	base := strings.TrimSpace(view.Description)
	if base == "" {
		base = strings.TrimSpace(view.Name)
	}
	if component == nil {
		return base
	}
	title := strings.TrimSpace(component.Title)
	switch {
	case title == "":
		return base
	case base == "" || base == title:
		return title
	default:
		return title + ": " + base
	}
}

func readFormattedCache(cached map[string]any, cacheKey string) (map[string]any, bool) {
	if cached == nil {
		return nil, false
	}
	if cachedKey, _ := cached[formattedOutputCacheKeyField].(string); cachedKey != cacheKey {
		return nil, false
	}
	out := make(map[string]any, len(cached))
	for key, value := range cached {
		if key == formattedOutputCacheKeyField {
			continue
		}
		out[key] = value
	}
	return out, true
}

func writeFormattedCache(formatted map[string]any, cacheKey string) map[string]any {
	out := make(map[string]any, len(formatted)+1)
	for key, value := range formatted {
		out[key] = value
	}
	out[formattedOutputCacheKeyField] = cacheKey
	return out
}

// ---------------------------------------------------------------------------
// Draft management
// ---------------------------------------------------------------------------

const viewDraftSessionTTL = 30 * time.Minute

type viewDraftSession struct {
	mu          sync.Mutex
	draft       *views.Draft
	lastTouched time.Time
}

var viewDraftsStore = struct {
	sync.Mutex
	m map[string]*viewDraftSession
}{m: make(map[string]*viewDraftSession)}

type createViewDraftRequest struct {
	Description string `json:"description"`
	ViewID      string `json:"view_id,omitempty"`
	ViewName    string `json:"view_name,omitempty"`
	ViewContent string `json:"view_content,omitempty"`
}

type createViewDraftResponse struct {
	DraftID string `json:"draft_id"`
}

type viewChatRequest struct {
	Message string `json:"message"`
}

type viewSSEEvent struct {
	Event       string `json:"event"`
	Message     string `json:"message,omitempty"`
	ViewContent string `json:"view_content,omitempty"`
	UpdateType  string `json:"update_type,omitempty"`
	Error       string `json:"error,omitempty"`
	OpType      string `json:"type,omitempty"`
	OpName      string `json:"name,omitempty"`
	OpStatus    string `json:"status,omitempty"`
}

func getCachedViewDraftSession(draftID string) *viewDraftSession {
	now := time.Now()
	viewDraftsStore.Lock()
	defer viewDraftsStore.Unlock()
	pruneViewDraftSessionsLocked(now)
	session := viewDraftsStore.m[draftID]
	if session != nil {
		session.lastTouched = now
	}
	return session
}

func putViewDraftSession(draft *views.Draft) *viewDraftSession {
	if draft == nil {
		return nil
	}
	now := time.Now()
	viewDraftsStore.Lock()
	defer viewDraftsStore.Unlock()
	pruneViewDraftSessionsLocked(now)

	if existing := viewDraftsStore.m[draft.ID]; existing != nil {
		existing.draft = draft
		existing.lastTouched = now
		return existing
	}
	session := &viewDraftSession{draft: draft, lastTouched: now}
	viewDraftsStore.m[draft.ID] = session
	return session
}

func pruneViewDraftSessionsLocked(now time.Time) {
	for id, session := range viewDraftsStore.m {
		if session == nil || now.Sub(session.lastTouched) > viewDraftSessionTTL {
			delete(viewDraftsStore.m, id)
		}
	}
}

func cloneViewDraft(draft *views.Draft) *views.Draft {
	if draft == nil {
		return nil
	}
	out := *draft
	out.Messages = append([]views.DraftMessage(nil), draft.Messages...)
	return &out
}

func (vg *ViewsGroup) getViewDraftSession(c echo.Context, draftID string) (*viewDraftSession, error) {
	workspaceID := c.Param("workspace_id")
	if session := getCachedViewDraftSession(draftID); session != nil {
		session.mu.Lock()
		cachedWorkspaceID := session.draft.WorkspaceID
		session.mu.Unlock()
		if cachedWorkspaceID == workspaceID {
			return session, nil
		}
		if cachedWorkspaceID != "" {
			return nil, fmt.Errorf("draft not found")
		}
	}

	draft, err := vg.copilot.LoadDraft(c.Request().Context(), workspaceID, draftID)
	if err != nil {
		return nil, err
	}
	return putViewDraftSession(draft), nil
}

func (vg *ViewsGroup) CreateDraft(c echo.Context) error {
	if vg.copilot == nil {
		return ErrorResponse(c, http.StatusServiceUnavailable, "view copilot not configured")
	}
	workspaceID := c.Param("workspace_id")

	var req createViewDraftRequest
	if err := decodeStrictBody(c, &req); err != nil {
		return ErrorResponse(c, http.StatusBadRequest, "invalid request body")
	}

	draft := vg.copilot.CreateDraft(workspaceID)
	if err := vg.copilot.PersistMeta(c.Request().Context(), draft); err != nil {
		log.Error().Err(err).Str("workspace_id", workspaceID).Str("draft_id", draft.ID).Msg("persist draft meta failed")
		return ErrorResponse(c, http.StatusInternalServerError, "failed to persist draft")
	}
	if strings.TrimSpace(req.ViewContent) != "" {
		draft.ViewContent = req.ViewContent
		if err := vg.copilot.PersistViewContent(c.Request().Context(), draft.ID, req.ViewContent); err != nil {
			log.Error().Err(err).Str("draft_id", draft.ID).Msg("persist draft view content failed")
			return ErrorResponse(c, http.StatusInternalServerError, "failed to persist draft content")
		}
	}
	if strings.TrimSpace(req.ViewID) != "" {
		draft.PublishedViewID = req.ViewID
		if err := vg.copilot.PersistPublishedViewID(c.Request().Context(), draft.ID, req.ViewID); err != nil {
			log.Error().Err(err).Str("draft_id", draft.ID).Msg("persist draft published view ID failed")
			return ErrorResponse(c, http.StatusInternalServerError, "failed to persist draft published view ID")
		}
	}
	_ = vg.copilot.IndexDraftCreated(
		c.Request().Context(),
		workspaceID,
		draft.ID,
		req.Description,
		req.ViewName,
		req.ViewID,
	)
	putViewDraftSession(draft)

	return SuccessResponse(c, createViewDraftResponse{DraftID: draft.ID})
}

func (vg *ViewsGroup) ListDrafts(c echo.Context) error {
	if vg.copilot == nil {
		return ErrorResponse(c, http.StatusServiceUnavailable, "view copilot not configured")
	}
	workspaceID := c.Param("workspace_id")
	drafts, err := vg.copilot.ListDrafts(c.Request().Context(), workspaceID)
	if err != nil {
		log.Warn().Err(err).Str("workspace_id", workspaceID).Msg("list drafts failed, returning empty list")
		drafts = []views.DraftSummary{}
	}
	if drafts == nil {
		drafts = []views.DraftSummary{}
	}
	return SuccessResponse(c, drafts)
}

func (vg *ViewsGroup) GetDraft(c echo.Context) error {
	if vg.copilot == nil {
		return ErrorResponse(c, http.StatusServiceUnavailable, "view copilot not configured")
	}
	session, err := vg.getViewDraftSession(c, c.Param("draft_id"))
	if err != nil {
		return ErrorResponse(c, http.StatusNotFound, "draft not found")
	}
	session.mu.Lock()
	draft := cloneViewDraft(session.draft)
	session.mu.Unlock()
	return SuccessResponse(c, draft)
}

func (vg *ViewsGroup) DeleteDraft(c echo.Context) error {
	if vg.copilot == nil {
		return ErrorResponse(c, http.StatusServiceUnavailable, "view copilot not configured")
	}
	workspaceID := c.Param("workspace_id")
	draftID := c.Param("draft_id")
	if err := vg.copilot.DeleteDraft(c.Request().Context(), workspaceID, draftID); err != nil {
		return ErrorResponse(c, http.StatusInternalServerError, err.Error())
	}

	viewDraftsStore.Lock()
	delete(viewDraftsStore.m, draftID)
	viewDraftsStore.Unlock()

	return SuccessResponse(c, nil)
}

// ChatDraft streams view draft updates over SSE while the copilot edits the view.
func (vg *ViewsGroup) ChatDraft(c echo.Context) error {
	if vg.copilot == nil {
		return ErrorResponse(c, http.StatusServiceUnavailable, "view copilot not configured")
	}

	var req viewChatRequest
	if err := decodeStrictBody(c, &req); err != nil {
		return ErrorResponse(c, http.StatusBadRequest, "invalid request body")
	}
	if strings.TrimSpace(req.Message) == "" {
		return ErrorResponse(c, http.StatusBadRequest, "message is required")
	}

	session, err := vg.getViewDraftSession(c, c.Param("draft_id"))
	if err != nil {
		return ErrorResponse(c, http.StatusNotFound, "draft not found")
	}

	w := c.Response()
	flusher, ok := w.Writer.(http.Flusher)
	if !ok {
		return ErrorResponse(c, http.StatusInternalServerError, "streaming not supported")
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.WriteHeader(http.StatusOK)

	rc := http.NewResponseController(w)
	_ = rc.SetWriteDeadline(time.Now().Add(5 * time.Minute))

	writeSSE := func(evt viewSSEEvent) {
		data, _ := json.Marshal(evt)
		fmt.Fprintf(w, "data: %s\n\n", data)
		flusher.Flush()
		_ = rc.SetWriteDeadline(time.Now().Add(5 * time.Minute))
	}

	writeSSE(viewSSEEvent{Event: "generating"})

	genCtx, genCancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer genCancel()

	go func() {
		<-c.Request().Context().Done()
		genCancel()
	}()

	workspaceID, err := vg.workspaceID(c)
	if err != nil {
		writeSSE(viewSSEEvent{Event: "error", Error: "invalid workspace"})
		writeSSE(viewSSEEvent{Event: "done"})
		return nil
	}

	session.mu.Lock()
	defer session.mu.Unlock()

	resp, err := vg.copilot.GenerateStream(
		genCtx,
		session.draft,
		workspaceID,
		strings.TrimSpace(req.Message),
		func(partial *views.PartialViewDraftResponse) {
			writeSSE(viewSSEEvent{
				Event:       "chunk",
				Message:     partial.Message,
				ViewContent: partial.ViewContent,
				UpdateType:  partial.UpdateType,
			})
		},
	)
	if err != nil {
		log.Error().Err(err).Str("draft_id", c.Param("draft_id")).Msg("view generation failed")
		writeSSE(viewSSEEvent{Event: "error", Error: err.Error()})
		writeSSE(viewSSEEvent{Event: "done"})
		return nil
	}

	if len(resp.Operations) > 0 {
		for _, op := range resp.Operations {
			writeSSE(viewSSEEvent{
				Event:    "operation",
				OpType:   string(op.Type),
				OpStatus: "executing",
			})
		}
		results := vg.copilot.ExecuteOperations(genCtx, workspaceID, resp.Operations)
		for _, r := range results {
			writeSSE(viewSSEEvent{
				Event:    "operation",
				OpType:   r.Type,
				OpName:   r.Name,
				OpStatus: r.Status,
			})
			if r.Status == "error" {
				log.Warn().Str("type", r.Type).Str("name", r.Name).Str("error", r.Error).Msg("copilot operation failed")
			}
		}
		if session.draft.ViewContent != "" {
			if reconciled, reconcileErr := vg.copilot.ReconcileViewContent(genCtx, workspaceID, session.draft.ViewContent, results); reconcileErr != nil {
				log.Warn().Err(reconcileErr).Str("draft_id", session.draft.ID).Msg("failed to reconcile generated view content")
			} else if reconciled != "" && reconciled != session.draft.ViewContent {
				session.draft.ViewContent = reconciled
				resp.View_content = reconciled
				_ = vg.copilot.PersistViewContent(genCtx, session.draft.ID, reconciled)
			}
		}
	}

	writeSSE(viewSSEEvent{
		Event:       "done",
		Message:     resp.Message,
		ViewContent: resp.View_content,
		UpdateType:  string(resp.Update_type),
	})

	return nil
}

func (vg *ViewsGroup) PublishDraft(c echo.Context) error {
	if vg.copilot == nil {
		return ErrorResponse(c, http.StatusServiceUnavailable, "view copilot not configured")
	}

	workspaceID, err := vg.workspaceID(c)
	if err != nil {
		return ErrorResponse(c, http.StatusBadRequest, err.Error())
	}

	session, err := vg.getViewDraftSession(c, c.Param("draft_id"))
	if err != nil {
		return ErrorResponse(c, http.StatusNotFound, "draft not found")
	}

	session.mu.Lock()
	defer session.mu.Unlock()

	v, err := vg.copilot.PublishView(c.Request().Context(), session.draft, workspaceID)
	if err != nil {
		return ErrorResponse(c, http.StatusInternalServerError, err.Error())
	}

	_ = vg.copilot.IndexDraftPublished(c.Request().Context(), c.Param("workspace_id"), session.draft.ID, v.Name, v.ID)

	return SuccessResponse(c, v)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func (vg *ViewsGroup) workspaceID(c echo.Context) (uint, error) {
	externalID := c.Param("workspace_id")
	if externalID == "" {
		return 0, fmt.Errorf("workspace_id is required")
	}
	ws, err := vg.backend.GetWorkspaceByExternalId(c.Request().Context(), externalID)
	if err != nil || ws == nil {
		return 0, fmt.Errorf("workspace not found")
	}
	return ws.Id, nil
}
