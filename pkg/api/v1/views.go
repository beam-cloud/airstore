package apiv1

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/beam-cloud/airstore/pkg/repository"
	"github.com/beam-cloud/airstore/pkg/types"
	"github.com/beam-cloud/airstore/pkg/views"
	"github.com/labstack/echo/v4"
	"github.com/rs/zerolog/log"
)

type ViewsGroup struct {
	g        *echo.Group
	backend  repository.BackendRepository
	copilot  *views.Copilot
	store    *views.ViewStore
	resolver *views.DataResolver
}

const viewRefreshQueryParam = "refresh"

func NewViewsGroup(g *echo.Group, backend repository.BackendRepository, copilot *views.Copilot, store *views.ViewStore) *ViewsGroup {
	vg := &ViewsGroup{
		g:        g,
		backend:  backend,
		copilot:  copilot,
		store:    store,
		resolver: views.NewDataResolver(backend, store),
	}
	vg.g.GET("", vg.List)
	vg.g.POST("", vg.Create)
	vg.g.GET("/:view_id", vg.Get)
	vg.g.PATCH("/:view_id", vg.Update)
	vg.g.DELETE("/:view_id", vg.Delete)
	vg.g.GET("/:view_id/data", vg.ResolveData)
	if store.Available() {
		vg.g.PATCH("/:view_id/sheets/:sheet_id/rows/:row_id", vg.UpdateRow)
		vg.g.POST("/:view_id/sheets/:sheet_id/rows/:row_id/regenerate", vg.RegenerateRow)
	}
	if copilot != nil && copilot.DraftsAvailable() {
		vg.g.POST("/drafts", vg.CreateDraft)
		vg.g.GET("/drafts", vg.ListDrafts)
		vg.g.GET("/drafts/:draft_id", vg.GetDraft)
		vg.g.DELETE("/drafts/:draft_id", vg.DeleteDraft)
		vg.g.POST("/drafts/:draft_id/chat", vg.ChatDraft)
		vg.g.POST("/drafts/:draft_id/publish", vg.PublishDraft)
	}
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

type columnRename struct {
	SheetID  string `json:"sheet_id"`
	OldKey   string `json:"old_key"`
	NewKey   string `json:"new_key"`
	NewLabel string `json:"new_label,omitempty"`
}

type updateViewRequest struct {
	Name          *string               `json:"name,omitempty"`
	Description   *string               `json:"description,omitempty"`
	Definition    *types.ViewDefinition `json:"definition,omitempty"`
	ColumnRenames []columnRename        `json:"column_renames,omitempty"`
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
	previousDefinition := v.Definition

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
	if len(req.ColumnRenames) > 0 {
		applyColumnRenamesToDefinition(&v.Definition, req.ColumnRenames)
	}
	if err := vg.backend.UpdateView(ctx, v); err != nil {
		return ErrorResponse(c, http.StatusInternalServerError, err.Error())
	}
	if vg.store != nil {
		for _, rename := range req.ColumnRenames {
			if rename.SheetID == "" || rename.OldKey == "" || rename.NewKey == "" || rename.OldKey == rename.NewKey {
				continue
			}
			if err := vg.store.RenameColumn(ctx, v.ID, rename.SheetID, rename.OldKey, rename.NewKey); err != nil {
				log.Warn().Err(err).
					Str("view_id", v.ID).
					Str("sheet_id", rename.SheetID).
					Str("old_key", rename.OldKey).
					Str("new_key", rename.NewKey).
					Msg("failed to rename column in MongoDB view store")
			}
		}
		if req.Definition != nil {
			for _, sheetID := range deletedViewSheets(previousDefinition, v.Definition) {
				if err := vg.store.DeleteSheet(ctx, v.ID, sheetID); err != nil {
					log.Warn().Err(err).Str("view_id", v.ID).Str("sheet_id", sheetID).Msg("failed to delete sheet rows from MongoDB view store")
				}
			}
			for _, deleted := range deletedViewColumns(previousDefinition, v.Definition) {
				if err := vg.store.DeleteColumn(ctx, v.ID, deleted.SheetID, deleted.Key); err != nil {
					log.Warn().Err(err).
						Str("view_id", v.ID).
						Str("sheet_id", deleted.SheetID).
						Str("column", deleted.Key).
						Msg("failed to delete column from MongoDB view store")
				}
			}
		}
	}
	return SuccessResponse(c, v)
}

func (vg *ViewsGroup) Delete(c echo.Context) error {
	workspaceID, err := vg.workspaceID(c)
	if err != nil {
		return ErrorResponse(c, http.StatusBadRequest, err.Error())
	}
	viewID := c.Param("view_id")
	if err := vg.backend.DeleteView(c.Request().Context(), workspaceID, viewID); err != nil {
		return ErrorResponse(c, http.StatusNotFound, "view not found")
	}
	if vg.store != nil {
		if err := vg.store.DropView(c.Request().Context(), viewID); err != nil {
			log.Warn().Err(err).Str("view_id", viewID).Msg("failed to drop view MongoDB collection on delete")
		}
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
	sheetID := c.QueryParam("sheet")
	componentID := c.QueryParam("component")
	if sheetID == "" {
		return ErrorResponse(c, http.StatusBadRequest, "sheet query parameter is required")
	}
	if componentID == "" {
		return ErrorResponse(c, http.StatusBadRequest, "component query parameter is required")
	}

	var sheet *types.SheetSpec
	for i := range v.Definition.Sheets {
		if v.Definition.Sheets[i].ID == sheetID {
			sheet = &v.Definition.Sheets[i]
			break
		}
	}
	if sheet == nil {
		return ErrorResponse(c, http.StatusNotFound, "sheet not found in view")
	}

	var comp *types.ComponentSpec
	for i := range sheet.Components {
		if sheet.Components[i].ID == componentID {
			comp = &sheet.Components[i]
			break
		}
	}
	if comp == nil {
		return ErrorResponse(c, http.StatusNotFound, "component not found in sheet")
	}

	data, err := vg.resolver.Resolve(ctx, workspaceID, v.ID, *sheet, *comp, views.ResolveOptions{
		ForceRefresh:  queryBool(c.QueryParam(viewRefreshQueryParam)),
		ViewAgentRefs: v.Definition.Agents,
	})
	if err != nil {
		log.Error().Err(err).Str("view_id", v.ID).Str("sheet_id", sheetID).Str("component", componentID).Msg("data resolve failed")
		return ErrorResponse(c, http.StatusInternalServerError, "failed to resolve data")
	}

	return SuccessResponse(c, data)
}

func queryBool(value string) bool {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "1", "true", "t", "yes", "y", "on":
		return true
	default:
		return false
	}
}

type deletedSheetColumn struct {
	SheetID string
	Key     string
}

func deletedViewSheets(previous, next types.ViewDefinition) []string {
	nextSheetIDs := make(map[string]bool, len(next.Sheets))
	for _, sheet := range next.Sheets {
		nextSheetIDs[sheet.ID] = true
	}
	var deleted []string
	for _, sheet := range previous.Sheets {
		if !nextSheetIDs[sheet.ID] {
			deleted = append(deleted, sheet.ID)
		}
	}
	return deleted
}

func deletedViewColumns(previous, next types.ViewDefinition) []deletedSheetColumn {
	nextKeys := viewColumnKeys(next)
	var deleted []deletedSheetColumn
	for sheetID, keys := range viewColumnKeys(previous) {
		current := nextKeys[sheetID]
		for key := range keys {
			if current == nil || !current[key] {
				deleted = append(deleted, deletedSheetColumn{SheetID: sheetID, Key: key})
			}
		}
	}
	return deleted
}

func viewColumnKeys(def types.ViewDefinition) map[string]map[string]bool {
	keys := make(map[string]map[string]bool)
	for _, sheet := range def.Sheets {
		sheetKeys := make(map[string]bool)
		for _, component := range sheet.Components {
			if !component.IsTable() {
				continue
			}
			addConfigColumnKeys(sheetKeys, component.Config)
			if component.DataSource == nil {
				continue
			}
			for _, rule := range component.DataSource.Transform {
				if strings.TrimSpace(rule.Column) != "" {
					sheetKeys[rule.Column] = true
				}
			}
		}
		keys[sheet.ID] = sheetKeys
	}
	return keys
}

func addConfigColumnKeys(keys map[string]bool, config map[string]any) {
	if len(config) == 0 {
		return
	}
	rawColumns, ok := config["columns"]
	if !ok {
		return
	}
	data, err := json.Marshal(rawColumns)
	if err != nil {
		return
	}
	var columns []types.ColumnMeta
	if err := json.Unmarshal(data, &columns); err != nil {
		return
	}
	for _, column := range columns {
		key := strings.TrimSpace(column.Key)
		if key != "" {
			keys[key] = true
		}
	}
}

func applyColumnRenamesToDefinition(def *types.ViewDefinition, renames []columnRename) {
	if def == nil || len(renames) == 0 {
		return
	}
	for _, rename := range renames {
		sheetID := strings.TrimSpace(rename.SheetID)
		oldKey := strings.TrimSpace(rename.OldKey)
		newKey := strings.TrimSpace(rename.NewKey)
		newLabel := strings.TrimSpace(rename.NewLabel)
		if sheetID == "" || oldKey == "" || newKey == "" || oldKey == newKey {
			continue
		}
		for i := range def.Sheets {
			sheet := &def.Sheets[i]
			if sheet.ID == sheetID {
				renameSheetColumns(sheet, oldKey, newKey, newLabel)
			}
			for j := range sheet.Relations {
				relation := &sheet.Relations[j]
				if sheet.ID == sheetID && relation.FromColumn == oldKey {
					relation.FromColumn = newKey
				}
				if relation.ToSheetID == sheetID && relation.ToColumn == oldKey {
					relation.ToColumn = newKey
				}
			}
		}
	}
}

func renameSheetColumns(sheet *types.SheetSpec, oldKey, newKey, newLabel string) {
	if sheet == nil {
		return
	}
	for i := range sheet.Components {
		component := &sheet.Components[i]
		renameComponentColumns(component, oldKey, newKey, newLabel)
	}
}

func renameComponentColumns(component *types.ComponentSpec, oldKey, newKey, newLabel string) {
	if component == nil {
		return
	}
	if component.DataSource != nil {
		for i := range component.DataSource.Transform {
			rule := &component.DataSource.Transform[i]
			if rule.Column == oldKey {
				rule.Column = newKey
			}
			// User-added columns often use source == column key as a placeholder hint.
			// Keep that hint aligned with the renamed schema key.
			if strings.TrimSpace(rule.Source) == oldKey {
				rule.Source = newKey
			}
		}
	}
	if len(component.Config) == 0 {
		return
	}
	rawColumns, ok := component.Config["columns"]
	if !ok {
		return
	}
	data, err := json.Marshal(rawColumns)
	if err != nil {
		return
	}
	var columns []types.ColumnMeta
	if err := json.Unmarshal(data, &columns); err != nil {
		return
	}
	changed := false
	for i := range columns {
		if strings.TrimSpace(columns[i].Key) != oldKey {
			continue
		}
		columns[i].Key = newKey
		if newLabel != "" {
			columns[i].Label = newLabel
		}
		changed = true
	}
	if changed {
		component.Config["columns"] = columns
	}
}

// ---------------------------------------------------------------------------
// Row-level cell edits
// ---------------------------------------------------------------------------

type updateRowRequest struct {
	Cells map[string]string `json:"cells"`
}

func (vg *ViewsGroup) UpdateRow(c echo.Context) error {
	if vg.store == nil || !vg.store.Available() {
		return ErrorResponse(c, http.StatusServiceUnavailable, "view row persistence not configured")
	}
	workspaceID, err := vg.workspaceID(c)
	if err != nil {
		return ErrorResponse(c, http.StatusBadRequest, err.Error())
	}

	ctx := c.Request().Context()
	viewID := c.Param("view_id")
	sheetID := c.Param("sheet_id")
	rowID := c.Param("row_id")

	v, err := vg.backend.GetView(ctx, workspaceID, viewID)
	if err != nil {
		return ErrorResponse(c, http.StatusNotFound, "view not found")
	}
	sheetExists := false
	for _, sheet := range v.Definition.Sheets {
		if sheet.ID == sheetID {
			sheetExists = true
			break
		}
	}
	if !sheetExists {
		return ErrorResponse(c, http.StatusNotFound, "sheet not found")
	}

	var req updateRowRequest
	if err := decodeStrictBody(c, &req); err != nil {
		return ErrorResponse(c, http.StatusBadRequest, "invalid request body")
	}
	if len(req.Cells) == 0 {
		return ErrorResponse(c, http.StatusBadRequest, "cells is required")
	}

	if err := vg.store.UpdateCells(ctx, viewID, sheetID, rowID, req.Cells); err != nil {
		if errors.Is(err, views.ErrInvalidViewColumnKey) {
			return ErrorResponse(c, http.StatusBadRequest, "invalid column key")
		}
		if errors.Is(err, views.ErrViewRowNotFound) {
			return ErrorResponse(c, http.StatusNotFound, "row not found")
		}
		log.Error().Err(err).Str("view_id", viewID).Str("sheet_id", sheetID).Str("row_id", rowID).Msg("failed to update row cells")
		return ErrorResponse(c, http.StatusInternalServerError, "failed to update cells")
	}

	row, err := vg.store.GetRow(ctx, viewID, sheetID, rowID)
	if err != nil || row == nil {
		return SuccessResponse(c, map[string]any{"sheet_id": sheetID, "row_id": rowID, "cells": req.Cells})
	}
	return SuccessResponse(c, map[string]any{
		"sheet_id": sheetID,
		"row_id":   rowID,
		"cells":    row.MergedCells(),
	})
}

// ---------------------------------------------------------------------------
// Row regeneration
// ---------------------------------------------------------------------------

func (vg *ViewsGroup) RegenerateRow(c echo.Context) error {
	if vg.store == nil || !vg.store.Available() {
		return ErrorResponse(c, http.StatusServiceUnavailable, "view row persistence not configured")
	}
	workspaceID, err := vg.workspaceID(c)
	if err != nil {
		return ErrorResponse(c, http.StatusBadRequest, err.Error())
	}

	ctx := c.Request().Context()
	viewID := c.Param("view_id")
	sheetID := c.Param("sheet_id")
	rowID := c.Param("row_id")

	v, err := vg.backend.GetView(ctx, workspaceID, viewID)
	if err != nil {
		return ErrorResponse(c, http.StatusNotFound, "view not found")
	}

	var sheet *types.SheetSpec
	for i := range v.Definition.Sheets {
		if v.Definition.Sheets[i].ID == sheetID {
			sheet = &v.Definition.Sheets[i]
			break
		}
	}
	if sheet == nil {
		return ErrorResponse(c, http.StatusNotFound, "sheet not found")
	}

	row, err := vg.store.GetRow(ctx, viewID, sheetID, rowID)
	if err != nil || row == nil {
		return ErrorResponse(c, http.StatusNotFound, "row not found")
	}
	taskID := row.TaskID
	if taskID == "" {
		return ErrorResponse(c, http.StatusBadRequest, "row has no task association")
	}

	var comp *types.ComponentSpec
	for i := range sheet.Components {
		if sheet.Components[i].IsTable() {
			comp = &sheet.Components[i]
			break
		}
	}
	if comp == nil {
		return ErrorResponse(c, http.StatusNotFound, "no table component in sheet")
	}

	data, err := vg.resolver.RegenerateRow(ctx, workspaceID, viewID, *sheet, *comp, taskID, views.ResolveOptions{
		ViewAgentRefs: v.Definition.Agents,
	})
	if err != nil {
		log.Error().Err(err).
			Str("view_id", viewID).
			Str("sheet_id", sheetID).
			Str("row_id", rowID).
			Str("task_id", taskID).
			Msg("failed to regenerate row")
		return ErrorResponse(c, http.StatusInternalServerError, "failed to regenerate row")
	}

	return SuccessResponse(c, data)
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
	if vg.copilot == nil || !vg.copilot.DraftsAvailable() {
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
	if vg.copilot == nil || !vg.copilot.DraftsAvailable() {
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
	if vg.copilot == nil || !vg.copilot.DraftsAvailable() {
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
	if vg.copilot == nil || !vg.copilot.DraftsAvailable() {
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
	if vg.copilot == nil || !vg.copilot.DraftsAvailable() {
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
	if vg.copilot == nil || !vg.copilot.DraftsAvailable() {
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
