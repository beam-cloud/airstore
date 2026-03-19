package apiv1

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
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
	vg.g.GET("/:view_id/sheets/:sheet_id/widgets", vg.ResolveWidgets)
	if store.Available() {
		vg.g.PATCH("/:view_id/sheets/:sheet_id/rows/:row_id", vg.UpdateRow)
		vg.g.POST("/:view_id/sheets/:sheet_id/rows/:row_id/regenerate", vg.RegenerateRow)
		vg.g.DELETE("/:view_id/sheets/:sheet_id/rows/:row_id", vg.ExcludeRow)
		vg.g.POST("/:view_id/sheets/:sheet_id/rows/:row_id/restore", vg.RestoreRow)
	}
	vg.g.GET("/:view_id/rows/:row_id/detail", vg.RowDetail)
	vg.g.POST("/drafts", vg.CreateDraft)
	vg.g.GET("/drafts", vg.ListDrafts)
	vg.g.GET("/drafts/:draft_id", vg.GetDraft)
	vg.g.PATCH("/drafts/:draft_id", vg.UpdateDraft)
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
	if err := json.NewDecoder(c.Request().Body).Decode(&req); err != nil {
		return ErrorResponse(c, http.StatusBadRequest, "invalid request body")
	}
	v := &types.View{
		WorkspaceID: workspaceID,
		Name:        req.Name,
		Description: req.Description,
		Definition:  req.Definition,
	}
	views.NormalizeDefinition(&v.Definition)
	v.SyncNameDescription()
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
	SheetID     string `json:"sheet_id"`
	ComponentID string `json:"component_id,omitempty"`
	OldKey      string `json:"old_key"`
	NewKey      string `json:"new_key"`
	NewLabel    string `json:"new_label,omitempty"`
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
	if err := json.NewDecoder(c.Request().Body).Decode(&req); err != nil {
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
		if req.Name == nil && strings.TrimSpace(v.Definition.Name) != "" {
			v.Name = v.Definition.Name
		}
		if req.Description == nil && strings.TrimSpace(v.Definition.Description) != "" {
			v.Description = v.Definition.Description
		}
	}
	if len(req.ColumnRenames) > 0 {
		applyColumnRenamesToDefinition(&v.Definition, req.ColumnRenames)
	}
	if req.Definition != nil || len(req.ColumnRenames) > 0 {
		views.NormalizeDefinition(&v.Definition)
	}
	v.SyncNameDescription()
	if err := vg.backend.UpdateView(ctx, v); err != nil {
		return ErrorResponse(c, http.StatusInternalServerError, err.Error())
	}
	if vg.store != nil {
		for _, rename := range req.ColumnRenames {
			if rename.SheetID == "" || rename.OldKey == "" || rename.NewKey == "" || rename.OldKey == rename.NewKey {
				continue
			}
			schemaHash := schemaHashForComponent(v.Definition, rename.SheetID, rename.ComponentID)
			if err := vg.store.RenameColumn(ctx, v.ID, rename.SheetID, rename.ComponentID, rename.OldKey, rename.NewKey, schemaHash); err != nil {
				log.Warn().Err(err).
					Str("view_id", v.ID).
					Str("sheet_id", rename.SheetID).
					Str("component_id", rename.ComponentID).
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
				schemaHash := schemaHashForComponent(v.Definition, deleted.SheetID, deleted.ComponentID)
				if err := vg.store.DeleteColumn(ctx, v.ID, deleted.SheetID, deleted.ComponentID, deleted.Key, schemaHash); err != nil {
					log.Warn().Err(err).
						Str("view_id", v.ID).
						Str("sheet_id", deleted.SheetID).
						Str("component_id", deleted.ComponentID).
						Str("column", deleted.Key).
						Msg("failed to delete column from MongoDB view store")
				}
			}
			for _, added := range addedViewColumns(previousDefinition, v.Definition) {
				schemaHash := schemaHashForComponent(v.Definition, added.SheetID, added.ComponentID)
				if err := vg.store.UpdateSchemaHash(ctx, v.ID, added.SheetID, added.ComponentID, schemaHash); err != nil {
					log.Warn().Err(err).
						Str("view_id", v.ID).
						Str("sheet_id", added.SheetID).
						Str("component_id", added.ComponentID).
						Msg("failed to update schema hash for added columns in MongoDB view store")
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

func (vg *ViewsGroup) ResolveWidgets(c echo.Context) error {
	workspaceID, err := vg.workspaceID(c)
	if err != nil {
		return ErrorResponse(c, http.StatusBadRequest, err.Error())
	}

	ctx := c.Request().Context()
	v, err := vg.backend.GetView(ctx, workspaceID, c.Param("view_id"))
	if err != nil {
		return ErrorResponse(c, http.StatusNotFound, "view not found")
	}

	sheetID := c.Param("sheet_id")
	if sheetID == "" {
		return ErrorResponse(c, http.StatusBadRequest, "sheet_id is required")
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

	if len(sheet.Widgets) == 0 {
		return SuccessResponse(c, []types.WidgetData{})
	}

	var comp *types.ComponentSpec
	for i := range sheet.Components {
		if sheet.Components[i].IsTable() {
			comp = &sheet.Components[i]
			break
		}
	}
	if comp == nil {
		return SuccessResponse(c, []types.WidgetData{})
	}

	widgets, err := vg.resolver.ResolveWidgets(ctx, workspaceID, v.ID, *sheet, *comp, views.ResolveOptions{
		ForceRefresh:  queryBool(c.QueryParam(viewRefreshQueryParam)),
		ViewAgentRefs: v.Definition.Agents,
	})
	if err != nil {
		log.Error().Err(err).Str("view_id", v.ID).Str("sheet_id", sheetID).Msg("widget resolve failed")
		return ErrorResponse(c, http.StatusInternalServerError, "failed to resolve widgets")
	}

	return SuccessResponse(c, widgets)
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
	SheetID     string
	ComponentID string
	Key         string
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
	var deleted []deletedSheetColumn
	for _, sheet := range previous.Sheets {
		for _, component := range sheet.Components {
			if !component.IsTable() {
				continue
			}
			nextComponent := findComponent(next, sheet.ID, component.ID)
			if nextComponent == nil || !nextComponent.IsTable() {
				continue
			}
			current := componentColumnKeys(*nextComponent)
			for key := range componentColumnKeys(component) {
				if !current[key] {
					deleted = append(deleted, deletedSheetColumn{
						SheetID:     sheet.ID,
						ComponentID: component.ID,
						Key:         key,
					})
				}
			}
		}
	}
	return deleted
}

type addedSheetComponent struct {
	SheetID     string
	ComponentID string
}

// addedViewColumns detects components where new columns were added. Returns
// one entry per affected (sheet, component) pair — the caller stamps the new
// schema_hash on existing rows so the resolver treats them as fresh instead of
// triggering a full BAML remap.
func addedViewColumns(previous, next types.ViewDefinition) []addedSheetComponent {
	seen := make(map[string]bool)
	var added []addedSheetComponent
	for _, sheet := range next.Sheets {
		for _, component := range sheet.Components {
			if !component.IsTable() {
				continue
			}
			prevComponent := findComponent(previous, sheet.ID, component.ID)
			if prevComponent == nil || !prevComponent.IsTable() {
				continue
			}
			prevKeys := componentColumnKeys(*prevComponent)
			if len(prevKeys) == 0 {
				continue
			}
			for key := range componentColumnKeys(component) {
				if !prevKeys[key] {
					k := sheet.ID + ":" + component.ID
					if !seen[k] {
						seen[k] = true
						added = append(added, addedSheetComponent{
							SheetID:     sheet.ID,
							ComponentID: component.ID,
						})
					}
					break
				}
			}
		}
	}
	return added
}

func findComponent(def types.ViewDefinition, sheetID, componentID string) *types.ComponentSpec {
	for i := range def.Sheets {
		if def.Sheets[i].ID != sheetID {
			continue
		}
		for j := range def.Sheets[i].Components {
			if def.Sheets[i].Components[j].ID == componentID {
				return &def.Sheets[i].Components[j]
			}
		}
	}
	return nil
}

func findSheet(def types.ViewDefinition, sheetID string) *types.SheetSpec {
	for i := range def.Sheets {
		if def.Sheets[i].ID == sheetID {
			return &def.Sheets[i]
		}
	}
	return nil
}

func schemaHashForComponent(def types.ViewDefinition, sheetID, componentID string) string {
	sheet := findSheet(def, sheetID)
	if sheet == nil {
		return ""
	}
	component := findComponent(def, sheetID, componentID)
	if component == nil {
		return ""
	}
	return views.MappingSchemaHash(*sheet, *component)
}

func componentColumnKeys(component types.ComponentSpec) map[string]bool {
	keys := make(map[string]bool)
	addConfigColumnKeys(keys, component.Config)
	if component.DataSource == nil {
		return keys
	}
	for _, rule := range component.DataSource.Transform {
		if strings.TrimSpace(rule.Column) != "" {
			keys[rule.Column] = true
		}
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
	renamesBySheet := buildColumnRenameMap(renames)
	if len(renamesBySheet) == 0 {
		return
	}
	for i := range def.Sheets {
		sheet := &def.Sheets[i]
		if sheetRenames := renamesBySheet[sheet.ID]; len(sheetRenames) > 0 {
			renameSheetColumns(sheet, sheetRenames)
		}
		for j := range sheet.Relations {
			relation := &sheet.Relations[j]
			if next, ok := renamedColumnKey(renamesBySheet[sheet.ID], relation.FromColumn); ok {
				relation.FromColumn = next.NewKey
			}
			if next, ok := renamedColumnKey(renamesBySheet[relation.ToSheetID], relation.ToColumn); ok {
				relation.ToColumn = next.NewKey
			}
		}
	}
}

func buildColumnRenameMap(renames []columnRename) map[string]map[string]columnRename {
	bySheet := make(map[string]map[string]columnRename)
	for _, rename := range renames {
		sheetID := strings.TrimSpace(rename.SheetID)
		componentID := strings.TrimSpace(rename.ComponentID)
		oldKey := strings.TrimSpace(rename.OldKey)
		newKey := strings.TrimSpace(rename.NewKey)
		newLabel := strings.TrimSpace(rename.NewLabel)
		if sheetID == "" || oldKey == "" || newKey == "" || oldKey == newKey {
			continue
		}
		if bySheet[sheetID] == nil {
			bySheet[sheetID] = make(map[string]columnRename)
		}
		bySheet[sheetID][oldKey] = columnRename{
			SheetID:     sheetID,
			ComponentID: componentID,
			OldKey:      oldKey,
			NewKey:      newKey,
			NewLabel:    newLabel,
		}
	}
	return bySheet
}

func renamedColumnKey(renames map[string]columnRename, key string) (columnRename, bool) {
	if len(renames) == 0 {
		return columnRename{}, false
	}
	rename, ok := renames[strings.TrimSpace(key)]
	return rename, ok
}

func renameSheetColumns(sheet *types.SheetSpec, renames map[string]columnRename) {
	if sheet == nil {
		return
	}
	for i := range sheet.Components {
		component := &sheet.Components[i]
		filtered := renamesForComponent(renames, component.ID)
		if len(filtered) == 0 {
			continue
		}
		renameComponentColumns(component, filtered)
	}
}

func renamesForComponent(renames map[string]columnRename, componentID string) map[string]columnRename {
	if len(renames) == 0 {
		return nil
	}
	filtered := make(map[string]columnRename)
	for key, rename := range renames {
		if rename.ComponentID != "" && rename.ComponentID != componentID {
			continue
		}
		filtered[key] = rename
	}
	return filtered
}

func renameComponentColumns(component *types.ComponentSpec, renames map[string]columnRename) {
	if component == nil {
		return
	}
	if component.DataSource != nil {
		for i := range component.DataSource.Transform {
			rule := &component.DataSource.Transform[i]
			if next, ok := renamedColumnKey(renames, rule.Column); ok {
				rule.Column = next.NewKey
			}
			// User-added columns often use source == column key as a placeholder hint.
			// Keep that hint aligned with the renamed schema key.
			if next, ok := renamedColumnKey(renames, rule.Source); ok {
				rule.Source = next.NewKey
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
		next, ok := renamedColumnKey(renames, columns[i].Key)
		if !ok {
			continue
		}
		columns[i].Key = next.NewKey
		if next.NewLabel != "" {
			columns[i].Label = next.NewLabel
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

func decodeViewRowID(raw string) (string, error) {
	decoded, err := url.PathUnescape(strings.TrimSpace(raw))
	if err != nil {
		return "", fmt.Errorf("decode row id: %w", err)
	}
	return decoded, nil
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
	rowID, err := decodeViewRowID(c.Param("row_id"))
	if err != nil {
		return ErrorResponse(c, http.StatusBadRequest, "invalid row_id")
	}

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
// Row detail — reads the schema-level layout template from the component
// config, resolves per-row section visibility, fetches row data.
// No BAML per click.
// ---------------------------------------------------------------------------

func (vg *ViewsGroup) RowDetail(c echo.Context) error {
	ctx := c.Request().Context()
	workspaceID, err := vg.workspaceID(c)
	if err != nil {
		return ErrorResponse(c, http.StatusBadRequest, err.Error())
	}
	viewID := c.Param("view_id")
	rowID, err := decodeViewRowID(c.Param("row_id"))
	if err != nil {
		return ErrorResponse(c, http.StatusBadRequest, "invalid row_id")
	}
	parentTaskID := c.QueryParam("task_id")
	if parentTaskID == "" {
		return ErrorResponse(c, http.StatusBadRequest, "task_id query param is required")
	}

	// Single row fetch — used for both subtask binding and component lookup.
	var row *views.ViewRow
	if vg.store != nil && vg.store.Available() {
		row, _ = vg.store.GetRowByID(ctx, viewID, rowID)
	}

	// Resolve the schema-level layout template from the component config.
	template := vg.detailTemplateForRow(ctx, workspaceID, viewID, row)

	// Resolve bound subtask: row.SourceOutputIDs → task_spawn_binding → subtask.
	primaryTaskID := parentTaskID
	var boundSubtasks []*types.AgentTask
	if row != nil && len(row.SourceOutputIDs) > 0 {
		if bound, _ := vg.backend.ListSubtasksByOutputIDs(ctx, row.SourceOutputIDs); len(bound) > 0 {
			boundSubtasks = bound
			primaryTaskID = bound[0].ID
		}
	}

	task, err := vg.backend.GetTask(ctx, workspaceID, primaryTaskID)
	if err != nil {
		return ErrorResponse(c, http.StatusNotFound, "task not found")
	}

	outputs, _ := vg.backend.ListTaskOutputs(ctx, workspaceID, primaryTaskID)

	var subtasks []*types.AgentTask
	if len(boundSubtasks) > 0 {
		subtasks = boundSubtasks
	} else {
		subtasks, _ = vg.backend.ListSubtasks(ctx, parentTaskID)
	}

	var emailThreads map[string][]views.ThreadMessage
	if threadIDs := extractThreadIDs(outputs); len(threadIDs) > 0 {
		fetcher := views.NewEmailThreadFetcher(vg.backend)
		emailThreads = fetcher.FetchThreads(ctx, workspaceID, threadIDs)
	}

	layout := views.ResolveLayout(template, task, outputs, subtasks)

	type subtaskSummary struct {
		ID     string               `json:"id"`
		State  types.AgentTaskState `json:"state"`
		Label  string               `json:"label,omitempty"`
		WakeAt *time.Time           `json:"wake_at,omitempty"`
	}
	subtaskList := make([]subtaskSummary, 0, len(subtasks))
	for _, st := range subtasks {
		label := ""
		if st.PayloadJSON != nil {
			if l, ok := st.PayloadJSON["label"].(string); ok {
				label = l
			}
		}
		subtaskList = append(subtaskList, subtaskSummary{
			ID:     st.ID,
			State:  st.State,
			Label:  label,
			WakeAt: st.WakeAt,
		})
	}

	return SuccessResponse(c, map[string]any{
		"layout":         layout,
		"task":           task,
		"outputs":        outputs,
		"email_threads":  emailThreads,
		"subtasks":       subtaskList,
		"row_id":         rowID,
		"parent_task_id": parentTaskID,
	})
}

// detailTemplateForRow finds the table component that owns the row and
// returns its cached or inferred detail layout template.
func (vg *ViewsGroup) detailTemplateForRow(ctx context.Context, workspaceID uint, viewID string, row *views.ViewRow) views.DetailLayoutResponse {
	view, err := vg.backend.GetView(ctx, workspaceID, viewID)
	if err != nil || view == nil {
		return views.InferDetailTemplate(nil)
	}

	componentID := ""
	if row != nil {
		componentID = row.ComponentID
	}

	for _, sheet := range view.Definition.Sheets {
		for _, comp := range sheet.Components {
			if componentID != "" && comp.ID != componentID {
				continue
			}
			if !comp.IsTable() {
				continue
			}
			return views.DetailTemplateForComponent(&comp)
		}
	}

	return views.InferDetailTemplate(nil)
}

func extractThreadIDs(outputs []*types.TaskOutput) []string {
	seen := make(map[string]bool)
	var ids []string
	for _, o := range outputs {
		if o.OutputType != "email" {
			continue
		}
		tid, _ := o.Data["thread_id"].(string)
		if tid == "" {
			continue
		}
		if !seen[tid] {
			seen[tid] = true
			ids = append(ids, tid)
		}
	}
	return ids
}

// ---------------------------------------------------------------------------
// Row exclusion (soft-delete)
// ---------------------------------------------------------------------------

func (vg *ViewsGroup) ExcludeRow(c echo.Context) error {
	if vg.store == nil || !vg.store.Available() {
		return ErrorResponse(c, http.StatusServiceUnavailable, "view row persistence not configured")
	}
	viewID := c.Param("view_id")
	sheetID := c.Param("sheet_id")
	rowID, err := decodeViewRowID(c.Param("row_id"))
	if err != nil {
		return ErrorResponse(c, http.StatusBadRequest, "invalid row_id")
	}
	if err := vg.store.ExcludeRow(c.Request().Context(), viewID, sheetID, rowID); err != nil {
		log.Error().Err(err).Str("view_id", viewID).Str("sheet_id", sheetID).Str("row_id", rowID).Msg("failed to exclude row")
		return ErrorResponse(c, http.StatusInternalServerError, "failed to exclude row")
	}
	return SuccessResponse(c, map[string]any{"sheet_id": sheetID, "row_id": rowID, "excluded": true})
}

func (vg *ViewsGroup) RestoreRow(c echo.Context) error {
	if vg.store == nil || !vg.store.Available() {
		return ErrorResponse(c, http.StatusServiceUnavailable, "view row persistence not configured")
	}
	viewID := c.Param("view_id")
	sheetID := c.Param("sheet_id")
	rowID, err := decodeViewRowID(c.Param("row_id"))
	if err != nil {
		return ErrorResponse(c, http.StatusBadRequest, "invalid row_id")
	}
	if err := vg.store.RestoreRow(c.Request().Context(), viewID, sheetID, rowID); err != nil {
		log.Error().Err(err).Str("view_id", viewID).Str("sheet_id", sheetID).Str("row_id", rowID).Msg("failed to restore row")
		return ErrorResponse(c, http.StatusInternalServerError, "failed to restore row")
	}
	return SuccessResponse(c, map[string]any{"sheet_id": sheetID, "row_id": rowID, "excluded": false})
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
	rowID, err := decodeViewRowID(c.Param("row_id"))
	if err != nil {
		return ErrorResponse(c, http.StatusBadRequest, "invalid row_id")
	}

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
		if !sheet.Components[i].IsTable() {
			continue
		}
		if row.ComponentID != "" && sheet.Components[i].ID != row.ComponentID {
			continue
		}
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
		if errors.Is(err, views.ErrNoOutputsForTask) {
			return ErrorResponse(c, http.StatusNotFound, "no outputs found for this task")
		}
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
	Message     string `json:"message"`
	ViewContent string `json:"view_content,omitempty"`
	ViewID      string `json:"view_id,omitempty"`
}

type viewSSECitation struct {
	SheetID string `json:"sheet_id"`
	RowID   string `json:"row_id"`
	Label   string `json:"label"`
}

type viewSSEEvent struct {
	Event       string            `json:"event"`
	Message     string            `json:"message,omitempty"`
	ViewContent string            `json:"view_content,omitempty"`
	UpdateType  string            `json:"update_type,omitempty"`
	Error       string            `json:"error,omitempty"`
	OpType      string            `json:"type,omitempty"`
	OpName      string            `json:"name,omitempty"`
	OpStatus    string            `json:"status,omitempty"`
	Citations   []viewSSECitation `json:"citations,omitempty"`
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
	drafts = mergeCachedViewDraftSummaries(workspaceID, drafts)
	return SuccessResponse(c, drafts)
}

func mergeCachedViewDraftSummaries(workspaceID string, drafts []views.DraftSummary) []views.DraftSummary {
	now := time.Now()
	viewDraftsStore.Lock()
	pruneViewDraftSessionsLocked(now)
	cached := make(map[string]*views.Draft, len(viewDraftsStore.m))
	for draftID, session := range viewDraftsStore.m {
		if session == nil || session.draft == nil {
			continue
		}
		if session.draft.WorkspaceID != workspaceID {
			continue
		}
		cached[draftID] = cloneViewDraft(session.draft)
	}
	viewDraftsStore.Unlock()

	if len(cached) == 0 {
		return drafts
	}

	merged := make([]views.DraftSummary, 0, len(drafts))
	seen := make(map[string]bool, len(drafts))
	for _, draft := range drafts {
		if cachedDraft := cached[draft.ID]; cachedDraft != nil {
			applyCachedDraftSummary(&draft, cachedDraft)
		}
		merged = append(merged, draft)
		seen[draft.ID] = true
	}

	for _, cachedDraft := range cached {
		if seen[cachedDraft.ID] {
			continue
		}
		summary := views.DraftSummary{
			ID:        cachedDraft.ID,
			Status:    firstNonEmptyString(cachedDraft.Status, "active"),
			ViewID:    strings.TrimSpace(cachedDraft.PublishedViewID),
			CreatedAt: cachedDraft.CreatedAt,
			UpdatedAt: cachedDraft.UpdatedAt,
		}
		merged = append(merged, summary)
	}

	sort.SliceStable(merged, func(i, j int) bool {
		if merged[i].UpdatedAt != merged[j].UpdatedAt {
			return merged[i].UpdatedAt > merged[j].UpdatedAt
		}
		return merged[i].CreatedAt > merged[j].CreatedAt
	})
	return merged
}

func isTerminalDraftStatus(status string) bool {
	switch strings.TrimSpace(status) {
	case "published", "discarded":
		return true
	default:
		return false
	}
}

func applyCachedDraftSummary(summary *views.DraftSummary, draft *views.Draft) {
	if summary == nil || draft == nil {
		return
	}
	if draft.UpdatedAt > summary.UpdatedAt {
		summary.UpdatedAt = draft.UpdatedAt
	}
	if publishedViewID := strings.TrimSpace(draft.PublishedViewID); publishedViewID != "" {
		summary.Status = "published"
		summary.ViewID = publishedViewID
		return
	}
	if status := strings.TrimSpace(draft.Status); status != "" {
		if isTerminalDraftStatus(summary.Status) && !isTerminalDraftStatus(status) {
			return
		}
		summary.Status = status
	}
}

func firstNonEmptyString(values ...string) string {
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			return trimmed
		}
	}
	return ""
}

func (vg *ViewsGroup) GetDraft(c echo.Context) error {
	if vg.copilot == nil || !vg.copilot.DraftsAvailable() {
		return ErrorResponse(c, http.StatusServiceUnavailable, "view copilot not configured")
	}
	draft, err := vg.copilot.LoadDraft(c.Request().Context(), c.Param("workspace_id"), c.Param("draft_id"))
	if err != nil {
		return ErrorResponse(c, http.StatusNotFound, "draft not found")
	}
	return SuccessResponse(c, draft)
}

type updateViewDraftRequest struct {
	ViewContent string `json:"view_content"`
}

func (vg *ViewsGroup) UpdateDraft(c echo.Context) error {
	if vg.copilot == nil || !vg.copilot.DraftsAvailable() {
		return ErrorResponse(c, http.StatusServiceUnavailable, "view copilot not configured")
	}
	session, err := vg.getViewDraftSession(c, c.Param("draft_id"))
	if err != nil {
		return ErrorResponse(c, http.StatusNotFound, "draft not found")
	}

	var req updateViewDraftRequest
	if err := decodeStrictBody(c, &req); err != nil {
		return ErrorResponse(c, http.StatusBadRequest, "invalid request body")
	}
	trimmed := strings.TrimSpace(req.ViewContent)
	if trimmed == "" {
		return ErrorResponse(c, http.StatusBadRequest, "view_content is required")
	}

	session.mu.Lock()
	defer session.mu.Unlock()
	if trimmed != session.draft.ViewContent {
		if err := vg.copilot.PersistViewContent(c.Request().Context(), session.draft.ID, trimmed); err != nil {
			log.Error().Err(err).Str("draft_id", session.draft.ID).Msg("persist draft view content failed")
			return ErrorResponse(c, http.StatusInternalServerError, "failed to persist draft content")
		}
		session.draft.ViewContent = trimmed
		session.draft.UpdatedAt = time.Now().UnixMilli()
	}
	return SuccessResponse(c, cloneViewDraft(session.draft))
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

	if trimmed := strings.TrimSpace(req.ViewContent); trimmed != "" && trimmed != session.draft.ViewContent {
		session.draft.ViewContent = trimmed
		if err := vg.copilot.PersistViewContent(genCtx, session.draft.ID, trimmed); err != nil {
			log.Warn().Err(err).Str("draft_id", session.draft.ID).Msg("failed to persist latest view content before chat")
		}
	}

	viewID := strings.TrimSpace(req.ViewID)
	if viewID == "" {
		viewID = strings.TrimSpace(session.draft.PublishedViewID)
	}

	resp, err := vg.copilot.GenerateStream(
		genCtx,
		session.draft,
		workspaceID,
		strings.TrimSpace(req.Message),
		viewID,
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

	var citations []viewSSECitation
	for _, cr := range resp.Cited_rows {
		parts := strings.SplitN(cr.Row_ref, ":", 3)
		if len(parts) != 3 || parts[0] != "row" {
			continue
		}
		citations = append(citations, viewSSECitation{
			SheetID: parts[1],
			RowID:   parts[2],
			Label:   cr.Label,
		})
	}

	writeSSE(viewSSEEvent{
		Event:       "done",
		Message:     resp.Message,
		ViewContent: resp.View_content,
		UpdateType:  string(resp.Update_type),
		Citations:   citations,
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

	var req struct {
		ViewContent string `json:"view_content"`
	}
	if err := decodeStrictBody(c, &req); err != nil && !errors.Is(err, io.EOF) {
		return ErrorResponse(c, http.StatusBadRequest, "invalid request body")
	}

	session, err := vg.getViewDraftSession(c, c.Param("draft_id"))
	if err != nil {
		return ErrorResponse(c, http.StatusNotFound, "draft not found")
	}

	session.mu.Lock()

	// The frontend publishes mid-stream before the chat endpoint persists
	// ViewContent to S2.  When this request lands on a different pod the
	// draft loaded from S2 will have empty ViewContent.  The caller can
	// supply it in the request body to bridge the gap.
	if vc := strings.TrimSpace(req.ViewContent); vc != "" && session.draft.ViewContent != vc {
		session.draft.ViewContent = vc
		_ = vg.copilot.PersistViewContent(c.Request().Context(), session.draft.ID, vc)
	}

	v, err := vg.copilot.PublishView(c.Request().Context(), session.draft, workspaceID)
	wsID := c.Param("workspace_id")
	draftID := session.draft.ID
	session.mu.Unlock()

	if err != nil {
		return ErrorResponse(c, http.StatusInternalServerError, err.Error())
	}

	// Publish success is determined by the view write plus the durable draft
	// stream update inside PublishView. The workspace draft index is a
	// secondary projection, so keep its append off the request path.
	vg.copilot.IndexDraftPublishedAsync(wsID, draftID, v.Name, v.ID)

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
