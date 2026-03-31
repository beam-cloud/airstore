package apiv1

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/beam-cloud/airstore/pkg/clients"
	"github.com/beam-cloud/airstore/pkg/repository"
	"github.com/beam-cloud/airstore/pkg/types"
	"github.com/beam-cloud/airstore/pkg/views"
	"github.com/labstack/echo/v4"
	"github.com/rs/zerolog/log"
)

type ViewsGroup struct {
	g          *echo.Group
	backend    repository.BackendRepository
	copilot    *views.Copilot
	store      *views.ViewStore
	resolver   *views.DataResolver
	storage    *clients.StorageClient
	viewSync   *views.ViewSync
	compactor  *views.ContextCompactor
}

const viewRefreshQueryParam = "refresh"

func NewViewsGroup(g *echo.Group, backend repository.BackendRepository, copilot *views.Copilot, store *views.ViewStore, storage *clients.StorageClient, viewSync *views.ViewSync, compactor *views.ContextCompactor) *ViewsGroup {
	vg := &ViewsGroup{
		g:         g,
		backend:   backend,
		copilot:   copilot,
		store:     store,
		resolver:  views.NewDataResolver(backend, store),
		storage:   storage,
		viewSync:  viewSync,
		compactor: compactor,
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
		vg.g.POST("/:view_id/sheets/:sheet_id/components/:component_id/run", vg.RunRows)
	}
	vg.g.POST("/:view_id/sheets/:sheet_id/import", vg.ImportData)
	vg.g.GET("/:view_id/rows/:row_id/detail", vg.RowDetail)
	vg.g.GET("/:view_id/mailbox", vg.Mailbox)
	vg.g.POST("/:view_id/chat", vg.ChatView)
	vg.g.GET("/:view_id/chat/messages", vg.ChatMessages)
	vg.g.POST("/:view_id/context", vg.IngestContext)
	vg.g.GET("/:view_id/context", vg.GetContext)
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
	ctx := c.Request().Context()
	views.NormalizeDefinition(&v.Definition)
	views.PopulateStatusOptions(ctx, &v.Definition)
	v.SyncNameDescription()
	if err := vg.backend.CreateView(ctx, v); err != nil {
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
	Name          *string            `json:"name,omitempty"`
	Description   *string            `json:"description,omitempty"`
	Definition    json.RawMessage    `json:"definition,omitempty"`
	ColumnRenames []columnRename     `json:"column_renames,omitempty"`
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
	hasDefinition := len(req.Definition) > 0 && string(req.Definition) != "null"
	if hasDefinition {
		if err := json.Unmarshal(req.Definition, &v.Definition); err != nil {
			return ErrorResponse(c, http.StatusBadRequest, "invalid definition: "+err.Error())
		}
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
	if hasDefinition || len(req.ColumnRenames) > 0 {
		views.NormalizeDefinition(&v.Definition)
		views.PopulateStatusOptions(ctx, &v.Definition)
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
		if hasDefinition {
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
		SourceViewID:  v.ID,
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

	// Row is the primary entity. Task context is optional enrichment.
	var row *views.ViewRow
	if vg.store != nil && vg.store.Available() {
		row, _ = vg.store.GetRowByID(ctx, viewID, rowID)
	}

	// Row cell data is always included — this is a data store, not just a task viewer.
	var rowCells map[string]string
	if row != nil {
		rowCells = row.MergedCells()
	}

	// Resolve task ID from the row or query param. Either can be absent.
	parentTaskID := c.QueryParam("task_id")
	if row != nil && strings.TrimSpace(row.TaskID) != "" {
		parentTaskID = strings.TrimSpace(row.TaskID)
	}

	// Base response — always valid, even with no task.
	resp := map[string]any{
		"row_id":          rowID,
		"row_data":        rowCells,
		"task":            nil,
		"outputs":         []any{},
		"gallery_outputs": []any{},
		"email_threads":   map[string]any{},
		"subtasks":        []any{},
		"blocker":         nil,
		"layout": views.DetailLayoutResponse{
			Sections: []views.DetailSectionJSON{
				{Type: "data", Title: "Record", Emphasis: "standard"},
			},
			Actions: []views.ActionSpecJSON{},
		},
	}

	// If no task is associated, still check for thread_id in row cells.
	if parentTaskID == "" {
		if rowCells != nil {
			if tid := strings.TrimSpace(rowCells["thread_id"]); tid != "" {
				var threadIDs []string
				for _, t := range strings.Split(tid, ",") {
					if t = strings.TrimSpace(t); t != "" {
						threadIDs = append(threadIDs, t)
					}
				}
				if len(threadIDs) > 0 {
					fetcher := views.NewEmailThreadFetcher(vg.backend)
					resp["email_threads"] = fetcher.FetchThreads(ctx, workspaceID, threadIDs)
				}
			}
		}
		return SuccessResponse(c, resp)
	}

	// Enrich with task context.
	template := vg.detailTemplateForRow(ctx, workspaceID, viewID, row)

	parentTask, err := vg.backend.GetTask(ctx, workspaceID, parentTaskID)
	if err != nil {
		// Task not found — still return the row data rather than 404.
		log.Warn().Err(err).Str("task_id", parentTaskID).Msg("row detail: task not found, returning row data only")
		return SuccessResponse(c, resp)
	}

	parentOutputs, err := vg.backend.ListTaskOutputs(ctx, workspaceID, parentTaskID)
	if err != nil {
		return ErrorResponse(c, http.StatusInternalServerError, "failed to load task outputs")
	}

	detailContext, err := views.ResolveRowDetailContext(ctx, vg.backend, workspaceID, parentTask, parentOutputs, row)
	if err != nil {
		return ErrorResponse(c, http.StatusInternalServerError, "failed to resolve row detail")
	}

	projection := views.ProjectDetail(detailContext.Task, detailContext.Outputs, detailContext.Subtasks)

	// Primary: thread_id written by the agent onto the row cell
	var cellThreadIDs []string
	if rowCells != nil {
		if tid := strings.TrimSpace(rowCells["thread_id"]); tid != "" {
			for _, t := range strings.Split(tid, ",") {
				if t = strings.TrimSpace(t); t != "" {
					cellThreadIDs = append(cellThreadIDs, t)
				}
			}
		}
	}

	var emailThreads map[string][]views.ThreadMessage
	threadIDs := cellThreadIDs
	if len(threadIDs) == 0 {
		threadIDs = extractThreadIDs(projection.ThreadOutputs)
	}
	if len(threadIDs) > 0 {
		fetcher := views.NewEmailThreadFetcher(vg.backend)
		emailThreads = fetcher.FetchThreads(ctx, workspaceID, threadIDs)
	}
	if synth := syntheticEmailThreads(projection.ThreadOutputs, emailThreads); len(synth) > 0 {
		if emailThreads == nil {
			emailThreads = synth
		} else {
			for k, v := range synth {
				emailThreads[k] = v
			}
		}
	}

	layout := views.ResolveProjectedLayout(template, projection)

	type subtaskSummary struct {
		ID     string               `json:"id"`
		State  types.AgentTaskState `json:"state"`
		Label  string               `json:"label,omitempty"`
		WakeAt *time.Time           `json:"wake_at,omitempty"`
	}
	subtaskList := make([]subtaskSummary, 0, len(detailContext.Subtasks))
	for _, st := range detailContext.Subtasks {
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

	resp["layout"] = layout
	resp["blocker"] = projection.Blocker
	resp["task"] = detailContext.Task
	resp["outputs"] = projection.Outputs
	resp["gallery_outputs"] = projection.GalleryOutputs
	resp["email_threads"] = emailThreads
	resp["subtasks"] = subtaskList
	resp["parent_task_id"] = parentTaskID
	resp["feedback_counts"] = vg.loadFeedbackCounts(ctx, viewID)

	return SuccessResponse(c, resp)
}

// loadFeedbackCounts reads the view context stream and returns a map of
// thread_id to the number of anchored feedback entries. Returns an empty map
// if the compactor is unavailable.
func (vg *ViewsGroup) loadFeedbackCounts(ctx context.Context, viewID string) map[string]int {
	if vg.compactor == nil || !vg.compactor.Available() {
		return map[string]int{}
	}
	entries, err := vg.compactor.ReadContext(ctx, viewID)
	if err != nil || len(entries) == 0 {
		return map[string]int{}
	}
	return views.FeedbackCountsByThread(entries)
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
		tid := emailOutputThreadID(o)
		if tid == "" || seen[tid] {
			continue
		}
		seen[tid] = true
		ids = append(ids, tid)
	}
	return ids
}

func isEmailOutput(o *types.TaskOutput) bool {
	if o.OutputType == types.TaskOutputTypeEmail {
		return true
	}
	return metadataString(o.Metadata, "_tool") == "gmail" &&
		strings.TrimSpace(dataString(o.Data, "thread_id")) != ""
}

func emailOutputThreadID(o *types.TaskOutput) string {
	if o == nil || !isEmailOutput(o) {
		return ""
	}
	tid := strings.TrimSpace(dataString(o.Data, "thread_id"))
	if tid == "" {
		tid = gmailThreadIDFromURL(dataString(o.Data, "email_link", "uri"))
	}
	if tid == "" {
		tid = gmailThreadIDFromURL(metadataString(o.Metadata, "deeplink"))
	}
	return strings.TrimSpace(tid)
}

func gmailThreadIDFromURL(u string) string {
	u = strings.TrimSpace(u)
	if !strings.Contains(u, "mail.google.com") {
		return ""
	}
	if idx := strings.LastIndex(u, "/"); idx >= 0 && idx < len(u)-1 {
		return u[idx+1:]
	}
	return ""
}

func dataString(data map[string]any, keys ...string) string {
	for _, k := range keys {
		if v, ok := data[k].(string); ok && strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}

func metadataString(m map[string]any, key string) string {
	if m == nil {
		return ""
	}
	v, _ := m[key].(string)
	return v
}

// syntheticEmailThreads builds thread messages from output data for email
// outputs that don't have a real Gmail thread. This ensures the email thread
// section always shows sent email content.
func syntheticEmailThreads(outputs []*types.TaskOutput, existing map[string][]views.ThreadMessage) map[string][]views.ThreadMessage {
	synth := make(map[string][]views.ThreadMessage)
	for _, o := range outputs {
		if o == nil || o.OutputType != types.TaskOutputTypeEmail {
			continue
		}
		if o.Status == types.TaskOutputStatusPending || o.Status == types.TaskOutputStatusApproved {
			continue
		}
		if threadID := emailOutputThreadID(o); threadID != "" {
			continue
		}

		recipient := dataString(o.Data, "recipient", "recipient_email", "to")
		subject := dataString(o.Data, "subject")
		body := dataString(o.Data, "content", "body", "snippet")
		if recipient == "" && subject == "" && body == "" {
			continue
		}

		threadKey := "output:" + o.ID
		deeplink := dataString(o.Data, "email_link", "uri")
		if deeplink == "" {
			deeplink = metadataString(o.Metadata, "deeplink")
		}

		synth[threadKey] = []views.ThreadMessage{{
			ID:         o.ID,
			ThreadID:   threadKey,
			From:       "me",
			To:         recipient,
			Subject:    subject,
			Body:       body,
			Snippet:    dataString(o.Data, "summary"),
			Date:       o.CreatedAt.UTC().Format(time.RFC3339),
			Timestamp:  o.CreatedAt.UnixMilli(),
			IsOutbound: true,
			Deeplink:   deeplink,
		}}
	}
	return synth
}

// ---------------------------------------------------------------------------
// Mailbox — aggregate email threads for a view
// ---------------------------------------------------------------------------

type mailboxThread struct {
	Messages  []views.ThreadMessage `json:"messages"`
	RowID     string                `json:"row_id,omitempty"`
	RowLabel  string                `json:"row_label,omitempty"`
	RowCells  map[string]string     `json:"row_fields,omitempty"`
	OutputIDs []string              `json:"output_ids,omitempty"`
}

const mailboxOutputLimit = 200

func (vg *ViewsGroup) Mailbox(c echo.Context) error {
	ctx := c.Request().Context()
	workspaceID, err := vg.workspaceID(c)
	if err != nil {
		return ErrorResponse(c, http.StatusBadRequest, err.Error())
	}
	viewID := c.Param("view_id")

	// Load view definition to get schema column order for row labeling.
	view, err := vg.backend.GetView(ctx, workspaceID, viewID)
	if err != nil || view == nil {
		return ErrorResponse(c, http.StatusNotFound, "view not found")
	}
	var labelColumnKeys []string
	for _, sheet := range view.Definition.Sheets {
		for _, comp := range sheet.Components {
			if comp.IsTable() {
				sc := types.BuildViewOutputSchemaContext(view, sheet, comp)
				if sc != nil {
					labelColumnKeys = sc.ColumnKeys()
				}
				break
			}
		}
		if len(labelColumnKeys) > 0 {
			break
		}
	}

	// Load view rows to collect task IDs and row-level thread_ids.
	taskRowMap := make(map[string]*views.ViewRow)
	var viewTaskIDs []string
	var cachedRows []views.ViewRow
	if vg.store != nil && vg.store.Available() {
		allRows, rowErr := vg.store.GetRows(ctx, viewID, "", "")
		if rowErr == nil {
			cachedRows = allRows
			seen := make(map[string]bool)
			for i := range cachedRows {
				r := &cachedRows[i]
				if r.TaskID != "" {
					taskRowMap[r.TaskID] = r
					if !seen[r.TaskID] {
						viewTaskIDs = append(viewTaskIDs, r.TaskID)
						seen[r.TaskID] = true
					}
				}
			}
		}
	}

	// Expand viewTaskIDs with subtask IDs so we also catch email outputs
	// produced by fan-out children whose IDs aren't stored on the view row.
	// childToParent lets us resolve subtask → parent for row linkage later.
	childToParent, _ := vg.backend.ListChildTaskIDsByParents(ctx, viewTaskIDs)
	if len(childToParent) > 0 {
		seen := make(map[string]bool, len(viewTaskIDs))
		for _, id := range viewTaskIDs {
			seen[id] = true
		}
		for childID := range childToParent {
			if !seen[childID] {
				viewTaskIDs = append(viewTaskIDs, childID)
				seen[childID] = true
			}
		}
	}

	// Query email outputs using two strategies:
	//  1. Tasks whose payload has source_view_id matching this view
	//  2. Tasks referenced by this view's rows or their subtasks
	emailType := types.TaskOutputTypeEmail
	outputsByID := make(map[string]*types.TaskOutput)

	byView, err := vg.backend.ListWorkspaceTaskOutputs(ctx, workspaceID, types.TaskOutputListFilter{
		OutputType:   &emailType,
		SourceViewID: &viewID,
		Limit:        mailboxOutputLimit,
	})
	if err != nil {
		return ErrorResponse(c, http.StatusInternalServerError, "failed to list email outputs")
	}
	for _, o := range byView {
		outputsByID[o.ID] = o
	}

	if len(viewTaskIDs) > 0 {
		byTasks, taskErr := vg.backend.ListWorkspaceTaskOutputs(ctx, workspaceID, types.TaskOutputListFilter{
			OutputType: &emailType,
			TaskIDs:    viewTaskIDs,
			Limit:      mailboxOutputLimit,
		})
		if taskErr == nil {
			for _, o := range byTasks {
				outputsByID[o.ID] = o
			}
		}
	}

	outputs := make([]*types.TaskOutput, 0, len(outputsByID))
	for _, o := range outputsByID {
		outputs = append(outputs, o)
	}

	// Build task_id -> []*TaskOutput lookup and collect thread IDs from outputs.
	taskOutputs := make(map[string][]*types.TaskOutput)
	for _, o := range outputs {
		taskOutputs[o.TaskID] = append(taskOutputs[o.TaskID], o)
	}

	threadIDs := extractThreadIDs(outputs)

	// Also collect thread_ids written directly to row cells (agent-driven
	// denormalization). This ensures threads appear in the mailbox even when
	// the BAML classifier didn't create an email output, and provides row
	// association for threads that came from outputs but lack a task→row link.
	rowThreadRows := make(map[string]*views.ViewRow)
	{
		seen := make(map[string]bool, len(threadIDs))
		for _, tid := range threadIDs {
			seen[tid] = true
		}
		for i := range cachedRows {
			r := &cachedRows[i]
			cells := r.MergedCells()
			raw := strings.TrimSpace(cells["thread_id"])
			if raw == "" {
				continue
			}
			for _, tid := range strings.Split(raw, ",") {
				tid = strings.TrimSpace(tid)
				if tid == "" {
					continue
				}
				if !seen[tid] {
					threadIDs = append(threadIDs, tid)
					seen[tid] = true
				}
				if rowThreadRows[tid] == nil {
					rowThreadRows[tid] = r
				}
			}
		}
	}

	if len(outputs) == 0 && len(threadIDs) == 0 {
		return SuccessResponse(c, map[string]any{
			"threads":            map[string]any{},
			"has_email_activity": false,
		})
	}

	// Fetch Gmail threads.
	var emailThreads map[string][]views.ThreadMessage
	if len(threadIDs) > 0 {
		fetcher := views.NewEmailThreadFetcher(vg.backend)
		emailThreads = fetcher.FetchThreads(ctx, workspaceID, threadIDs)
	}
	if synth := syntheticEmailThreads(outputs, emailThreads); len(synth) > 0 {
		if emailThreads == nil {
			emailThreads = synth
		} else {
			for k, v := range synth {
				emailThreads[k] = v
			}
		}
	}

	// Build output -> threadKey lookup so we can associate row data with threads.
	outputThreadKey := make(map[string]string)
	for _, o := range outputs {
		if tid := emailOutputThreadID(o); tid != "" {
			outputThreadKey[o.ID] = tid
		} else {
			outputThreadKey[o.ID] = "output:" + o.ID
		}
	}

	// Associate row data with each thread. If the output's task isn't directly
	// in a view row, resolve through the child→parent chain (subtask emails).
	threadRows := make(map[string]*views.ViewRow)
	for taskID, outs := range taskOutputs {
		row := taskRowMap[taskID]
		if row == nil {
			if parentID, ok := childToParent[taskID]; ok {
				row = taskRowMap[parentID]
			}
		}
		if row == nil {
			continue
		}
		for _, o := range outs {
			if tk, ok := outputThreadKey[o.ID]; ok {
				if threadRows[tk] == nil {
					threadRows[tk] = row
				}
			}
		}
	}

	// Build per-thread output IDs.
	threadOutputIDs := make(map[string][]string)
	for _, o := range outputs {
		tk, ok := outputThreadKey[o.ID]
		if !ok {
			continue
		}
		threadOutputIDs[tk] = append(threadOutputIDs[tk], o.ID)
	}

	// Merge row-cell-sourced thread→row associations into threadRows.
	for tid, row := range rowThreadRows {
		if threadRows[tid] == nil {
			threadRows[tid] = row
		}
	}

	result := make(map[string]mailboxThread, len(emailThreads))
	for threadKey, messages := range emailThreads {
		mt := mailboxThread{Messages: messages}
		if row := threadRows[threadKey]; row != nil {
			mt.RowID = row.ID
			mt.RowCells = row.MergedCells()
			mt.RowLabel = rowLabelFromSchema(mt.RowCells, labelColumnKeys)
		}
		mt.OutputIDs = threadOutputIDs[threadKey]
		result[threadKey] = mt
	}

	feedbackCounts := vg.loadFeedbackCounts(ctx, viewID)

	return SuccessResponse(c, map[string]any{
		"threads":            result,
		"has_email_activity": len(result) > 0,
		"feedback_counts":    feedbackCounts,
	})
}

// rowLabelFromSchema returns the value of the first schema column that has a
// non-empty value in cells. The schema column order is the user's chosen
// identity hierarchy, so this is always the right label regardless of workload.
func rowLabelFromSchema(cells map[string]string, columnKeys []string) string {
	for _, key := range columnKeys {
		if v := strings.TrimSpace(cells[key]); v != "" && len(v) < 120 {
			return v
		}
	}
	return ""
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
// Tiered row remapping (Run 10 / Run all)
// ---------------------------------------------------------------------------

type runRowsRequest struct {
	Limit int `json:"limit"`
}

func (vg *ViewsGroup) RunRows(c echo.Context) error {
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
	componentID := c.Param("component_id")

	var body runRowsRequest
	if err := c.Bind(&body); err != nil {
		body.Limit = 0
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

	var comp *types.ComponentSpec
	for i := range sheet.Components {
		if sheet.Components[i].ID == componentID && sheet.Components[i].IsTable() {
			comp = &sheet.Components[i]
			break
		}
	}
	if comp == nil {
		return ErrorResponse(c, http.StatusNotFound, "no matching table component")
	}

	data, err := vg.resolver.RunRows(ctx, workspaceID, viewID, *sheet, *comp, body.Limit, views.ResolveOptions{
		ViewAgentRefs: v.Definition.Agents,
	})
	if err != nil {
		log.Error().Err(err).
			Str("view_id", viewID).
			Str("sheet_id", sheetID).
			Str("component_id", componentID).
			Int("limit", body.Limit).
			Msg("failed to run rows")
		return ErrorResponse(c, http.StatusInternalServerError, "failed to run rows")
	}

	return SuccessResponse(c, data)
}

// ---------------------------------------------------------------------------
// Data import
// ---------------------------------------------------------------------------

type importDataRequest struct {
	FilePath      string            `json:"file_path"`
	ColumnMapping map[string]string `json:"column_mapping"`
}

func (vg *ViewsGroup) ImportData(c echo.Context) error {
	if !vg.store.Available() {
		return ErrorResponse(c, http.StatusServiceUnavailable, "data store not configured")
	}
	if vg.storage == nil {
		return ErrorResponse(c, http.StatusServiceUnavailable, "storage not configured")
	}

	viewID := c.Param("view_id")
	sheetID := c.Param("sheet_id")

	workspaceID, err := vg.workspaceID(c)
	if err != nil {
		return ErrorResponse(c, http.StatusBadRequest, err.Error())
	}

	var req importDataRequest
	if err := decodeStrictBody(c, &req); err != nil {
		return ErrorResponse(c, http.StatusBadRequest, "invalid request body")
	}
	if strings.TrimSpace(req.FilePath) == "" {
		return ErrorResponse(c, http.StatusBadRequest, "file_path is required")
	}

	ctx := c.Request().Context()

	v, err := vg.backend.GetView(ctx, workspaceID, viewID)
	if err != nil {
		return ErrorResponse(c, http.StatusNotFound, "view not found")
	}

	var comp *types.ComponentSpec
	for i := range v.Definition.Sheets {
		if v.Definition.Sheets[i].ID == sheetID {
			for j := range v.Definition.Sheets[i].Components {
				if v.Definition.Sheets[i].Components[j].IsTable() {
					comp = &v.Definition.Sheets[i].Components[j]
					break
				}
			}
			break
		}
	}
	if comp == nil {
		return ErrorResponse(c, http.StatusNotFound, "no table component on sheet")
	}

	ws, err := vg.backend.GetWorkspace(ctx, workspaceID)
	if err != nil {
		return ErrorResponse(c, http.StatusInternalServerError, "workspace lookup failed")
	}
	bucket := vg.storage.WorkspaceBucketName(ws.ExternalId)
	key := strings.TrimPrefix(req.FilePath, "/")
	data, err := vg.storage.Download(ctx, bucket, key)
	if err != nil {
		log.Error().Err(err).Str("path", req.FilePath).Msg("import: failed to download file")
		return ErrorResponse(c, http.StatusBadRequest, "could not read file")
	}

	result, err := views.ImportData(ctx, views.ImportParams{
		Store:       vg.store,
		Backend:     vg.backend,
		Data:        data,
		FilePath:    req.FilePath,
		ViewID:      viewID,
		WorkspaceID: workspaceID,
		SheetID:     sheetID,
		ComponentID: comp.ID,
		ColMapping:  req.ColumnMapping,
	})
	if err != nil {
		log.Error().Err(err).Str("view_id", viewID).Msg("import: failed")
		return ErrorResponse(c, http.StatusBadRequest, err.Error())
	}

	if vg.viewSync != nil && result.RowCount > 0 {
		go vg.propagateImportRows(viewID, workspaceID, sheetID, comp.ID)
	}

	return SuccessResponse(c, result)
}

// propagateImportRows syncs imported rows to other sheets in the view.
// Runs in a background goroutine so the import response returns immediately.
func (vg *ViewsGroup) propagateImportRows(viewID string, workspaceID uint, sourceSheetID, sourceComponentID string) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	rows, err := vg.store.GetRowsBySource(ctx, viewID, sourceSheetID, sourceComponentID, views.RowSourceImport)
	if err != nil || len(rows) == 0 {
		return
	}

	log.Info().Str("view_id", viewID).Int("rows", len(rows)).Msg("import: propagating to other sheets")

	synced := 0
	for _, row := range rows {
		if ctx.Err() != nil {
			break
		}
		cells := row.MergedCells()
		if len(cells) == 0 {
			continue
		}
		vg.viewSync.SyncToolWrite(ctx, views.ToolWriteInput{
			ViewID:            viewID,
			WorkspaceID:       workspaceID,
			SourceSheetID:     sourceSheetID,
			SourceComponentID: sourceComponentID,
			Cells:             cells,
			RowID:             row.ID,
			ForceInsert:       true,
		})
		synced++
	}
	log.Info().Str("view_id", viewID).Int("synced", synced).Msg("import: propagation complete")
}

// ---------------------------------------------------------------------------
// View copilot chat
// ---------------------------------------------------------------------------

const viewChatSessionTTL = 30 * time.Minute

type viewChatSession struct {
	mu          sync.Mutex
	chatState   *views.ChatState
	view        *types.View
	lastTouched time.Time
}

var viewChatStore = struct {
	sync.Mutex
	m map[string]*viewChatSession
}{m: make(map[string]*viewChatSession)}

type viewChatAttachedFile struct {
	Path        string `json:"path"`
	Name        string `json:"name"`
	ContentType string `json:"content_type,omitempty"`
}

type viewChatRequest struct {
	Message       string                 `json:"message"`
	ViewContent   string                 `json:"view_content,omitempty"`
	AttachedFiles []viewChatAttachedFile `json:"attached_files,omitempty"`
}

type viewSSECitation struct {
	SheetID string `json:"sheet_id"`
	RowID   string `json:"row_id"`
	Label   string `json:"label"`
}

type viewSSEEvent struct {
	Event        string            `json:"event"`
	Message      string            `json:"message,omitempty"`
	ViewContent  string            `json:"view_content,omitempty"`
	UpdateType   string            `json:"update_type,omitempty"`
	Error        string            `json:"error,omitempty"`
	OpType       string            `json:"type,omitempty"`
	OpName       string            `json:"name,omitempty"`
	OpStatus     string            `json:"status,omitempty"`
	OpAgentID    string            `json:"agent_id,omitempty"`
	OpTaskID     string            `json:"task_id,omitempty"`
	OpAgentName  string            `json:"agent_name,omitempty"`
	OpScheduleID string            `json:"schedule_id,omitempty"`
	Citations    []viewSSECitation `json:"citations,omitempty"`
}

func (vg *ViewsGroup) getViewChatSession(c echo.Context, viewID string) (*viewChatSession, error) {
	now := time.Now()
	wsID := c.Param("workspace_id")

	viewChatStore.Lock()
	for id, s := range viewChatStore.m {
		if s == nil || now.Sub(s.lastTouched) > viewChatSessionTTL {
			delete(viewChatStore.m, id)
		}
	}
	if existing := viewChatStore.m[viewID]; existing != nil {
		existing.lastTouched = now
		viewChatStore.Unlock()
		return existing, nil
	}
	viewChatStore.Unlock()

	workspaceID, err := vg.workspaceID(c)
	if err != nil {
		return nil, err
	}
	v, err := vg.backend.GetView(c.Request().Context(), workspaceID, viewID)
	if err != nil {
		return nil, err
	}

	var messages []views.ChatMessage
	if vg.copilot != nil && vg.copilot.ChatAvailable() {
		if existing, loadErr := vg.copilot.LoadChatState(c.Request().Context(), wsID, viewID); loadErr == nil && existing != nil {
			messages = existing.Messages
		}
	}
	if messages == nil {
		messages = []views.ChatMessage{}
		if vg.copilot != nil && vg.copilot.ChatAvailable() {
			_ = vg.copilot.PersistChatMeta(c.Request().Context(), &views.ChatState{
				ID:          viewID,
				WorkspaceID: wsID,
				CreatedAt:   v.CreatedAt.UnixMilli(),
			})
		}
	}

	viewContent, _ := json.Marshal(v.Definition)
	cs := &views.ChatState{
		ID:              viewID,
		WorkspaceID:     wsID,
		ViewContent:     string(viewContent),
		PublishedViewID: viewID,
		Messages:        messages,
		CreatedAt:       v.CreatedAt.UnixMilli(),
		UpdatedAt:       v.UpdatedAt.UnixMilli(),
	}

	session := &viewChatSession{
		chatState:   cs,
		view:        v,
		lastTouched: now,
	}

	viewChatStore.Lock()
	viewChatStore.m[viewID] = session
	viewChatStore.Unlock()

	return session, nil
}

// ChatView streams copilot updates over SSE for a published view.
func (vg *ViewsGroup) ChatView(c echo.Context) error {
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

	viewID := c.Param("view_id")
	session, err := vg.getViewChatSession(c, viewID)
	if err != nil {
		return ErrorResponse(c, http.StatusNotFound, "view not found")
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

	if trimmed := strings.TrimSpace(req.ViewContent); trimmed != "" {
		session.chatState.ViewContent = trimmed
	}

	var attachedFiles []views.AttachedFile
	for _, f := range req.AttachedFiles {
		attachedFiles = append(attachedFiles, views.AttachedFile{Path: f.Path, Name: f.Name, ContentType: f.ContentType})
	}

	resp, err := vg.copilot.GenerateStream(
		genCtx,
		session.chatState,
		workspaceID,
		strings.TrimSpace(req.Message),
		viewID,
		attachedFiles,
		func(partial *views.PartialChatResponse) {
			writeSSE(viewSSEEvent{
				Event:       "chunk",
				Message:     partial.Message,
				ViewContent: partial.ViewContent,
				UpdateType:  partial.UpdateType,
			})
		},
	)
	if err != nil {
		log.Error().Err(err).Str("view_id", viewID).Msg("view copilot generation failed")
		writeSSE(viewSSEEvent{Event: "error", Error: err.Error()})
		writeSSE(viewSSEEvent{Event: "done"})
		return nil
	}

	var opResults []views.OperationResult
	if len(resp.Operations) > 0 {
		for _, op := range resp.Operations {
			opName := operationPayloadName(op.Payload)
			writeSSE(viewSSEEvent{
				Event:    "operation",
				OpType:   string(op.Type),
				OpName:   opName,
				OpStatus: "executing",
			})
		}
		opResults = vg.copilot.ExecuteOperations(genCtx, workspaceID, resp.Operations, viewID)
		for _, r := range opResults {
			writeSSE(viewSSEEvent{
				Event:        "operation",
				OpType:       r.Type,
				OpName:       r.Name,
				OpStatus:     r.Status,
				OpAgentID:    r.AgentID,
				OpTaskID:     r.TaskID,
				OpAgentName:  r.AgentName,
				OpScheduleID: r.ScheduleID,
				Message:      r.Message,
				Error:        r.Error,
			})
			if r.Status == "error" {
				log.Warn().Str("type", r.Type).Str("name", r.Name).Str("error", r.Error).Msg("copilot operation failed")
			}
		}
		if vg.copilot != nil && vg.copilot.ChatAvailable() {
			vg.copilot.PersistOperations(genCtx, viewID, opResults)
		}
	}

	// Ensure all agents referenced in the view actually exist. The model may
	// reference agents by name/key without emitting CREATE_AGENT operations.
	if session.chatState.ViewContent != "" {
		ensured := vg.copilot.EnsureViewAgentsExist(genCtx, workspaceID, session.chatState.ViewContent)
		for _, r := range ensured {
			writeSSE(viewSSEEvent{
				Event:     "operation",
				OpType:    r.Type,
				OpName:    r.Name,
				OpStatus:  r.Status,
				OpAgentID: r.AgentID,
			})
		}
		opResults = append(opResults, ensured...)
	}

	// Reconcile view content with fresh agents from the DB so agent
	// references are resolved to real UUIDs.
	if session.chatState.ViewContent != "" {
		if reconciled, reconcileErr := vg.copilot.ReconcileViewContent(genCtx, workspaceID, session.chatState.ViewContent, opResults); reconcileErr != nil {
			log.Warn().Err(reconcileErr).Str("view_id", viewID).Msg("failed to reconcile view content")
		} else if reconciled != "" && reconciled != session.chatState.ViewContent {
			session.chatState.ViewContent = reconciled
			resp.View_content = reconciled
		}
	}

	if resp.View_content != "" && string(resp.Update_type) != views.UpdateTypeConversation {
		var def types.ViewDefinition
		if err := json.Unmarshal([]byte(resp.View_content), &def); err == nil {
			views.NormalizeDefinition(&def)
			session.view.Definition = def
			if n := strings.TrimSpace(def.Name); n != "" {
				session.view.Name = n
			}
			if d := strings.TrimSpace(def.Description); d != "" {
				session.view.Description = d
			}
			session.view.SyncNameDescription()
			if err := vg.backend.UpdateView(genCtx, session.view); err != nil {
				log.Warn().Err(err).Str("view_id", viewID).Msg("failed to persist view definition after chat")
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


// ChatMessages returns the persisted chat history for a view.
func (vg *ViewsGroup) ChatMessages(c echo.Context) error {
	viewID := c.Param("view_id")
	wsID := c.Param("workspace_id")

	var messages []views.ChatMessage
	if vg.copilot != nil && vg.copilot.ChatAvailable() {
		if existing, err := vg.copilot.LoadChatState(c.Request().Context(), wsID, viewID); err == nil && existing != nil {
			messages = existing.Messages
		}
	}
	if messages == nil {
		messages = []views.ChatMessage{}
	}
	return c.JSON(http.StatusOK, map[string]any{"success": true, "data": messages})
}

func operationPayloadName(payload string) string {
	var m map[string]any
	if err := json.Unmarshal([]byte(payload), &m); err != nil {
		return ""
	}
	for _, key := range []string{"name", "task_name", "schedule_name", "agent_name", "skill_name", "prompt"} {
		if v, ok := m[key].(string); ok && strings.TrimSpace(v) != "" {
			return strings.TrimSpace(v)
		}
	}
	return ""
}

// ---------------------------------------------------------------------------
// Context stream
// ---------------------------------------------------------------------------

func (vg *ViewsGroup) IngestContext(c echo.Context) error {
	workspaceID, err := vg.workspaceID(c)
	if err != nil {
		return ErrorResponse(c, http.StatusBadRequest, err.Error())
	}
	viewID := c.Param("view_id")
	if _, err := vg.backend.GetView(c.Request().Context(), workspaceID, viewID); err != nil {
		return ErrorResponse(c, http.StatusNotFound, "view not found")
	}
	if vg.compactor == nil || !vg.compactor.Available() {
		return ErrorResponse(c, http.StatusServiceUnavailable, "context stream not available")
	}

	var req struct {
		EntryType    string         `json:"entry_type"`
		Content      string         `json:"content"`
		SourceTaskID string         `json:"source_task_id,omitempty"`
		Metadata     map[string]any `json:"metadata,omitempty"`
	}
	if err := c.Bind(&req); err != nil {
		return ErrorResponse(c, http.StatusBadRequest, "invalid request body")
	}
	if strings.TrimSpace(req.Content) == "" {
		return ErrorResponse(c, http.StatusBadRequest, "content is required")
	}
	if req.EntryType == "" {
		req.EntryType = "note"
	}

	entry := types.ViewContextEntry{
		ID:           fmt.Sprintf("ctx-%d", time.Now().UnixNano()),
		ViewID:       viewID,
		Timestamp:    time.Now().UnixMilli(),
		EntryType:    req.EntryType,
		Content:      strings.TrimSpace(req.Content),
		SourceTaskID: req.SourceTaskID,
		Metadata:     req.Metadata,
	}
	if err := vg.compactor.AppendEntry(c.Request().Context(), entry); err != nil {
		return ErrorResponse(c, http.StatusInternalServerError, "failed to append context entry")
	}

	// Check if compaction should trigger.
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		entries, err := vg.compactor.ReadContext(ctx, viewID)
		if err != nil {
			return
		}
		if vg.compactor.ShouldCompact(entries) {
			_ = vg.compactor.Compact(ctx, viewID, entries)
		}
	}()

	return c.JSON(http.StatusCreated, entry)
}

func (vg *ViewsGroup) GetContext(c echo.Context) error {
	workspaceID, err := vg.workspaceID(c)
	if err != nil {
		return ErrorResponse(c, http.StatusBadRequest, err.Error())
	}
	viewID := c.Param("view_id")
	if _, err := vg.backend.GetView(c.Request().Context(), workspaceID, viewID); err != nil {
		return ErrorResponse(c, http.StatusNotFound, "view not found")
	}
	if vg.compactor == nil || !vg.compactor.Available() {
		return c.JSON(http.StatusOK, map[string]any{"entries": []any{}, "formatted": ""})
	}

	entries, err := vg.compactor.ReadContext(c.Request().Context(), viewID)
	if err != nil {
		return ErrorResponse(c, http.StatusInternalServerError, "failed to read context")
	}
	if entries == nil {
		entries = []types.ViewContextEntry{}
	}

	if threadID := c.QueryParam("thread_id"); threadID != "" {
		entries = views.FilterByThreadID(entries, threadID)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"entries":   entries,
		"formatted": views.FormatForPrompt(entries),
	})
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
