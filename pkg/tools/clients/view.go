package clients

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"time"

	"github.com/beam-cloud/airstore/pkg/auth"
	"github.com/beam-cloud/airstore/pkg/repository"
	"github.com/beam-cloud/airstore/pkg/types"
	"github.com/beam-cloud/airstore/pkg/views"
	"github.com/rs/zerolog/log"
)

const (
	viewCmdListSheets = "list-sheets"
	viewCmdGetSchema  = "get-schema"
	viewCmdListRows   = "list-rows"
	viewCmdGetRow     = "get-row"
	viewCmdUpdateRow  = "update-row"
	viewCmdAddRow     = "add-row"
	viewCmdFindRows   = "find-rows"

	viewMaxListLimit = 200
	viewMaxFindLimit = 100
	viewCellTruncate = 500
)

type ViewClient struct {
	store   *views.ViewStore
	backend repository.BackendRepository
	sync    *views.ViewSync
}

func NewViewClient(store *views.ViewStore, backend repository.BackendRepository, sync *views.ViewSync) *ViewClient {
	return &ViewClient{store: store, backend: backend, sync: sync}
}

func (c *ViewClient) Name() types.IntegrationName {
	return types.ViewTool
}

func (c *ViewClient) Execute(ctx context.Context, command string, args map[string]any, creds *types.IntegrationCredentials, stdout, stderr io.Writer) error {
	if c.store == nil || !c.store.Available() {
		return WriteToolError(stdout, "view store not available")
	}

	workspaceID := auth.WorkspaceId(ctx)
	if workspaceID == 0 {
		return WriteToolError(stdout, "workspace context required")
	}

	viewID := GetStringArg(args, "view_id", "")
	if viewID == "" {
		return WriteToolError(stdout, "view_id is required (use $AIRSTORE_SOURCE_VIEW_ID)")
	}

	if err := c.validateViewAccess(ctx, workspaceID, viewID); err != nil {
		return WriteToolError(stdout, err.Error())
	}

	switch command {
	case viewCmdListSheets:
		return c.listSheets(ctx, viewID, workspaceID, stdout)
	case viewCmdGetSchema:
		return c.getSchema(ctx, viewID, workspaceID, args, stdout)
	case viewCmdListRows:
		return c.listRows(ctx, viewID, workspaceID, args, stdout)
	case viewCmdGetRow:
		return c.getRow(ctx, viewID, workspaceID, args, stdout)
	case viewCmdUpdateRow:
		return c.updateRow(ctx, viewID, workspaceID, args, stdout)
	case viewCmdAddRow:
		return c.addRow(ctx, viewID, workspaceID, args, stdout)
	case viewCmdFindRows:
		return c.findRows(ctx, viewID, workspaceID, args, stdout)
	default:
		return fmt.Errorf("unknown command: %s", command)
	}
}

func (c *ViewClient) validateViewAccess(ctx context.Context, workspaceID uint, viewID string) error {
	if c.backend == nil {
		return nil
	}
	v, err := c.backend.GetView(ctx, workspaceID, viewID)
	if err != nil || v == nil {
		return fmt.Errorf("view %s not found in this workspace", viewID)
	}
	return nil
}

func (c *ViewClient) getViewDefinition(ctx context.Context, workspaceID uint, viewID string) (*types.View, error) {
	if c.backend == nil {
		return nil, fmt.Errorf("backend not available")
	}
	v, err := c.backend.GetView(ctx, workspaceID, viewID)
	if err != nil || v == nil {
		return nil, fmt.Errorf("view %s not found", viewID)
	}
	return v, nil
}

func (c *ViewClient) listSheets(ctx context.Context, viewID string, workspaceID uint, stdout io.Writer) error {
	v, err := c.getViewDefinition(ctx, workspaceID, viewID)
	if err != nil {
		return WriteToolError(stdout, err.Error())
	}

	type colInfo struct {
		Key   string `json:"key"`
		Label string `json:"label"`
		Type  string `json:"type,omitempty"`
	}
	type compInfo struct {
		ID      string    `json:"id"`
		Title   string    `json:"title"`
		Type    string    `json:"type"`
		Columns []colInfo `json:"columns,omitempty"`
	}
	type sheetInfo struct {
		ID          string     `json:"id"`
		Name        string     `json:"name"`
		Description string     `json:"description,omitempty"`
		Tables      []compInfo `json:"tables"`
	}

	sheets := make([]sheetInfo, 0, len(v.Definition.Sheets))
	for _, sheet := range v.Definition.Sheets {
		si := sheetInfo{
			ID:          sheet.ID,
			Name:        sheet.Name,
			Description: sheet.Description,
		}
		for _, comp := range sheet.Components {
			if !comp.IsTable() {
				continue
			}
			ci := compInfo{
				ID:    comp.ID,
				Title: comp.Title,
				Type:  comp.Type,
			}
			cols := viewComponentColumnMeta(comp)
			for _, col := range cols {
				ci.Columns = append(ci.Columns, colInfo{
					Key:   col.Key,
					Label: col.Label,
					Type:  col.Type,
				})
			}
			si.Tables = append(si.Tables, ci)
		}
		sheets = append(sheets, si)
	}

	return WriteJSON(stdout, map[string]any{
		"view_id":   viewID,
		"view_name": v.Definition.Name,
		"sheets":    sheets,
	})
}

func (c *ViewClient) getSchema(ctx context.Context, viewID string, workspaceID uint, args map[string]any, stdout io.Writer) error {
	sheetID := GetStringArg(args, "sheet_id", "")
	componentID := GetStringArg(args, "component_id", "")
	if sheetID == "" || componentID == "" {
		return WriteToolError(stdout, "sheet_id and component_id are required")
	}

	v, err := c.getViewDefinition(ctx, workspaceID, viewID)
	if err != nil {
		return WriteToolError(stdout, err.Error())
	}

	for _, sheet := range v.Definition.Sheets {
		if sheet.ID != sheetID {
			continue
		}
		for _, comp := range sheet.Components {
			if comp.ID != componentID {
				continue
			}
			cols := viewComponentColumnMeta(comp)
			type colDetail struct {
				Key     string             `json:"key"`
				Label   string             `json:"label"`
				Type    string             `json:"type,omitempty"`
				Options []types.StatusOption `json:"options,omitempty"`
			}
			out := make([]colDetail, 0, len(cols))
			for _, col := range cols {
				out = append(out, colDetail{
					Key:     col.Key,
					Label:   col.Label,
					Type:    col.Type,
					Options: col.Options,
				})
			}
			return WriteJSON(stdout, map[string]any{
				"sheet_id":     sheetID,
				"sheet_name":   sheet.Name,
				"component_id": componentID,
				"component_title": comp.Title,
				"columns":      out,
			})
		}
		return WriteToolError(stdout, fmt.Sprintf("component %s not found in sheet %s", componentID, sheetID))
	}
	return WriteToolError(stdout, fmt.Sprintf("sheet %s not found", sheetID))
}

func viewComponentColumnMeta(comp types.ComponentSpec) []types.ColumnMeta {
	rawCols, ok := comp.Config["columns"]
	if !ok {
		return nil
	}
	data, err := json.Marshal(rawCols)
	if err != nil {
		return nil
	}
	var cols []types.ColumnMeta
	if json.Unmarshal(data, &cols) != nil {
		return nil
	}
	return cols
}

func (c *ViewClient) schemaColumns(ctx context.Context, workspaceID uint, viewID, sheetID, componentID string) map[string]string {
	if c.backend == nil {
		return nil
	}
	v, err := c.backend.GetView(ctx, workspaceID, viewID)
	if err != nil || v == nil {
		return nil
	}
	for _, sheet := range v.Definition.Sheets {
		if sheet.ID != sheetID {
			continue
		}
		for _, comp := range sheet.Components {
			if comp.ID != componentID {
				continue
			}
			cols := viewComponentColumns(comp)
			if len(cols) > 0 {
				return cols
			}
		}
	}
	return nil
}

func viewComponentColumns(comp types.ComponentSpec) map[string]string {
	rawCols, ok := comp.Config["columns"]
	if !ok {
		return nil
	}
	data, err := json.Marshal(rawCols)
	if err != nil {
		return nil
	}
	var cols []struct {
		Key   string `json:"key"`
		Label string `json:"label"`
	}
	if json.Unmarshal(data, &cols) != nil {
		return nil
	}
	result := make(map[string]string, len(cols))
	for _, col := range cols {
		if col.Key != "" {
			label := col.Label
			if label == "" {
				label = col.Key
			}
			result[col.Key] = label
		}
	}
	return result
}

func (c *ViewClient) listRows(ctx context.Context, viewID string, workspaceID uint, args map[string]any, stdout io.Writer) error {
	sheetID := GetStringArg(args, "sheet_id", "")
	componentID := GetStringArg(args, "component_id", "")
	if sheetID == "" || componentID == "" {
		return WriteToolError(stdout, "sheet_id and component_id are required")
	}

	limit := GetIntArg(args, "limit", 50)
	if limit > viewMaxListLimit {
		limit = viewMaxListLimit
	}
	offset := GetIntArg(args, "offset", 0)

	rows, err := c.store.GetRows(ctx, viewID, sheetID, componentID)
	if err != nil {
		return WriteToolError(stdout, fmt.Sprintf("failed to load rows: %v", err))
	}

	schemaCols := c.schemaColumns(ctx, workspaceID, viewID, sheetID, componentID)

	if offset > 0 && offset < len(rows) {
		rows = rows[offset:]
	} else if offset >= len(rows) {
		rows = nil
	}
	if len(rows) > limit {
		rows = rows[:limit]
	}

	return c.writeRows(stdout, rows, schemaCols)
}

func (c *ViewClient) getRow(ctx context.Context, viewID string, workspaceID uint, args map[string]any, stdout io.Writer) error {
	rowID := GetStringArg(args, "row_id", "")
	if rowID == "" {
		return WriteToolError(stdout, "row_id is required")
	}

	row, err := c.store.GetRowByID(ctx, viewID, rowID)
	if err != nil {
		return WriteToolError(stdout, fmt.Sprintf("failed to load row: %v", err))
	}
	if row == nil {
		return WriteToolError(stdout, fmt.Sprintf("row %s not found", rowID))
	}

	var schemaCols map[string]string
	if row.SheetID != "" && row.ComponentID != "" {
		schemaCols = c.schemaColumns(ctx, workspaceID, viewID, row.SheetID, row.ComponentID)
	}

	return c.writeRows(stdout, []views.ViewRow{*row}, schemaCols)
}

func (c *ViewClient) updateRow(ctx context.Context, viewID string, workspaceID uint, args map[string]any, stdout io.Writer) error {
	rowID := GetStringArg(args, "row_id", "")
	cellsJSON := GetStringArg(args, "cells", "")
	if rowID == "" || cellsJSON == "" {
		return WriteToolError(stdout, "row_id and cells are required")
	}

	var cells map[string]string
	if err := json.Unmarshal([]byte(cellsJSON), &cells); err != nil {
		return WriteToolError(stdout, fmt.Sprintf("cells must be valid JSON: %v", err))
	}
	if len(cells) == 0 {
		return WriteToolError(stdout, "cells object is empty")
	}

	if err := c.store.UpdateRow(ctx, viewID, rowID, cells, ""); err != nil {
		return WriteToolError(stdout, fmt.Sprintf("failed to update row: %v", err))
	}

	resp := map[string]any{
		"ok":            true,
		"row_id":        rowID,
		"cells_updated": len(cells),
	}

	// Temporary diagnostic
	if f, err := os.OpenFile("/tmp/viewsync-debug.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644); err == nil {
		fmt.Fprintf(f, "[%s] updateRow: sync=%v viewID=%s rowID=%s wksp=%d\n",
			time.Now().Format(time.RFC3339), c.sync != nil, viewID, rowID, workspaceID)
		f.Close()
	}

	if c.sync != nil {
		row, err := c.store.GetRowByID(ctx, viewID, rowID)
		if err == nil && row != nil {
			sr := c.sync.SyncToolWrite(ctx, views.ToolWriteInput{
				ViewID:            viewID,
				WorkspaceID:       workspaceID,
				SourceSheetID:     row.SheetID,
				SourceComponentID: row.ComponentID,
				Cells:             row.MergedCells(),
				RowID:             rowID,
			})
			if sr != nil {
				resp["cross_sheet_updated"] = len(sr.Updated)
				resp["cross_sheet_created"] = len(sr.Created)
			}
		}
	}

	return WriteJSON(stdout, resp)
}

func (c *ViewClient) addRow(ctx context.Context, viewID string, workspaceID uint, args map[string]any, stdout io.Writer) error {
	sheetID := GetStringArg(args, "sheet_id", "")
	componentID := GetStringArg(args, "component_id", "")
	cellsJSON := GetStringArg(args, "cells", "")
	if sheetID == "" || componentID == "" || cellsJSON == "" {
		return WriteToolError(stdout, "sheet_id, component_id, and cells are required")
	}

	var cells map[string]string
	if err := json.Unmarshal([]byte(cellsJSON), &cells); err != nil {
		return WriteToolError(stdout, fmt.Sprintf("cells must be valid JSON: %v", err))
	}
	if len(cells) == 0 {
		return WriteToolError(stdout, "cells object is empty")
	}

	rowID, created, matchedExisting, err := c.smartUpsertRow(ctx, viewID, sheetID, componentID, cells)
	if err != nil {
		return WriteToolError(stdout, fmt.Sprintf("failed to upsert row: %v", err))
	}

	resp := map[string]any{
		"ok":               true,
		"row_id":           rowID,
		"created":          created,
		"matched_existing": matchedExisting,
		"cells":            len(cells),
	}

	if c.sync != nil {
		sr := c.sync.SyncToolWrite(ctx, views.ToolWriteInput{
			ViewID:            viewID,
			WorkspaceID:       workspaceID,
			SourceSheetID:     sheetID,
			SourceComponentID: componentID,
			Cells:             cells,
			RowID:             rowID,
		})
		if sr != nil {
			resp["cross_sheet_updated"] = len(sr.Updated)
			resp["cross_sheet_created"] = len(sr.Created)
		}
	}

	return WriteJSON(stdout, resp)
}

// smartUpsertRow uses vector search to find semantically matching rows on the
// target sheet before inserting. If a high-confidence match is found, the
// existing row is updated instead of creating a duplicate.
// Returns (rowID, created, matchedExisting, error).
func (c *ViewClient) smartUpsertRow(
	ctx context.Context,
	viewID, sheetID, componentID string,
	cells map[string]string,
) (string, bool, bool, error) {
	ec := c.store.Embedder()
	if ec == nil || !ec.Available() {
		rowID, created, err := c.store.UpsertRow(ctx, viewID, sheetID, componentID, cells, views.UpsertOpts{})
		return rowID, created, false, err
	}

	tempRow := &views.ViewRow{Cells: cells}
	searchText := views.RowSearchText(tempRow)
	if searchText == "" {
		rowID, created, err := c.store.UpsertRow(ctx, viewID, sheetID, componentID, cells, views.UpsertOpts{})
		return rowID, created, false, err
	}

	_ = c.store.EnsureVectorIndex(ctx, viewID, ec.Dims())

	queryVec, err := ec.EmbedOne(ctx, searchText)
	if err != nil {
		log.Debug().Err(err).Msg("view-tool: embed failed, falling back to content-hash upsert")
		rowID, created, err := c.store.UpsertRow(ctx, viewID, sheetID, componentID, cells, views.UpsertOpts{})
		return rowID, created, false, err
	}

	results, err := c.store.VectorSearch(ctx, viewID, sheetID, queryVec, 5)
	if err != nil {
		log.Debug().Err(err).Msg("view-tool: vector search failed, falling back")
		rowID, created, err := c.store.UpsertRow(ctx, viewID, sheetID, componentID, cells, views.UpsertOpts{})
		return rowID, created, false, err
	}

	threshold := 0.87
	if c.sync != nil {
		threshold = c.sync.HighMatchThreshold()
	}

	for _, r := range results {
		if r.Score >= threshold {
			if err := c.store.UpdateRow(ctx, viewID, r.ID, cells, ""); err != nil {
				log.Warn().Err(err).Str("row_id", r.ID).Msg("view-tool: merge into matched row failed")
				continue
			}
			log.Info().
				Str("view_id", viewID).
				Str("sheet_id", sheetID).
				Str("row_id", r.ID).
				Float64("score", r.Score).
				Int("cells", len(cells)).
				Msg("view-tool: merged into existing row via vector search")
			return r.ID, false, true, nil
		}
	}

	rowID, created, err := c.store.UpsertRow(ctx, viewID, sheetID, componentID, cells, views.UpsertOpts{})
	return rowID, created, false, err
}

func (c *ViewClient) findRows(ctx context.Context, viewID string, workspaceID uint, args map[string]any, stdout io.Writer) error {
	column := GetStringArg(args, "column", "")
	value := GetStringArg(args, "value", "")
	if column == "" || value == "" {
		return WriteToolError(stdout, "column and value are required")
	}

	sheetID := GetStringArg(args, "sheet_id", "")
	limit := GetIntArg(args, "limit", 20)
	if limit > viewMaxFindLimit {
		limit = viewMaxFindLimit
	}

	matched, err := c.store.FindRows(ctx, viewID, sheetID, column, value, limit)
	if err != nil {
		return WriteToolError(stdout, fmt.Sprintf("failed to find rows: %v", err))
	}

	componentID := GetStringArg(args, "component_id", "")
	var schemaCols map[string]string
	if sheetID != "" && componentID != "" {
		schemaCols = c.schemaColumns(ctx, workspaceID, viewID, sheetID, componentID)
	}

	return c.writeRows(stdout, matched, schemaCols)
}

func (c *ViewClient) writeRows(stdout io.Writer, rows []views.ViewRow, schemaCols map[string]string) error {
	type outputRow struct {
		ID     string            `json:"row_id"`
		RowKey string            `json:"row_key,omitempty"`
		Cells  map[string]string `json:"cells"`
	}

	out := make([]outputRow, 0, len(rows))
	for _, row := range rows {
		merged := row.MergedCells()
		filtered := filterCells(merged, schemaCols)
		out = append(out, outputRow{
			ID:     row.ID,
			RowKey: row.RowKey,
			Cells:  filtered,
		})
	}

	return WriteJSON(stdout, map[string]any{
		"total": len(out),
		"rows":  out,
	})
}

func filterCells(merged map[string]string, schemaCols map[string]string) map[string]string {
	if len(schemaCols) == 0 {
		result := make(map[string]string, len(merged))
		for k, v := range merged {
			if v != "" {
				if len(v) > viewCellTruncate {
					v = v[:viewCellTruncate] + "..."
				}
				result[k] = v
			}
		}
		return result
	}

	keys := make([]string, 0, len(schemaCols))
	for k := range schemaCols {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	result := make(map[string]string, len(keys))
	for _, k := range keys {
		v := merged[k]
		if v == "" {
			continue
		}
		if len(v) > viewCellTruncate {
			v = v[:viewCellTruncate] + "..."
		}
		result[k] = v
	}
	return result
}
