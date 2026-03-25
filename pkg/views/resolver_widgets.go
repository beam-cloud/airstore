package views

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/beam-cloud/airstore/pkg/types"
	"github.com/rs/zerolog/log"

	baml "github.com/beam-cloud/airstore/pkg/views/baml_client"
	bamltypes "github.com/beam-cloud/airstore/pkg/views/baml_client/types"
)

func (r *DataResolver) ResolveWidgets(ctx context.Context, workspaceID uint, viewID string, sheet types.SheetSpec, comp types.ComponentSpec, opts ResolveOptions) ([]types.WidgetData, error) {
	if len(sheet.Widgets) == 0 {
		return nil, nil
	}

	rows, err := r.store.GetRows(ctx, viewID, sheet.ID, comp.ID)
	if err != nil {
		return nil, fmt.Errorf("load view rows for widgets: %w", err)
	}

	tableCols := buildColumnSchemas(comp)
	columnsStr := serializeWidgetColumns(tableCols)
	dataStr := serializeWidgetRows(tableCols, rows)
	dataHash := widgetDataHash(columnsStr, dataStr)

	stored, err := r.store.GetWidgetRows(ctx, viewID, sheet.ID)
	if err != nil {
		log.Warn().Err(err).Str("view_id", viewID).Str("sheet_id", sheet.ID).Msg("widget row load failed")
	}
	byWidget := make(map[string]WidgetRow, len(stored))
	for _, wr := range stored {
		byWidget[wr.WidgetID] = wr
	}

	results := make([]types.WidgetData, 0, len(sheet.Widgets))
	for _, widget := range sheet.Widgets {
		if !opts.ForceRefresh {
			if wr, ok := byWidget[widget.ID]; ok && wr.SchemaHash == dataHash {
				results = append(results, widgetRowToData(wr))
				continue
			}
		}

		wr := resolveOneWidget(ctx, sheet.Name, sheet.ID, widget, columnsStr, dataStr, dataHash)
		results = append(results, widgetRowToData(wr))

		if err := r.store.UpsertWidgetRow(ctx, viewID, wr); err != nil {
			log.Warn().Err(err).Str("widget_id", widget.ID).Msg("widget row write failed")
		}
	}
	return results, nil
}

func resolveOneWidget(ctx context.Context, sheetName, sheetID string, widget types.WidgetSpec, columnsStr, dataStr, dataHash string) WidgetRow {
	configJSON, _ := json.Marshal(widget.Config)
	now := time.Now()

	result, err := baml.MapViewToWidget(ctx, sheetName, widget.Type, widget.Title, widget.Description, string(configJSON), columnsStr, dataStr)
	if err != nil {
		log.Warn().Err(err).Str("widget_id", widget.ID).Str("type", widget.Type).Msg("BAML widget mapping failed")
		return WidgetRow{
			SheetID:    sheetID,
			WidgetID:   widget.ID,
			Type:       widget.Type,
			Status:     types.ResolvedDataStatusRequestError,
			Error:      "failed to resolve widget data",
			SchemaHash: dataHash,
			UpdatedAt:  now,
		}
	}

	wr := WidgetRow{
		SheetID:    sheetID,
		WidgetID:   widget.ID,
		Type:       widget.Type,
		Status:     types.ResolvedDataStatusOK,
		SchemaHash: dataHash,
		UpdatedAt:  now,
	}
	switch widget.Type {
	case "metric":
		if result.Metric != nil {
			wr.Metric = &WidgetMetric{Value: result.Metric.Value, Label: result.Metric.Label, Comparison: result.Metric.Comparison}
		}
	case "map":
		if result.Map_data != nil {
			markers := make([]WidgetMapMarker, 0, len(result.Map_data.Markers))
			for _, m := range result.Map_data.Markers {
				markers = append(markers, WidgetMapMarker{Lat: m.Lat, Lng: m.Lng, Label: m.Label, Detail: m.Detail})
			}
			wr.MapData = &WidgetMapData{Markers: markers}
		}
	case "list":
		if result.List_data != nil {
			items := make([]WidgetListItem, 0, len(result.List_data.Items))
			for _, item := range result.List_data.Items {
				items = append(items, WidgetListItem{Label: item.Label, Value: item.Value, Detail: item.Detail})
			}
			wr.ListData = &WidgetListData{Items: items}
		}
	}
	return wr
}

func widgetRowToData(wr WidgetRow) types.WidgetData {
	wd := types.WidgetData{
		WidgetID: wr.WidgetID,
		Type:     wr.Type,
		Status:   wr.Status,
		Error:    wr.Error,
		CachedAt: &wr.UpdatedAt,
	}
	if wr.Metric != nil {
		wd.Metric = &types.MetricData{Value: wr.Metric.Value, Label: wr.Metric.Label, Comparison: wr.Metric.Comparison}
	}
	if wr.MapData != nil {
		markers := make([]types.MapMarker, 0, len(wr.MapData.Markers))
		for _, m := range wr.MapData.Markers {
			markers = append(markers, types.MapMarker{Lat: m.Lat, Lng: m.Lng, Label: m.Label, Detail: m.Detail})
		}
		wd.MapData = &types.MapWidgetData{Markers: markers}
	}
	if wr.ListData != nil {
		items := make([]types.ListItem, 0, len(wr.ListData.Items))
		for _, item := range wr.ListData.Items {
			items = append(items, types.ListItem{Label: item.Label, Value: item.Value, Detail: item.Detail})
		}
		wd.ListData = &types.ListWidgetData{Items: items}
	}
	return wd
}

func serializeWidgetColumns(cols []bamltypes.ColumnSchema) string {
	var sb strings.Builder
	for i, col := range cols {
		if i > 0 {
			sb.WriteString("\n")
		}
		fmt.Fprintf(&sb, "- %s [key=%s] (%s)", col.Name, col.Key, col.Type)
	}
	return sb.String()
}

func serializeWidgetRows(cols []bamltypes.ColumnSchema, rows []ViewRow) string {
	if len(rows) == 0 {
		return "(no data)"
	}

	var sb strings.Builder
	keys := make([]string, len(cols))
	for i, col := range cols {
		keys[i] = col.Key
	}

	for i, row := range rows {
		if i >= 100 {
			fmt.Fprintf(&sb, "\n... and %d more rows", len(rows)-100)
			break
		}
		cells := row.MergedCells()
		if i > 0 {
			sb.WriteString("\n")
		}
		fmt.Fprintf(&sb, "ROW %d:", i+1)
		for _, key := range keys {
			v := cells[key]
			if v != "" {
				fmt.Fprintf(&sb, " %s=%q", key, v)
			}
		}
	}
	return sb.String()
}

func widgetDataHash(columns, data string) string {
	h := sha256.New()
	h.Write([]byte(columns))
	h.Write([]byte("\x00"))
	h.Write([]byte(data))
	return hex.EncodeToString(h.Sum(nil))[:16]
}
