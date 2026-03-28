package views

import (
	"encoding/json"
	"strings"

	"github.com/beam-cloud/airstore/pkg/types"
	viewbamltypes "github.com/beam-cloud/airstore/pkg/views/baml_client/types"
)

const cellMaxLen = 200

func skipOutput(output *types.TaskOutput) bool {
	ot := strings.TrimSpace(strings.ToLower(output.OutputType))
	if ot == "" {
		return true
	}
	for _, skip := range []string{"approval", "status", "progress", "log", "system"} {
		if ot == skip {
			return true
		}
	}
	title := strings.ToLower(output.Title)
	if strings.Contains(title, "approval required") || strings.Contains(title, "waiting for") {
		return true
	}
	return false
}

func bamlColumns(sc types.ViewOutputSchemaContext) []viewbamltypes.ViewColumn {
	out := make([]viewbamltypes.ViewColumn, len(sc.Columns))
	for i, c := range sc.Columns {
		out[i] = viewbamltypes.ViewColumn{Key: c.Key, Label: c.Label, Type: c.Type}
	}
	return out
}

func extractCells(baml []viewbamltypes.ViewCell) map[string]string {
	out := make(map[string]string, len(baml))
	for _, c := range baml {
		if c.Column != "" && c.Value != "" {
			out[c.Column] = c.Value
		}
	}
	return out
}

func buildSearchCriteria(plan *viewbamltypes.RowSearchPlan) []SearchCriterion {
	var criteria []SearchCriterion
	if plan != nil {
		for _, c := range plan.Criteria {
			criteria = append(criteria, SearchCriterion{
				Column: strings.TrimSpace(c.Column),
				Value:  strings.TrimSpace(c.Value),
			})
		}
	}
	return dedupeSearchCriteria(criteria)
}

func entityHints(plan *viewbamltypes.RowSearchPlan, preferred []string) []string {
	if len(preferred) > 0 {
		return dedupeStrings(preferred)
	}
	if plan == nil {
		return nil
	}
	return dedupeStrings(plan.Entity_labels)
}

const maxVectorQueries = 6

func vectorQueryTexts(
	criteria []SearchCriterion,
	hints []string,
	outputType, title, summary, data string,
) []string {
	queries := []string{OutputSearchText(outputType, title, summary, data)}
	for _, hint := range hints {
		queries = append(queries, hint)
	}
	for _, c := range criteria {
		value := strings.TrimSpace(c.Value)
		if value == "" {
			continue
		}
		if col := strings.TrimSpace(c.Column); col != "" {
			queries = append(queries, col+": "+value)
		} else {
			queries = append(queries, value)
		}
	}
	deduped := dedupeStrings(queries)
	if len(deduped) > maxVectorQueries {
		deduped = deduped[:maxVectorQueries]
	}
	return deduped
}

func dedupeSearchCriteria(criteria []SearchCriterion) []SearchCriterion {
	out := make([]SearchCriterion, 0, len(criteria))
	seen := map[string]struct{}{}
	for _, c := range criteria {
		column := strings.TrimSpace(c.Column)
		value := strings.TrimSpace(c.Value)
		if value == "" {
			continue
		}
		key := strings.ToLower(column) + "|" + NormalizeRowKey(value)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, SearchCriterion{Column: column, Value: value})
	}
	return out
}

func dedupeStrings(values []string) []string {
	out := make([]string, 0, len(values))
	seen := map[string]struct{}{}
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		key := NormalizeRowKey(value)
		if key == "" {
			key = strings.ToLower(value)
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, value)
	}
	return out
}

func safeDeref(s *string) string {
	if s != nil {
		return *s
	}
	return ""
}

func serializeOutput(output *types.TaskOutput) string {
	compact := map[string]any{
		"id":          output.ID,
		"output_type": output.OutputType,
		"title":       output.Title,
	}
	if output.Summary != nil && *output.Summary != "" {
		compact["summary"] = *output.Summary
	}
	if output.URI != nil && *output.URI != "" {
		compact["uri"] = *output.URI
	}
	if len(output.Data) > 0 {
		compact["data"] = output.Data
	}
	if output.Metadata != nil {
		filtered := make(map[string]any)
		for k, v := range output.Metadata {
			if !strings.HasPrefix(k, "_") {
				filtered[k] = v
			}
		}
		if len(filtered) > 0 {
			compact["metadata"] = filtered
		}
	}
	b, _ := json.Marshal(compact)
	if b == nil {
		return "{}"
	}
	return string(b)
}

func cellsJSON(row *ViewRow, keys []string) string {
	if row == nil {
		return "{}"
	}
	b, _ := json.Marshal(projectCells(row.MergedCells(), keys))
	if b == nil {
		return "{}"
	}
	return string(b)
}

func projectCells(merged map[string]string, keys []string) map[string]string {
	allowed := make(map[string]struct{}, len(keys))
	for _, k := range keys {
		allowed[k] = struct{}{}
	}
	out := make(map[string]string)
	for k, v := range merged {
		if v == "" {
			continue
		}
		if _, ok := allowed[k]; !ok && len(allowed) > 0 {
			continue
		}
		if len(v) > cellMaxLen {
			v = v[:cellMaxLen] + "..."
		}
		out[k] = v
	}
	return out
}

func buildViewContext(schemas []types.ViewOutputSchemaContext) string {
	type sheetInfo struct {
		name string
		cols []string
	}
	seen := make(map[string]*sheetInfo)
	var order []string
	viewName := ""
	for _, sc := range schemas {
		if viewName == "" && sc.ViewName != "" {
			viewName = sc.ViewName
		}
		if _, ok := seen[sc.SheetID]; ok {
			continue
		}
		var colLabels []string
		for _, c := range sc.Columns {
			colLabels = append(colLabels, c.Label)
		}
		name := sc.SheetName
		if name == "" {
			name = sc.SheetID
		}
		seen[sc.SheetID] = &sheetInfo{name: name, cols: colLabels}
		order = append(order, sc.SheetID)
	}
	if len(order) == 0 {
		return ""
	}
	var b strings.Builder
	if viewName != "" {
		b.WriteString("View: ")
		b.WriteString(viewName)
		b.WriteByte('\n')
	}
	b.WriteString("Sheets:\n")
	for _, id := range order {
		info := seen[id]
		b.WriteString("  - ")
		b.WriteString(info.name)
		b.WriteString(": [")
		b.WriteString(strings.Join(info.cols, ", "))
		b.WriteString("]\n")
	}
	return b.String()
}

func formatCrossSheetContext(rows []ViewRow, schemaKeys map[string][]string, sheetNames map[string]string) string {
	if len(rows) == 0 {
		return ""
	}
	var b strings.Builder
	b.WriteString("RELATED ROWS FROM OTHER SHEETS IN THIS VIEW:\n")
	for _, r := range rows {
		keys := schemaKeys[r.SheetID]
		c := projectCells(r.MergedCells(), keys)
		if len(c) == 0 {
			continue
		}
		name := sheetNames[r.SheetID]
		if name == "" {
			name = r.SheetID
		}
		b.WriteString("[")
		b.WriteString(name)
		b.WriteString("] ")
		first := true
		for k, v := range c {
			if !first {
				b.WriteString(", ")
			}
			b.WriteString(k)
			b.WriteString(": ")
			b.WriteString(v)
			first = false
		}
		b.WriteByte('\n')
	}
	return b.String()
}

func serializeRows(rows []ViewRow, keys []string) string {
	if len(rows) == 0 {
		return ""
	}
	type row struct {
		ID    string            `json:"_id"`
		Cells map[string]string `json:"cells"`
	}
	compact := make([]row, 0, len(rows))
	for _, r := range rows {
		c := projectCells(r.MergedCells(), keys)
		if len(c) > 0 {
			compact = append(compact, row{ID: r.ID, Cells: c})
		}
	}
	if len(compact) == 0 {
		return ""
	}
	b, _ := json.Marshal(compact)
	return string(b)
}
