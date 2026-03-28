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

func matchesSchema(output *types.TaskOutput, sc types.ViewOutputSchemaContext) bool {
	ot := strings.TrimSpace(strings.ToLower(output.OutputType))
	if ot == "" {
		return false
	}
	if sc.OutputType != "" && strings.TrimSpace(strings.ToLower(sc.OutputType)) == ot {
		return true
	}
	ak := ""
	if output.Metadata != nil {
		if v, ok := output.Metadata["artifact_key"].(string); ok {
			ak = strings.TrimSpace(strings.ToLower(v))
		}
	}
	if sc.ArtifactKey != "" && ak != "" && strings.TrimSpace(strings.ToLower(sc.ArtifactKey)) == ak {
		return true
	}
	return sc.OutputType == "" && sc.ArtifactKey == ""
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

func isConcatenated(cells map[string]string) bool {
	for _, v := range cells {
		if strings.Count(v, ";") >= 2 {
			return true
		}
	}
	return false
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
