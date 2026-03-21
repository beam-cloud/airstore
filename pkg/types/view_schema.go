package types

import (
	"encoding/json"
	"strings"
)

type ViewOutputSchemaColumn struct {
	Key     string         `json:"key"`
	Label   string         `json:"label,omitempty"`
	Type    string         `json:"type,omitempty"`
	Options []StatusOption `json:"options,omitempty"`
}

type ViewOutputSchemaContext struct {
	ViewID         string                   `json:"view_id"`
	ViewName       string                   `json:"view_name,omitempty"`
	SheetID        string                   `json:"sheet_id"`
	SheetName      string                   `json:"sheet_name,omitempty"`
	ComponentID    string                   `json:"component_id"`
	ComponentTitle string                   `json:"component_title,omitempty"`
	ArtifactKey    string                   `json:"artifact_key,omitempty"`
	OutputType     string                   `json:"output_type,omitempty"`
	Columns        []ViewOutputSchemaColumn `json:"columns,omitempty"`
	TransformHints []string                 `json:"transform_hints,omitempty"`
}

func ParseViewOutputSchemaContexts(value any) []ViewOutputSchemaContext {
	if value == nil {
		return nil
	}
	body, err := json.Marshal(value)
	if err != nil || len(body) == 0 {
		return nil
	}
	var contexts []ViewOutputSchemaContext
	if err := json.Unmarshal(body, &contexts); err != nil {
		return nil
	}
	return contexts
}

func ViewOutputSchemaPolicyValue(contexts []ViewOutputSchemaContext) any {
	if len(contexts) == 0 {
		return nil
	}
	body, err := json.Marshal(contexts)
	if err != nil || len(body) == 0 {
		return nil
	}
	var decoded []map[string]any
	if err := json.Unmarshal(body, &decoded); err != nil || len(decoded) == 0 {
		return nil
	}
	return decoded
}

func (c ViewOutputSchemaContext) SortKey() string {
	return strings.TrimSpace(c.ViewID) + "\x00" +
		strings.TrimSpace(c.SheetID) + "\x00" +
		strings.TrimSpace(c.ComponentID)
}

func (c ViewOutputSchemaContext) MatchLabel() string {
	parts := make([]string, 0, 3)
	for _, value := range []string{c.ViewName, c.SheetName, c.ComponentTitle} {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			parts = append(parts, trimmed)
		}
	}
	return strings.Join(parts, " / ")
}

func (c ViewOutputSchemaContext) ColumnKeys() []string {
	keys := make([]string, 0, len(c.Columns))
	for _, column := range c.Columns {
		if key := strings.TrimSpace(column.Key); key != "" {
			keys = append(keys, key)
		}
	}
	return keys
}

func (c ViewOutputSchemaContext) CompactColumns() []map[string]any {
	out := make([]map[string]any, 0, len(c.Columns))
	for _, column := range c.Columns {
		key := strings.TrimSpace(column.Key)
		if key == "" {
			continue
		}
		record := map[string]any{"key": key}
		if label := strings.TrimSpace(column.Label); label != "" {
			record["label"] = label
		}
		if columnType := strings.TrimSpace(column.Type); columnType != "" {
			record["type"] = columnType
		}
		if len(column.Options) > 0 {
			record["options"] = column.Options
		}
		out = append(out, record)
	}
	return out
}
