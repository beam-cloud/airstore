package viewschema

import (
	"context"
	"encoding/json"
	"sort"
	"strings"

	"github.com/beam-cloud/airstore/pkg/types"
)

const (
	columnLimit = 12
	hintLimit   = 6
)

type Backend interface {
	GetAgentProfile(ctx context.Context, workspaceID uint, agentID string) (*types.AgentProfile, error)
	ListViews(ctx context.Context, workspaceID uint) ([]*types.View, error)
}

func LoadViewOutputSchemaContexts(
	ctx context.Context,
	backend Backend,
	workspaceID uint,
	agentID string,
) ([]types.ViewOutputSchemaContext, error) {
	agentID = strings.TrimSpace(agentID)
	if backend == nil || agentID == "" {
		return nil, nil
	}
	views, err := backend.ListViews(ctx, workspaceID)
	if err != nil {
		return nil, err
	}
	matchRefs := agentMatchRefs(ctx, backend, workspaceID, agentID)
	if len(matchRefs) == 0 {
		return nil, nil
	}
	contexts := make([]types.ViewOutputSchemaContext, 0, len(views))
	for _, view := range views {
		if view == nil {
			continue
		}
		for _, sheet := range view.Definition.Sheets {
			for _, component := range sheet.Components {
				if !component.IsTable() || !componentMatchesAgent(view.Definition, component, matchRefs) {
					continue
				}
				if context := buildContext(view, sheet, component); context != nil {
					contexts = append(contexts, *context)
				}
			}
		}
	}
	sort.SliceStable(contexts, func(i, j int) bool {
		return contexts[i].SortKey() < contexts[j].SortKey()
	})
	return contexts, nil
}

func agentMatchRefs(
	ctx context.Context,
	backend Backend,
	workspaceID uint,
	agentID string,
) map[string]struct{} {
	refs := map[string]struct{}{}
	if normalized := normalizeAgentRef(agentID); normalized != "" {
		refs[normalized] = struct{}{}
	}
	profile, err := backend.GetAgentProfile(ctx, workspaceID, agentID)
	if err != nil || profile == nil {
		return refs
	}
	for _, raw := range []string{profile.ID, profile.AgentKey, profile.Name} {
		if normalized := normalizeAgentRef(raw); normalized != "" {
			refs[normalized] = struct{}{}
		}
	}
	return refs
}

func componentMatchesAgent(
	definition types.ViewDefinition,
	component types.ComponentSpec,
	matchRefs map[string]struct{},
) bool {
	for _, ref := range componentAgentRefs(definition, component) {
		if _, ok := matchRefs[normalizeAgentRef(ref)]; ok {
			return true
		}
	}
	return false
}

func componentAgentRefs(definition types.ViewDefinition, component types.ComponentSpec) []string {
	refs := make([]string, 0, len(definition.Agents)+4)
	if component.DataSource != nil {
		refs = append(refs, component.DataSource.AgentIDs...)
		if strings.TrimSpace(component.DataSource.AgentID) != "" {
			refs = append(refs, component.DataSource.AgentID)
		}
	}
	if component.Config != nil {
		if ref := stringFromPayload(component.Config, "agent_id"); ref != "" {
			refs = append(refs, ref)
		}
		if raw, ok := component.Config["agent_ids"]; ok {
			refs = append(refs, stringSliceFromAny(raw)...)
		}
	}
	if len(refs) == 0 {
		refs = append(refs, definition.Agents...)
	}
	return uniqueTrimmedStrings(refs)
}

func buildContext(view *types.View, sheet types.SheetSpec, component types.ComponentSpec) *types.ViewOutputSchemaContext {
	columns := buildColumns(component)
	if len(columns) == 0 {
		return nil
	}
	context := &types.ViewOutputSchemaContext{
		ViewID:         strings.TrimSpace(view.ID),
		ViewName:       strings.TrimSpace(view.Name),
		SheetID:        strings.TrimSpace(sheet.ID),
		SheetName:      strings.TrimSpace(sheet.Name),
		ComponentID:    strings.TrimSpace(component.ID),
		ComponentTitle: strings.TrimSpace(component.Title),
		Columns:        columns,
	}
	if component.DataSource != nil {
		context.ArtifactKey = strings.TrimSpace(component.DataSource.ArtifactKey)
		context.OutputType = strings.TrimSpace(component.DataSource.OutputType)
		context.TransformHints = transformHints(component.DataSource.Transform)
	}
	if context.ComponentTitle == "" {
		context.ComponentTitle = context.SheetName
	}
	return context
}

func buildColumns(component types.ComponentSpec) []types.ViewOutputSchemaColumn {
	configByKey := make(map[string]types.ColumnMeta)
	configOrder := make([]string, 0)
	for _, column := range configColumns(component.Config) {
		key := canonicalColumnKey(column.Key)
		if key == "" {
			continue
		}
		column.Key = key
		if existing, ok := configByKey[key]; ok {
			if strings.TrimSpace(existing.Label) == "" && strings.TrimSpace(column.Label) != "" {
				existing.Label = column.Label
			}
			if strings.TrimSpace(existing.Type) == "" && strings.TrimSpace(column.Type) != "" {
				existing.Type = column.Type
			}
			if len(existing.Options) == 0 && len(column.Options) > 0 {
				existing.Options = column.Options
			}
			configByKey[key] = existing
			continue
		}
		configByKey[key] = column
		configOrder = append(configOrder, key)
	}

	columns := make([]types.ViewOutputSchemaColumn, 0, columnLimit)
	seen := make(map[string]struct{}, columnLimit)
	if component.DataSource != nil {
		for _, rule := range component.DataSource.Transform {
			key := canonicalColumnKey(rule.Column)
			if key == "" {
				key = canonicalColumnKey(sourceColumnHint(rule.Source))
			}
			if key == "" {
				continue
			}
			if _, ok := seen[key]; ok {
				continue
			}
			cfg := configByKey[key]
			columns = append(columns, types.ViewOutputSchemaColumn{
				Key:     key,
				Label:   firstNonEmptyTrimmed(cfg.Label, humanizeColumn(key)),
				Type:    inferColumnType(key, firstNonEmptyTrimmed(rule.Type, cfg.Type)),
				Options: append([]types.StatusOption(nil), cfg.Options...),
			})
			seen[key] = struct{}{}
			if len(columns) >= columnLimit {
				return columns
			}
		}
	}
	for _, key := range configOrder {
		if _, ok := seen[key]; ok {
			continue
		}
		cfg := configByKey[key]
		columns = append(columns, types.ViewOutputSchemaColumn{
			Key:     key,
			Label:   firstNonEmptyTrimmed(cfg.Label, humanizeColumn(key)),
			Type:    inferColumnType(key, cfg.Type),
			Options: append([]types.StatusOption(nil), cfg.Options...),
		})
		if len(columns) >= columnLimit {
			break
		}
	}
	return columns
}

func configColumns(config map[string]any) []types.ColumnMeta {
	raw, ok := config["columns"]
	if !ok || raw == nil {
		return nil
	}
	body, err := json.Marshal(raw)
	if err != nil || len(body) == 0 {
		return nil
	}
	var columns []types.ColumnMeta
	if err := json.Unmarshal(body, &columns); err != nil {
		return nil
	}
	return columns
}

func transformHints(rules []types.TransformRule) []string {
	hints := make([]string, 0, len(rules))
	seen := make(map[string]struct{}, len(rules))
	for _, rule := range rules {
		hint := strings.TrimSpace(rule.Source)
		if hint == "" {
			continue
		}
		if _, ok := seen[hint]; ok {
			continue
		}
		seen[hint] = struct{}{}
		hints = append(hints, hint)
		if len(hints) >= hintLimit {
			break
		}
	}
	return hints
}

func canonicalColumnKey(value string) string {
	normalized := normalizeColumnKey(value)
	if normalized == "" {
		return ""
	}
	switch normalized {
	case "task_id", "detail_task_id", "row_id", "stable_ref", "sheet_id", "output_id", "output_status", "blocker_output_ids", "blocker_kind", "blocker_input_kind", "blocker_wait_group_id", "approval_surface", "source_output_ids":
		return normalized + "_value"
	default:
		return normalized
	}
}

func normalizeColumnKey(value string) string {
	value = strings.TrimSpace(strings.ToLower(value))
	if value == "" {
		return ""
	}
	var b strings.Builder
	lastUnderscore := false
	for _, r := range value {
		switch {
		case (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9'):
			b.WriteRune(r)
			lastUnderscore = false
		default:
			if !lastUnderscore {
				b.WriteByte('_')
				lastUnderscore = true
			}
		}
	}
	return strings.Trim(b.String(), "_")
}

func sourceColumnHint(source string) string {
	source = strings.TrimSpace(strings.Split(source, "|")[0])
	source = strings.TrimPrefix(strings.TrimPrefix(source, "data."), "metadata.")
	parts := strings.FieldsFunc(source, func(r rune) bool {
		switch r {
		case '.', '[', ']', ' ':
			return true
		default:
			return false
		}
	})
	for i := len(parts) - 1; i >= 0; i-- {
		if part := strings.TrimSpace(parts[i]); part != "" {
			return part
		}
	}
	return ""
}

func inferColumnType(key, columnType string) string {
	if columnType = strings.TrimSpace(strings.ToLower(columnType)); columnType != "" {
		return columnType
	}
	switch {
	case strings.HasSuffix(key, "_at"), key == "date", key == "created", key == "updated":
		return "date"
	case key == "email", strings.HasSuffix(key, "_email"):
		return "email"
	case key == "url", key == "link", strings.HasSuffix(key, "_url"), strings.HasSuffix(key, "_link"):
		return "link"
	case key == "status":
		return "status"
	default:
		return "text"
	}
}

func humanizeColumn(key string) string {
	parts := strings.Split(strings.TrimSpace(key), "_")
	for i, part := range parts {
		if part != "" {
			parts[i] = strings.ToUpper(part[:1]) + part[1:]
		}
	}
	return strings.Join(parts, " ")
}

func stringSliceFromAny(value any) []string {
	body, err := json.Marshal(value)
	if err != nil || len(body) == 0 {
		return nil
	}
	var values []string
	if err := json.Unmarshal(body, &values); err != nil {
		return nil
	}
	return uniqueTrimmedStrings(values)
}

func stringFromPayload(payload map[string]any, key string) string {
	if payload == nil {
		return ""
	}
	value, ok := payload[key]
	if !ok {
		return ""
	}
	text, ok := value.(string)
	if !ok {
		return ""
	}
	return strings.TrimSpace(text)
}

func uniqueTrimmedStrings(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	unique := make([]string, 0, len(values))
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			if _, ok := seen[trimmed]; ok {
				continue
			}
			seen[trimmed] = struct{}{}
			unique = append(unique, trimmed)
		}
	}
	return unique
}

func normalizeAgentRef(value string) string {
	return strings.TrimSpace(strings.ToLower(value))
}

func firstNonEmptyTrimmed(values ...string) string {
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			return trimmed
		}
	}
	return ""
}
