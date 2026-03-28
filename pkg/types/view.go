package types

import (
	"context"
	"encoding/json"
	"sort"
	"strings"
	"time"
)

const (
	ResolvedDataStatusOK           = "ok"
	ResolvedDataStatusEmpty        = "empty"
	ResolvedDataStatusBindingError = "binding_error"
	ResolvedDataStatusRequestError = "request_error"

	ComponentTypeTable  = "table"
	ComponentTypeAction = "action"
)

func (c ComponentSpec) IsTable() bool {
	return c.Type == ComponentTypeTable || c.Type == "data-table"
}

// View is the persisted representation of a published view.
type View struct {
	ID            string         `json:"id"`
	WorkspaceID   uint           `json:"workspace_id"`
	Name          string         `json:"name"`
	Description   string         `json:"description"`
	SourceDraftID string         `json:"source_draft_id,omitempty"`
	Definition    ViewDefinition `json:"definition"`
	CreatedAt     time.Time      `json:"created_at"`
	UpdatedAt     time.Time      `json:"updated_at"`
}

// ViewDefinition is the JSON-serializable workbook schema for a view.
type ViewDefinition struct {
	Name        string          `json:"name"`
	Description string          `json:"description"`
	Agents      []string        `json:"agents"`
	Sheets      []SheetSpec     `json:"sheets"`
	Actions     []ComponentSpec `json:"actions,omitempty"`
}

// SyncNameDescription keeps the top-level view metadata and definition metadata
// aligned, preferring explicit top-level values when present.
func (v *View) SyncNameDescription() {
	if v == nil {
		return
	}

	name := strings.TrimSpace(v.Name)
	if name == "" {
		name = strings.TrimSpace(v.Definition.Name)
	}

	description := strings.TrimSpace(v.Description)
	if description == "" {
		description = strings.TrimSpace(v.Definition.Description)
	}

	v.Name = name
	v.Description = description
	v.Definition.Name = name
	v.Definition.Description = description
}

type SheetSpec struct {
	ID          string          `json:"id"`
	Name        string          `json:"name"`
	Description string          `json:"description,omitempty"`
	Layout      LayoutConfig    `json:"layout"`
	Components  []ComponentSpec `json:"components"`
	Relations   []SheetRelation `json:"relations,omitempty"`
	Widgets     []WidgetSpec    `json:"widgets,omitempty"`
}

type WidgetSpec struct {
	ID          string         `json:"id"`
	Type        string         `json:"type"`
	Title       string         `json:"title"`
	Description string         `json:"description,omitempty"`
	Config      map[string]any `json:"config,omitempty"`
	Size        string         `json:"size,omitempty"`
	W           int            `json:"w,omitempty"`
	H           int            `json:"h,omitempty"`
}

type SheetRelation struct {
	ID         string `json:"id"`
	Name       string `json:"name,omitempty"`
	ToSheetID  string `json:"to_sheet_id"`
	FromColumn string `json:"from_column"`
	ToColumn   string `json:"to_column"`
}

type LayoutConfig struct {
	Columns int `json:"columns"`
}

type ComponentSpec struct {
	ID         string         `json:"id"`
	Type       string         `json:"type"`
	Title      string         `json:"title"`
	Section    string         `json:"section,omitempty"`
	Position   Position       `json:"position"`
	DataSource *DataSource    `json:"dataSource,omitempty"`
	Config     map[string]any `json:"config,omitempty"`
}

type Position struct {
	Col     int `json:"col"`
	Row     int `json:"row"`
	ColSpan int `json:"colSpan"`
	RowSpan int `json:"rowSpan"`
}

type DataSource struct {
	AgentID     string          `json:"agent_id,omitempty"`
	AgentIDs    []string        `json:"agent_ids,omitempty"`
	OutputType  string          `json:"output_type,omitempty"`
	ArtifactKey string          `json:"artifact_key,omitempty"`
	TimeRange   string          `json:"time_range,omitempty"`
	Statuses    []string        `json:"statuses,omitempty"`
	Transform   []TransformRule `json:"transform,omitempty"`
}

type TransformRule struct {
	Column  string `json:"column"`
	Source  string `json:"source"`
	Type    string `json:"type"`
	Extract string `json:"extract,omitempty"`
	Format  string `json:"format,omitempty"`
}

type StatusOption struct {
	Value string `json:"value"`
	Color string `json:"color"`
}

type ColumnMeta struct {
	Key     string         `json:"key"`
	Label   string         `json:"label,omitempty"`
	Type    string         `json:"type"`
	Format  string         `json:"format,omitempty"`
	Options []StatusOption `json:"options,omitempty"`
	Frozen  bool           `json:"frozen,omitempty"`
	Hidden  bool           `json:"hidden,omitempty"`
}

// ResolvedData is the response from the DataResolver for a single component.
type ResolvedData struct {
	Columns     []string       `json:"columns"`
	ColumnMeta  []ColumnMeta   `json:"column_meta,omitempty"`
	Rows        [][]any        `json:"rows"`
	Total       int            `json:"total"`
	CachedAt    *time.Time     `json:"cached_at,omitempty"`
	Status      string         `json:"status,omitempty"`
	Error       string         `json:"error,omitempty"`
	Diagnostics map[string]any `json:"diagnostics,omitempty"`
}

// WidgetData is the resolved data for a single widget tile.
type WidgetData struct {
	WidgetID string          `json:"widget_id"`
	Type     string          `json:"type"`
	Metric   *MetricData     `json:"metric,omitempty"`
	MapData  *MapWidgetData  `json:"map_data,omitempty"`
	ListData *ListWidgetData `json:"list_data,omitempty"`
	Status   string          `json:"status,omitempty"`
	Error    string          `json:"error,omitempty"`
	CachedAt *time.Time      `json:"cached_at,omitempty"`
}

type MetricData struct {
	Value      string `json:"value"`
	Label      string `json:"label"`
	Comparison string `json:"comparison,omitempty"`
}

type MapWidgetData struct {
	Markers []MapMarker `json:"markers"`
}

type MapMarker struct {
	Lat    float64 `json:"lat"`
	Lng    float64 `json:"lng"`
	Label  string  `json:"label"`
	Detail string  `json:"detail,omitempty"`
}

type ListWidgetData struct {
	Items []ListItem `json:"items"`
}

type ListItem struct {
	Label  string `json:"label"`
	Value  string `json:"value"`
	Detail string `json:"detail,omitempty"`
}

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
	Transform      []TransformRule          `json:"transform,omitempty"`
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

const (
	viewOutputSchemaColumnLimit = 12
	viewOutputSchemaHintLimit   = 6
)

type ViewOutputSchemaBackend interface {
	GetAgentProfile(ctx context.Context, workspaceID uint, agentID string) (*AgentProfile, error)
	ListViews(ctx context.Context, workspaceID uint) ([]*View, error)
}

func LoadViewOutputSchemaContexts(
	ctx context.Context,
	backend ViewOutputSchemaBackend,
	workspaceID uint,
	agentID string,
) ([]ViewOutputSchemaContext, error) {
	agentID = strings.TrimSpace(agentID)
	if backend == nil || agentID == "" {
		return nil, nil
	}
	views, err := backend.ListViews(ctx, workspaceID)
	if err != nil {
		return nil, err
	}
	matchRefs := viewOutputSchemaAgentMatchRefs(ctx, backend, workspaceID, agentID)
	if len(matchRefs) == 0 {
		return nil, nil
	}
	contexts := make([]ViewOutputSchemaContext, 0, len(views))
	for _, view := range views {
		if view == nil {
			continue
		}
		for _, sheet := range view.Definition.Sheets {
			for _, component := range sheet.Components {
				if !component.IsTable() || !viewOutputSchemaComponentMatchesAgent(view.Definition, component, matchRefs) {
					continue
				}
				if context := BuildViewOutputSchemaContext(view, sheet, component); context != nil {
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

func viewOutputSchemaAgentMatchRefs(
	ctx context.Context,
	backend ViewOutputSchemaBackend,
	workspaceID uint,
	agentID string,
) map[string]struct{} {
	refs := map[string]struct{}{}
	if normalized := normalizeViewOutputSchemaAgentRef(agentID); normalized != "" {
		refs[normalized] = struct{}{}
	}
	profile, err := backend.GetAgentProfile(ctx, workspaceID, agentID)
	if err != nil || profile == nil {
		return refs
	}
	for _, raw := range []string{profile.ID, profile.AgentKey, profile.Name} {
		if normalized := normalizeViewOutputSchemaAgentRef(raw); normalized != "" {
			refs[normalized] = struct{}{}
		}
	}
	return refs
}

func viewOutputSchemaComponentMatchesAgent(
	definition ViewDefinition,
	component ComponentSpec,
	matchRefs map[string]struct{},
) bool {
	for _, ref := range viewOutputSchemaComponentRefs(definition, component) {
		if _, ok := matchRefs[normalizeViewOutputSchemaAgentRef(ref)]; ok {
			return true
		}
	}
	return false
}

func viewOutputSchemaComponentRefs(definition ViewDefinition, component ComponentSpec) []string {
	refs := make([]string, 0, len(definition.Agents)+4)
	if component.DataSource != nil {
		refs = append(refs, component.DataSource.AgentIDs...)
		if strings.TrimSpace(component.DataSource.AgentID) != "" {
			refs = append(refs, component.DataSource.AgentID)
		}
	}
	if component.Config != nil {
		if ref := stringFromSchemaPayload(component.Config, "agent_id"); ref != "" {
			refs = append(refs, ref)
		}
		if raw, ok := component.Config["agent_ids"]; ok {
			refs = append(refs, stringSliceFromSchemaValue(raw)...)
		}
	}
	if len(refs) == 0 {
		refs = append(refs, definition.Agents...)
	}
	return uniqueTrimmedSchemaStrings(refs)
}

func BuildViewOutputSchemaContext(view *View, sheet SheetSpec, component ComponentSpec) *ViewOutputSchemaContext {
	columns := buildViewOutputSchemaColumns(component)
	if len(columns) == 0 {
		return nil
	}
	context := &ViewOutputSchemaContext{
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
		context.Transform = component.DataSource.Transform
		context.TransformHints = viewOutputSchemaTransformHints(component.DataSource.Transform)
	}
	if context.ComponentTitle == "" {
		context.ComponentTitle = context.SheetName
	}
	return context
}

func buildViewOutputSchemaColumns(component ComponentSpec) []ViewOutputSchemaColumn {
	configByKey := make(map[string]ColumnMeta)
	configOrder := make([]string, 0)
	for _, column := range viewOutputSchemaConfigColumns(component.Config) {
		key := canonicalViewOutputSchemaColumnKey(column.Key)
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

	columns := make([]ViewOutputSchemaColumn, 0, viewOutputSchemaColumnLimit)
	seen := make(map[string]struct{}, viewOutputSchemaColumnLimit)
	if component.DataSource != nil {
		for _, rule := range component.DataSource.Transform {
			key := canonicalViewOutputSchemaColumnKey(rule.Column)
			if key == "" {
				key = canonicalViewOutputSchemaColumnKey(viewOutputSchemaSourceColumnHint(rule.Source))
			}
			if key == "" {
				continue
			}
			if _, ok := seen[key]; ok {
				continue
			}
			cfg := configByKey[key]
			columns = append(columns, ViewOutputSchemaColumn{
				Key:   key,
				Label: firstNonEmptyTrimmedSchemaString(cfg.Label, humanizeViewOutputSchemaColumn(key)),
				Type: firstNonEmptyTrimmedSchemaString(
					rule.Type,
					cfg.Type,
					inferViewOutputSchemaColumnType(key),
				),
				Options: append([]StatusOption(nil), cfg.Options...),
			})
			seen[key] = struct{}{}
			if len(columns) >= viewOutputSchemaColumnLimit {
				return columns
			}
		}
	}
	for _, key := range configOrder {
		if _, ok := seen[key]; ok {
			continue
		}
		cfg := configByKey[key]
		columns = append(columns, ViewOutputSchemaColumn{
			Key:     key,
			Label:   firstNonEmptyTrimmedSchemaString(cfg.Label, humanizeViewOutputSchemaColumn(key)),
			Type:    firstNonEmptyTrimmedSchemaString(cfg.Type, inferViewOutputSchemaColumnType(key)),
			Options: append([]StatusOption(nil), cfg.Options...),
		})
		if len(columns) >= viewOutputSchemaColumnLimit {
			break
		}
	}
	return columns
}

func viewOutputSchemaConfigColumns(config map[string]any) []ColumnMeta {
	raw, ok := config["columns"]
	if !ok || raw == nil {
		return nil
	}
	body, err := json.Marshal(raw)
	if err != nil || len(body) == 0 {
		return nil
	}
	var columns []ColumnMeta
	if err := json.Unmarshal(body, &columns); err != nil {
		return nil
	}
	return columns
}

func viewOutputSchemaTransformHints(rules []TransformRule) []string {
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
		if len(hints) >= viewOutputSchemaHintLimit {
			break
		}
	}
	return hints
}

func canonicalViewOutputSchemaColumnKey(value string) string {
	normalized := normalizeViewOutputSchemaColumnKey(value)
	if normalized == "" {
		return ""
	}
	switch normalized {
	case "task_id", "detail_task_id", "row_id", "stable_ref", "sheet_id", "output_id", "output_status", "blocker_output_ids", "blocker_kind", "blocker_input_kind", "blocker_wait_group_id", "source_output_ids":
		return normalized + "_value"
	default:
		return normalized
	}
}

func normalizeViewOutputSchemaColumnKey(value string) string {
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

func normalizeViewOutputSchemaAgentRef(value string) string {
	return strings.TrimSpace(strings.ToLower(value))
}

func viewOutputSchemaSourceColumnHint(source string) string {
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

func inferViewOutputSchemaColumnType(key string) string {
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

func humanizeViewOutputSchemaColumn(key string) string {
	parts := strings.Split(strings.TrimSpace(key), "_")
	for i, part := range parts {
		if part == "" {
			continue
		}
		parts[i] = strings.ToUpper(part[:1]) + part[1:]
	}
	return strings.Join(parts, " ")
}

func stringSliceFromSchemaValue(value any) []string {
	body, err := json.Marshal(value)
	if err != nil || len(body) == 0 {
		return nil
	}
	var values []string
	if err := json.Unmarshal(body, &values); err != nil {
		return nil
	}
	return uniqueTrimmedSchemaStrings(values)
}

func stringFromSchemaPayload(payload map[string]any, key string) string {
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

func uniqueTrimmedSchemaStrings(values []string) []string {
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

func firstNonEmptyTrimmedSchemaString(values ...string) string {
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			return trimmed
		}
	}
	return ""
}
