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
	Settings    *ViewSettings   `json:"settings,omitempty"`
}

// ViewSettings holds per-view configuration that controls agent behavior.
type ViewSettings struct {
	ApprovalPolicy string `json:"approval_policy,omitempty"`
}

// ApprovalPolicy encapsulates a per-view approval policy and provides
// deterministic methods for deciding whether a blocker should be skipped.
type ApprovalPolicy struct {
	Key string
}

func NewApprovalPolicy(key string) ApprovalPolicy {
	return ApprovalPolicy{Key: strings.TrimSpace(key)}
}

func (p ApprovalPolicy) IsSet() bool { return p.Key != "" }

// AllowsWrite returns true if the policy permits a write command on the given
// tool without requiring user approval.
func (p ApprovalPolicy) AllowsWrite(tool IntegrationName) bool {
	switch p.Key {
	case "auto_approve_all":
		return true
	case "approve_emails_only":
		return tool != Gmail
	default:
		return false
	}
}

func (p ApprovalPolicy) String() string { return p.Key }

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

// ---------------------------------------------------------------------------
// View output schema — used to tell agents about table schemas they should
// align their outputs with.
// ---------------------------------------------------------------------------

const (
	schemaColumnLimit = 12
	schemaHintLimit   = 6
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
	Transform      []TransformRule          `json:"transform,omitempty"`
	TransformHints []string                 `json:"transform_hints,omitempty"`
}

// ParseViewOutputSchemaContexts recovers typed schema contexts from a generic
// value (typically read back from JSON-serialized execution policy).
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

func (c ViewOutputSchemaContext) SortKey() string {
	return strings.TrimSpace(c.ViewID) + "\x00" +
		strings.TrimSpace(c.SheetID) + "\x00" +
		strings.TrimSpace(c.ComponentID)
}

func (c ViewOutputSchemaContext) MatchLabel() string {
	parts := make([]string, 0, 3)
	for _, v := range []string{c.ViewName, c.SheetName, c.ComponentTitle} {
		if t := strings.TrimSpace(v); t != "" {
			parts = append(parts, t)
		}
	}
	return strings.Join(parts, " / ")
}

func (c ViewOutputSchemaContext) ColumnKeys() []string {
	keys := make([]string, 0, len(c.Columns))
	for _, col := range c.Columns {
		if k := strings.TrimSpace(col.Key); k != "" {
			keys = append(keys, k)
		}
	}
	return keys
}

func (c ViewOutputSchemaContext) CompactColumns() []map[string]any {
	out := make([]map[string]any, 0, len(c.Columns))
	for _, col := range c.Columns {
		k := strings.TrimSpace(col.Key)
		if k == "" {
			continue
		}
		rec := map[string]any{"key": k}
		if l := strings.TrimSpace(col.Label); l != "" {
			rec["label"] = l
		}
		if t := strings.TrimSpace(col.Type); t != "" {
			rec["type"] = t
		}
		if len(col.Options) > 0 {
			rec["options"] = col.Options
		}
		out = append(out, rec)
	}
	return out
}

// ---------------------------------------------------------------------------
// Schema loading — resolves which views reference a given agent and builds
// the schema contexts for injection into the agent's runtime environment.
// ---------------------------------------------------------------------------

type ViewOutputSchemaBackend interface {
	GetAgentProfile(ctx context.Context, workspaceID uint, agentID string) (*AgentProfile, error)
	GetView(ctx context.Context, workspaceID uint, viewID string) (*View, error)
	ListViews(ctx context.Context, workspaceID uint) ([]*View, error)
}

func LoadViewOutputSchemaContexts(
	ctx context.Context,
	backend ViewOutputSchemaBackend,
	workspaceID uint,
	agentID string,
	sourceViewIDs ...string,
) ([]ViewOutputSchemaContext, error) {
	agentID = strings.TrimSpace(agentID)
	if backend == nil || agentID == "" {
		return nil, nil
	}

	// If a source view is specified, only load schemas from that view
	// to enforce per-view data isolation.
	var viewList []*View
	if len(sourceViewIDs) > 0 {
		svid := strings.TrimSpace(sourceViewIDs[0])
		if svid != "" {
			v, err := backend.GetView(ctx, workspaceID, svid)
			if err == nil && v != nil {
				viewList = []*View{v}
			}
		}
	}
	if len(viewList) == 0 {
		var err error
		viewList, err = backend.ListViews(ctx, workspaceID)
		if err != nil {
			return nil, err
		}
	}

	matchRefs := agentMatchRefs(ctx, backend, workspaceID, agentID)
	if len(matchRefs) == 0 {
		return nil, nil
	}
	var contexts []ViewOutputSchemaContext
	for _, view := range viewList {
		if view == nil {
			continue
		}
		for _, sheet := range view.Definition.Sheets {
			for _, comp := range sheet.Components {
				if !comp.IsTable() || !view.Definition.ComponentMatchesAgent(comp, matchRefs) {
					continue
				}
				if sc := BuildViewOutputSchemaContext(view, sheet, comp); sc != nil {
					contexts = append(contexts, *sc)
				}
			}
		}
	}
	sort.SliceStable(contexts, func(i, j int) bool {
		return contexts[i].SortKey() < contexts[j].SortKey()
	})
	return contexts, nil
}

// ComponentMatchesAgent returns true if the component references any of the
// given agent match refs (case-insensitive).
func (d ViewDefinition) ComponentMatchesAgent(comp ComponentSpec, refs map[string]struct{}) bool {
	for _, r := range d.componentAgentRefs(comp) {
		if _, ok := refs[strings.TrimSpace(strings.ToLower(r))]; ok {
			return true
		}
	}
	return false
}

func (d ViewDefinition) componentAgentRefs(comp ComponentSpec) []string {
	var refs []string
	if comp.DataSource != nil {
		refs = append(refs, comp.DataSource.AgentIDs...)
		if id := strings.TrimSpace(comp.DataSource.AgentID); id != "" {
			refs = append(refs, id)
		}
	}
	if comp.Config != nil {
		if id, _ := comp.Config["agent_id"].(string); strings.TrimSpace(id) != "" {
			refs = append(refs, id)
		}
		if raw, ok := comp.Config["agent_ids"]; ok {
			if body, err := json.Marshal(raw); err == nil {
				var ids []string
				if json.Unmarshal(body, &ids) == nil {
					refs = append(refs, ids...)
				}
			}
		}
	}
	if len(refs) == 0 {
		refs = append(refs, d.Agents...)
	}
	return dedup(refs)
}

func agentMatchRefs(ctx context.Context, backend ViewOutputSchemaBackend, workspaceID uint, agentID string) map[string]struct{} {
	refs := map[string]struct{}{}
	if n := strings.TrimSpace(strings.ToLower(agentID)); n != "" {
		refs[n] = struct{}{}
	}
	profile, err := backend.GetAgentProfile(ctx, workspaceID, agentID)
	if err != nil || profile == nil {
		return refs
	}
	for _, raw := range []string{profile.ID, profile.AgentKey, profile.Name} {
		if n := strings.TrimSpace(strings.ToLower(raw)); n != "" {
			refs[n] = struct{}{}
		}
	}
	return refs
}

// BuildViewOutputSchemaContext builds a schema context for a single table
// component. Returns nil if the component has no usable columns.
func BuildViewOutputSchemaContext(view *View, sheet SheetSpec, comp ComponentSpec) *ViewOutputSchemaContext {
	columns := comp.SchemaColumns()
	if len(columns) == 0 {
		return nil
	}
	sc := &ViewOutputSchemaContext{
		ViewID:         strings.TrimSpace(view.ID),
		ViewName:       strings.TrimSpace(view.Name),
		SheetID:        strings.TrimSpace(sheet.ID),
		SheetName:      strings.TrimSpace(sheet.Name),
		ComponentID:    strings.TrimSpace(comp.ID),
		ComponentTitle: strings.TrimSpace(comp.Title),
		Columns:        columns,
	}
	if comp.DataSource != nil {
		sc.ArtifactKey = strings.TrimSpace(comp.DataSource.ArtifactKey)
		sc.OutputType = strings.TrimSpace(comp.DataSource.OutputType)
		sc.Transform = comp.DataSource.Transform
		sc.TransformHints = transformHints(comp.DataSource.Transform)
	}
	if sc.ComponentTitle == "" {
		sc.ComponentTitle = sc.SheetName
	}
	return sc
}

// SchemaColumns derives the output schema columns from the component's
// data source transforms and config column metadata.
func (c ComponentSpec) SchemaColumns() []ViewOutputSchemaColumn {
	configByKey := make(map[string]ColumnMeta)
	var configOrder []string
	for _, col := range configColumns(c.Config) {
		key := canonicalColumnKey(col.Key)
		if key == "" {
			continue
		}
		col.Key = key
		if existing, ok := configByKey[key]; ok {
			if strings.TrimSpace(existing.Label) == "" && strings.TrimSpace(col.Label) != "" {
				existing.Label = col.Label
			}
			if strings.TrimSpace(existing.Type) == "" && strings.TrimSpace(col.Type) != "" {
				existing.Type = col.Type
			}
			if len(existing.Options) == 0 && len(col.Options) > 0 {
				existing.Options = col.Options
			}
			configByKey[key] = existing
			continue
		}
		configByKey[key] = col
		configOrder = append(configOrder, key)
	}

	columns := make([]ViewOutputSchemaColumn, 0, schemaColumnLimit)
	seen := make(map[string]struct{}, schemaColumnLimit)

	addColumn := func(key string, ruleType string) {
		if _, ok := seen[key]; ok || key == "" || len(columns) >= schemaColumnLimit {
			return
		}
		cfg := configByKey[key]
		columns = append(columns, ViewOutputSchemaColumn{
			Key:     key,
			Label:   coalesce(cfg.Label, humanizeColumn(key)),
			Type:    coalesce(ruleType, cfg.Type, inferColumnType(key)),
			Options: append([]StatusOption(nil), cfg.Options...),
		})
		seen[key] = struct{}{}
	}

	if c.DataSource != nil {
		for _, rule := range c.DataSource.Transform {
			key := canonicalColumnKey(rule.Column)
			if key == "" {
				key = canonicalColumnKey(sourceColumnHint(rule.Source))
			}
			addColumn(key, rule.Type)
		}
	}
	for _, key := range configOrder {
		addColumn(key, "")
	}
	return columns
}

// ---------------------------------------------------------------------------
// Private helpers — short names, scoped to this file.
// ---------------------------------------------------------------------------

func configColumns(config map[string]any) []ColumnMeta {
	raw, ok := config["columns"]
	if !ok || raw == nil {
		return nil
	}
	body, err := json.Marshal(raw)
	if err != nil || len(body) == 0 {
		return nil
	}
	var cols []ColumnMeta
	if json.Unmarshal(body, &cols) != nil {
		return nil
	}
	return cols
}

func transformHints(rules []TransformRule) []string {
	var hints []string
	seen := make(map[string]struct{}, len(rules))
	for _, rule := range rules {
		h := strings.TrimSpace(rule.Source)
		if h == "" {
			continue
		}
		if _, ok := seen[h]; ok {
			continue
		}
		seen[h] = struct{}{}
		hints = append(hints, h)
		if len(hints) >= schemaHintLimit {
			break
		}
	}
	return hints
}

var reservedColumnKeys = map[string]struct{}{
	"task_id": {}, "detail_task_id": {}, "row_id": {}, "stable_ref": {},
	"sheet_id": {}, "output_id": {}, "output_status": {},
	"blocker_output_ids": {}, "blocker_kind": {}, "blocker_input_kind": {},
	"blocker_wait_group_id": {}, "source_output_ids": {},
}

func canonicalColumnKey(value string) string {
	n := normalizeColumnKey(value)
	if n == "" {
		return ""
	}
	if _, reserved := reservedColumnKeys[n]; reserved {
		return n + "_value"
	}
	return n
}

func normalizeColumnKey(value string) string {
	value = strings.TrimSpace(strings.ToLower(value))
	if value == "" {
		return ""
	}
	var b strings.Builder
	lastUnderscore := false
	for _, r := range value {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			b.WriteRune(r)
			lastUnderscore = false
		} else if !lastUnderscore {
			b.WriteByte('_')
			lastUnderscore = true
		}
	}
	return strings.Trim(b.String(), "_")
}

func sourceColumnHint(source string) string {
	source = strings.TrimSpace(strings.Split(source, "|")[0])
	source = strings.TrimPrefix(strings.TrimPrefix(source, "data."), "metadata.")
	parts := strings.FieldsFunc(source, func(r rune) bool {
		return r == '.' || r == '[' || r == ']' || r == ' '
	})
	for i := len(parts) - 1; i >= 0; i-- {
		if p := strings.TrimSpace(parts[i]); p != "" {
			return p
		}
	}
	return ""
}

func inferColumnType(key string) string {
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
	for i, p := range parts {
		if p != "" {
			parts[i] = strings.ToUpper(p[:1]) + p[1:]
		}
	}
	return strings.Join(parts, " ")
}

// coalesce returns the first non-empty trimmed string.
func coalesce(values ...string) string {
	for _, v := range values {
		if t := strings.TrimSpace(v); t != "" {
			return t
		}
	}
	return ""
}

// dedup returns unique non-empty trimmed strings preserving order.
func dedup(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, v := range values {
		if t := strings.TrimSpace(v); t != "" {
			if _, ok := seen[t]; !ok {
				seen[t] = struct{}{}
				out = append(out, t)
			}
		}
	}
	return out
}
