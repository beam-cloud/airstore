package types

import (
	"strings"
	"time"
)

const (
	TaskOutputMetadataArtifactKey   = "artifact_key"
	TaskOutputMetadataArtifactLabel = "artifact_label"
	TaskOutputMetadataArtifactKind  = "artifact_kind"
	TaskOutputMetadataArtifactRole  = "artifact_role"

	TaskOutputArtifactRolePrimary    = "primary"
	TaskOutputArtifactRoleSupporting = "supporting"
	TaskOutputArtifactRoleIncidental = "incidental"

	ResolvedDataStatusOK           = "ok"
	ResolvedDataStatusEmpty        = "empty"
	ResolvedDataStatusBindingError = "binding_error"
	ResolvedDataStatusRequestError = "request_error"

	ComponentTypeTable  = "table"
	ComponentTypeAction = "action"

	RowStrategyModeTask  = "task"
	RowStrategyModeSplit = "split"
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
	Name        string      `json:"name"`
	Description string      `json:"description"`
	Agents      []string    `json:"agents"`
	Sheets      []SheetSpec `json:"sheets"`
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
	RowStrategy *RowStrategy    `json:"row_strategy,omitempty"`
	Transform   []TransformRule `json:"transform,omitempty"`
}

type RowStrategy struct {
	Mode        string `json:"mode"`
	Description string `json:"description,omitempty"`
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
