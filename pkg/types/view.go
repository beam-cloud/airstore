package types

import "time"

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

	ComponentTypeTable    = "table"
	ComponentTypeMetric   = "metric"
	ComponentTypeAction   = "action"
	ComponentTypeTaskList = "task_list"
)

func (c ComponentSpec) IsTable() bool {
	return c.Type == ComponentTypeTable || c.Type == "data-table"
}

// View is the persisted representation of a published view.
type View struct {
	ID          string         `json:"id"`
	WorkspaceID uint           `json:"workspace_id"`
	Name        string         `json:"name"`
	Description string         `json:"description"`
	Definition  ViewDefinition `json:"definition"`
	CreatedAt   time.Time      `json:"created_at"`
	UpdatedAt   time.Time      `json:"updated_at"`
}

// ViewDefinition is the JSON-serializable view layout and component configuration.
type ViewDefinition struct {
	Name        string          `json:"name"`
	Description string          `json:"description"`
	Agents      []string        `json:"agents"`
	Sections    []SectionSpec   `json:"sections,omitempty"`
	Layout      LayoutConfig    `json:"layout"`
	Components  []ComponentSpec `json:"components"`
}

// SectionSpec groups related components under a heading within a multi-workflow view.
type SectionSpec struct {
	ID    string `json:"id"`
	Title string `json:"title"`
	Row   int    `json:"row"`
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
