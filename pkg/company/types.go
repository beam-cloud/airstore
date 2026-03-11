package company

import "time"

// ---------------------------------------------------------------------------
// Company Snapshot — holistic view of the workspace, injected into every BAML call
// ---------------------------------------------------------------------------

type CompanySnapshot struct {
	Agents         []AgentSummary      `json:"agents"`
	RunningTasks   []TaskSummary       `json:"running_tasks"`
	ScheduledTasks []ScheduleSummary   `json:"scheduled_tasks"`
	RecentResults  []TaskResultSummary `json:"recent_results"`
	Sources        []SourceSummary     `json:"sources"`
	Channels       []ChannelSummary    `json:"channels"`
	CostSummary    CostSummary         `json:"cost_summary"`
}

type ZoneKind string

const (
	ZoneKindCommandCenter  ZoneKind = "command_center"
	ZoneKindActiveOps      ZoneKind = "active_operations"
	ZoneKindSchedulingHall ZoneKind = "scheduling_hall"
	ZoneKindSourceDistrict ZoneKind = "source_district"
	ZoneKindResultsArchive ZoneKind = "results_archive"
	ZoneKindAttentionTower ZoneKind = "attention_tower"
)

type EntityKind string

const (
	EntityKindAgent    EntityKind = "agent"
	EntityKindSource   EntityKind = "source"
	EntityKindResult   EntityKind = "result"
	EntityKindSchedule EntityKind = "schedule"
)

type EntityAnimationState string

const (
	EntityAnimationStateIdle      EntityAnimationState = "idle"
	EntityAnimationStateWalk      EntityAnimationState = "walk"
	EntityAnimationStateWorking   EntityAnimationState = "working"
	EntityAnimationStateCasting   EntityAnimationState = "casting"
	EntityAnimationStateWaiting   EntityAnimationState = "waiting"
	EntityAnimationStateSleeping  EntityAnimationState = "sleeping"
	EntityAnimationStateError     EntityAnimationState = "error"
	EntityAnimationStateCelebrate EntityAnimationState = "celebrate"
)

type WorldVec2 struct {
	X float64 `json:"x"`
	Y float64 `json:"y"`
}

type WorldCameraPreset struct {
	Mode   string    `json:"mode"`
	Center WorldVec2 `json:"center"`
	Zoom   float64   `json:"zoom"`
}

type ZoneSummary struct {
	ID          string   `json:"id"`
	Kind        ZoneKind `json:"kind"`
	Name        string   `json:"name"`
	Subtitle    string   `json:"subtitle,omitempty"`
	Accent      string   `json:"accent"`
	GridX       float64  `json:"grid_x"`
	GridY       float64  `json:"grid_y"`
	Width       int      `json:"width"`
	Height      int      `json:"height"`
	EntityCount int      `json:"entity_count"`
	ActiveCount int      `json:"active_count"`
	TaskCount   int      `json:"task_count"`
}

type EntitySummary struct {
	ID         string               `json:"id"`
	Kind       EntityKind           `json:"kind"`
	ZoneID     string               `json:"zone_id"`
	Name       string               `json:"name"`
	Subtitle   string               `json:"subtitle,omitempty"`
	Accent     string               `json:"accent"`
	State      string               `json:"state"`
	Animation  EntityAnimationState `json:"animation"`
	GridX      float64              `json:"grid_x"`
	GridY      float64              `json:"grid_y"`
	Facing     string               `json:"facing,omitempty"`
	TaskCount  int                  `json:"task_count"`
	CostUSD    float64              `json:"cost_usd"`
	Skills     []string             `json:"skills,omitempty"`
	StatusText string               `json:"status_text,omitempty"`
	Health     float64              `json:"health"`
	Mana       float64              `json:"mana"`
	CastLabel  string               `json:"cast_label,omitempty"`
	Level      int                  `json:"level"`
	Badges     []string             `json:"badges,omitempty"`
	Model      string               `json:"model,omitempty"`
}

type TaskBeaconSummary struct {
	ID          string  `json:"id"`
	ZoneID      string  `json:"zone_id"`
	AgentID     string  `json:"agent_id,omitempty"`
	Label       string  `json:"label"`
	State       string  `json:"state"`
	Priority    string  `json:"priority"`
	GridX       float64 `json:"grid_x"`
	GridY       float64 `json:"grid_y"`
	Progress    float64 `json:"progress"`
	DurationSec int     `json:"duration_sec,omitempty"`
	CreatedAt   int64   `json:"created_at"`
}

type ActivityFeedEvent struct {
	ID        string `json:"id"`
	Kind      string `json:"kind"`
	Channel   string `json:"channel"`
	Message   string `json:"message"`
	EntityID  string `json:"entity_id,omitempty"`
	Severity  string `json:"severity,omitempty"`
	Timestamp int64  `json:"timestamp"`
}

type WorldHudSummary struct {
	AgentCount      int     `json:"agent_count"`
	ActiveTaskCount int     `json:"active_task_count"`
	ScheduleCount   int     `json:"schedule_count"`
	SourceCount     int     `json:"source_count"`
	TotalSpend      float64 `json:"total_spend"`
	Connected       bool    `json:"connected"`
	RuntimeVersion  int64   `json:"runtime_version"`
	Tick            int64   `json:"tick"`
}

type CompanyWorldSnapshot struct {
	WorkspaceID string              `json:"workspace_id"`
	Version     int64               `json:"version"`
	GeneratedAt int64               `json:"generated_at"`
	Camera      WorldCameraPreset   `json:"camera"`
	Zones       []ZoneSummary       `json:"zones"`
	Entities    []EntitySummary     `json:"entities"`
	TaskBeacons []TaskBeaconSummary `json:"task_beacons"`
	Activity    []ActivityFeedEvent `json:"activity"`
	Hud         WorldHudSummary     `json:"hud"`
}

type CompanyWorldDelta struct {
	Sequence         int64               `json:"sequence"`
	GeneratedAt      int64               `json:"generated_at"`
	UpdatedZones     []ZoneSummary       `json:"updated_zones,omitempty"`
	UpdatedEntities  []EntitySummary     `json:"updated_entities,omitempty"`
	RemovedEntityIDs []string            `json:"removed_entity_ids,omitempty"`
	TaskBeacons      []TaskBeaconSummary `json:"task_beacons,omitempty"`
	Activity         []ActivityFeedEvent `json:"activity,omitempty"`
	Hud              *WorldHudSummary    `json:"hud,omitempty"`
	Camera           *WorldCameraPreset  `json:"camera,omitempty"`
}

type AgentDerivedState string

const (
	AgentDerivedStateIdle     AgentDerivedState = "idle"
	AgentDerivedStateWorking  AgentDerivedState = "working"
	AgentDerivedStateWaiting  AgentDerivedState = "waiting"
	AgentDerivedStateSleeping AgentDerivedState = "sleeping"
	AgentDerivedStateError    AgentDerivedState = "error"
)

type AgentSummary struct {
	ID              string            `json:"id"`
	Key             string            `json:"key"`
	Name            string            `json:"name"`
	Role            string            `json:"role"`
	Active          bool              `json:"active"`
	State           AgentDerivedState `json:"state"`
	ActiveTaskCount int               `json:"active_task_count"`
	TotalCostUSD    float64           `json:"total_cost_usd"`
	Model           string            `json:"model,omitempty"`
	Skills          []string          `json:"skills,omitempty"`
	SystemPrompt    string            `json:"system_prompt,omitempty"`
}

type TaskSummary struct {
	ID            string  `json:"id"`
	AgentID       string  `json:"agent_id,omitempty"`
	AgentName     string  `json:"agent_name,omitempty"`
	State         string  `json:"state"`
	PromptSummary string  `json:"prompt_summary"`
	Priority      string  `json:"priority"`
	CostUSD       float64 `json:"cost_usd"`
	CreatedAt     int64   `json:"created_at"`
	DurationSec   int     `json:"duration_sec,omitempty"`
}

type ScheduleSummary struct {
	ID        string `json:"id"`
	AgentID   string `json:"agent_id"`
	AgentName string `json:"agent_name,omitempty"`
	CronExpr  string `json:"cron_expr"`
	Timezone  string `json:"timezone"`
	Prompt    string `json:"prompt"`
	Active    bool   `json:"active"`
	NextRunAt int64  `json:"next_run_at"`
}

type TaskResultSummary struct {
	ID        string  `json:"id"`
	AgentID   string  `json:"agent_id,omitempty"`
	AgentName string  `json:"agent_name,omitempty"`
	State     string  `json:"state"`
	Prompt    string  `json:"prompt"`
	CostUSD   float64 `json:"cost_usd"`
	EndedAt   int64   `json:"ended_at"`
}

type SourceSummary struct {
	IntegrationType string `json:"integration_type"`
	Status          string `json:"status"`
}

type ChannelSummary struct {
	AgentID     string `json:"agent_id,omitempty"`
	AgentName   string `json:"agent_name,omitempty"`
	ChannelType string `json:"channel_type"`
	Address     string `json:"address"`
}

type CostSummary struct {
	TotalUSD    float64            `json:"total_usd"`
	PerAgentUSD map[string]float64 `json:"per_agent_usd"`
}

// ---------------------------------------------------------------------------
// Copilot Session — S2 persisted conversation with action history
// ---------------------------------------------------------------------------

type SessionStatus string

const (
	SessionStatusActive   SessionStatus = "active"
	SessionStatusArchived SessionStatus = "archived"
)

type CopilotSession struct {
	ID          string           `json:"id"`
	WorkspaceID string           `json:"workspace_id"`
	Status      SessionStatus    `json:"status"`
	Name        string           `json:"name,omitempty"`
	Messages    []CopilotMessage `json:"messages"`
	Actions     []ActionResult   `json:"actions"`
	CreatedAt   int64            `json:"created_at"`
	UpdatedAt   int64            `json:"updated_at"`
}

type CopilotMessage struct {
	Role      string `json:"role"` // "user" or "assistant"
	Content   string `json:"content"`
	Timestamp int64  `json:"ts"`
}

// ---------------------------------------------------------------------------
// Company Actions — high-level operations the copilot can execute
// ---------------------------------------------------------------------------

type ActionType string

const (
	ActionTypeProvisionAgent ActionType = "provision_agent"
	ActionTypeModifyAgent    ActionType = "modify_agent"
	ActionTypeCreateTask     ActionType = "create_task"
	ActionTypeLaunchCampaign ActionType = "launch_campaign"
	ActionTypeCreateSchedule ActionType = "create_schedule"
	ActionTypeConfigSource   ActionType = "configure_source"
)

type CompanyAction struct {
	Type        ActionType   `json:"type"`
	Description string       `json:"description"`
	Params      ActionParams `json:"params"`
}

type ActionParams struct {
	// provision_agent / modify_agent
	AgentID      string   `json:"agent_id,omitempty"`
	AgentKey     string   `json:"agent_key,omitempty"`
	AgentName    string   `json:"agent_name,omitempty"`
	AgentRole    string   `json:"agent_role,omitempty"`
	SystemPrompt string   `json:"system_prompt,omitempty"`
	Model        string   `json:"model,omitempty"`
	Skills       []string `json:"skills,omitempty"`
	Active       *bool    `json:"active,omitempty"`

	// create_task
	Message  string `json:"message,omitempty"`
	Priority string `json:"priority,omitempty"`

	// launch_campaign
	PromptTemplate string   `json:"prompt_template,omitempty"`
	TargetAgentIDs []string `json:"target_agent_ids,omitempty"`
	Count          int      `json:"count,omitempty"`

	// create_schedule
	CronExpr   string   `json:"cron_expr,omitempty"`
	Timezone   string   `json:"timezone,omitempty"`
	SkillPaths []string `json:"skill_paths,omitempty"`

	// configure_source
	IntegrationType string `json:"integration_type,omitempty"`
}

type ActionStatus string

const (
	ActionStatusPending ActionStatus = "pending"
	ActionStatusSuccess ActionStatus = "success"
	ActionStatusError   ActionStatus = "error"
)

type ActionResult struct {
	Action      CompanyAction `json:"action"`
	Status      ActionStatus  `json:"status"`
	ResourceIDs []string      `json:"resource_ids,omitempty"`
	Error       string        `json:"error,omitempty"`
	Timestamp   int64         `json:"ts"`
}

// ---------------------------------------------------------------------------
// Company Changeset — what the BAML planner proposes
// ---------------------------------------------------------------------------

type CompanyChangeset struct {
	Explanation string          `json:"explanation"`
	Actions     []CompanyAction `json:"actions"`
	Warnings    []string        `json:"warnings,omitempty"`
}

// ---------------------------------------------------------------------------
// Intent Classification — first BAML call per user message
// ---------------------------------------------------------------------------

type IntentType string

const (
	IntentTypeCreateAgents    IntentType = "CREATE_AGENTS"
	IntentTypeModifyAgent     IntentType = "MODIFY_AGENT"
	IntentTypeCreateTask      IntentType = "CREATE_TASK"
	IntentTypeLaunchCampaign  IntentType = "LAUNCH_CAMPAIGN"
	IntentTypeConfigureSource IntentType = "CONFIGURE_SOURCE"
	IntentTypeCreateSchedule  IntentType = "CREATE_SCHEDULE"
	IntentTypeQueryStatus     IntentType = "QUERY_STATUS"
	IntentTypeGeneralFeedback IntentType = "GENERAL_FEEDBACK"
)

type CompanyIntent struct {
	Intent  IntentType `json:"intent"`
	Summary string     `json:"summary"`
}

func (i IntentType) RequiresAction() bool {
	switch i {
	case IntentTypeQueryStatus, IntentTypeGeneralFeedback:
		return false
	default:
		return true
	}
}

// ---------------------------------------------------------------------------
// SSE Stream Events — for the 3D company view
// ---------------------------------------------------------------------------

type CompanyStreamEventType string

const (
	CompanyStreamEventSnapshot  CompanyStreamEventType = "snapshot"
	CompanyStreamEventUpdate    CompanyStreamEventType = "update"
	CompanyStreamEventHeartbeat CompanyStreamEventType = "heartbeat"
)

type CompanyStreamEvent struct {
	Event     CompanyStreamEventType `json:"event"`
	Snapshot  *CompanySnapshot       `json:"snapshot,omitempty"`
	Pulse     *ActivityPulse         `json:"pulse,omitempty"`
	Timestamp int64                  `json:"ts"`
}

type CompanyWorldStreamEventType string

const (
	CompanyWorldStreamEventSnapshot  CompanyWorldStreamEventType = "world_snapshot"
	CompanyWorldStreamEventDelta     CompanyWorldStreamEventType = "world_delta"
	CompanyWorldStreamEventHeartbeat CompanyWorldStreamEventType = "heartbeat"
)

type CompanyWorldStreamEvent struct {
	Event         CompanyWorldStreamEventType `json:"event"`
	WorldSnapshot *CompanyWorldSnapshot       `json:"world_snapshot,omitempty"`
	WorldDelta    *CompanyWorldDelta          `json:"world_delta,omitempty"`
	Timestamp     int64                       `json:"ts"`
}

type ActivityPulse struct {
	ActiveAgents int               `json:"active_agents"`
	TotalTasks   int               `json:"total_tasks"`
	AgentStates  map[string]string `json:"agent_states"`
	ServerTimeMs int64             `json:"server_time_ms"`
}

// ---------------------------------------------------------------------------
// S2 Stream Persistence
// ---------------------------------------------------------------------------

type sessionStreamEntry struct {
	Type        string        `json:"type"` // "meta", "message", "action", "snapshot"
	WorkspaceID string        `json:"workspace_id,omitempty"`
	Name        string        `json:"name,omitempty"`
	Role        string        `json:"role,omitempty"`
	Content     string        `json:"content,omitempty"`
	Action      *ActionResult `json:"action,omitempty"`
	Timestamp   int64         `json:"ts"`
}

// ---------------------------------------------------------------------------
// Session Index
// ---------------------------------------------------------------------------

type SessionIndexEntry struct {
	Type      string `json:"type"` // "created" or "archived"
	SessionID string `json:"session_id"`
	Name      string `json:"name,omitempty"`
	Timestamp int64  `json:"ts"`
}

type SessionSummary struct {
	ID        string `json:"id"`
	Name      string `json:"name,omitempty"`
	Status    string `json:"status"`
	CreatedAt int64  `json:"created_at"`
	UpdatedAt int64  `json:"updated_at"`
}

// ---------------------------------------------------------------------------
// Copilot SSE events (chat endpoint)
// ---------------------------------------------------------------------------

type CopilotSSEEvent struct {
	Event     string            `json:"event"` // thinking, plan, action, chunk, done, error
	Message   string            `json:"message,omitempty"`
	Changeset *CompanyChangeset `json:"changeset,omitempty"`
	Action    *ActionResult     `json:"action,omitempty"`
	Snapshot  *CompanySnapshot  `json:"snapshot,omitempty"`
	Error     string            `json:"error,omitempty"`
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func nowMs() int64 {
	return time.Now().UnixMilli()
}
