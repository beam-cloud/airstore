package game

import "github.com/beam-cloud/airstore/pkg/company"

type CommandType string

const (
	CommandTypeSyncBusinessSnapshot CommandType = "sync_business_snapshot"
	CommandTypeRecordActivity       CommandType = "record_activity"
	CommandTypeRecordActionBatch    CommandType = "record_action_batch"
)

type ActionEnvelope struct {
	Type        string         `json:"type"`
	Description string         `json:"description"`
	Params      map[string]any `json:"params,omitempty"`
}

type ActionResultEnvelope struct {
	Type        string   `json:"type"`
	Description string   `json:"description"`
	Status      string   `json:"status"`
	ResourceIDs []string `json:"resource_ids,omitempty"`
	Error       string   `json:"error,omitempty"`
}

type Command struct {
	WorkspaceID string                   `json:"workspace_id"`
	Type        CommandType              `json:"type"`
	Snapshot    *company.CompanySnapshot `json:"snapshot,omitempty"`
	Actions     []ActionEnvelope         `json:"actions,omitempty"`
	Results     []ActionResultEnvelope   `json:"results,omitempty"`
	Message     string                   `json:"message,omitempty"`
	Channel     string                   `json:"channel,omitempty"`
	EntityID    string                   `json:"entity_id,omitempty"`
	Timestamp   int64                    `json:"timestamp"`
}

type EventType string

const (
	EventTypeWorldSynced  EventType = "world_synced"
	EventTypeActivity     EventType = "activity"
	EventTypeActionBatch  EventType = "action_batch"
	EventTypeAgentChanged EventType = "agent_changed"
	EventTypeTaskChanged  EventType = "task_changed"
)

type Event struct {
	ID          string         `json:"id"`
	WorkspaceID string         `json:"workspace_id"`
	Type        EventType      `json:"type"`
	Message     string         `json:"message,omitempty"`
	Channel     string         `json:"channel,omitempty"`
	EntityID    string         `json:"entity_id,omitempty"`
	Timestamp   int64          `json:"timestamp"`
	Metadata    map[string]any `json:"metadata,omitempty"`
}

type WorldState struct {
	WorkspaceID string
	Version     int64
	Sequence    int64
	Business    *company.CompanySnapshot
	Snapshot    *company.CompanyWorldSnapshot
	Activity    []company.ActivityFeedEvent
}
