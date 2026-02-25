package channels

import (
	"context"
	"fmt"
	"strings"

	"github.com/beam-cloud/airstore/pkg/orchestration"
	"github.com/beam-cloud/airstore/pkg/types"
	"github.com/google/uuid"
)

type ChannelType string

const (
	ChannelTypeDirect ChannelType = "direct"
)

type Message struct {
	Message           string                            `json:"message"`
	IdempotencyKey    string                            `json:"idempotency_key,omitempty"`
	SessionID         string                            `json:"session_id,omitempty"`
	SessionKey        *string                           `json:"session_key,omitempty"`
	Deliver           *bool                             `json:"deliver,omitempty"`
	TimeoutMs         *int                              `json:"timeout_ms,omitempty"`
	Policy            *orchestration.RunExecutionPolicy `json:"policy,omitempty"`
	Lane              *string                           `json:"lane,omitempty"`
	ExtraSystemPrompt *string                           `json:"extra_system_prompt,omitempty"`
	InputProvenance   *orchestration.InputProvenance    `json:"input_provenance,omitempty"`
	Routing           *orchestration.RoutingContext     `json:"routing,omitempty"`
	Attachments       []map[string]any                  `json:"attachments,omitempty"`
	Label             *string                           `json:"label,omitempty"`
	SpawnedBy         *string                           `json:"spawned_by,omitempty"`
	QueueMode         types.AgentQueueMode              `json:"queue_mode,omitempty"`
}

type SendResult struct {
	Accepted      bool             `json:"accepted"`
	IdempotentHit bool             `json:"idempotent_hit"`
	Task          *types.AgentTask `json:"task,omitempty"`
	RunID         *string          `json:"run_id,omitempty"`
}

type Channel interface {
	Type() ChannelType
	SendToAgent(ctx context.Context, workspaceID uint, agentID string, message Message) (*SendResult, error)
	SendToRun(ctx context.Context, workspaceID uint, runID string, message Message) (*SendResult, error)
}

type Registry struct {
	channels map[ChannelType]Channel
}

type ErrChannelTypeNotRegistered struct {
	ChannelType ChannelType
}

func (e *ErrChannelTypeNotRegistered) Error() string {
	return fmt.Sprintf("channel type %q is not registered", e.ChannelType)
}

func NewRegistry(channelsList ...Channel) *Registry {
	r := &Registry{channels: map[ChannelType]Channel{}}
	for _, channel := range channelsList {
		if channel == nil {
			continue
		}
		r.channels[channel.Type()] = channel
	}
	return r
}

func (r *Registry) Register(channel Channel) error {
	if r == nil {
		return fmt.Errorf("channel registry is nil")
	}
	if channel == nil {
		return fmt.Errorf("channel is required")
	}
	if r.channels == nil {
		r.channels = map[ChannelType]Channel{}
	}
	r.channels[channel.Type()] = channel
	return nil
}

func (r *Registry) Resolve(channelType ChannelType) (Channel, error) {
	if r == nil {
		return nil, fmt.Errorf("channel registry is nil")
	}
	channel, ok := r.channels[channelType]
	if !ok || channel == nil {
		return nil, &ErrChannelTypeNotRegistered{ChannelType: channelType}
	}
	return channel, nil
}

type Direct struct {
	agents *orchestration.AgentAPI
}

func NewDirect(agents *orchestration.AgentAPI) *Direct {
	return &Direct{agents: agents}
}

func (d *Direct) Type() ChannelType {
	return ChannelTypeDirect
}

func (d *Direct) SendToAgent(ctx context.Context, workspaceID uint, agentID string, message Message) (*SendResult, error) {
	if d == nil || d.agents == nil {
		return nil, fmt.Errorf("direct channel is unavailable")
	}

	trimmedAgentID := strings.TrimSpace(agentID)
	trimmedMessage := strings.TrimSpace(message.Message)
	if trimmedAgentID == "" {
		return nil, fmt.Errorf("agent_id is required")
	}
	if trimmedMessage == "" {
		return nil, fmt.Errorf("message is required")
	}

	sessionID := strings.TrimSpace(message.SessionID)
	if sessionID == "" {
		sessionID = uuid.NewString()
	}

	idempotencyKey := strings.TrimSpace(message.IdempotencyKey)
	if idempotencyKey == "" {
		idempotencyKey = uuid.NewString()
	}

	routing := orchestration.RoutingContext{}
	if message.Routing != nil {
		routing = *message.Routing
	}
	if routing.Channel == nil || strings.TrimSpace(*routing.Channel) == "" {
		direct := string(ChannelTypeDirect)
		routing.Channel = &direct
	}

	params := orchestration.AgentCommandParams{
		Message:           trimmedMessage,
		AgentID:           &trimmedAgentID,
		SessionID:         sessionID,
		SessionKey:        message.SessionKey,
		Deliver:           message.Deliver,
		TimeoutMs:         message.TimeoutMs,
		Policy:            message.Policy,
		Lane:              message.Lane,
		ExtraSystemPrompt: message.ExtraSystemPrompt,
		InputProvenance:   message.InputProvenance,
		Routing:           routing,
		Attachments:       message.Attachments,
		IdempotencyKey:    idempotencyKey,
		Label:             message.Label,
		SpawnedBy:         message.SpawnedBy,
	}
	task, deduped, err := d.agents.AcceptAgentCommand(ctx, workspaceID, params)
	if err != nil {
		return nil, err
	}
	return &SendResult{
		Accepted:      true,
		IdempotentHit: deduped,
		Task:          task,
		RunID:         task.TargetRunID,
	}, nil
}

func (d *Direct) SendToRun(ctx context.Context, workspaceID uint, runID string, message Message) (*SendResult, error) {
	if d == nil || d.agents == nil {
		return nil, fmt.Errorf("direct channel is unavailable")
	}

	trimmedRunID := strings.TrimSpace(runID)
	trimmedMessage := strings.TrimSpace(message.Message)
	if trimmedRunID == "" {
		return nil, fmt.Errorf("run_id is required")
	}
	if trimmedMessage == "" {
		return nil, fmt.Errorf("message is required")
	}

	queueMode := message.QueueMode
	if queueMode == "" {
		queueMode = types.AgentQueueModeFollowup
	}

	idempotencyKey := strings.TrimSpace(message.IdempotencyKey)
	if idempotencyKey == "" {
		idempotencyKey = uuid.NewString()
	}

	task, deduped, err := d.agents.EnqueueRunInput(ctx, workspaceID, trimmedRunID, queueMode, trimmedMessage, idempotencyKey)
	if err != nil {
		return nil, err
	}

	return &SendResult{
		Accepted:      true,
		IdempotentHit: deduped,
		Task:          task,
		RunID:         task.TargetRunID,
	}, nil
}
