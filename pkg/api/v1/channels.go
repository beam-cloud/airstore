package apiv1

import (
	"errors"
	"net/http"
	"strings"

	"github.com/beam-cloud/airstore/pkg/channels"
	"github.com/beam-cloud/airstore/pkg/orchestration"
	"github.com/beam-cloud/airstore/pkg/types"
	"github.com/labstack/echo/v4"
)

var errChannelTypeRequired = errors.New("channel_type is required")

type WorkspaceChannelsGroup struct {
	routerGroup *echo.Group
	registry    *channels.Registry
}

type sendChannelAgentMessageRequest struct {
	Message           string                            `json:"message"`
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
	IdempotencyKey    string                            `json:"idempotency_key,omitempty"`
	Label             *string                           `json:"label,omitempty"`
	SpawnedBy         *string                           `json:"spawned_by,omitempty"`
}

type sendChannelRunMessageRequest struct {
	Message        string               `json:"message"`
	TaskID         string               `json:"task_id,omitempty"`
	QueueMode      types.AgentQueueMode `json:"queue_mode,omitempty"`
	IdempotencyKey string               `json:"idempotency_key,omitempty"`
}

func NewWorkspaceChannelsGroup(routerGroup *echo.Group, registry *channels.Registry) *WorkspaceChannelsGroup {
	g := &WorkspaceChannelsGroup{
		routerGroup: routerGroup,
		registry:    registry,
	}
	g.registerRoutes()
	return g
}

func (g *WorkspaceChannelsGroup) registerRoutes() {
	g.routerGroup.POST("/:channel_type/agents/:agent_id/messages", g.SendAgentMessage)
	g.routerGroup.POST("/:channel_type/runs/:run_id/messages", g.SendRunMessage)
}

func (g *WorkspaceChannelsGroup) SendAgentMessage(c echo.Context) error {
	channel, err := g.resolveChannel(c.Param("channel_type"))
	if err != nil {
		return ErrorResponse(c, statusForResolveChannelError(err), err.Error())
	}

	var req sendChannelAgentMessageRequest
	if err := decodeStrictBody(c, &req); err != nil {
		return ErrorResponse(c, http.StatusBadRequest, "invalid request body")
	}

	workspaceID, err := requireWorkspaceID(c)
	if err != nil {
		return err
	}

	result, err := channel.SendToAgent(c.Request().Context(), workspaceID, c.Param("agent_id"), channels.Message{
		Message:           req.Message,
		SessionID:         req.SessionID,
		SessionKey:        req.SessionKey,
		Deliver:           req.Deliver,
		TimeoutMs:         req.TimeoutMs,
		Policy:            req.Policy,
		Lane:              req.Lane,
		ExtraSystemPrompt: req.ExtraSystemPrompt,
		InputProvenance:   req.InputProvenance,
		Routing:           req.Routing,
		Attachments:       req.Attachments,
		IdempotencyKey:    req.IdempotencyKey,
		Label:             req.Label,
		SpawnedBy:         req.SpawnedBy,
	})
	if err != nil {
		var profileErr *types.ErrAgentProfileNotFound
		if errors.As(err, &profileErr) {
			return ErrorResponse(c, http.StatusNotFound, "agent not found")
		}
		return ErrorResponse(c, statusForAcceptAgentCommandError(err), err.Error())
	}

	return c.JSON(statusCodeForDeduped(result.IdempotentHit), Response{
		Success: true,
		Data: map[string]any{
			"accepted":       result.Accepted,
			"idempotent_hit": result.IdempotentHit,
			"task":           result.Task,
			"run_id":         result.RunID,
		},
	})
}

func (g *WorkspaceChannelsGroup) SendRunMessage(c echo.Context) error {
	channel, err := g.resolveChannel(c.Param("channel_type"))
	if err != nil {
		return ErrorResponse(c, statusForResolveChannelError(err), err.Error())
	}

	var req sendChannelRunMessageRequest
	if err := decodeStrictBody(c, &req); err != nil {
		return ErrorResponse(c, http.StatusBadRequest, "invalid request body")
	}

	workspaceID, err := requireWorkspaceID(c)
	if err != nil {
		return err
	}
	if strings.TrimSpace(req.TaskID) == "" {
		return ErrorResponse(c, http.StatusBadRequest, "task_id is required")
	}

	result, err := channel.SendToRun(c.Request().Context(), workspaceID, c.Param("run_id"), channels.Message{
		Message:        req.Message,
		TaskID:         req.TaskID,
		QueueMode:      req.QueueMode,
		IdempotencyKey: req.IdempotencyKey,
	})
	if err != nil {
		var runErr *types.ErrAgentRunNotFound
		if errors.As(err, &runErr) {
			return ErrorResponse(c, http.StatusNotFound, "run not found")
		}
		var taskErr *types.ErrAgentTaskNotFound
		if errors.As(err, &taskErr) {
			return ErrorResponse(c, http.StatusNotFound, "task not found")
		}
		if isAgentCommandValidationError(err) {
			return ErrorResponse(c, http.StatusBadRequest, err.Error())
		}
		return ErrorResponse(c, http.StatusBadRequest, err.Error())
	}

	return c.JSON(statusCodeForDeduped(result.IdempotentHit), Response{
		Success: true,
		Data: map[string]any{
			"accepted":         result.Accepted,
			"idempotent_hit":   result.IdempotentHit,
			"task":             result.Task,
			"run_id":           result.RunID,
			"decision":         result.Decision,
			"delivery_outcome": result.DeliveryOutcome,
			"interaction":      result.Interaction,
		},
	})
}

func (g *WorkspaceChannelsGroup) resolveChannel(channelTypeRaw string) (channels.Channel, error) {
	if g == nil || g.registry == nil {
		return nil, errors.New("channel service unavailable")
	}
	channelType := channels.ChannelType(strings.ToLower(strings.TrimSpace(channelTypeRaw)))
	if channelType == "" {
		return nil, errChannelTypeRequired
	}
	return g.registry.Resolve(channelType)
}

func statusForResolveChannelError(err error) int {
	if errors.Is(err, errChannelTypeRequired) {
		return http.StatusBadRequest
	}
	var unknownChannelErr *channels.ErrChannelTypeNotRegistered
	if errors.As(err, &unknownChannelErr) {
		return http.StatusBadRequest
	}
	return http.StatusServiceUnavailable
}

func statusCodeForDeduped(deduped bool) int {
	if deduped {
		return http.StatusOK
	}
	return http.StatusAccepted
}
