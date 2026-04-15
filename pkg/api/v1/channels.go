package apiv1

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/mail"
	"strings"

	"github.com/beam-cloud/airstore/pkg/channels"
	"github.com/beam-cloud/airstore/pkg/channels/inbound"
	bamltypes "github.com/beam-cloud/airstore/pkg/channels/inbound/baml_client/types"
	"github.com/beam-cloud/airstore/pkg/clients"
	"github.com/beam-cloud/airstore/pkg/orchestration"
	"github.com/beam-cloud/airstore/pkg/types"
	"github.com/labstack/echo/v4"
	"github.com/rs/zerolog/log"
)

var errChannelTypeRequired = errors.New("channel_type is required")

// ---------------------------------------------------------------------------
// Workspace Channels (authenticated): send messages + manage bindings
// ---------------------------------------------------------------------------

type WorkspaceChannelsGroup struct {
	routerGroup  *echo.Group
	registry     *channels.Registry
	agents       *orchestration.AgentAPI
	emailChannel *channels.Email
}

func NewWorkspaceChannelsGroup(routerGroup *echo.Group, registry *channels.Registry, agents *orchestration.AgentAPI, emailCh *channels.Email) *WorkspaceChannelsGroup {
	g := &WorkspaceChannelsGroup{routerGroup: routerGroup, registry: registry, agents: agents, emailChannel: emailCh}
	g.routerGroup.POST("/:channel_type/agents/:agent_id/messages", g.SendAgentMessage)
	g.routerGroup.POST("/:channel_type/runs/:run_id/messages", g.SendRunMessage)
	g.routerGroup.GET("", g.ListChannels)
	g.routerGroup.PUT("", g.UpsertChannels)
	g.routerGroup.DELETE("/:channel_type", g.DeleteChannel)
	return g
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
			"delivery_outcome": result.DeliveryOutcome,
			"interaction":      result.Interaction,
		},
	})
}

// ListChannels returns workspace-level channel bindings (agent_id IS NULL).
func (g *WorkspaceChannelsGroup) ListChannels(c echo.Context) error {
	if g.agents == nil {
		return ErrorResponse(c, http.StatusServiceUnavailable, "channel service unavailable")
	}
	workspaceID, err := requireWorkspaceID(c)
	if err != nil {
		return err
	}
	bindings, err := g.agents.ListChannelBindings(c.Request().Context(), workspaceID, nil)
	if err != nil {
		return ErrorResponse(c, http.StatusInternalServerError, err.Error())
	}
	return SuccessResponse(c, bindings)
}

// UpsertChannels creates or updates workspace-level channel bindings.
func (g *WorkspaceChannelsGroup) UpsertChannels(c echo.Context) error {
	if g.agents == nil {
		return ErrorResponse(c, http.StatusServiceUnavailable, "channel service unavailable")
	}
	workspaceID, err := requireWorkspaceID(c)
	if err != nil {
		return err
	}

	var req upsertChannelsRequest
	if err := decodeStrictBody(c, &req); err != nil {
		return ErrorResponse(c, http.StatusBadRequest, "invalid request body")
	}

	results := make([]*types.ChannelBinding, 0, len(req.Channels))
	for _, ch := range req.Channels {
		if strings.TrimSpace(ch.ChannelType) == "" {
			return ErrorResponse(c, http.StatusBadRequest, "channel_type is required")
		}

		active := true
		if ch.Active != nil {
			active = *ch.Active
		}
		address := strings.TrimSpace(ch.Address)

		if ch.ChannelType == string(channels.ChannelTypeEmail) && address == "" && g.emailChannel != nil && g.emailChannel.Mail() != nil {
			ws, err := g.agents.GetWorkspace(c.Request().Context(), workspaceID)
			if err != nil {
				return ErrorResponse(c, http.StatusBadRequest, "failed to look up workspace: "+err.Error())
			}
			emailAddr, err := g.emailChannel.ProvisionInbox(c.Request().Context(), "ws-"+ws.Name, ws.Name)
			if err != nil {
				return ErrorResponse(c, http.StatusBadGateway, "failed to provision inbox: "+err.Error())
			}
			address = emailAddr
		}

		if address == "" {
			return ErrorResponse(c, http.StatusBadRequest, "address is required (or enable agentmail for auto-provisioning)")
		}

		binding := &types.ChannelBinding{
			WorkspaceID: workspaceID,
			AgentID:     nil,
			ChannelType: ch.ChannelType,
			Address:     address,
			ConfigJSON:  ch.ConfigJSON,
			Active:      active,
		}
		if err := g.agents.UpsertChannelBinding(c.Request().Context(), binding); err != nil {
			return ErrorResponse(c, http.StatusInternalServerError, err.Error())
		}
		results = append(results, binding)
	}
	return c.JSON(http.StatusOK, Response{Success: true, Data: results})
}

// DeleteChannel removes a workspace-level channel binding by type.
func (g *WorkspaceChannelsGroup) DeleteChannel(c echo.Context) error {
	if g.agents == nil {
		return ErrorResponse(c, http.StatusServiceUnavailable, "channel service unavailable")
	}
	workspaceID, err := requireWorkspaceID(c)
	if err != nil {
		return err
	}
	channelType := strings.TrimSpace(c.Param("channel_type"))
	if channelType == "" {
		return ErrorResponse(c, http.StatusBadRequest, "channel_type is required")
	}

	if channelType == string(channels.ChannelTypeEmail) && g.emailChannel != nil && g.emailChannel.Mail() != nil {
		bindings, err := g.agents.ListChannelBindings(c.Request().Context(), workspaceID, nil)
		if err != nil {
			return ErrorResponse(c, http.StatusInternalServerError, "failed to look up existing bindings: "+err.Error())
		}
		for _, b := range bindings {
			if b.ChannelType == string(channels.ChannelTypeEmail) && b.Address != "" {
				if depErr := g.emailChannel.DeprovisionInbox(c.Request().Context(), b.Address); depErr != nil {
					log.Warn().Err(depErr).Str("address", b.Address).Msg("failed to deprovision inbox")
				}
			}
		}
	}

	if err := g.agents.DeleteChannelBinding(c.Request().Context(), workspaceID, nil, channelType); err != nil {
		return ErrorResponse(c, http.StatusNotFound, err.Error())
	}
	return SuccessResponse(c, nil)
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

// ---------------------------------------------------------------------------
// Request types (shared by workspace + agent handlers)
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// Inbound Channel Handlers (global, no workspace auth — address-based routing)
// ---------------------------------------------------------------------------

type InboundChannelsGroup struct {
	routerGroup *echo.Group
	registry    *channels.Registry
	processor   *inbound.Processor
	mail        *clients.AgentMailClient
	agents      *orchestration.AgentAPI
}

func NewInboundChannelsGroup(
	routerGroup *echo.Group,
	registry *channels.Registry,
	processor *inbound.Processor,
	mail *clients.AgentMailClient,
	agents *orchestration.AgentAPI,
) *InboundChannelsGroup {
	g := &InboundChannelsGroup{
		routerGroup: routerGroup, registry: registry,
		processor: processor, mail: mail, agents: agents,
	}
	g.routerGroup.POST("/email/inbound", g.InboundEmail)
	g.routerGroup.POST("/sms/inbound", g.InboundSMS)
	return g
}

// inboundMessage is the channel-agnostic representation of an inbound message
// after protocol-specific parsing (AgentMail webhook, Twilio form, etc).
type inboundMessage struct {
	from        string
	to          string
	subject     string
	body        string
	channelType channels.ChannelType
}

func newInboundRoutingContext(channelType channels.ChannelType, from, to string) *orchestration.RoutingContext {
	chStr := string(channelType)
	routing := &orchestration.RoutingContext{Channel: &chStr}
	if trimmedTo := strings.TrimSpace(to); trimmedTo != "" {
		routing.To = &trimmedTo
	}
	if trimmedFrom := strings.TrimSpace(from); trimmedFrom != "" {
		routing.ReplyTo = &trimmedFrom
	}
	return routing
}

// --- AgentMail webhook ---

type agentMailWebhookRequest struct {
	EventType string              `json:"event_type"`
	EventID   string              `json:"event_id"`
	Message   agentMailMessageObj `json:"message"`
}

type agentMailMessageObj struct {
	From    string   `json:"from"`
	To      []string `json:"to"`
	ReplyTo []string `json:"reply_to"`
	Subject string   `json:"subject"`
	Text    string   `json:"text"`
	InboxID string   `json:"inbox_id"`
}

func (g *InboundChannelsGroup) InboundEmail(c echo.Context) error {
	ec, err := resolveTypedChannel[*channels.Email](g.registry, channels.ChannelTypeEmail)
	if err != nil {
		return ErrorResponse(c, http.StatusServiceUnavailable, err.Error())
	}

	var req agentMailWebhookRequest
	if err := c.Bind(&req); err != nil {
		log.Error().Err(err).Str("content_type", c.Request().Header.Get("Content-Type")).Msg("agentmail webhook: bind failed")
		return ErrorResponse(c, http.StatusBadRequest, "invalid request body")
	}
	if req.EventType != "message.received" {
		return c.JSON(http.StatusOK, Response{Success: true, Data: "ignored"})
	}
	if len(req.Message.To) == 0 {
		return ErrorResponse(c, http.StatusBadRequest, "no recipient address")
	}

	from := extractEmailAddress(req.Message.From)
	to := extractEmailAddress(req.Message.To[0])
	subject := strings.TrimSpace(req.Message.Subject)
	body := strings.TrimSpace(req.Message.Text)
	if body == "" {
		body = subject
	}

	msg := inboundMessage{
		from: from, to: to,
		subject: subject, body: body, channelType: channels.ChannelTypeEmail,
	}
	return g.processInbound(c, ec, msg)
}

// extractEmailAddress parses an RFC 5322 address like "Display Name <user@example.com>"
// and returns just the email part. Falls back to the trimmed input on parse failure.
func extractEmailAddress(raw string) string {
	raw = strings.TrimSpace(raw)
	if addr, err := mail.ParseAddress(raw); err == nil {
		return addr.Address
	}
	return raw
}

// --- Twilio webhook ---

type twilioInboundRequest struct {
	From string `form:"From" json:"From"`
	To   string `form:"To" json:"To"`
	Body string `form:"Body" json:"Body"`
}

func (g *InboundChannelsGroup) InboundSMS(c echo.Context) error {
	sc, err := resolveTypedChannel[*channels.SMS](g.registry, channels.ChannelTypeSMS)
	if err != nil {
		return ErrorResponse(c, http.StatusServiceUnavailable, err.Error())
	}

	var req twilioInboundRequest
	if err := c.Bind(&req); err != nil {
		return ErrorResponse(c, http.StatusBadRequest, "invalid request body")
	}
	to := strings.TrimSpace(req.To)
	if to == "" {
		return ErrorResponse(c, http.StatusBadRequest, "To number is required")
	}
	body := strings.TrimSpace(req.Body)
	if body == "" {
		return ErrorResponse(c, http.StatusBadRequest, "Body is required")
	}

	msg := inboundMessage{
		from: strings.TrimSpace(req.From), to: to,
		body: body, channelType: channels.ChannelTypeSMS,
	}
	return g.processInbound(c, sc, msg)
}

// processInbound is the unified inbound pipeline shared by email and SMS.
//
// Flow:
//  1. Resolve binding by address → (workspaceID, agentID)
//  2. If workspace-level (agentID == ""): load all agents for BAML routing
//  3. Run BAML processor → classify message, pick agent, create tasks
//  4. Fallback (agent-level only): create task directly via channel
func (g *InboundChannelsGroup) processInbound(c echo.Context, ch channels.InboundChannel, msg inboundMessage) error {
	workspaceID, agentID, err := ch.ResolveInbound(c.Request().Context(), msg.to)
	if err != nil {
		return ErrorResponse(c, http.StatusNotFound, err.Error())
	}

	ctx := c.Request().Context()
	isWorkspaceLevel := agentID == ""

	agentName := agentID
	var availableAgents []bamltypes.AvailableAgent

	if isWorkspaceLevel {
		agentName = "Workspace"
		availableAgents = g.loadAvailableAgents(ctx, workspaceID)
	} else if g.agents != nil {
		if profile, err := g.agents.GetAgent(ctx, workspaceID, agentID); err == nil {
			agentName = profile.Name
		}
	}

	// BAML classification + routing
	if g.processor != nil {
		result, err := g.processor.Process(ctx, workspaceID, agentID, agentName, string(msg.channelType), msg.from, msg.subject, msg.body, availableAgents)
		if err != nil {
			log.Warn().Err(err).Str("agent", agentID).Bool("workspace_level", isWorkspaceLevel).Msg("BAML inbound processing failed")
		} else {
			return g.handleProcessedResult(c, ch, workspaceID, agentID, msg, result, isWorkspaceLevel)
		}
	}

	if isWorkspaceLevel {
		return ErrorResponse(c, http.StatusServiceUnavailable, "workspace-level routing requires BAML processor")
	}

	// Fallback (per-agent only): create task directly
	label := msg.subject
	if label == "" {
		label = fmt.Sprintf("%s from %s", msg.channelType, msg.from)
	}
	routing := newInboundRoutingContext(msg.channelType, msg.from, msg.to)
	taskResult, err := ch.SendToAgent(ctx, workspaceID, agentID, channels.Message{
		Message: msg.body, Label: &label, Routing: routing,
	})
	if err != nil {
		return ErrorResponse(c, http.StatusInternalServerError, err.Error())
	}
	return c.JSON(http.StatusAccepted, Response{Success: true, Data: map[string]any{
		"accepted": taskResult.Accepted, "task": taskResult.Task,
	}})
}

// handleProcessedResult dispatches tasks from BAML classification, optionally replies.
func (g *InboundChannelsGroup) handleProcessedResult(
	c echo.Context, ch channels.InboundChannel,
	workspaceID uint, agentID string, msg inboundMessage,
	r *inbound.Result, isWorkspaceLevel bool,
) error {
	ctx := c.Request().Context()
	routing := newInboundRoutingContext(msg.channelType, msg.from, msg.to)

	streamID := agentID
	if isWorkspaceLevel {
		streamID = fmt.Sprintf("ws-%d", workspaceID)
	}

	// Send auto-reply via email if applicable
	if r.ShouldReply && r.Reply != "" && msg.channelType == channels.ChannelTypeEmail && g.mail != nil {
		replySubject := msg.subject
		if replySubject != "" {
			replySubject = "Re: " + replySubject
		}
		if err := g.mail.SendMessage(ctx, msg.to, clients.SendMessageParams{
			To: msg.from, Subject: replySubject, Text: r.Reply,
		}); err != nil {
			log.Warn().Err(err).Msg("failed to send reply")
		} else if g.processor != nil {
			g.processor.AppendOutbound(ctx, streamID, msg.from, r.Reply)
		}
	}

	var createdTasks []map[string]any
	for _, task := range r.Tasks {
		targetAgentID := agentID
		if isWorkspaceLevel && task.Agent_id != nil && *task.Agent_id != "" {
			targetAgentID = *task.Agent_id
		}
		if targetAgentID == "" {
			log.Warn().Msg("skipping task with no agent_id in workspace-level routing")
			continue
		}

		switch task.Task_type {
		case bamltypes.InboundTaskTypeCREATE_TASK, bamltypes.InboundTaskTypeESCALATE:
			label := deref(task.Label)
			if label == "" {
				label = msg.subject
			}
			if label == "" {
				label = fmt.Sprintf("%s from %s", msg.channelType, msg.from)
			}
			result, err := ch.SendToAgent(ctx, workspaceID, targetAgentID, channels.Message{
				Message: task.Message, Label: &label, Routing: routing,
			})
			if err != nil {
				log.Warn().Err(err).Str("type", string(task.Task_type)).Str("agent", targetAgentID).Msg("failed to create task from inbound")
				continue
			}
			createdTasks = append(createdTasks, map[string]any{
				"action": string(task.Task_type), "task": result.Task,
				"agent_id": targetAgentID, "priority": task.Priority,
			})

		case bamltypes.InboundTaskTypeUPDATE_TASK:
			if taskID := deref(task.Existing_task_id); taskID != "" {
				label := "Follow-up from " + msg.from
				_, err := ch.SendToAgent(ctx, workspaceID, targetAgentID, channels.Message{
					Message: task.Message, Label: &label, Routing: routing,
				})
				if err != nil {
					log.Warn().Err(err).Str("task_id", taskID).Msg("failed to send update for existing task")
				}
			}
		}
	}

	return c.JSON(http.StatusAccepted, Response{Success: true, Data: map[string]any{
		"reply": r.Reply, "replied": r.ShouldReply, "tasks": createdTasks,
	}})
}

func (g *InboundChannelsGroup) loadAvailableAgents(ctx context.Context, workspaceID uint) []bamltypes.AvailableAgent {
	if g.agents == nil {
		return nil
	}
	profiles, err := g.agents.ListAgents(ctx, workspaceID)
	if err != nil {
		return nil
	}
	out := make([]bamltypes.AvailableAgent, 0, len(profiles))
	for _, p := range profiles {
		aa := bamltypes.AvailableAgent{Id: p.ID, Name: p.Name}
		if sp, ok := p.ConfigJSON["system_prompt"].(string); ok && sp != "" {
			desc := sp
			if len(desc) > 120 {
				desc = desc[:120]
			}
			aa.Description = &desc
		}
		out = append(out, aa)
	}
	return out
}

// resolveTypedChannel resolves a channel from the registry and asserts its concrete type.
func resolveTypedChannel[T channels.Channel](registry *channels.Registry, ct channels.ChannelType) (T, error) {
	var zero T
	if registry == nil {
		return zero, fmt.Errorf("%s channel not configured", ct)
	}
	ch, err := registry.Resolve(ct)
	if err != nil {
		return zero, fmt.Errorf("%s channel not configured", ct)
	}
	typed, ok := ch.(T)
	if !ok {
		return zero, fmt.Errorf("%s channel misconfigured", ct)
	}
	return typed, nil
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
