package inbound

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	baml "github.com/beam-cloud/airstore/pkg/channels/inbound/baml_client"
	bamltypes "github.com/beam-cloud/airstore/pkg/channels/inbound/baml_client/types"
	"github.com/beam-cloud/airstore/pkg/common"
	"github.com/beam-cloud/airstore/pkg/orchestration"
	"github.com/beam-cloud/airstore/pkg/types"
)

// ConversationEntry is a single message in a channel conversation, stored in S2.
type ConversationEntry struct {
	Role    string `json:"role"` // "inbound" or "outbound"
	Sender  string `json:"sender"`
	Content string `json:"content"`
	Action  string `json:"action,omitempty"`
	Ts      int64  `json:"ts"`
}

// Result is what the handler receives after processing.
type Result struct {
	Reply       string
	ShouldReply bool
	Tasks       []bamltypes.InboundTask
}

// Processor handles inbound channel messages using BAML + S2 conversation context.
type Processor struct {
	s2     *common.S2Client
	agents *orchestration.AgentAPI
}

func NewProcessor(s2 *common.S2Client, agents *orchestration.AgentAPI) *Processor {
	return &Processor{s2: s2, agents: agents}
}

// Process classifies an inbound message using conversation history + active tasks, via BAML.
// When availableAgents is non-empty (workspace-level routing), BAML picks the best agent per task.
func (p *Processor) Process(ctx context.Context, workspaceID uint, agentID, agentName, channel, sender, subject, body string, availableAgents []bamltypes.AvailableAgent) (*Result, error) {
	streamID := agentID
	if streamID == "" {
		streamID = fmt.Sprintf("ws-%d", workspaceID)
	}

	history := p.loadHistory(ctx, streamID, sender)
	activeTasks := p.loadActiveTasks(ctx, workspaceID, agentID, sender)

	resp, err := baml.ProcessInboundMessage(ctx, channel, sender, subject, body, history, agentName, activeTasks, availableAgents)
	if err != nil {
		return nil, fmt.Errorf("baml ProcessInboundMessage: %w", err)
	}

	actions := make([]string, len(resp.Tasks))
	for i, t := range resp.Tasks {
		actions[i] = string(t.Task_type)
	}
	p.appendEntry(ctx, streamID, sender, ConversationEntry{
		Role:    "inbound",
		Sender:  sender,
		Content: body,
		Action:  strings.Join(actions, ","),
		Ts:      time.Now().UnixMilli(),
	})

	return &Result{
		Reply:       resp.Reply,
		ShouldReply: resp.Should_reply,
		Tasks:       resp.Tasks,
	}, nil
}

// AppendOutbound records an outbound reply in the conversation stream.
// streamID is the agentID for per-agent channels, or "ws-{workspaceID}" for workspace-level.
func (p *Processor) AppendOutbound(ctx context.Context, streamID, sender, content string) {
	p.appendEntry(ctx, streamID, sender, ConversationEntry{
		Role:    "outbound",
		Sender:  "agent",
		Content: content,
		Ts:      time.Now().UnixMilli(),
	})
}

func (p *Processor) loadActiveTasks(ctx context.Context, workspaceID uint, agentID, sender string) []bamltypes.ActiveTask {
	if p.agents == nil {
		return nil
	}
	tasks, _, _, err := p.agents.ListTasksFiltered(ctx, workspaceID, types.AgentTaskListFilter{
		AgentID: &agentID,
		States:  []types.AgentTaskState{"pending", "running"},
		Limit:   10,
	})
	if err != nil || len(tasks) == 0 {
		return nil
	}
	out := make([]bamltypes.ActiveTask, 0, len(tasks))
	for _, t := range tasks {
		at := bamltypes.ActiveTask{
			Id:    t.ID,
			State: string(t.State),
			Label: taskLabel(t),
		}
		if fromSender := taskSender(t); fromSender != "" {
			at.From_sender = &fromSender
		}
		out = append(out, at)
	}
	return out
}

func (p *Processor) loadHistory(ctx context.Context, agentID, sender string) string {
	if p.s2 == nil || !p.s2.Enabled() {
		return "(no history)"
	}
	stream := common.Streams.ChannelConversation(agentID, hashSender(sender))
	records, err := p.s2.Read(ctx, stream, 0, 50)
	if err != nil || len(records) == 0 {
		return "(no history)"
	}
	var sb strings.Builder
	for _, r := range records {
		var entry ConversationEntry
		if err := json.Unmarshal([]byte(r.Body), &entry); err != nil {
			continue
		}
		role := "User"
		if entry.Role == "outbound" {
			role = "Agent"
		}
		sb.WriteString(fmt.Sprintf("[%s] %s: %s\n", time.UnixMilli(entry.Ts).Format("Jan 2 15:04"), role, entry.Content))
	}
	if sb.Len() == 0 {
		return "(no history)"
	}
	return sb.String()
}

func (p *Processor) appendEntry(ctx context.Context, agentID, sender string, entry ConversationEntry) {
	if p.s2 == nil || !p.s2.Enabled() {
		return
	}
	stream := common.Streams.ChannelConversation(agentID, hashSender(sender))
	_ = p.s2.Append(ctx, stream, entry)
}

func hashSender(sender string) string {
	h := sha256.Sum256([]byte(strings.ToLower(strings.TrimSpace(sender))))
	return fmt.Sprintf("%x", h[:8])
}

func taskLabel(t *types.AgentTask) string {
	if t.PayloadJSON != nil {
		if label, ok := t.PayloadJSON["label"].(string); ok && label != "" {
			return label
		}
		if msg, ok := t.PayloadJSON["message"].(string); ok && len(msg) > 60 {
			return msg[:60] + "..."
		} else if ok {
			return msg
		}
	}
	return t.ID
}

func taskSender(t *types.AgentTask) string {
	if t.RoutingJSON != nil {
		if replyTo, ok := t.RoutingJSON["reply_to"].(string); ok {
			return replyTo
		}
	}
	return ""
}
