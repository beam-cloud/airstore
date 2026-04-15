package apiv1

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/beam-cloud/airstore/pkg/channels"
	"github.com/beam-cloud/airstore/pkg/channels/inbound"
	bamltypes "github.com/beam-cloud/airstore/pkg/channels/inbound/baml_client/types"
	"github.com/beam-cloud/airstore/pkg/orchestration"
	"github.com/beam-cloud/airstore/pkg/types"
	"github.com/labstack/echo/v4"
)

type recordingInboundChannel struct {
	channelType channels.ChannelType
	workspaceID uint
	agentID     string
	sent        []channels.Message
}

func (c *recordingInboundChannel) Type() channels.ChannelType { return c.channelType }

func (c *recordingInboundChannel) SendToAgent(_ context.Context, _ uint, _ string, msg channels.Message) (*channels.SendResult, error) {
	c.sent = append(c.sent, msg)
	return &channels.SendResult{Accepted: true, Task: &types.AgentTask{ID: "task-1"}}, nil
}

func (c *recordingInboundChannel) SendToRun(_ context.Context, _ uint, _ string, _ channels.Message) (*channels.SendResult, error) {
	return nil, nil
}

func (c *recordingInboundChannel) ResolveInbound(_ context.Context, _ string) (uint, string, error) {
	return c.workspaceID, c.agentID, nil
}

func TestProcessInboundPreservesRoutingToAndReplyTo(t *testing.T) {
	group := &InboundChannelsGroup{}
	channel := &recordingInboundChannel{
		channelType: channels.ChannelTypeEmail,
		workspaceID: 7,
		agentID:     "agent-1",
	}

	err := group.processInbound(
		newInboundTestContext(),
		channel,
		inboundMessage{
			from:        "lead@example.com",
			to:          "agent@agentmail.to",
			subject:     "Hello",
			body:        "Can you help?",
			channelType: channels.ChannelTypeEmail,
		},
	)
	if err != nil {
		t.Fatalf("processInbound: %v", err)
	}
	if got, want := len(channel.sent), 1; got != want {
		t.Fatalf("sent message count = %d, want %d", got, want)
	}
	assertRouting(t, channel.sent[0].Routing, "agent@agentmail.to", "lead@example.com")
}

func TestHandleProcessedResultPreservesRoutingToAndReplyTo(t *testing.T) {
	group := &InboundChannelsGroup{}
	channel := &recordingInboundChannel{
		channelType: channels.ChannelTypeEmail,
		workspaceID: 7,
		agentID:     "agent-1",
	}

	err := group.handleProcessedResult(
		newInboundTestContext(),
		channel,
		7,
		"agent-1",
		inboundMessage{
			from:        "lead@example.com",
			to:          "agent@agentmail.to",
			subject:     "Hello",
			body:        "Can you help?",
			channelType: channels.ChannelTypeEmail,
		},
		&inbound.Result{
			Tasks: []bamltypes.InboundTask{{
				Task_type: bamltypes.InboundTaskTypeCREATE_TASK,
				Message:   "Follow up",
			}},
		},
		false,
	)
	if err != nil {
		t.Fatalf("handleProcessedResult: %v", err)
	}
	if got, want := len(channel.sent), 1; got != want {
		t.Fatalf("sent message count = %d, want %d", got, want)
	}
	assertRouting(t, channel.sent[0].Routing, "agent@agentmail.to", "lead@example.com")
}

func newInboundTestContext() echo.Context {
	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/channels/email/inbound", nil)
	rec := httptest.NewRecorder()
	return e.NewContext(req, rec)
}

func assertRouting(t *testing.T, routing *orchestration.RoutingContext, to, replyTo string) {
	t.Helper()
	if routing == nil {
		t.Fatal("expected routing context")
	}
	if routing.To == nil || *routing.To != to {
		t.Fatalf("routing.to = %#v, want %q", routing.To, to)
	}
	if routing.ReplyTo == nil || *routing.ReplyTo != replyTo {
		t.Fatalf("routing.reply_to = %#v, want %q", routing.ReplyTo, replyTo)
	}
}
