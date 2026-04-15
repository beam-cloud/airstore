package clients

import (
	"context"
	"fmt"
	"io"

	"github.com/beam-cloud/airstore/pkg/clients"
	"github.com/beam-cloud/airstore/pkg/types"
)

const (
	amCmdListMessages = "list-messages"
	amCmdGetMessage   = "get-message"
	amCmdGetThread    = "get-thread"
	amCmdSend         = "send"
	amCmdReply        = "reply"
)

// AgentMailToolClient implements tool execution for AgentMail.
// It uses the server-level AgentMailClient rather than per-user credentials.
type AgentMailToolClient struct {
	client *clients.AgentMailClient
}

func NewAgentMailToolClient(client *clients.AgentMailClient) *AgentMailToolClient {
	return &AgentMailToolClient{client: client}
}

func (a *AgentMailToolClient) Name() types.IntegrationName {
	return types.AgentMail
}

func (a *AgentMailToolClient) Execute(ctx context.Context, command string, args map[string]any, creds *types.IntegrationCredentials, stdout, _ io.Writer) error {
	if a.client == nil {
		return WriteToolError(stdout, "agentmail: not configured — set channels.agentMail.apiKey in server config")
	}

	var result any
	var err error

	switch command {
	case amCmdListMessages:
		result, err = a.listMessages(ctx, args)
	case amCmdGetMessage:
		result, err = a.getMessage(ctx, args)
	case amCmdGetThread:
		result, err = a.getThread(ctx, args)
	case amCmdSend:
		result, err = a.send(ctx, args)
	case amCmdReply:
		result, err = a.reply(ctx, args)
	default:
		return fmt.Errorf("unknown command: %s", command)
	}

	if err != nil {
		return WriteToolError(stdout, err.Error())
	}
	return WriteJSON(stdout, result)
}

func (a *AgentMailToolClient) listMessages(ctx context.Context, args map[string]any) (any, error) {
	required, err := RequireStringArgs(args, "inbox_id")
	if err != nil {
		return nil, err
	}
	limit := GetIntArg(args, "limit", 20)
	if limit < 1 {
		limit = 1
	} else if limit > 50 {
		limit = 50
	}

	msgs, _, err := a.client.ListMessages(ctx, required["inbox_id"], limit, "")
	if err != nil {
		return nil, err
	}

	type msgSummary struct {
		MessageID string `json:"message_id"`
		ThreadID  string `json:"thread_id"`
		From      string `json:"from"`
		Subject   string `json:"subject"`
		CreatedAt string `json:"created_at"`
		Preview   string `json:"preview"`
	}

	results := make([]msgSummary, 0, len(msgs))
	for _, m := range msgs {
		from := m.From
		preview := m.Text
		if len(preview) > 200 {
			preview = preview[:200] + "..."
		}
		results = append(results, msgSummary{
			MessageID: m.MessageID,
			ThreadID:  m.ThreadID,
			From:      from,
			Subject:   m.Subject,
			CreatedAt: m.CreatedAt,
			Preview:   preview,
		})
	}
	return results, nil
}

func (a *AgentMailToolClient) getMessage(ctx context.Context, args map[string]any) (any, error) {
	required, err := RequireStringArgs(args, "inbox_id", "message_id")
	if err != nil {
		return nil, err
	}
	return a.client.GetMessage(ctx, required["inbox_id"], required["message_id"])
}

func (a *AgentMailToolClient) getThread(ctx context.Context, args map[string]any) (any, error) {
	required, err := RequireStringArgs(args, "inbox_id", "thread_id")
	if err != nil {
		return nil, err
	}
	return a.client.GetThread(ctx, required["inbox_id"], required["thread_id"])
}

func (a *AgentMailToolClient) send(ctx context.Context, args map[string]any) (any, error) {
	required, err := RequireStringArgs(args, "inbox_id", "to", "subject", "body")
	if err != nil {
		return nil, err
	}
	err = a.client.SendMessage(ctx, required["inbox_id"], clients.SendMessageParams{
		To:      required["to"],
		Subject: required["subject"],
		Text:    required["body"],
	})
	if err != nil {
		return nil, err
	}
	return map[string]any{
		"status":  "sent",
		"to":      required["to"],
		"subject": required["subject"],
	}, nil
}

func (a *AgentMailToolClient) reply(ctx context.Context, args map[string]any) (any, error) {
	required, err := RequireStringArgs(args, "inbox_id", "message_id", "body")
	if err != nil {
		return nil, err
	}
	err = a.client.ReplyToMessage(ctx, required["inbox_id"], required["message_id"], required["body"])
	if err != nil {
		return nil, err
	}
	return map[string]any{
		"status":     "replied",
		"message_id": required["message_id"],
	}, nil
}
