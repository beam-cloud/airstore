package clients

import (
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"strings"

	"github.com/beam-cloud/airstore/pkg/types"
)

const (
	gmailAPIBase        = "https://gmail.googleapis.com/gmail/v1/users/me"
	gmailCmdCreateDraft = "create-draft"
	gmailCmdSendEmail   = "send-email"
)

type GmailClient struct {
	api *oauthHTTPClient
}

func NewGmailClient() *GmailClient {
	return &GmailClient{
		api: newOAuthHTTPClient("gmail", gmailAPIBase, nil),
	}
}

func (g *GmailClient) Name() types.IntegrationName {
	return types.Gmail
}

func (g *GmailClient) Execute(ctx context.Context, command string, args map[string]any, creds *types.IntegrationCredentials, stdout, _ io.Writer) error {
	return ExecuteOAuthCommand(ctx, "gmail", command, args, creds, map[string]OAuthCommandHandler{
		gmailCmdCreateDraft: func(ctx context.Context, token string, args map[string]any) (any, error) {
			required, err := RequireStringArgs(args, "to", "subject", "body")
			if err != nil {
				return nil, err
			}
			return g.createDraft(ctx, token, required["to"], required["subject"], required["body"])
		},
		gmailCmdSendEmail: func(ctx context.Context, token string, args map[string]any) (any, error) {
			required, err := RequireStringArgs(args, "to", "subject", "body")
			if err != nil {
				return nil, err
			}
			threadID := GetStringArg(args, "thread_id", "")
			return g.sendEmail(ctx, token, required["to"], required["subject"], required["body"], threadID)
		},
	}, stdout)
}

func buildRawEmail(to, subject, body string) string {
	to = sanitizeEmailHeaderValue(to)
	subject = sanitizeEmailHeaderValue(subject)
	return fmt.Sprintf(
		"To: %s\r\nSubject: %s\r\nContent-Type: text/plain; charset=UTF-8\r\n\r\n%s",
		to,
		subject,
		body,
	)
}

func sanitizeEmailHeaderValue(value string) string {
	value = strings.ReplaceAll(value, "\r", " ")
	value = strings.ReplaceAll(value, "\n", " ")
	return strings.TrimSpace(value)
}

func formatGmailMessageResult(to, subject string, result map[string]any) map[string]any {
	out := map[string]any{
		"to":      to,
		"subject": subject,
	}
	if id := getString(result, "id"); id != "" {
		out["message_id"] = id
	}
	if threadID := getString(result, "threadId"); threadID != "" {
		out["thread_id"] = threadID
	}
	if labels, ok := result["labelIds"].([]any); ok && len(labels) > 0 {
		out["label_ids"] = labels
	}
	return out
}

func (g *GmailClient) createDraft(ctx context.Context, token, to, subject, body string) (map[string]any, error) {
	encoded := base64.RawURLEncoding.EncodeToString([]byte(buildRawEmail(to, subject, body)))
	payload := map[string]any{
		"message": map[string]any{
			"raw": encoded,
		},
	}

	var result map[string]any
	if err := g.api.RequestJSON(ctx, token, "POST", "/drafts", payload, &result); err != nil {
		return nil, err
	}

	out := formatGmailMessageResult(to, subject, result)
	if id := getString(result, "id"); id != "" {
		out["draft_id"] = id
	}
	if message, ok := result["message"].(map[string]any); ok {
		msg := formatGmailMessageResult(to, subject, message)
		for key, value := range msg {
			out[key] = value
		}
	}
	return out, nil
}

func (g *GmailClient) sendEmail(ctx context.Context, token, to, subject, body, threadID string) (map[string]any, error) {
	encoded := base64.RawURLEncoding.EncodeToString([]byte(buildRawEmail(to, subject, body)))
	payload := map[string]any{
		"raw": encoded,
	}
	if threadID != "" {
		payload["threadId"] = threadID
	}
	var result map[string]any
	if err := g.api.RequestJSON(ctx, token, "POST", "/messages/send", payload, &result); err != nil {
		return nil, err
	}
	return formatGmailMessageResult(to, subject, result), nil
}
