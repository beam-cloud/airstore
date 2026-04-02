package clients

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"strings"

	"github.com/beam-cloud/airstore/pkg/types"
)

const (
	outlookGraphBase      = "https://graph.microsoft.com/v1.0"
	outlookCmdSearch      = "search"
	outlookCmdGetMessage  = "get-message"
	outlookCmdGetThread   = "get-thread"
	outlookCmdCreateDraft = "create-draft"
	outlookCmdSendEmail   = "send-email"
)

// outlookSearchSelect is the $select for search/list queries (no body).
var outlookSearchSelect = strings.Join([]string{
	"id", "subject", "bodyPreview", "from", "toRecipients",
	"receivedDateTime", "isRead", "conversationId", "webLink",
}, ",")

// outlookFullSelect adds body to the select list.
var outlookFullSelect = outlookSearchSelect + ",body"

type OutlookToolClient struct {
	api *oauthHTTPClient
}

func NewOutlookToolClient() *OutlookToolClient {
	return &OutlookToolClient{
		api: newOAuthHTTPClient("outlook", outlookGraphBase, map[string]string{
			"Prefer": `outlook.body-content-type="text"`,
		}),
	}
}

func (o *OutlookToolClient) Name() types.IntegrationName {
	return types.Outlook
}

func (o *OutlookToolClient) Execute(ctx context.Context, command string, args map[string]any, creds *types.IntegrationCredentials, stdout, _ io.Writer) error {
	return ExecuteOAuthCommand(ctx, "outlook", command, args, creds, map[string]OAuthCommandHandler{
		outlookCmdSearch: func(ctx context.Context, token string, args map[string]any) (any, error) {
			required, err := RequireStringArgs(args, "query")
			if err != nil {
				return nil, err
			}
			limit := GetIntArg(args, "limit", 10)
			if limit < 1 {
				limit = 1
			} else if limit > 50 {
				limit = 50
			}
			return o.search(ctx, token, required["query"], limit)
		},
		outlookCmdGetMessage: func(ctx context.Context, token string, args map[string]any) (any, error) {
			required, err := RequireStringArgs(args, "message_id")
			if err != nil {
				return nil, err
			}
			return o.getMessage(ctx, token, required["message_id"])
		},
		outlookCmdGetThread: func(ctx context.Context, token string, args map[string]any) (any, error) {
			required, err := RequireStringArgs(args, "conversation_id")
			if err != nil {
				return nil, err
			}
			return o.getThread(ctx, token, required["conversation_id"])
		},
		outlookCmdCreateDraft: func(ctx context.Context, token string, args map[string]any) (any, error) {
			required, err := RequireStringArgs(args, "to", "subject", "body")
			if err != nil {
				return nil, err
			}
			conversationID := GetStringArg(args, "conversation_id", "")
			return o.createDraft(ctx, token, required["to"], required["subject"], required["body"], conversationID)
		},
		outlookCmdSendEmail: func(ctx context.Context, token string, args map[string]any) (any, error) {
			required, err := RequireStringArgs(args, "to", "subject", "body")
			if err != nil {
				return nil, err
			}
			conversationID := GetStringArg(args, "conversation_id", "")
			draftID := GetStringArg(args, "draft_id", "")
			return o.sendEmail(ctx, token, required["to"], required["subject"], required["body"], conversationID, draftID)
		},
	}, stdout)
}

func (o *OutlookToolClient) search(ctx context.Context, token, query string, limit int) (any, error) {
	path := fmt.Sprintf("/me/messages?$search=%q&$top=%d&$select=%s",
		query, limit, url.QueryEscape(outlookSearchSelect))

	var resp struct {
		Value []map[string]any `json:"value"`
	}
	if err := o.api.RequestJSON(ctx, token, "GET", path, nil, &resp); err != nil {
		return nil, err
	}

	results := make([]map[string]any, 0, len(resp.Value))
	for _, msg := range resp.Value {
		results = append(results, formatOutlookMessage(msg))
	}
	return map[string]any{"results": results, "total": len(results), "query": query}, nil
}

func (o *OutlookToolClient) getMessage(ctx context.Context, token, messageID string) (any, error) {
	path := fmt.Sprintf("/me/messages/%s?$select=%s", messageID, url.QueryEscape(outlookFullSelect))

	var msg map[string]any
	if err := o.api.RequestJSON(ctx, token, "GET", path, nil, &msg); err != nil {
		return nil, err
	}

	result := formatOutlookMessage(msg)
	result["body"] = extractOutlookBody(msg)
	return result, nil
}

func (o *OutlookToolClient) getThread(ctx context.Context, token, conversationID string) (any, error) {
	filter := fmt.Sprintf("conversationId eq '%s'", conversationID)
	path := fmt.Sprintf("/me/messages?$filter=%s&$orderby=%s&$select=%s&$top=50",
		url.QueryEscape(filter),
		url.QueryEscape("receivedDateTime asc"),
		url.QueryEscape(outlookFullSelect))

	var resp struct {
		Value []map[string]any `json:"value"`
	}
	if err := o.api.RequestJSON(ctx, token, "GET", path, nil, &resp); err != nil {
		return nil, err
	}

	senderEmail := o.detectSenderEmail(ctx, token)

	messages := make([]map[string]any, 0, len(resp.Value))
	var webLink string
	for _, raw := range resp.Value {
		msg := formatOutlookMessage(raw)
		msg["body"] = extractOutlookBody(raw)
		if senderEmail != "" {
			from, _ := msg["from"].(string)
			msg["is_outbound"] = strings.Contains(strings.ToLower(from), senderEmail)
		}
		if wl, _ := msg["url"].(string); wl != "" {
			webLink = wl
		}
		messages = append(messages, msg)
	}

	result := map[string]any{
		"conversation_id": conversationID,
		"messages":        messages,
	}
	if webLink != "" {
		result["url"] = webLink
	}
	return result, nil
}

func (o *OutlookToolClient) detectSenderEmail(ctx context.Context, token string) string {
	var profile map[string]any
	if err := o.api.RequestJSON(ctx, token, "GET", "/me?$select=mail,userPrincipalName", nil, &profile); err != nil {
		return ""
	}
	email := getString(profile, "mail")
	if email == "" {
		email = getString(profile, "userPrincipalName")
	}
	return strings.ToLower(email)
}

func (o *OutlookToolClient) createDraft(ctx context.Context, token, to, subject, body, conversationID string) (map[string]any, error) {
	payload := map[string]any{
		"subject": subject,
		"body": map[string]any{
			"contentType": "text",
			"content":     body,
		},
		"toRecipients": []map[string]any{
			{"emailAddress": map[string]any{"address": to}},
		},
	}
	if conversationID != "" {
		payload["conversationId"] = conversationID
	}

	var result map[string]any
	if err := o.api.RequestJSON(ctx, token, "POST", "/me/messages", payload, &result); err != nil {
		return nil, err
	}

	return formatOutlookMessageResult(to, subject, result), nil
}

func (o *OutlookToolClient) sendEmail(ctx context.Context, token, to, subject, body, conversationID, draftID string) (map[string]any, error) {
	if draftID != "" {
		// Send existing draft — returns 202 with no body
		endpoint := fmt.Sprintf("/me/messages/%s/send", draftID)
		if err := o.api.RequestJSON(ctx, token, "POST", endpoint, nil, nil); err != nil {
			return nil, err
		}
		out := map[string]any{
			"to":         to,
			"subject":    subject,
			"message_id": draftID,
			"status":     "sent",
		}
		if conversationID != "" {
			out["conversation_id"] = conversationID
		}
		return out, nil
	}

	// Compose and send new email
	message := map[string]any{
		"subject": subject,
		"body": map[string]any{
			"contentType": "text",
			"content":     body,
		},
		"toRecipients": []map[string]any{
			{"emailAddress": map[string]any{"address": to}},
		},
	}
	if conversationID != "" {
		message["conversationId"] = conversationID
	}
	payload := map[string]any{
		"message":         message,
		"saveToSentItems": true,
	}

	// sendMail returns 202 Accepted with no body
	if err := o.api.RequestJSON(ctx, token, "POST", "/me/sendMail", payload, nil); err != nil {
		return nil, err
	}

	out := map[string]any{
		"to":      to,
		"subject": subject,
		"status":  "sent",
	}
	if conversationID != "" {
		out["conversation_id"] = conversationID
	}
	return out, nil
}

// formatOutlookMessage extracts a normalized map from a Graph API message response.
func formatOutlookMessage(msg map[string]any) map[string]any {
	out := map[string]any{}

	if id := getString(msg, "id"); id != "" {
		out["message_id"] = id
	}
	if convID := getString(msg, "conversationId"); convID != "" {
		out["conversation_id"] = convID
	}
	if subject := getString(msg, "subject"); subject != "" {
		out["subject"] = subject
	}
	if snippet := getString(msg, "bodyPreview"); snippet != "" {
		out["snippet"] = snippet
	}
	if date := getString(msg, "receivedDateTime"); date != "" {
		out["date"] = date
	}
	if isRead, ok := msg["isRead"].(bool); ok {
		out["is_read"] = isRead
	}
	if webLink := getString(msg, "webLink"); webLink != "" {
		out["url"] = webLink
	}

	if from, ok := msg["from"].(map[string]any); ok {
		if ea, ok := from["emailAddress"].(map[string]any); ok {
			name := getString(ea, "name")
			addr := getString(ea, "address")
			if name != "" && addr != "" {
				out["from"] = fmt.Sprintf("%s <%s>", name, addr)
			} else if addr != "" {
				out["from"] = addr
			}
		}
	}

	if toList, ok := msg["toRecipients"].([]any); ok && len(toList) > 0 {
		var addrs []string
		for _, r := range toList {
			if recip, ok := r.(map[string]any); ok {
				if ea, ok := recip["emailAddress"].(map[string]any); ok {
					if addr := getString(ea, "address"); addr != "" {
						addrs = append(addrs, addr)
					}
				}
			}
		}
		if len(addrs) > 0 {
			out["to"] = strings.Join(addrs, ", ")
		}
	}

	return out
}

// extractOutlookBody gets the plain text body from a Graph message, falling back to HTML stripping.
func extractOutlookBody(msg map[string]any) string {
	body, ok := msg["body"].(map[string]any)
	if !ok {
		return getString(msg, "bodyPreview")
	}
	content := getString(body, "content")
	if content == "" {
		return getString(msg, "bodyPreview")
	}
	if strings.EqualFold(getString(body, "contentType"), "html") {
		return stripOutlookHTML(content)
	}
	return content
}

// stripOutlookHTML does basic HTML tag stripping for messages that ignore the Prefer: text header.
func stripOutlookHTML(html string) string {
	for _, tag := range []string{"<br>", "<br/>", "<br />", "</p>", "</div>", "</li>"} {
		html = strings.ReplaceAll(html, tag, "\n")
	}
	result := strings.Builder{}
	inTag := false
	for _, r := range html {
		switch {
		case r == '<':
			inTag = true
		case r == '>':
			inTag = false
		case !inTag:
			result.WriteRune(r)
		}
	}
	text := result.String()
	text = strings.ReplaceAll(text, "&nbsp;", " ")
	text = strings.ReplaceAll(text, "&amp;", "&")
	text = strings.ReplaceAll(text, "&lt;", "<")
	text = strings.ReplaceAll(text, "&gt;", ">")
	text = strings.ReplaceAll(text, "&quot;", `"`)
	text = strings.ReplaceAll(text, "&#39;", "'")

	lines := strings.Split(text, "\n")
	cleaned := make([]string, 0, len(lines))
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if trimmed != "" {
			cleaned = append(cleaned, trimmed)
		}
	}
	return strings.Join(cleaned, "\n")
}

// formatOutlookMessageResult formats a create/send response.
func formatOutlookMessageResult(to, subject string, result map[string]any) map[string]any {
	out := map[string]any{
		"to":      to,
		"subject": subject,
	}
	if id := getString(result, "id"); id != "" {
		out["message_id"] = id
	}
	if convID := getString(result, "conversationId"); convID != "" {
		out["conversation_id"] = convID
	}
	if webLink := getString(result, "webLink"); webLink != "" {
		out["url"] = webLink
	}
	return out
}
