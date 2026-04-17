package clients

import (
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"mime"
	"net/url"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/beam-cloud/airstore/pkg/types"
)

const (
	gmailAPIBase        = "https://gmail.googleapis.com/gmail/v1/users/me"
	gmailCmdSearch      = "search"
	gmailCmdGetThread   = "get-thread"
	gmailCmdGetMessage  = "get-message"
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
		gmailCmdSearch: func(ctx context.Context, token string, args map[string]any) (any, error) {
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
			return g.search(ctx, token, required["query"], limit)
		},
		gmailCmdGetThread: func(ctx context.Context, token string, args map[string]any) (any, error) {
			required, err := RequireStringArgs(args, "thread_id")
			if err != nil {
				return nil, err
			}
			return g.getThread(ctx, token, required["thread_id"])
		},
		gmailCmdGetMessage: func(ctx context.Context, token string, args map[string]any) (any, error) {
			required, err := RequireStringArgs(args, "message_id")
			if err != nil {
				return nil, err
			}
			return g.getMessage(ctx, token, required["message_id"])
		},
		gmailCmdCreateDraft: func(ctx context.Context, token string, args map[string]any) (any, error) {
			required, err := RequireStringArgs(args, "to", "subject", "body")
			if err != nil {
				return nil, err
			}
			threadID := GetStringArg(args, "thread_id", "")
			return g.createDraft(ctx, token, required["to"], required["subject"], required["body"], threadID)
		},
		gmailCmdSendEmail: func(ctx context.Context, token string, args map[string]any) (any, error) {
			required, err := RequireStringArgs(args, "to", "subject", "body")
			if err != nil {
				return nil, err
			}
			threadID := GetStringArg(args, "thread_id", "")
			draftID := GetStringArg(args, "draft_id", "")
			return g.sendEmail(ctx, token, required["to"], required["subject"], required["body"], threadID, draftID)
		},
	}, stdout)
}

func (g *GmailClient) search(ctx context.Context, token, query string, limit int) (any, error) {
	path := fmt.Sprintf("/messages?q=%s&maxResults=%d", url.QueryEscape(query), limit)
	var listResult map[string]any
	if err := g.api.RequestJSON(ctx, token, "GET", path, nil, &listResult); err != nil {
		return nil, err
	}

	rawMessages, _ := listResult["messages"].([]any)
	if len(rawMessages) == 0 {
		return map[string]any{"results": []any{}, "total": 0, "query": query}, nil
	}

	results := make([]map[string]any, 0, len(rawMessages))
	for _, rm := range rawMessages {
		msgRef, ok := rm.(map[string]any)
		if !ok {
			continue
		}
		msgID := getString(msgRef, "id")
		if msgID == "" {
			continue
		}

		var msg map[string]any
		msgPath := fmt.Sprintf("/messages/%s?format=metadata&metadataHeaders=From&metadataHeaders=To&metadataHeaders=Subject&metadataHeaders=Date", msgID)
		if err := g.api.RequestJSON(ctx, token, "GET", msgPath, nil, &msg); err != nil {
			continue
		}

		results = append(results, formatGmailReadMessage(msg))
	}

	return map[string]any{"results": results, "total": len(results), "query": query}, nil
}

func (g *GmailClient) getThread(ctx context.Context, token, threadID string) (any, error) {
	path := fmt.Sprintf("/threads/%s?format=full", threadID)
	var raw map[string]any
	if err := g.api.RequestJSON(ctx, token, "GET", path, nil, &raw); err != nil {
		return nil, err
	}

	rawMessages, _ := raw["messages"].([]any)
	if len(rawMessages) == 0 {
		return map[string]any{"thread_id": threadID, "messages": []any{}}, nil
	}

	senderEmail := g.detectSenderEmail(ctx, token)

	messages := make([]map[string]any, 0, len(rawMessages))
	for _, rm := range rawMessages {
		msg, ok := rm.(map[string]any)
		if !ok {
			continue
		}
		parsed := formatGmailReadMessage(msg)
		parsed["body"] = extractGmailBody(msg)
		if senderEmail != "" {
			from, _ := parsed["from"].(string)
			parsed["is_outbound"] = strings.Contains(strings.ToLower(from), senderEmail)
		}
		messages = append(messages, parsed)
	}

	sort.Slice(messages, func(i, j int) bool {
		ti, _ := messages[i]["timestamp"].(int64)
		tj, _ := messages[j]["timestamp"].(int64)
		return ti < tj
	})

	// Strip internal timestamp from output
	for _, m := range messages {
		delete(m, "timestamp")
	}

	return map[string]any{
		"thread_id": threadID,
		"url":       "https://mail.google.com/mail/u/0/#inbox/" + threadID,
		"messages":  messages,
	}, nil
}

func (g *GmailClient) getMessage(ctx context.Context, token, messageID string) (any, error) {
	path := fmt.Sprintf("/messages/%s?format=full", messageID)
	var msg map[string]any
	if err := g.api.RequestJSON(ctx, token, "GET", path, nil, &msg); err != nil {
		return nil, err
	}

	result := formatGmailReadMessage(msg)
	result["body"] = extractGmailBody(msg)
	delete(result, "timestamp")
	return result, nil
}

func (g *GmailClient) detectSenderEmail(ctx context.Context, token string) string {
	var profile map[string]any
	if err := g.api.RequestJSON(ctx, token, "GET", "/profile", nil, &profile); err != nil {
		return ""
	}
	email := getString(profile, "emailAddress")
	return strings.ToLower(email)
}

func formatGmailReadMessage(msg map[string]any) map[string]any {
	out := map[string]any{}
	msgID := getString(msg, "id")
	threadID := getString(msg, "threadId")
	snippet := getString(msg, "snippet")

	if msgID != "" {
		out["message_id"] = msgID
	}
	if threadID != "" {
		out["thread_id"] = threadID
	}
	if snippet != "" {
		out["snippet"] = snippet
	}

	if labels, ok := msg["labelIds"].([]any); ok && len(labels) > 0 {
		out["labels"] = labels
	}

	var timestamp int64
	if raw, ok := msg["internalDate"].(string); ok {
		fmt.Sscanf(raw, "%d", &timestamp)
		out["timestamp"] = timestamp
	}

	if payload, ok := msg["payload"].(map[string]any); ok {
		if hdrs, ok := payload["headers"].([]any); ok {
			for _, h := range hdrs {
				hdr, ok := h.(map[string]any)
				if !ok {
					continue
				}
				name, _ := hdr["name"].(string)
				value, _ := hdr["value"].(string)
				switch name {
				case "From":
					out["from"] = value
				case "To":
					out["to"] = value
				case "Subject":
					out["subject"] = value
				case "Date":
					out["date"] = value
				}
			}
		}
	}

	linkID := threadID
	if linkID == "" {
		linkID = msgID
	}
	if linkID != "" {
		out["url"] = "https://mail.google.com/mail/u/0/#inbox/" + linkID
	}

	return out
}

// extractGmailBody extracts the best text body from a full-format Gmail message.
func extractGmailBody(msg map[string]any) string {
	payload, ok := msg["payload"].(map[string]any)
	if !ok {
		return ""
	}

	if text := extractGmailMimePart(payload, "text/plain"); text != "" {
		return text
	}
	if html := extractGmailMimePart(payload, "text/html"); html != "" {
		return stripGmailHTML(html)
	}
	if body, ok := payload["body"].(map[string]any); ok {
		return decodeGmailBodyData(body)
	}
	return ""
}

func extractGmailMimePart(part map[string]any, targetMime string) string {
	mimeType, _ := part["mimeType"].(string)
	if mimeType == targetMime {
		if body, ok := part["body"].(map[string]any); ok {
			return decodeGmailBodyData(body)
		}
	}
	if parts, ok := part["parts"].([]any); ok {
		for _, p := range parts {
			sub, ok := p.(map[string]any)
			if !ok {
				continue
			}
			if text := extractGmailMimePart(sub, targetMime); text != "" {
				return text
			}
		}
	}
	return ""
}

func decodeGmailBodyData(body map[string]any) string {
	data, ok := body["data"].(string)
	if !ok || data == "" {
		return ""
	}
	decoded, err := base64.RawURLEncoding.DecodeString(data)
	if err != nil {
		decoded, err = base64.URLEncoding.DecodeString(data)
		if err != nil {
			return ""
		}
	}
	return string(decoded)
}

func stripGmailHTML(html string) string {
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

func (g *GmailClient) createDraft(ctx context.Context, token, to, subject, body, threadID string) (map[string]any, error) {
	var inReplyTo, refs string
	if threadID != "" {
		inReplyTo, refs = g.fetchThreadReplyHeaders(ctx, token, threadID)
	}
	encoded := base64.RawURLEncoding.EncodeToString([]byte(buildRawEmail(to, subject, body, inReplyTo, refs)))
	payload := map[string]any{
		"message": map[string]any{
			"raw": encoded,
		},
	}
	if threadID != "" {
		payload["message"].(map[string]any)["threadId"] = threadID
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
	if threadID != "" && getString(out, "thread_id") == "" {
		out["thread_id"] = threadID
	}
	return out, nil
}

func (g *GmailClient) sendEmail(ctx context.Context, token, to, subject, body, threadID, draftID string) (map[string]any, error) {
	var inReplyTo, refs string
	if threadID != "" {
		inReplyTo, refs = g.fetchThreadReplyHeaders(ctx, token, threadID)
	}
	raw := base64.RawURLEncoding.EncodeToString([]byte(buildRawEmail(to, subject, body, inReplyTo, refs)))

	var (
		endpoint string
		payload  map[string]any
	)
	if draftID != "" {
		endpoint = "/drafts/send"
		msg := map[string]any{"raw": raw}
		if threadID != "" {
			msg["threadId"] = threadID
		}
		payload = map[string]any{"id": draftID, "message": msg}
	} else {
		endpoint = "/messages/send"
		payload = map[string]any{"raw": raw}
		if threadID != "" {
			payload["threadId"] = threadID
		}
	}

	var result map[string]any
	if err := g.api.RequestJSON(ctx, token, "POST", endpoint, payload, &result); err != nil {
		return nil, err
	}
	return formatGmailMessageResult(to, subject, result), nil
}

func formatGmailMessageResult(to, subject string, result map[string]any) map[string]any {
	out := map[string]any{
		"to":      to,
		"subject": subject,
	}
	msgID := getString(result, "id")
	threadID := getString(result, "threadId")
	if msgID != "" {
		out["message_id"] = msgID
	}
	if threadID != "" {
		out["thread_id"] = threadID
	}
	if labels, ok := result["labelIds"].([]any); ok && len(labels) > 0 {
		out["label_ids"] = labels
	}
	linkID := threadID
	if linkID == "" {
		linkID = msgID
	}
	if linkID != "" {
		out["url"] = "https://mail.google.com/mail/u/0/#inbox/" + linkID
	}
	return out
}

// fetchThreadReplyHeaders retrieves Message-ID headers from a Gmail thread
// and returns the In-Reply-To value and the full References chain for RFC 2822
// compliant threading. Returns the Message-ID of the most recent non-self
// message for In-Reply-To, and all Message-IDs for References.
func (g *GmailClient) fetchThreadReplyHeaders(ctx context.Context, token, threadID string) (inReplyTo string, references string) {
	p := fmt.Sprintf("/threads/%s?format=metadata&metadataHeaders=Message-Id", threadID)
	var raw map[string]any
	if err := g.api.RequestJSON(ctx, token, "GET", p, nil, &raw); err != nil {
		return "", ""
	}
	return extractReplyHeaders(raw)
}

func extractReplyHeaders(threadResponse map[string]any) (inReplyTo string, references string) {
	msgs, _ := threadResponse["messages"].([]any)
	if len(msgs) == 0 {
		return "", ""
	}

	var allMessageIDs []string
	for _, m := range msgs {
		msg, _ := m.(map[string]any)
		if msg == nil {
			continue
		}
		if mid := extractMessageIDHeader(msg); mid != "" {
			allMessageIDs = append(allMessageIDs, mid)
		}
	}
	if len(allMessageIDs) == 0 {
		return "", ""
	}

	inReplyTo = allMessageIDs[len(allMessageIDs)-1]
	references = strings.Join(allMessageIDs, " ")
	return inReplyTo, references
}

func extractMessageIDHeader(gmailMsg map[string]any) string {
	payload, _ := gmailMsg["payload"].(map[string]any)
	if payload == nil {
		return ""
	}
	headers, _ := payload["headers"].([]any)
	for _, h := range headers {
		hdr, _ := h.(map[string]any)
		if hdr == nil {
			continue
		}
		name, _ := hdr["name"].(string)
		if strings.EqualFold(name, "Message-Id") {
			if val, _ := hdr["value"].(string); val != "" {
				return val
			}
		}
	}
	return ""
}

// --- email construction helpers ---

func buildRawEmail(to, subject, body, inReplyTo, references string) string {
	to = sanitizeHeaderValue(to)
	subject = sanitizeHeaderValue(subject)
	subject = repairMojibake(subject)
	subject = subjectNormalizer.Replace(subject)

	var buf strings.Builder
	buf.WriteString("MIME-Version: 1.0\r\n")
	buf.WriteString("To: " + to + "\r\n")
	if !utf8.ValidString(subject) || needsEncoding(subject) {
		buf.WriteString("Subject: " + mime.QEncoding.Encode("utf-8", subject) + "\r\n")
	} else {
		buf.WriteString("Subject: " + subject + "\r\n")
	}
	if inReplyTo != "" {
		buf.WriteString("In-Reply-To: " + inReplyTo + "\r\n")
		refs := references
		if refs == "" {
			refs = inReplyTo
		}
		buf.WriteString("References: " + refs + "\r\n")
	}
	buf.WriteString("Content-Type: text/plain; charset=UTF-8\r\n")
	buf.WriteString("\r\n")
	buf.WriteString(unwrapBody(body))
	return buf.String()
}

// unwrapBody collapses hard line wraps within paragraphs so the email client
// does its own word wrapping. Double newlines (paragraph breaks) are preserved.
func unwrapBody(text string) string {
	text = strings.ReplaceAll(text, "\r\n", "\n")
	blocks := strings.Split(text, "\n\n")
	var parts []string
	for _, b := range blocks {
		b = strings.TrimSpace(b)
		if b == "" {
			continue
		}
		parts = append(parts, strings.Join(strings.Fields(b), " "))
	}
	return strings.Join(parts, "\n\n")
}

func sanitizeHeaderValue(value string) string {
	value = strings.ReplaceAll(value, "\r", " ")
	value = strings.ReplaceAll(value, "\n", " ")
	return strings.TrimSpace(value)
}

func needsEncoding(s string) bool {
	for _, r := range s {
		if r > 126 {
			return true
		}
	}
	return false
}

// Normalizes fancy Unicode punctuation to plain ASCII equivalents.
var subjectNormalizer = strings.NewReplacer(
	"—", "-", "–", "-",
	"\u2018", "'", "\u2019", "'",
	"\u201C", `"`, "\u201D", `"`, "\u201E", `"`,
	"…", "...", "•", "-", "×", "x",
)

// repairMojibake reverses double-encoded UTF-8 (UTF-8 bytes misinterpreted as
// CP1252/Latin-1, then re-encoded). Applies iteratively to handle multiple
// rounds of corruption (e.g. "Ã¢Â€Â"" → "â€"" → "—").
func repairMojibake(s string) string {
	for range 3 {
		next := tryReverseMojibake(s)
		if next == s {
			break
		}
		s = next
	}
	return s
}

func tryReverseMojibake(s string) string {
	buf := make([]byte, 0, len(s))
	for _, r := range s {
		if r < 0x100 {
			buf = append(buf, byte(r))
		} else if b, ok := cp1252Byte[r]; ok {
			buf = append(buf, b)
		} else {
			return s
		}
	}
	if utf8.Valid(buf) && len(buf) < len(s) {
		return string(buf)
	}
	return s
}

// Windows-1252 characters (0x80–0x9F) to their original byte values.
var cp1252Byte = map[rune]byte{
	'\u20AC': 0x80, '\u201A': 0x82, '\u0192': 0x83, '\u201E': 0x84,
	'\u2026': 0x85, '\u2020': 0x86, '\u2021': 0x87, '\u02C6': 0x88,
	'\u2030': 0x89, '\u0160': 0x8A, '\u2039': 0x8B, '\u0152': 0x8C,
	'\u017D': 0x8E, '\u2018': 0x91, '\u2019': 0x92, '\u201C': 0x93,
	'\u201D': 0x94, '\u2022': 0x95, '\u2013': 0x96, '\u2014': 0x97,
	'\u02DC': 0x98, '\u2122': 0x99, '\u0161': 0x9A, '\u203A': 0x9B,
	'\u0153': 0x9C, '\u017E': 0x9E, '\u0178': 0x9F,
}
