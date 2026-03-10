package clients

import (
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"mime"
	"strings"
	"unicode/utf8"

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

// --- email construction helpers ---

func buildRawEmail(to, subject, body string) string {
	to = sanitizeHeaderValue(to)
	subject = sanitizeHeaderValue(subject)
	subject = repairMojibake(subject)
	subject = subjectNormalizer.Replace(subject)
	encodedSubject := mime.QEncoding.Encode("utf-8", subject)
	return fmt.Sprintf(
		"MIME-Version: 1.0\r\nTo: %s\r\nSubject: %s\r\nContent-Type: text/plain; charset=UTF-8\r\n\r\n%s",
		to,
		encodedSubject,
		body,
	)
}

func sanitizeHeaderValue(value string) string {
	value = strings.ReplaceAll(value, "\r", " ")
	value = strings.ReplaceAll(value, "\n", " ")
	return strings.TrimSpace(value)
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
