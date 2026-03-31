package views

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/beam-cloud/airstore/pkg/repository"
	"github.com/rs/zerolog/log"
)

const gmailFetchConcurrency = 10

const gmailAPIBase = "https://gmail.googleapis.com/gmail/v1"

type ThreadMessage struct {
	ID         string `json:"id"`
	ThreadID   string `json:"thread_id"`
	From       string `json:"from"`
	To         string `json:"to"`
	Subject    string `json:"subject"`
	Body       string `json:"body"`
	Snippet    string `json:"snippet"`
	Date       string `json:"date"`
	Timestamp  int64  `json:"timestamp"`
	IsOutbound bool   `json:"is_outbound"`
	Labels     []string `json:"labels,omitempty"`
	Deeplink   string `json:"deeplink,omitempty"`
}

type EmailThreadFetcher struct {
	backend    repository.BackendRepository
	httpClient *http.Client
}

func NewEmailThreadFetcher(backend repository.BackendRepository) *EmailThreadFetcher {
	return &EmailThreadFetcher{
		backend:    backend,
		httpClient: &http.Client{Timeout: 15 * time.Second},
	}
}

func (f *EmailThreadFetcher) FetchThreads(ctx context.Context, workspaceID uint, threadIDs []string) map[string][]ThreadMessage {
	if len(threadIDs) == 0 {
		return nil
	}

	token := f.getGmailToken(ctx, workspaceID)
	if token == "" {
		return nil
	}

	var mu sync.Mutex
	result := make(map[string][]ThreadMessage, len(threadIDs))
	sem := make(chan struct{}, gmailFetchConcurrency)
	var wg sync.WaitGroup

	for _, tid := range threadIDs {
		wg.Add(1)
		go func(tid string) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			messages, err := f.fetchThread(ctx, token, tid)
			if err != nil {
				log.Warn().Err(err).Str("thread_id", tid).Msg("failed to fetch gmail thread")
				return
			}
			if len(messages) > 0 {
				mu.Lock()
				result[tid] = messages
				mu.Unlock()
			}
		}(tid)
	}
	wg.Wait()
	return result
}

func (f *EmailThreadFetcher) getGmailToken(ctx context.Context, workspaceID uint) string {
	conn, err := f.backend.GetConnection(ctx, workspaceID, 0, "gmail")
	if err != nil {
		log.Debug().Err(err).Uint("workspace_id", workspaceID).Msg("no gmail connection for workspace")
		return ""
	}
	creds, err := repository.DecryptCredentials(conn)
	if err != nil || creds.AccessToken == "" {
		return ""
	}
	return creds.AccessToken
}

func (f *EmailThreadFetcher) fetchThread(ctx context.Context, token, threadID string) ([]ThreadMessage, error) {
	path := fmt.Sprintf("/users/me/threads/%s?format=full", threadID)
	var raw map[string]any
	if err := f.gmailGet(ctx, token, path, &raw); err != nil {
		return nil, err
	}

	rawMessages, _ := raw["messages"].([]any)
	if len(rawMessages) == 0 {
		return nil, nil
	}

	senderEmail := f.detectSenderEmail(ctx, token)
	if senderEmail == "" {
		log.Warn().Str("thread_id", threadID).Msg("gmail: sender email unknown, using SENT label fallback for outbound detection")
	}

	messages := make([]ThreadMessage, 0, len(rawMessages))
	for _, rm := range rawMessages {
		msg, ok := rm.(map[string]any)
		if !ok {
			continue
		}
		tm := f.parseThreadMessage(msg, threadID, senderEmail)
		messages = append(messages, tm)
	}

	sort.Slice(messages, func(i, j int) bool {
		return messages[i].Timestamp < messages[j].Timestamp
	})

	return stripSupersededDrafts(messages), nil
}

// stripSupersededDrafts removes draft messages from a thread when the thread
// also contains non-draft messages. A draft that has been sent gets replaced
// by a SENT message in the same thread — keeping both is confusing.
func stripSupersededDrafts(messages []ThreadMessage) []ThreadMessage {
	hasSent := false
	for _, m := range messages {
		if !hasLabel(m.Labels, "DRAFT") {
			hasSent = true
			break
		}
	}
	if !hasSent {
		return messages
	}
	filtered := make([]ThreadMessage, 0, len(messages))
	for _, m := range messages {
		if !hasLabel(m.Labels, "DRAFT") {
			filtered = append(filtered, m)
		}
	}
	if len(filtered) == 0 {
		return messages
	}
	return filtered
}

func hasLabel(labels []string, target string) bool {
	for _, l := range labels {
		if l == target {
			return true
		}
	}
	return false
}

func (f *EmailThreadFetcher) parseThreadMessage(msg map[string]any, threadID, senderEmail string) ThreadMessage {
	headers := extractPayloadHeaders(msg)
	msgID, _ := msg["id"].(string)
	snippet, _ := msg["snippet"].(string)

	var timestamp int64
	if raw, ok := msg["internalDate"].(string); ok {
		fmt.Sscanf(raw, "%d", &timestamp)
	}

	var labels []string
	if rawLabels, ok := msg["labelIds"].([]any); ok {
		for _, l := range rawLabels {
			if s, ok := l.(string); ok {
				labels = append(labels, s)
			}
		}
	}

	body := extractPlainBody(msg)

	from := headers["From"]
	isOutbound := senderEmail != "" && containsEmail(from, senderEmail)
	if !isOutbound && senderEmail == "" {
		for _, l := range labels {
			if l == "SENT" {
				isOutbound = true
				break
			}
		}
	}

	dateStr := headers["Date"]
	if dateStr == "" && timestamp > 0 {
		dateStr = time.UnixMilli(timestamp).UTC().Format(time.RFC3339)
	}

	return ThreadMessage{
		ID:         msgID,
		ThreadID:   threadID,
		From:       from,
		To:         headers["To"],
		Subject:    headers["Subject"],
		Body:       body,
		Snippet:    snippet,
		Date:       dateStr,
		Timestamp:  timestamp,
		IsOutbound: isOutbound,
		Labels:     labels,
		Deeplink:   fmt.Sprintf("https://mail.google.com/mail/u/0/#inbox/%s", threadID),
	}
}

func (f *EmailThreadFetcher) detectSenderEmail(ctx context.Context, token string) string {
	var profile map[string]any
	if err := f.gmailGet(ctx, token, "/users/me/profile", &profile); err != nil {
		log.Warn().Err(err).Msg("gmail: failed to detect sender email from profile, outbound detection will fall back to SENT label")
		return ""
	}
	email, _ := profile["emailAddress"].(string)
	return strings.ToLower(email)
}

func (f *EmailThreadFetcher) gmailGet(ctx context.Context, token, path string, result any) error {
	req, err := http.NewRequestWithContext(ctx, "GET", gmailAPIBase+path, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Accept", "application/json")

	resp, err := f.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("gmail API %d: %s", resp.StatusCode, string(body))
	}

	return json.NewDecoder(resp.Body).Decode(result)
}

func extractPayloadHeaders(msg map[string]any) map[string]string {
	headers := make(map[string]string)
	payload, _ := msg["payload"].(map[string]any)
	if payload == nil {
		return headers
	}
	hdrs, _ := payload["headers"].([]any)
	for _, h := range hdrs {
		hdr, ok := h.(map[string]any)
		if !ok {
			continue
		}
		name, _ := hdr["name"].(string)
		value, _ := hdr["value"].(string)
		switch name {
		case "From", "To", "Cc", "Subject", "Date", "Reply-To":
			headers[name] = value
		}
	}
	return headers
}

func extractPlainBody(msg map[string]any) string {
	payload, _ := msg["payload"].(map[string]any)
	if payload == nil {
		return ""
	}
	if text := extractMimePart(payload, "text/plain"); text != "" {
		return text
	}
	if body, ok := payload["body"].(map[string]any); ok {
		return decodeBase64Body(body)
	}
	return ""
}

func extractMimePart(part map[string]any, targetMime string) string {
	mimeType, _ := part["mimeType"].(string)
	if mimeType == targetMime {
		if body, ok := part["body"].(map[string]any); ok {
			return decodeBase64Body(body)
		}
	}
	if parts, ok := part["parts"].([]any); ok {
		for _, p := range parts {
			sub, ok := p.(map[string]any)
			if !ok {
				continue
			}
			if text := extractMimePart(sub, targetMime); text != "" {
				return text
			}
		}
	}
	return ""
}

func decodeBase64Body(body map[string]any) string {
	data, _ := body["data"].(string)
	if data == "" {
		return ""
	}
	data = strings.ReplaceAll(data, "-", "+")
	data = strings.ReplaceAll(data, "_", "/")
	decoded, err := base64.StdEncoding.DecodeString(data)
	if err != nil {
		decoded, err = base64.RawStdEncoding.DecodeString(data)
		if err != nil {
			return ""
		}
	}
	return string(decoded)
}

func containsEmail(headerValue, email string) bool {
	return strings.Contains(strings.ToLower(headerValue), email)
}
