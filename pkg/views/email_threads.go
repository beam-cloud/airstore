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
	sourceclients "github.com/beam-cloud/airstore/pkg/sources/clients"
	"github.com/beam-cloud/airstore/pkg/types"
	"github.com/rs/zerolog/log"
)

const gmailFetchConcurrency = 10

const (
	gmailAPIBase   = "https://gmail.googleapis.com/gmail/v1"
	outlookAPIBase = "https://graph.microsoft.com/v1.0"
)

type ThreadMessage struct {
	ID         string   `json:"id"`
	ThreadID   string   `json:"thread_id"`
	From       string   `json:"from"`
	To         string   `json:"to"`
	Subject    string   `json:"subject"`
	Body       string   `json:"body"`
	Snippet    string   `json:"snippet"`
	Date       string   `json:"date"`
	Timestamp  int64    `json:"timestamp"`
	IsOutbound bool     `json:"is_outbound"`
	Labels     []string `json:"labels,omitempty"`
	Deeplink   string   `json:"deeplink,omitempty"`
}

type EmailThreadRef struct {
	ID          string
	Integration string
}

type EmailThreadFetcher struct {
	backend    repository.BackendRepository
	httpClient *http.Client
}

type gmailThreadSession struct {
	token       string
	senderEmail string
}

type outlookThreadSession struct {
	creds       *types.IntegrationCredentials
	senderEmail string
	client      *sourceclients.OutlookClient
}

func NewEmailThreadFetcher(backend repository.BackendRepository) *EmailThreadFetcher {
	return &EmailThreadFetcher{
		backend:    backend,
		httpClient: &http.Client{Timeout: 15 * time.Second},
	}
}

func (f *EmailThreadFetcher) FetchThreads(ctx context.Context, workspaceID uint, refs []EmailThreadRef) map[string][]ThreadMessage {
	refs = normalizeThreadRefs(refs)
	if len(refs) == 0 {
		return nil
	}

	needsGmail := false
	needsOutlook := false
	for _, ref := range refs {
		switch normalizeThreadIntegration(ref.Integration) {
		case string(types.SourceGmail):
			needsGmail = true
		case string(types.SourceOutlook):
			needsOutlook = true
		default:
			needsGmail = true
			needsOutlook = true
		}
	}

	var gmailSession *gmailThreadSession
	var outlookSession *outlookThreadSession
	if needsGmail {
		gmailSession = f.loadGmailSession(ctx, workspaceID)
	}
	if needsOutlook {
		outlookSession = f.loadOutlookSession(ctx, workspaceID)
	}
	if gmailSession == nil && outlookSession == nil {
		return nil
	}

	var mu sync.Mutex
	result := make(map[string][]ThreadMessage, len(refs))
	sem := make(chan struct{}, gmailFetchConcurrency)
	var wg sync.WaitGroup

	for _, ref := range refs {
		wg.Add(1)
		go func(ref EmailThreadRef) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			messages, err := f.fetchThreadByRef(ctx, ref, gmailSession, outlookSession)
			if err != nil {
				log.Warn().
					Err(err).
					Str("thread_id", ref.ID).
					Str("integration", normalizeThreadIntegration(ref.Integration)).
					Msg("failed to fetch email thread")
				return
			}
			if len(messages) > 0 {
				mu.Lock()
				result[ref.ID] = messages
				mu.Unlock()
			}
		}(ref)
	}
	wg.Wait()
	return result
}

func normalizeThreadRefs(refs []EmailThreadRef) []EmailThreadRef {
	seen := make(map[string]struct{}, len(refs))
	out := make([]EmailThreadRef, 0, len(refs))
	for _, ref := range refs {
		id := strings.TrimSpace(ref.ID)
		if id == "" {
			continue
		}
		integration := normalizeThreadIntegration(ref.Integration)
		key := integration + ":" + id
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, EmailThreadRef{ID: id, Integration: integration})
	}
	return out
}

func normalizeThreadIntegration(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case string(types.SourceGmail):
		return string(types.SourceGmail)
	case string(types.SourceOutlook):
		return string(types.SourceOutlook)
	default:
		return ""
	}
}

func (f *EmailThreadFetcher) fetchThreadByRef(
	ctx context.Context,
	ref EmailThreadRef,
	gmailSession *gmailThreadSession,
	outlookSession *outlookThreadSession,
) ([]ThreadMessage, error) {
	providers := []string{}
	switch normalizeThreadIntegration(ref.Integration) {
	case string(types.SourceGmail):
		if gmailSession != nil {
			providers = append(providers, string(types.SourceGmail))
		}
	case string(types.SourceOutlook):
		if outlookSession != nil {
			providers = append(providers, string(types.SourceOutlook))
		}
	default:
		if gmailSession != nil {
			providers = append(providers, string(types.SourceGmail))
		}
		if outlookSession != nil {
			providers = append(providers, string(types.SourceOutlook))
		}
	}
	if len(providers) == 0 {
		return nil, nil
	}

	var lastErr error
	for _, provider := range providers {
		var (
			messages []ThreadMessage
			err      error
		)
		switch provider {
		case string(types.SourceGmail):
			messages, err = f.fetchGmailThread(ctx, gmailSession, ref.ID)
		case string(types.SourceOutlook):
			messages, err = f.fetchOutlookConversation(ctx, outlookSession, ref.ID)
		}
		if err != nil {
			lastErr = err
			continue
		}
		if len(messages) > 0 {
			return messages, nil
		}
	}
	return nil, lastErr
}

func (f *EmailThreadFetcher) getConnectionCredentials(ctx context.Context, workspaceID uint, integration string) *types.IntegrationCredentials {
	conn, err := f.backend.GetConnection(ctx, workspaceID, 0, integration)
	if err != nil {
		log.Debug().
			Err(err).
			Uint("workspace_id", workspaceID).
			Str("integration", integration).
			Msg("no email connection for workspace")
		return nil
	}
	creds, err := repository.DecryptCredentials(conn)
	if err != nil || creds.AccessToken == "" {
		return nil
	}
	return creds
}

func (f *EmailThreadFetcher) loadGmailSession(ctx context.Context, workspaceID uint) *gmailThreadSession {
	creds := f.getConnectionCredentials(ctx, workspaceID, string(types.SourceGmail))
	if creds == nil {
		return nil
	}
	return &gmailThreadSession{
		token:       creds.AccessToken,
		senderEmail: f.detectGmailSenderEmail(ctx, creds.AccessToken),
	}
}

func (f *EmailThreadFetcher) loadOutlookSession(ctx context.Context, workspaceID uint) *outlookThreadSession {
	creds := f.getConnectionCredentials(ctx, workspaceID, string(types.SourceOutlook))
	if creds == nil {
		return nil
	}
	client := sourceclients.NewOutlookClient()
	client.HTTPClient = f.httpClient
	return &outlookThreadSession{
		creds:       creds,
		senderEmail: f.detectOutlookSenderEmail(ctx, creds),
		client:      client,
	}
}

func (f *EmailThreadFetcher) fetchGmailThread(ctx context.Context, session *gmailThreadSession, threadID string) ([]ThreadMessage, error) {
	if session == nil || session.token == "" {
		return nil, nil
	}
	path := fmt.Sprintf("/users/me/threads/%s?format=full", threadID)
	var raw map[string]any
	if err := f.gmailGet(ctx, session.token, path, &raw); err != nil {
		return nil, err
	}

	rawMessages, _ := raw["messages"].([]any)
	if len(rawMessages) == 0 {
		return nil, nil
	}

	if session.senderEmail == "" {
		log.Warn().Str("thread_id", threadID).Msg("gmail: sender email unknown, using SENT label fallback for outbound detection")
	}

	messages := make([]ThreadMessage, 0, len(rawMessages))
	for _, rm := range rawMessages {
		msg, ok := rm.(map[string]any)
		if !ok {
			continue
		}
		tm := f.parseGmailThreadMessage(msg, threadID, session.senderEmail)
		messages = append(messages, tm)
	}

	sort.Slice(messages, func(i, j int) bool {
		return messages[i].Timestamp < messages[j].Timestamp
	})

	return stripSupersededDrafts(messages), nil
}

// stripSupersededDrafts removes a draft only when there's a SENT message in
// the same thread with the same normalized subject and recipient — meaning
// the draft was actually sent and shouldn't appear alongside its sent twin.
// Active drafts (no matching SENT counterpart) are preserved.
func stripSupersededDrafts(messages []ThreadMessage) []ThreadMessage {
	type key struct{ subject, to string }
	norm := func(s string) string { return strings.TrimSpace(strings.ToLower(s)) }

	sent := make(map[key]struct{})
	for _, m := range messages {
		if HasLabel(m.Labels, "SENT") {
			sent[key{norm(m.Subject), norm(m.To)}] = struct{}{}
		}
	}
	if len(sent) == 0 {
		return messages
	}

	filtered := make([]ThreadMessage, 0, len(messages))
	for _, m := range messages {
		if HasLabel(m.Labels, "DRAFT") {
			if _, superseded := sent[key{norm(m.Subject), norm(m.To)}]; superseded {
				continue
			}
		}
		filtered = append(filtered, m)
	}
	if len(filtered) == 0 {
		return messages
	}
	return filtered
}

// HasLabel reports whether the label list contains the given target label.
func HasLabel(labels []string, target string) bool {
	for _, l := range labels {
		if l == target {
			return true
		}
	}
	return false
}

func (f *EmailThreadFetcher) fetchOutlookConversation(
	ctx context.Context,
	session *outlookThreadSession,
	conversationID string,
) ([]ThreadMessage, error) {
	if session == nil || session.creds == nil || session.client == nil {
		return nil, nil
	}
	list, err := session.client.ListConversationMessages(ctx, session.creds, conversationID)
	if err != nil {
		return nil, err
	}
	if list == nil || len(list.Messages) == 0 {
		return nil, nil
	}

	messages := make([]ThreadMessage, 0, len(list.Messages))
	for i := range list.Messages {
		msg := list.Messages[i]
		messages = append(messages, f.parseOutlookThreadMessage(&msg, conversationID, session.senderEmail))
	}

	sort.Slice(messages, func(i, j int) bool {
		return messages[i].Timestamp < messages[j].Timestamp
	})

	return stripSupersededDrafts(messages), nil
}

func (f *EmailThreadFetcher) parseGmailThreadMessage(msg map[string]any, threadID, senderEmail string) ThreadMessage {
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

func (f *EmailThreadFetcher) parseOutlookThreadMessage(
	msg *sourceclients.OutlookMessage,
	threadID, senderEmail string,
) ThreadMessage {
	if msg == nil {
		return ThreadMessage{}
	}

	isOutbound := senderEmail != "" && strings.EqualFold(strings.TrimSpace(msg.SenderEmail()), senderEmail)
	labels := make([]string, 0, 2)
	if msg.IsDraft {
		labels = append(labels, "DRAFT")
	} else if isOutbound {
		labels = append(labels, "SENT")
	}

	receivedAt := msg.ReceivedTime()
	dateStr := strings.TrimSpace(msg.ReceivedDateTime)
	if dateStr == "" && !receivedAt.IsZero() {
		dateStr = receivedAt.UTC().Format(time.RFC3339)
	}

	return ThreadMessage{
		ID:         strings.TrimSpace(msg.ID),
		ThreadID:   threadID,
		From:       formatOutlookParticipant(msg.From),
		To:         formatOutlookRecipients(msg.ToRecipients),
		Subject:    strings.TrimSpace(msg.Subject),
		Body:       extractOutlookThreadBody(msg.Body, msg.BodyPreview),
		Snippet:    strings.TrimSpace(msg.BodyPreview),
		Date:       dateStr,
		Timestamp:  receivedAt.UnixMilli(),
		IsOutbound: isOutbound || msg.IsDraft,
		Labels:     labels,
		Deeplink:   strings.TrimSpace(msg.WebLink),
	}
}

func (f *EmailThreadFetcher) detectGmailSenderEmail(ctx context.Context, token string) string {
	var profile map[string]any
	if err := f.gmailGet(ctx, token, "/users/me/profile", &profile); err != nil {
		log.Warn().Err(err).Msg("gmail: failed to detect sender email from profile, outbound detection will fall back to SENT label")
		return ""
	}
	email, _ := profile["emailAddress"].(string)
	return strings.ToLower(email)
}

func (f *EmailThreadFetcher) detectOutlookSenderEmail(ctx context.Context, creds *types.IntegrationCredentials) string {
	if creds == nil || creds.AccessToken == "" {
		return ""
	}
	var profile map[string]any
	if err := f.outlookGet(ctx, creds, "/me?$select=mail,userPrincipalName", &profile); err != nil {
		log.Warn().Err(err).Msg("outlook: failed to detect sender email from profile")
		return ""
	}
	if email, _ := profile["mail"].(string); strings.TrimSpace(email) != "" {
		return strings.ToLower(strings.TrimSpace(email))
	}
	email, _ := profile["userPrincipalName"].(string)
	return strings.ToLower(strings.TrimSpace(email))
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

func (f *EmailThreadFetcher) outlookGet(
	ctx context.Context,
	creds *types.IntegrationCredentials,
	path string,
	result any,
) error {
	req, err := http.NewRequestWithContext(ctx, "GET", outlookAPIBase+path, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+creds.AccessToken)
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Prefer", `outlook.body-content-type="text"`)

	resp, err := f.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("outlook API %d: %s", resp.StatusCode, string(body))
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

func formatOutlookParticipant(recipient *sourceclients.OutlookRecipient) string {
	if recipient == nil {
		return "unknown"
	}
	name := strings.TrimSpace(recipient.EmailAddress.Name)
	address := strings.TrimSpace(recipient.EmailAddress.Address)
	switch {
	case name != "" && address != "":
		return fmt.Sprintf("%s <%s>", name, address)
	case address != "":
		return address
	default:
		return name
	}
}

func formatOutlookRecipients(recipients []sourceclients.OutlookRecipient) string {
	if len(recipients) == 0 {
		return ""
	}
	formatted := make([]string, 0, len(recipients))
	for i := range recipients {
		value := formatOutlookParticipant(&recipients[i])
		if strings.TrimSpace(value) != "" && value != "unknown" {
			formatted = append(formatted, value)
		}
	}
	return strings.Join(formatted, ", ")
}

func extractOutlookThreadBody(body *sourceclients.OutlookMessageBody, preview string) string {
	if body == nil || strings.TrimSpace(body.Content) == "" {
		return strings.TrimSpace(preview)
	}
	content := body.Content
	if strings.EqualFold(body.ContentType, "html") {
		return stripOutlookHTML(content)
	}
	return strings.TrimSpace(content)
}

func stripOutlookHTML(html string) string {
	for _, tag := range []string{"<br>", "<br/>", "<br />", "</p>", "</div>", "</li>"} {
		html = strings.ReplaceAll(html, tag, "\n")
	}

	var b strings.Builder
	inTag := false
	for _, r := range html {
		switch {
		case r == '<':
			inTag = true
		case r == '>':
			inTag = false
		case !inTag:
			b.WriteRune(r)
		}
	}

	text := b.String()
	text = strings.ReplaceAll(text, "&nbsp;", " ")
	text = strings.ReplaceAll(text, "&amp;", "&")
	text = strings.ReplaceAll(text, "&lt;", "<")
	text = strings.ReplaceAll(text, "&gt;", ">")
	text = strings.ReplaceAll(text, "&quot;", `"`)
	text = strings.ReplaceAll(text, "&#39;", "'")

	lines := strings.Split(text, "\n")
	cleaned := make([]string, 0, len(lines))
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line != "" {
			cleaned = append(cleaned, line)
		}
	}
	return strings.Join(cleaned, "\n")
}
