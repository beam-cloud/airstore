package providers

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/beam-cloud/airstore/pkg/sources"
)

const (
	gmailAPIBase = "https://gmail.googleapis.com/gmail/v1"
)

// Gmail categories (folders under /messages/)
var gmailCategories = []string{"unread", "inbox", "starred", "sent", "important"}

// categoryQueries maps category names to Gmail API query/label
var categoryQueries = map[string]string{
	"unread":    "q=is:unread",
	"inbox":     "labelIds=INBOX",
	"starred":   "labelIds=STARRED",
	"sent":      "labelIds=SENT",
	"important": "labelIds=IMPORTANT",
}

// GmailProvider implements sources.Provider for Gmail integration.
// Structure: /sources/gmail/messages/{category}/{sender}/{subject}/
type GmailProvider struct {
	httpClient *http.Client

	// Cache for message metadata (keyed by category, holds messages with headers)
	cacheMu      sync.RWMutex
	messageCache map[string]*categoryCache
}

// categoryCache holds cached messages for a category
type categoryCache struct {
	messages  []gmailMessage
	fetchedAt time.Time
}

// gmailMessage holds parsed message data for folder organization
type gmailMessage struct {
	ID       string
	ThreadID string
	From     string
	To       string
	Subject  string
	Date     string
	Snippet  string
	Labels   []string

	// Derived folder names
	SenderFolder  string // sanitized sender email
	SubjectFolder string // sanitized subject with ID suffix
}

// NewGmailProvider creates a new Gmail source provider
func NewGmailProvider() *GmailProvider {
	return &GmailProvider{
		httpClient:   &http.Client{Timeout: 30 * time.Second},
		messageCache: make(map[string]*categoryCache),
	}
}

func (g *GmailProvider) Name() string {
	return "gmail"
}

// Stat returns file/directory attributes
func (g *GmailProvider) Stat(ctx context.Context, pctx *sources.ProviderContext, path string) (*sources.FileInfo, error) {
	if pctx.Credentials == nil || pctx.Credentials.AccessToken == "" {
		return nil, sources.ErrNotConnected
	}

	if path == "" {
		return sources.DirInfo(), nil
	}

	parts := strings.Split(path, "/")

	switch parts[0] {
	case "messages":
		return g.statMessages(ctx, pctx, parts[1:])
	case "labels":
		return g.statLabels(ctx, pctx, parts[1:])
	default:
		return nil, sources.ErrNotFound
	}
}

// ReadDir lists directory contents
func (g *GmailProvider) ReadDir(ctx context.Context, pctx *sources.ProviderContext, path string) ([]sources.DirEntry, error) {
	if pctx.Credentials == nil || pctx.Credentials.AccessToken == "" {
		return nil, sources.ErrNotConnected
	}

	if path == "" {
		return []sources.DirEntry{
			{Name: "messages", Mode: sources.ModeDir, IsDir: true, Mtime: sources.NowUnix()},
			{Name: "labels", Mode: sources.ModeDir, IsDir: true, Mtime: sources.NowUnix()},
		}, nil
	}

	parts := strings.Split(path, "/")

	switch parts[0] {
	case "messages":
		return g.readdirMessages(ctx, pctx, parts[1:])
	case "labels":
		return g.readdirLabels(ctx, pctx, parts[1:])
	default:
		return nil, sources.ErrNotFound
	}
}

// Read reads file content
func (g *GmailProvider) Read(ctx context.Context, pctx *sources.ProviderContext, path string, offset, length int64) ([]byte, error) {
	if pctx.Credentials == nil || pctx.Credentials.AccessToken == "" {
		return nil, sources.ErrNotConnected
	}

	parts := strings.Split(path, "/")

	switch parts[0] {
	case "messages":
		return g.readMessages(ctx, pctx, parts[1:], offset, length)
	case "labels":
		return g.readLabels(ctx, pctx, parts[1:], offset, length)
	default:
		return nil, sources.ErrNotFound
	}
}

// Readlink is not supported for Gmail
func (g *GmailProvider) Readlink(ctx context.Context, pctx *sources.ProviderContext, path string) (string, error) {
	return "", sources.ErrNotFound
}

// --- Messages ---
// /messages/{category}/{sender}/{subject}/meta.json
// /messages/{category}/{sender}/{subject}/body.txt

func (g *GmailProvider) statMessages(ctx context.Context, pctx *sources.ProviderContext, parts []string) (*sources.FileInfo, error) {
	switch len(parts) {
	case 0:
		// /messages/
		return sources.DirInfo(), nil
	case 1:
		// /messages/{category}
		if !isValidCategory(parts[0]) {
			return nil, sources.ErrNotFound
		}
		return sources.DirInfo(), nil
	case 2:
		// /messages/{category}/{sender}
		return sources.DirInfo(), nil
	case 3:
		// /messages/{category}/{sender}/{subject}
		return sources.DirInfo(), nil
	case 4:
		// /messages/{category}/{sender}/{subject}/{file}
		category, senderFolder, subjectFolder, file := parts[0], parts[1], parts[2], parts[3]
		msg, err := g.findMessage(ctx, pctx, category, senderFolder, subjectFolder)
		if err != nil {
			return nil, err
		}
		if msg == nil {
			return nil, sources.ErrNotFound
		}
		data, err := g.getMessageFileData(ctx, pctx, msg.ID, file)
		if err != nil {
			return nil, err
		}
		return sources.FileInfoFromBytes(data), nil
	default:
		return nil, sources.ErrNotFound
	}
}

func (g *GmailProvider) readdirMessages(ctx context.Context, pctx *sources.ProviderContext, parts []string) ([]sources.DirEntry, error) {
	switch len(parts) {
	case 0:
		// List categories: unread, inbox, starred, sent, important
		entries := make([]sources.DirEntry, len(gmailCategories))
		for i, cat := range gmailCategories {
			entries[i] = sources.DirEntry{
				Name:  cat,
				Mode:  sources.ModeDir,
				IsDir: true,
				Mtime: sources.NowUnix(),
			}
		}
		return entries, nil

	case 1:
		// List senders in category
		category := parts[0]
		if !isValidCategory(category) {
			return nil, sources.ErrNotFound
		}
		messages, err := g.getCategoryMessages(ctx, pctx, category)
		if err != nil {
			return nil, err
		}
		return g.listSenders(messages), nil

	case 2:
		// List subjects from sender in category
		category, senderFolder := parts[0], parts[1]
		if !isValidCategory(category) {
			return nil, sources.ErrNotFound
		}
		messages, err := g.getCategoryMessages(ctx, pctx, category)
		if err != nil {
			return nil, err
		}
		return g.listSubjectsFromSender(messages, senderFolder), nil

	case 3:
		// List files in message: meta.json, body.txt
		// We need to fetch file sizes for proper display
		category, senderFolder, subjectFolder := parts[0], parts[1], parts[2]
		msg, err := g.findMessage(ctx, pctx, category, senderFolder, subjectFolder)
		if err != nil {
			return nil, err
		}
		if msg == nil {
			return nil, sources.ErrNotFound
		}

		// Fetch actual file data to get sizes
		metaData, _ := g.getMessageFileData(ctx, pctx, msg.ID, "meta.json")
		bodyData, _ := g.getMessageFileData(ctx, pctx, msg.ID, "body.txt")

		return []sources.DirEntry{
			{Name: "meta.json", Mode: sources.ModeFile, Size: int64(len(metaData)), Mtime: sources.NowUnix()},
			{Name: "body.txt", Mode: sources.ModeFile, Size: int64(len(bodyData)), Mtime: sources.NowUnix()},
		}, nil

	default:
		return nil, sources.ErrNotDir
	}
}

func (g *GmailProvider) readMessages(ctx context.Context, pctx *sources.ProviderContext, parts []string, offset, length int64) ([]byte, error) {
	if len(parts) < 4 {
		return nil, sources.ErrIsDir
	}

	category, senderFolder, subjectFolder, file := parts[0], parts[1], parts[2], parts[3]

	msg, err := g.findMessage(ctx, pctx, category, senderFolder, subjectFolder)
	if err != nil {
		return nil, err
	}
	if msg == nil {
		return nil, sources.ErrNotFound
	}

	data, err := g.getMessageFileData(ctx, pctx, msg.ID, file)
	if err != nil {
		return nil, err
	}

	return sliceData(data, offset, length), nil
}

// --- Labels ---

func (g *GmailProvider) statLabels(ctx context.Context, pctx *sources.ProviderContext, parts []string) (*sources.FileInfo, error) {
	if len(parts) == 0 {
		return sources.DirInfo(), nil
	}
	if parts[0] == "labels.json" {
		data, err := g.fetchLabels(ctx, pctx.Credentials.AccessToken)
		if err != nil {
			return nil, err
		}
		return sources.FileInfoFromBytes(data), nil
	}
	return nil, sources.ErrNotFound
}

func (g *GmailProvider) readdirLabels(ctx context.Context, pctx *sources.ProviderContext, parts []string) ([]sources.DirEntry, error) {
	if len(parts) == 0 {
		return []sources.DirEntry{
			{Name: "labels.json", Mode: sources.ModeFile, Mtime: sources.NowUnix()},
		}, nil
	}
	return nil, sources.ErrNotDir
}

func (g *GmailProvider) readLabels(ctx context.Context, pctx *sources.ProviderContext, parts []string, offset, length int64) ([]byte, error) {
	if len(parts) == 0 {
		return nil, sources.ErrIsDir
	}
	if parts[0] != "labels.json" {
		return nil, sources.ErrNotFound
	}
	data, err := g.fetchLabels(ctx, pctx.Credentials.AccessToken)
	if err != nil {
		return nil, err
	}
	return sliceData(data, offset, length), nil
}

// --- Message helpers ---

func (g *GmailProvider) getCategoryMessages(ctx context.Context, pctx *sources.ProviderContext, category string) ([]gmailMessage, error) {
	// Check cache
	g.cacheMu.RLock()
	if cached, ok := g.messageCache[category]; ok && time.Since(cached.fetchedAt) < 2*time.Minute {
		g.cacheMu.RUnlock()
		return cached.messages, nil
	}
	g.cacheMu.RUnlock()

	// Fetch from API
	token := pctx.Credentials.AccessToken
	messages, err := g.fetchCategoryMessagesWithMeta(ctx, token, category)
	if err != nil {
		return nil, err
	}

	// Update cache
	g.cacheMu.Lock()
	g.messageCache[category] = &categoryCache{
		messages:  messages,
		fetchedAt: time.Now(),
	}
	g.cacheMu.Unlock()

	return messages, nil
}

func (g *GmailProvider) fetchCategoryMessagesWithMeta(ctx context.Context, token, category string) ([]gmailMessage, error) {
	query := categoryQueries[category]
	if query == "" {
		return nil, sources.ErrNotFound
	}

	// Fetch message IDs
	var listResult map[string]any
	path := fmt.Sprintf("/users/me/messages?%s&maxResults=50", query)
	if err := g.request(ctx, token, path, &listResult); err != nil {
		return nil, err
	}

	rawMessages, _ := listResult["messages"].([]any)
	if len(rawMessages) == 0 {
		return nil, nil
	}

	// Fetch metadata for each message
	messages := make([]gmailMessage, 0, len(rawMessages))
	for _, m := range rawMessages {
		msgMap, ok := m.(map[string]any)
		if !ok {
			continue
		}
		msgID, _ := msgMap["id"].(string)
		if msgID == "" {
			continue
		}

		// Fetch message metadata
		var msgResult map[string]any
		msgPath := fmt.Sprintf("/users/me/messages/%s?format=metadata", msgID)
		if err := g.request(ctx, token, msgPath, &msgResult); err != nil {
			continue // Skip failed messages
		}

		msg := g.parseMessage(msgResult)
		messages = append(messages, msg)
	}

	return messages, nil
}

func (g *GmailProvider) parseMessage(result map[string]any) gmailMessage {
	msg := gmailMessage{
		ID:       getString(result, "id"),
		ThreadID: getString(result, "threadId"),
		Snippet:  getString(result, "snippet"),
	}

	// Parse labels
	if labels, ok := result["labelIds"].([]any); ok {
		for _, l := range labels {
			if s, ok := l.(string); ok {
				msg.Labels = append(msg.Labels, s)
			}
		}
	}

	// Extract headers
	if payload, ok := result["payload"].(map[string]any); ok {
		if hdrs, ok := payload["headers"].([]any); ok {
			for _, h := range hdrs {
				if hdr, ok := h.(map[string]any); ok {
					name, _ := hdr["name"].(string)
					value, _ := hdr["value"].(string)
					switch name {
					case "From":
						msg.From = value
					case "To":
						msg.To = value
					case "Subject":
						msg.Subject = value
					case "Date":
						msg.Date = value
					}
				}
			}
		}
	}

	// Generate folder names
	msg.SenderFolder = extractSenderName(msg.From)
	msg.SubjectFolder = formatSubjectFolder(msg.Subject, msg.Date, msg.ID)

	return msg
}

func (g *GmailProvider) listSenders(messages []gmailMessage) []sources.DirEntry {
	seen := make(map[string]bool)
	var entries []sources.DirEntry

	for _, msg := range messages {
		if msg.SenderFolder != "" && !seen[msg.SenderFolder] {
			seen[msg.SenderFolder] = true
			entries = append(entries, sources.DirEntry{
				Name:  msg.SenderFolder,
				Mode:  sources.ModeDir,
				IsDir: true,
				Mtime: sources.NowUnix(),
			})
		}
	}
	return entries
}

func (g *GmailProvider) listSubjectsFromSender(messages []gmailMessage, senderFolder string) []sources.DirEntry {
	var entries []sources.DirEntry

	for _, msg := range messages {
		if msg.SenderFolder == senderFolder {
			entries = append(entries, sources.DirEntry{
				Name:  msg.SubjectFolder,
				Mode:  sources.ModeDir,
				IsDir: true,
				Mtime: sources.NowUnix(),
			})
		}
	}
	return entries
}

func (g *GmailProvider) findMessage(ctx context.Context, pctx *sources.ProviderContext, category, senderFolder, subjectFolder string) (*gmailMessage, error) {
	messages, err := g.getCategoryMessages(ctx, pctx, category)
	if err != nil {
		return nil, err
	}

	for i := range messages {
		msg := &messages[i]
		if msg.SenderFolder == senderFolder && msg.SubjectFolder == subjectFolder {
			return msg, nil
		}
	}
	return nil, nil
}

func (g *GmailProvider) getMessageFileData(ctx context.Context, pctx *sources.ProviderContext, msgID, file string) ([]byte, error) {
	token := pctx.Credentials.AccessToken

	switch file {
	case "meta.json":
		return g.fetchMessageMeta(ctx, token, msgID)
	case "body.txt":
		return g.fetchMessageBody(ctx, token, msgID)
	default:
		return nil, sources.ErrNotFound
	}
}

// --- API methods ---

func (g *GmailProvider) fetchLabels(ctx context.Context, token string) ([]byte, error) {
	var result map[string]any
	if err := g.request(ctx, token, "/users/me/labels", &result); err != nil {
		return nil, err
	}

	labels := result["labels"]
	if labels == nil {
		labels = []any{}
	}

	return jsonMarshal(map[string]any{
		"labels": labels,
		"count":  len(labels.([]any)),
	})
}

func (g *GmailProvider) fetchMessageMeta(ctx context.Context, token, msgId string) ([]byte, error) {
	var result map[string]any
	path := fmt.Sprintf("/users/me/messages/%s?format=metadata", msgId)
	if err := g.request(ctx, token, path, &result); err != nil {
		return nil, err
	}

	msg := g.parseMessage(result)

	return jsonMarshal(map[string]any{
		"id":       msg.ID,
		"threadId": msg.ThreadID,
		"labelIds": msg.Labels,
		"snippet":  msg.Snippet,
		"from":     msg.From,
		"to":       msg.To,
		"subject":  msg.Subject,
		"date":     msg.Date,
	})
}

func (g *GmailProvider) fetchMessageBody(ctx context.Context, token, msgId string) ([]byte, error) {
	var result map[string]any
	path := fmt.Sprintf("/users/me/messages/%s?format=full", msgId)
	if err := g.request(ctx, token, path, &result); err != nil {
		return nil, err
	}

	body := extractPlainTextBody(result)
	if body == "" {
		body = fmt.Sprintf("(no plain text body available)\n\nSnippet: %s", result["snippet"])
	}

	return []byte(body), nil
}

func extractPlainTextBody(msg map[string]any) string {
	payload, ok := msg["payload"].(map[string]any)
	if !ok {
		return ""
	}

	// Check if body is in payload.body
	if body, ok := payload["body"].(map[string]any); ok {
		if data, ok := body["data"].(string); ok && data != "" {
			decoded, _ := base64.URLEncoding.DecodeString(data)
			return string(decoded)
		}
	}

	// Check parts for text/plain
	if parts, ok := payload["parts"].([]any); ok {
		for _, p := range parts {
			part, ok := p.(map[string]any)
			if !ok {
				continue
			}
			mimeType, _ := part["mimeType"].(string)
			if mimeType == "text/plain" {
				if body, ok := part["body"].(map[string]any); ok {
					if data, ok := body["data"].(string); ok {
						decoded, _ := base64.URLEncoding.DecodeString(data)
						return string(decoded)
					}
				}
			}
		}
	}

	return ""
}

func (g *GmailProvider) request(ctx context.Context, token, path string, result any) error {
	url := gmailAPIBase + path
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return err
	}

	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Accept", "application/json")

	resp, err := g.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		var apiErr struct {
			Error struct {
				Message string `json:"message"`
			} `json:"error"`
		}
		json.Unmarshal(body, &apiErr)
		if apiErr.Error.Message != "" {
			return fmt.Errorf("gmail API: %s", apiErr.Error.Message)
		}
		return fmt.Errorf("gmail API: %s", resp.Status)
	}

	return json.NewDecoder(resp.Body).Decode(result)
}

// --- Helpers ---

func isValidCategory(cat string) bool {
	for _, c := range gmailCategories {
		if c == cat {
			return true
		}
	}
	return false
}

func getString(m map[string]any, key string) string {
	if v, ok := m[key].(string); ok {
		return v
	}
	return ""
}

// extractEmail extracts email address from "Name <email>" format
func extractEmail(from string) string {
	if idx := strings.Index(from, "<"); idx >= 0 {
		end := strings.Index(from[idx:], ">")
		if end > 0 {
			return from[idx+1 : idx+end]
		}
	}
	return from
}

// extractSenderName extracts a clean sender name for folder display
// "Raymond Xu <ray@example.com>" -> "Raymond_Xu"
// "noreply@calendly.com" -> "Calendly"
// "KAYAK <kayak@msg.kayak.com>" -> "KAYAK"
func extractSenderName(from string) string {
	// Try to extract name from "Name <email>" format
	if idx := strings.Index(from, "<"); idx > 0 {
		name := strings.TrimSpace(from[:idx])
		// Remove quotes if present
		name = strings.Trim(name, `"'`)
		if name != "" {
			return sanitizeFolderName(name)
		}
	}

	// Fall back to domain name from email
	email := extractEmail(from)
	if atIdx := strings.Index(email, "@"); atIdx > 0 {
		domain := email[atIdx+1:]
		// Extract base domain (e.g., "calendly" from "msg.calendly.com")
		parts := strings.Split(domain, ".")
		if len(parts) >= 2 {
			// Use second-to-last part (the main domain name)
			name := parts[len(parts)-2]
			// Capitalize first letter
			if len(name) > 0 {
				return strings.ToUpper(name[:1]) + name[1:]
			}
		}
	}
	return sanitizeFolderName(email)
}

// parseEmailDate extracts YYYY-MM-DD from email Date header
// Input: "Mon, 27 Jan 2026 14:30:00 -0500"
// Output: "2026-01-27"
func parseEmailDate(dateStr string) string {
	// Common email date formats
	formats := []string{
		time.RFC1123Z,                        // "Mon, 02 Jan 2006 15:04:05 -0700"
		time.RFC1123,                         // "Mon, 02 Jan 2006 15:04:05 MST"
		"Mon, 2 Jan 2006 15:04:05 -0700",     // Single digit day
		"Mon, 2 Jan 2006 15:04:05 MST",       // Single digit day with timezone name
		"2 Jan 2006 15:04:05 -0700",          // No weekday
		"Mon, 02 Jan 2006 15:04:05 -0700 (MST)", // With timezone name in parens
	}

	for _, format := range formats {
		if t, err := time.Parse(format, dateStr); err == nil {
			return t.Format("2006-01-02")
		}
	}

	// If parsing fails, return today's date as fallback
	return time.Now().Format("2006-01-02")
}

// formatSubjectFolder creates a date-first folder name for messages
// Format: "2026-01-27_Meeting_reminder_abc12345"
func formatSubjectFolder(subject, date, msgID string) string {
	datePrefix := parseEmailDate(date)
	subj := sanitizeFolderName(truncateSubject(subject, 30))

	// Ensure we have at least 8 chars of message ID
	idSuffix := msgID
	if len(idSuffix) > 8 {
		idSuffix = idSuffix[:8]
	}

	return fmt.Sprintf("%s_%s_%s", datePrefix, subj, idSuffix)
}

// sanitizeFolderName makes a string safe for use as a folder name
var unsafeChars = regexp.MustCompile(`[/\\:*?"<>|@\s]`)

func sanitizeFolderName(s string) string {
	s = unsafeChars.ReplaceAllString(s, "_")
	// Collapse multiple underscores
	for strings.Contains(s, "__") {
		s = strings.ReplaceAll(s, "__", "_")
	}
	s = strings.TrimSpace(s)
	s = strings.Trim(s, "_")
	if s == "" {
		s = "_unknown_"
	}
	return s
}

func truncateSubject(s string, maxLen int) string {
	s = strings.TrimSpace(s)
	if s == "" {
		s = "no_subject"
	}
	if len(s) > maxLen {
		s = s[:maxLen]
	}
	return s
}
