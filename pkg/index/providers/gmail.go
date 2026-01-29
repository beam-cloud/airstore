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
	"sync/atomic"
	"time"

	"github.com/beam-cloud/airstore/pkg/index"
	"github.com/beam-cloud/airstore/pkg/types"
	"github.com/rs/zerolog/log"
)

const (
	gmailAPIBase = "https://gmail.googleapis.com/gmail/v1"

	// Gmail API quota: 15000 queries/minute/user
	// We use a conservative limit to leave headroom
	gmailRequestsPerSecond = 100 // 6000/minute - well under quota
)

// GmailIndexProvider implements IndexProvider for Gmail.
// It syncs emails from Gmail into the local index for fast searching.
type GmailIndexProvider struct {
	httpClient *http.Client

	// API call counter for metrics
	apiCalls int64

	// Rate limiter - token bucket
	rateLimiter chan struct{}
	rateOnce    sync.Once
}

// NewGmailIndexProvider creates a new Gmail index provider
func NewGmailIndexProvider() *GmailIndexProvider {
	g := &GmailIndexProvider{
		httpClient:  &http.Client{Timeout: 60 * time.Second},
		rateLimiter: make(chan struct{}, gmailRequestsPerSecond),
	}

	// Start rate limiter - refill tokens at gmailRequestsPerSecond rate
	go func() {
		ticker := time.NewTicker(time.Second / gmailRequestsPerSecond)
		defer ticker.Stop()
		for range ticker.C {
			select {
			case g.rateLimiter <- struct{}{}:
			default:
				// Bucket is full
			}
		}
	}()

	// Pre-fill bucket
	for i := 0; i < gmailRequestsPerSecond; i++ {
		g.rateLimiter <- struct{}{}
	}

	return g
}

// waitForRateLimit blocks until a rate limit token is available
func (g *GmailIndexProvider) waitForRateLimit(ctx context.Context) error {
	select {
	case <-g.rateLimiter:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Integration returns the integration name
func (g *GmailIndexProvider) Integration() string {
	return types.ToolGmail.String()
}

// SyncInterval returns the recommended sync interval
func (g *GmailIndexProvider) SyncInterval() time.Duration {
	return 30 * time.Second
}

// Sync fetches emails from Gmail and updates the index
func (g *GmailIndexProvider) Sync(ctx context.Context, store index.IndexStore, creds *types.IntegrationCredentials, since time.Time) error {
	if creds == nil || creds.AccessToken == "" {
		return fmt.Errorf("no credentials provided")
	}

	token := creds.AccessToken
	log.Info().Time("since", since).Msg("starting gmail index sync")

	// Paginate through ALL messages
	// For incremental sync, we could use historyId, but for now we sync all
	var allMessages []string
	pageToken := ""
	maxResultsPerPage := 500 // Max allowed by Gmail API

	for {
		path := fmt.Sprintf("/users/me/messages?maxResults=%d", maxResultsPerPage)
		if pageToken != "" {
			path += "&pageToken=" + pageToken
		}

		var listResult map[string]any
		if err := g.request(ctx, token, path, &listResult); err != nil {
			return fmt.Errorf("failed to list messages: %w", err)
		}

		rawMessages, _ := listResult["messages"].([]any)
		for _, m := range rawMessages {
			if msgMap, ok := m.(map[string]any); ok {
				if msgID, ok := msgMap["id"].(string); ok && msgID != "" {
					allMessages = append(allMessages, msgID)
				}
			}
		}

		log.Info().
			Int("page_count", len(rawMessages)).
			Int("total_so_far", len(allMessages)).
			Msg("fetched messages page")

		// Check for next page
		nextPageToken, _ := listResult["nextPageToken"].(string)
		if nextPageToken == "" {
			break
		}
		pageToken = nextPageToken
	}

	if len(allMessages) == 0 {
		log.Info().Msg("no messages to sync")
		return nil
	}

	// Filter out messages we already have - emails are immutable
	var newMessages []string
	for _, msgID := range allMessages {
		existing, err := store.Get(ctx, g.Integration(), msgID)
		if err == nil && existing != nil {
			// Already have this message, skip it
			continue
		}
		newMessages = append(newMessages, msgID)
	}

	log.Info().
		Int("total_messages", len(allMessages)).
		Int("already_indexed", len(allMessages)-len(newMessages)).
		Int("new_messages", len(newMessages)).
		Msg("gmail sync - filtering existing messages")

	if len(newMessages) == 0 {
		log.Info().Msg("all messages already indexed, nothing to sync")
		return nil
	}

	// Fetch message details in batches with concurrency limit
	var wg sync.WaitGroup
	semaphore := make(chan struct{}, 5) // Limit concurrent requests (reduced from 10)
	var errorCount int64
	var syncedCount int64

	for _, msgID := range newMessages {
		wg.Add(1)
		go func(msgID string) {
			defer wg.Done()
			semaphore <- struct{}{}
			defer func() { <-semaphore }()

			if err := g.syncMessage(ctx, store, token, msgID); err != nil {
				log.Warn().Err(err).Str("msg_id", msgID).Msg("failed to sync message")
				atomic.AddInt64(&errorCount, 1)
			} else {
				atomic.AddInt64(&syncedCount, 1)
				// Log progress every 100 messages
				if count := atomic.LoadInt64(&syncedCount); count%100 == 0 {
					log.Info().Int64("synced", count).Int("total", len(newMessages)).Msg("sync progress")
				}
			}
		}(msgID)
	}

	wg.Wait()

	log.Info().
		Int64("synced", syncedCount).
		Int64("errors", errorCount).
		Int64("api_calls", atomic.LoadInt64(&g.apiCalls)).
		Msg("gmail sync completed")

	return nil
}

// syncMessage fetches a single message and stores it in the index
func (g *GmailIndexProvider) syncMessage(ctx context.Context, store index.IndexStore, token, msgID string) error {
	// Fetch full message
	var result map[string]any
	path := fmt.Sprintf("/users/me/messages/%s?format=full", msgID)
	if err := g.request(ctx, token, path, &result); err != nil {
		return err
	}

	// Parse message
	msg := g.parseMessage(result)

	// Extract body text
	body := extractPlainTextBody(result)
	if body == "" {
		body = msg.Snippet
	}

	// Determine category (primary folder for this message)
	category := g.detectCategory(msg.Labels)

	// Build the filesystem path as it would appear in FUSE
	// Format: messages/{category}/{sender}/{date}_{subject}_{id}/
	folderPath := fmt.Sprintf("messages/%s/%s/%s", category, msg.SenderFolder, msg.SubjectFolder)

	// Create metadata JSON
	metadata := map[string]any{
		"id":        msg.ID,
		"thread_id": msg.ThreadID,
		"from":      msg.From,
		"to":        msg.To,
		"subject":   msg.Subject,
		"date":      msg.Date,
		"labels":    msg.Labels,
		"snippet":   msg.Snippet,
	}
	metadataJSON, _ := json.Marshal(metadata)

	// Create index entry for the email folder
	folderEntry := &index.IndexEntry{
		Integration: types.ToolGmail.String(),
		EntityID:    msgID,
		Path:        folderPath,
		ParentPath:  fmt.Sprintf("messages/%s/%s", category, msg.SenderFolder),
		Name:        msg.SubjectFolder,
		Type:        index.EntryTypeDir,
		Size:        0,
		ModTime:     parseDate(msg.Date),
		Title:       msg.Subject,
		Body:        body,
		Metadata:    string(metadataJSON),
	}

	if err := store.Upsert(ctx, folderEntry); err != nil {
		return fmt.Errorf("failed to upsert folder entry: %w", err)
	}

	// Create index entry for body.txt file
	bodyEntry := &index.IndexEntry{
		Integration: types.ToolGmail.String(),
		EntityID:    msgID + ":body",
		Path:        folderPath + "/body.txt",
		ParentPath:  folderPath,
		Name:        "body.txt",
		Type:        index.EntryTypeFile,
		Size:        int64(len(body)),
		ModTime:     parseDate(msg.Date),
		Title:       msg.Subject,
		Body:        body,
		Metadata:    string(metadataJSON),
	}

	if err := store.Upsert(ctx, bodyEntry); err != nil {
		return fmt.Errorf("failed to upsert body entry: %w", err)
	}

	// Create index entry for meta.json file
	metaEntry := &index.IndexEntry{
		Integration: types.ToolGmail.String(),
		EntityID:    msgID + ":meta",
		Path:        folderPath + "/meta.json",
		ParentPath:  folderPath,
		Name:        "meta.json",
		Type:        index.EntryTypeFile,
		Size:        int64(len(metadataJSON)),
		ModTime:     parseDate(msg.Date),
		Title:       msg.Subject,
		Body:        string(metadataJSON),
		Metadata:    string(metadataJSON),
	}

	if err := store.Upsert(ctx, metaEntry); err != nil {
		return fmt.Errorf("failed to upsert meta entry: %w", err)
	}

	// Create virtual directory entries for parent paths
	// This enables directory listing to work from the index
	ensureParentDirs(ctx, store, types.ToolGmail.String(), folderPath)

	return nil
}

// FetchContent retrieves email content for an entity ID
func (g *GmailIndexProvider) FetchContent(ctx context.Context, creds *types.IntegrationCredentials, entityID string) ([]byte, error) {
	if creds == nil || creds.AccessToken == "" {
		return nil, fmt.Errorf("no credentials provided")
	}

	// Parse entity ID to get message ID and type
	parts := strings.Split(entityID, ":")
	msgID := parts[0]
	contentType := ""
	if len(parts) > 1 {
		contentType = parts[1]
	}

	token := creds.AccessToken

	// Fetch full message
	var result map[string]any
	path := fmt.Sprintf("/users/me/messages/%s?format=full", msgID)
	if err := g.request(ctx, token, path, &result); err != nil {
		return nil, err
	}

	switch contentType {
	case "body":
		body := extractPlainTextBody(result)
		if body == "" {
			if snippet, ok := result["snippet"].(string); ok {
				body = fmt.Sprintf("(no plain text body)\n\nSnippet: %s", snippet)
			}
		}
		return []byte(body), nil
	case "meta":
		msg := g.parseMessage(result)
		return json.MarshalIndent(map[string]any{
			"id":        msg.ID,
			"thread_id": msg.ThreadID,
			"from":      msg.From,
			"to":        msg.To,
			"subject":   msg.Subject,
			"date":      msg.Date,
			"labels":    msg.Labels,
		}, "", "  ")
	default:
		body := extractPlainTextBody(result)
		return []byte(body), nil
	}
}

// gmailMessage holds parsed message data
type gmailMessage struct {
	ID            string
	ThreadID      string
	From          string
	To            string
	Subject       string
	Date          string
	Snippet       string
	Labels        []string
	SenderFolder  string
	SubjectFolder string
}

// parseMessage extracts relevant fields from a Gmail API response
func (g *GmailIndexProvider) parseMessage(result map[string]any) gmailMessage {
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

	// Parse headers
	if payload, ok := result["payload"].(map[string]any); ok {
		if headers, ok := payload["headers"].([]any); ok {
			for _, h := range headers {
				hdr, ok := h.(map[string]any)
				if !ok {
					continue
				}
				name, _ := hdr["name"].(string)
				value, _ := hdr["value"].(string)
				switch strings.ToLower(name) {
				case "from":
					msg.From = value
				case "to":
					msg.To = value
				case "subject":
					msg.Subject = value
				case "date":
					msg.Date = value
				}
			}
		}
	}

	// Generate folder names
	msg.SenderFolder = sanitizeFolderName(extractEmail(msg.From))
	msg.SubjectFolder = formatSubjectFolder(msg.Date, msg.Subject, msg.ID)

	return msg
}

// detectCategory determines the primary category for a message
func (g *GmailIndexProvider) detectCategory(labels []string) string {
	for _, label := range labels {
		switch label {
		case "UNREAD":
			return "unread"
		case "STARRED":
			return "starred"
		case "SENT":
			return "sent"
		case "IMPORTANT":
			return "important"
		}
	}
	return "inbox"
}

// request makes an HTTP request to the Gmail API with rate limiting and retries
func (g *GmailIndexProvider) request(ctx context.Context, token, path string, result any) error {
	// Wait for rate limit token
	if err := g.waitForRateLimit(ctx); err != nil {
		return err
	}

	atomic.AddInt64(&g.apiCalls, 1)

	url := gmailAPIBase + path

	// Retry with exponential backoff on rate limit errors
	maxRetries := 5
	backoff := time.Second

	for attempt := 0; attempt < maxRetries; attempt++ {
		req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+token)

		resp, err := g.httpClient.Do(req)
		if err != nil {
			return err
		}

		if resp.StatusCode == 200 {
			defer resp.Body.Close()
			return json.NewDecoder(resp.Body).Decode(result)
		}

		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		// Handle rate limiting with backoff
		if resp.StatusCode == 429 || resp.StatusCode == 403 {
			if strings.Contains(string(body), "rateLimitExceeded") || strings.Contains(string(body), "RATE_LIMIT_EXCEEDED") {
				log.Debug().
					Int("attempt", attempt+1).
					Dur("backoff", backoff).
					Str("path", path).
					Msg("rate limited, backing off")

				select {
				case <-time.After(backoff):
					backoff *= 2 // Exponential backoff
					if backoff > 30*time.Second {
						backoff = 30 * time.Second
					}
					continue
				case <-ctx.Done():
					return ctx.Err()
				}
			}
		}

		return fmt.Errorf("gmail API error %d: %s", resp.StatusCode, string(body))
	}

	return fmt.Errorf("gmail API rate limit exceeded after %d retries", maxRetries)
}

// Helper functions

func extractEmail(from string) string {
	if idx := strings.Index(from, "<"); idx >= 0 {
		end := strings.Index(from[idx:], ">")
		if end > 0 {
			return from[idx+1 : idx+end]
		}
	}
	return from
}

func sanitizeFolderName(name string) string {
	name = unsafeFilenameChars.ReplaceAllString(name, "_")
	name = strings.TrimSpace(name)
	if name == "" {
		name = "unknown"
	}
	if len(name) > 100 {
		name = name[:100]
	}
	return name
}

func formatSubjectFolder(dateStr, subject, msgID string) string {
	// Parse date
	datePrefix := ""
	if t, err := time.Parse(time.RFC1123Z, dateStr); err == nil {
		datePrefix = t.Format("2006-01-02")
	} else if t, err := time.Parse("Mon, 2 Jan 2006 15:04:05 -0700", dateStr); err == nil {
		datePrefix = t.Format("2006-01-02")
	} else {
		datePrefix = "unknown"
	}

	// Sanitize subject
	cleanSubject := sanitizeFolderName(subject)
	if cleanSubject == "" {
		cleanSubject = "no_subject"
	}
	if len(cleanSubject) > 50 {
		cleanSubject = cleanSubject[:50]
	}

	// Use first 8 chars of message ID
	shortID := msgID
	if len(shortID) > 8 {
		shortID = shortID[:8]
	}

	return fmt.Sprintf("%s_%s_%s", datePrefix, cleanSubject, shortID)
}

func parseDate(dateStr string) time.Time {
	formats := []string{
		time.RFC1123Z,
		"Mon, 2 Jan 2006 15:04:05 -0700",
		"Mon, 2 Jan 2006 15:04:05 MST",
		time.RFC3339,
	}
	for _, fmt := range formats {
		if t, err := time.Parse(fmt, dateStr); err == nil {
			return t
		}
	}
	return time.Now()
}

// extractPlainTextBody extracts plain text from a Gmail message payload
func extractPlainTextBody(result map[string]any) string {
	payload, ok := result["payload"].(map[string]any)
	if !ok {
		return ""
	}

	// Try to find text/plain part
	if body := findTextPart(payload, "text/plain"); body != "" {
		return body
	}

	// Fall back to text/html and strip tags
	if body := findTextPart(payload, "text/html"); body != "" {
		return stripHTML(body)
	}

	return ""
}

func findTextPart(part map[string]any, mimeType string) string {
	// Check this part's mime type
	if mime, ok := part["mimeType"].(string); ok && strings.HasPrefix(mime, mimeType) {
		if body, ok := part["body"].(map[string]any); ok {
			if data, ok := body["data"].(string); ok {
				decoded, _ := base64.URLEncoding.DecodeString(data)
				return string(decoded)
			}
		}
	}

	// Check child parts
	if parts, ok := part["parts"].([]any); ok {
		for _, p := range parts {
			if pMap, ok := p.(map[string]any); ok {
				if body := findTextPart(pMap, mimeType); body != "" {
					return body
				}
			}
		}
	}

	return ""
}

var htmlTagRegex = regexp.MustCompile(`<[^>]*>`)

func stripHTML(html string) string {
	text := htmlTagRegex.ReplaceAllString(html, " ")
	text = strings.ReplaceAll(text, "&nbsp;", " ")
	text = strings.ReplaceAll(text, "&amp;", "&")
	text = strings.ReplaceAll(text, "&lt;", "<")
	text = strings.ReplaceAll(text, "&gt;", ">")
	// Collapse whitespace
	text = regexp.MustCompile(`\s+`).ReplaceAllString(text, " ")
	return strings.TrimSpace(text)
}
