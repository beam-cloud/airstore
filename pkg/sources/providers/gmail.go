package providers

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/beam-cloud/airstore/pkg/sources"
	"github.com/beam-cloud/airstore/pkg/types"
	"github.com/rs/zerolog/log"
)

// API call counter for A/B testing
var gmailAPICallCount int64

// GetGmailAPICallCount returns the current API call count
func GetGmailAPICallCount() int64 {
	return atomic.LoadInt64(&gmailAPICallCount)
}

// ResetGmailAPICallCount resets the API call counter
func ResetGmailAPICallCount() {
	atomic.StoreInt64(&gmailAPICallCount, 0)
}

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

// gmailReadme is the README.md content for the Gmail integration
const gmailReadme = `# Gmail Integration

## Quick Start
- ` + "`cat unread.json`" + ` - See all unread emails
- ` + "`cat recent.json`" + ` - See 20 most recent emails
- ` + "`cat status.json`" + ` - Check connection status

## Structure
- ` + "`messages/unread/`" + ` - Unread emails by sender
- ` + "`messages/inbox/`" + ` - Inbox emails by sender
- ` + "`messages/starred/`" + ` - Starred emails
- ` + "`messages/sent/`" + ` - Sent emails
- ` + "`messages/important/`" + ` - Important emails

## Finding Emails
- By sender: ` + "`ls messages/unread/`" + ` shows senders
- By date: Folders sorted as ` + "`YYYY-MM-DD_Subject_id`" + `
- Full email: ` + "`cat messages/unread/Sender/2026-01-29_Subject_id/body.txt`" + `

## File Types
- ` + "`meta.json`" + ` - Email metadata (from, to, subject, date)
- ` + "`body.txt`" + ` - Plain text email body
- ` + "`index.json`" + ` - Summary of all emails in a category
`

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
	ID           string
	ThreadID     string
	From         string
	To           string
	Subject      string
	Date         string
	InternalDate int64 // Unix timestamp in milliseconds (reliable, from Gmail API)
	Snippet      string
	Labels       []string
	SizeEstimate int64 // Approximate size in bytes

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
	return types.ToolGmail.String()
}

// checkAuth validates that credentials are present
func checkAuth(pctx *sources.ProviderContext) error {
	if pctx.Credentials == nil || pctx.Credentials.AccessToken == "" {
		return sources.ErrNotConnected
	}
	return nil
}

// Stat returns file/directory attributes
func (g *GmailProvider) Stat(ctx context.Context, pctx *sources.ProviderContext, path string) (*sources.FileInfo, error) {
	if err := checkAuth(pctx); err != nil {
		return nil, err
	}

	if path == "" {
		return sources.DirInfo(), nil
	}

	parts := strings.Split(path, "/")

	switch parts[0] {
	case "README.md":
		return sources.FileInfoFromBytes([]byte(gmailReadme)), nil
	case "unread.json":
		data, err := g.generateUnreadJSON(ctx, pctx)
		if err != nil {
			return nil, err
		}
		return sources.FileInfoFromBytes(data), nil
	case "recent.json":
		data, err := g.generateRecentJSON(ctx, pctx)
		if err != nil {
			return nil, err
		}
		return sources.FileInfoFromBytes(data), nil
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
	if err := checkAuth(pctx); err != nil {
		return nil, err
	}

	if path == "" {
		// Root directory - return 0 for dynamic files to force stat lookup for real size
		return []sources.DirEntry{
			fileEntry("README.md", int64(len(gmailReadme))),
			fileEntry("unread.json", 0), // Dynamic - stat will return real size
			fileEntry("recent.json", 0), // Dynamic - stat will return real size
			dirEntry("messages"),
			dirEntry("labels"),
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
	if err := checkAuth(pctx); err != nil {
		return nil, err
	}

	parts := strings.Split(path, "/")

	switch parts[0] {
	case "README.md":
		return sliceData([]byte(gmailReadme), offset, length), nil
	case "unread.json":
		data, err := g.generateUnreadJSON(ctx, pctx)
		if err != nil {
			return nil, err
		}
		return sliceData(data, offset, length), nil
	case "recent.json":
		data, err := g.generateRecentJSON(ctx, pctx)
		if err != nil {
			return nil, err
		}
		return sliceData(data, offset, length), nil
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

// Search executes a Gmail search query and returns results
// The query is passed directly to Gmail's search API (e.g., "is:unread", "from:john newer_than:7d")
func (g *GmailProvider) Search(ctx context.Context, pctx *sources.ProviderContext, query string, limit int) ([]sources.SearchResult, error) {
	if err := checkAuth(pctx); err != nil {
		return nil, err
	}

	if limit <= 0 {
		limit = 50
	}

	token := pctx.Credentials.AccessToken

	// Fetch message IDs using the query
	var listResult map[string]any
	path := fmt.Sprintf("/users/me/messages?q=%s&maxResults=%d", url.QueryEscape(query), limit)
	if err := g.request(ctx, token, path, &listResult); err != nil {
		return nil, err
	}

	rawMessages, _ := listResult["messages"].([]any)
	if len(rawMessages) == 0 {
		return []sources.SearchResult{}, nil
	}

	// Collect message IDs
	msgIDs := make([]string, 0, len(rawMessages))
	for _, m := range rawMessages {
		msgMap, ok := m.(map[string]any)
		if !ok {
			continue
		}
		msgID, _ := msgMap["id"].(string)
		if msgID != "" {
			msgIDs = append(msgIDs, msgID)
		}
	}

	// Fetch metadata concurrently (with semaphore to limit parallelism)
	const maxConcurrent = 10
	sem := make(chan struct{}, maxConcurrent)
	var wg sync.WaitGroup
	var mu sync.Mutex

	results := make([]sources.SearchResult, 0, len(msgIDs))

	for _, msgID := range msgIDs {
		wg.Add(1)
		sem <- struct{}{} // Acquire semaphore

		go func(id string) {
			defer wg.Done()
			defer func() { <-sem }() // Release semaphore

			// Fetch message metadata
			var msgResult map[string]any
			msgPath := fmt.Sprintf("/users/me/messages/%s?format=metadata", id)
			if err := g.request(ctx, token, msgPath, &msgResult); err != nil {
				return // Skip failed messages
			}

			msg := g.parseMessage(msgResult)

			// Generate a meaningful filename: {date}_{subject}_{id}.txt
			filename := g.searchResultFilename(msg)

			mu.Lock()
			results = append(results, sources.SearchResult{
				Name:    filename,
				Id:      id,
				Mode:    sources.ModeFile,
				Size:    0, // Unknown until read
				Mtime:   getMessageTimestamp(msg),
				Preview: msg.Snippet,
			})
			mu.Unlock()
		}(msgID)
	}

	wg.Wait()
	return results, nil
}

// searchResultFilename generates a meaningful filename for a search result
// Format: {date}_{subject_slug}_{id}.txt
func (g *GmailProvider) searchResultFilename(msg gmailMessage) string {
	datePrefix := parseEmailDate(msg.Date)
	subj := sanitizeFolderName(truncateSubject(msg.Subject, 40))

	// Ensure we have at least 8 chars of message ID for uniqueness
	idSuffix := msg.ID
	if len(idSuffix) > 8 {
		idSuffix = idSuffix[:8]
	}

	return fmt.Sprintf("%s_%s_%s.txt", datePrefix, subj, idSuffix)
}

// parseEmailTimestamp converts email date string to Unix timestamp
// getMessageTimestamp returns the Unix timestamp for a message.
// Prefers internalDate (reliable millisecond timestamp from Gmail API),
// falls back to parsing the Date header if internalDate is not available.
func getMessageTimestamp(msg gmailMessage) int64 {
	// Prefer internalDate (milliseconds since epoch) - most reliable
	if msg.InternalDate > 0 {
		return msg.InternalDate / 1000 // Convert ms to seconds
	}
	// Fall back to parsing Date header
	return parseEmailTimestamp(msg.Date)
}

func parseEmailTimestamp(dateStr string) int64 {
	if dateStr == "" {
		return time.Now().Unix()
	}

	formats := []string{
		time.RFC1123Z,
		time.RFC1123,
		"Mon, 2 Jan 2006 15:04:05 -0700",
		"Mon, 2 Jan 2006 15:04:05 MST",
		"2 Jan 2006 15:04:05 -0700",
		"Mon, 02 Jan 2006 15:04:05 -0700 (MST)",
		"Mon, 2 Jan 2006 15:04:05 -0700 (MST)",
		"2006-01-02T15:04:05Z",                 // ISO 8601
		"2006-01-02T15:04:05-07:00",            // ISO 8601 with timezone
		time.RFC3339,                           // Standard RFC3339
		"Mon Jan 2 15:04:05 MST 2006",          // Another common format
		"Mon Jan 02 15:04:05 MST 2006",         // Variant
		"02 Jan 2006 15:04:05 -0700",           // Variant without day name
	}

	for _, format := range formats {
		if t, err := time.Parse(format, dateStr); err == nil {
			return t.Unix()
		}
	}

	log.Debug().Str("date", dateStr).Msg("failed to parse email date, using current time")
	return time.Now().Unix()
}

// ReadSearchResult reads the content of a search result by message ID
func (g *GmailProvider) ReadSearchResult(ctx context.Context, pctx *sources.ProviderContext, messageID string) ([]byte, error) {
	if err := checkAuth(pctx); err != nil {
		return nil, err
	}
	return g.fetchMessageBody(ctx, pctx.Credentials.AccessToken, messageID)
}

// ============================================================================
// QueryExecutor implementation
// ============================================================================

// ExecuteQuery runs a Gmail search query and returns results with generated filenames.
// This implements the sources.QueryExecutor interface for filesystem queries.
func (g *GmailProvider) ExecuteQuery(ctx context.Context, pctx *sources.ProviderContext, spec sources.QuerySpec) ([]sources.QueryResult, error) {
	if err := checkAuth(pctx); err != nil {
		return nil, err
	}

	limit := spec.Limit
	if limit <= 0 {
		limit = 50
	}

	token := pctx.Credentials.AccessToken

	// Fetch message IDs using the query
	var listResult map[string]any
	path := fmt.Sprintf("/users/me/messages?q=%s&maxResults=%d", url.QueryEscape(spec.Query), limit)
	if err := g.request(ctx, token, path, &listResult); err != nil {
		return nil, err
	}

	rawMessages, _ := listResult["messages"].([]any)
	if len(rawMessages) == 0 {
		return []sources.QueryResult{}, nil
	}

	// Collect message IDs
	msgIDs := make([]string, 0, len(rawMessages))
	for _, m := range rawMessages {
		msgMap, ok := m.(map[string]any)
		if !ok {
			continue
		}
		msgID, _ := msgMap["id"].(string)
		if msgID != "" {
			msgIDs = append(msgIDs, msgID)
		}
	}

	metadataResults, err := g.fetchMessagesMetadataBatch(ctx, token, msgIDs)
	if err != nil {
		return nil, err
	}

	results := make([]sources.QueryResult, 0, len(metadataResults))
	filenameFormat := spec.FilenameFormat
	if filenameFormat == "" {
		filenameFormat = sources.DefaultFilenameFormat("gmail")
	}

	for _, msgResult := range metadataResults {
		msg := g.parseMessage(msgResult)
		if msg.ID == "" {
			continue
		}

		metadata := map[string]string{
			"id":      msg.ID,
			"from":    extractSenderName(msg.From),
			"to":      msg.To,
			"subject": sanitizeFolderName(truncateSubject(msg.Subject, 40)),
			"date":    parseEmailDate(msg.Date),
			"snippet": msg.Snippet,
		}

		filename := g.FormatFilename(filenameFormat, metadata)

		results = append(results, sources.QueryResult{
			ID:       msg.ID,
			Filename: filename,
			Metadata: metadata,
			Size:     msg.SizeEstimate,
			Mtime:    getMessageTimestamp(msg),
		})
	}

	return results, nil
}

// ReadResult fetches the content of an email by its message ID.
// This implements the sources.QueryExecutor interface.
func (g *GmailProvider) ReadResult(ctx context.Context, pctx *sources.ProviderContext, resultID string) ([]byte, error) {
	if err := checkAuth(pctx); err != nil {
		return nil, err
	}
	return g.fetchMessageBody(ctx, pctx.Credentials.AccessToken, resultID)
}

// FormatFilename generates a filename from metadata using a format template.
// Supported placeholders: {id}, {date}, {from}, {to}, {subject}, {snippet}
// This implements the sources.QueryExecutor interface.
func (g *GmailProvider) FormatFilename(format string, metadata map[string]string) string {
	if format == "" {
		format = "{date}_{from}_{subject}_{id}.txt"
	}

	result := format
	for key, value := range metadata {
		placeholder := "{" + key + "}"
		// Sanitize the value for filesystem use
		safeValue := sanitizeFolderName(value)
		// Truncate long values (except id)
		if key != "id" && len(safeValue) > 40 {
			safeValue = safeValue[:40]
		}
		result = strings.ReplaceAll(result, placeholder, safeValue)
	}

	// Ensure filename is not empty
	if result == "" || result == ".txt" {
		if id, ok := metadata["id"]; ok {
			result = id + ".txt"
		} else {
			result = "unknown.txt"
		}
	}

	return result
}

// Compile-time interface check for QueryExecutor
var _ sources.QueryExecutor = (*GmailProvider)(nil)

// ============================================================================
// detectCategory determines which category a message belongs to based on its labels
func (g *GmailProvider) detectCategory(labels []string) string {
	labelSet := make(map[string]bool)
	for _, l := range labels {
		labelSet[l] = true
	}

	// Check in order of specificity
	if labelSet["UNREAD"] {
		return "unread"
	}
	if labelSet["STARRED"] {
		return "starred"
	}
	if labelSet["SENT"] {
		return "sent"
	}
	if labelSet["IMPORTANT"] {
		return "important"
	}
	if labelSet["INBOX"] {
		return "inbox"
	}

	// Default to inbox
	return "inbox"
}

// isGmailQueryOperator checks if a pattern looks like a Gmail query operator

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
		// /messages/{category}/{sender} OR /messages/{category}/index.json
		category := parts[0]
		if parts[1] == "index.json" {
			data, err := g.generateCategoryIndexJSON(ctx, pctx, category)
			if err != nil {
				return nil, err
			}
			return sources.FileInfoFromBytes(data), nil
		}
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
		// List categories
		entries := make([]sources.DirEntry, len(gmailCategories))
		for i, cat := range gmailCategories {
			entries[i] = dirEntry(cat)
		}
		return entries, nil

	case 1:
		// List senders in category (plus index.json)
		messages, err := g.getValidatedCategoryMessages(ctx, pctx, parts[0])
		if err != nil {
			return nil, err
		}
		entries := []sources.DirEntry{fileEntry("index.json", 0)} // Stat returns real size
		entries = append(entries, g.listSenders(messages)...)
		return entries, nil

	case 2:
		// List subjects from sender
		messages, err := g.getValidatedCategoryMessages(ctx, pctx, parts[0])
		if err != nil {
			return nil, err
		}
		return g.listSubjectsFromSender(messages, parts[1]), nil

	case 3:
		// List files in message: meta.json, body.txt
		msg, err := g.findMessage(ctx, pctx, parts[0], parts[1], parts[2])
		if err != nil {
			return nil, err
		}
		if msg == nil {
			return nil, sources.ErrNotFound
		}
		metaData, _ := g.getMessageFileData(ctx, pctx, msg.ID, "meta.json")
		bodyData, _ := g.getMessageFileData(ctx, pctx, msg.ID, "body.txt")
		return []sources.DirEntry{
			fileEntry("meta.json", int64(len(metaData))),
			fileEntry("body.txt", int64(len(bodyData))),
		}, nil

	default:
		return nil, sources.ErrNotDir
	}
}

func (g *GmailProvider) readMessages(ctx context.Context, pctx *sources.ProviderContext, parts []string, offset, length int64) ([]byte, error) {
	// Handle index.json at category level: /messages/{category}/index.json
	if len(parts) == 2 && parts[1] == "index.json" {
		category := parts[0]
		data, err := g.generateCategoryIndexJSON(ctx, pctx, category)
		if err != nil {
			return nil, err
		}
		return sliceData(data, offset, length), nil
	}

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
		return []sources.DirEntry{fileEntry("labels.json", 0)}, nil // Stat returns real size
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

const categoryCacheTTL = 10 * time.Minute // Cache Gmail data for 10 minutes

// getValidatedCategoryMessages validates the category and returns cached/fetched messages
func (g *GmailProvider) getValidatedCategoryMessages(ctx context.Context, pctx *sources.ProviderContext, category string) ([]gmailMessage, error) {
	if !isValidCategory(category) {
		return nil, sources.ErrNotFound
	}
	return g.getCategoryMessages(ctx, pctx, category)
}

func (g *GmailProvider) getCategoryMessages(ctx context.Context, pctx *sources.ProviderContext, category string) ([]gmailMessage, error) {
	// Check cache
	g.cacheMu.RLock()
	if cached, ok := g.messageCache[category]; ok && time.Since(cached.fetchedAt) < categoryCacheTTL {
		g.cacheMu.RUnlock()
		return cached.messages, nil
	}
	g.cacheMu.RUnlock()

	// Trigger background prefetch of all categories (only fetches uncached ones)
	go g.prefetchAllCategories(pctx.Credentials.AccessToken)

	// Fetch the requested category synchronously
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

// prefetchAllCategories fetches all categories in parallel (background)
func (g *GmailProvider) prefetchAllCategories(token string) {
	var wg sync.WaitGroup
	for _, cat := range gmailCategories {
		// Skip if already cached
		g.cacheMu.RLock()
		cached, ok := g.messageCache[cat]
		isFresh := ok && time.Since(cached.fetchedAt) < categoryCacheTTL
		g.cacheMu.RUnlock()

		if isFresh {
			continue
		}

		wg.Add(1)
		go func(category string) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			messages, err := g.fetchCategoryMessagesWithMeta(ctx, token, category)
			if err != nil {
				return // Silently fail background prefetch
			}

			g.cacheMu.Lock()
			g.messageCache[category] = &categoryCache{
				messages:  messages,
				fetchedAt: time.Now(),
			}
			g.cacheMu.Unlock()
		}(cat)
	}
	wg.Wait()
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

	// Parse sizeEstimate (Gmail API returns this at the message level)
	if size, ok := result["sizeEstimate"].(float64); ok {
		msg.SizeEstimate = int64(size)
	}

	// Parse internalDate - a reliable Unix timestamp in milliseconds
	if internalDate, ok := result["internalDate"].(string); ok {
		if ms, err := strconv.ParseInt(internalDate, 10, 64); err == nil {
			msg.InternalDate = ms
		}
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
			entries = append(entries, dirEntry(msg.SenderFolder))
		}
	}
	return entries
}

func (g *GmailProvider) listSubjectsFromSender(messages []gmailMessage, senderFolder string) []sources.DirEntry {
	var entries []sources.DirEntry
	for _, msg := range messages {
		if msg.SenderFolder == senderFolder {
			entries = append(entries, dirEntry(msg.SubjectFolder))
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

func (g *GmailProvider) fetchMessagesMetadataBatch(ctx context.Context, token string, msgIDs []string) ([]map[string]any, error) {
	if len(msgIDs) == 0 {
		return []map[string]any{}, nil
	}

	boundary := fmt.Sprintf("batch_%d", time.Now().UnixNano())
	var body bytes.Buffer
	for _, id := range msgIDs {
		if id == "" {
			continue
		}
		fmt.Fprintf(&body, "--%s\r\n", boundary)
		body.WriteString("Content-Type: application/http\r\n")
		body.WriteString("Content-Transfer-Encoding: binary\r\n\r\n")
		fmt.Fprintf(&body, "GET /gmail/v1/users/me/messages/%s?format=metadata HTTP/1.1\r\n\r\n", id)
	}
	fmt.Fprintf(&body, "--%s--\r\n", boundary)

	count := atomic.AddInt64(&gmailAPICallCount, 1)
	log.Debug().Int64("api_calls", count).Str("path", "/batch/gmail/v1").Msg("gmail API call")

	req, err := http.NewRequestWithContext(ctx, "POST", "https://gmail.googleapis.com/batch/gmail/v1", &body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "multipart/mixed; boundary="+boundary)

	resp, err := g.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		bodyBytes, _ := io.ReadAll(resp.Body)
		var apiErr struct {
			Error struct {
				Message string `json:"message"`
			} `json:"error"`
		}
		_ = json.Unmarshal(bodyBytes, &apiErr)
		if apiErr.Error.Message != "" {
			return nil, fmt.Errorf("gmail batch API: %s", apiErr.Error.Message)
		}
		return nil, fmt.Errorf("gmail batch API: %s", resp.Status)
	}

	contentType := resp.Header.Get("Content-Type")
	mediaType, params, err := mime.ParseMediaType(contentType)
	if err != nil || !strings.HasPrefix(mediaType, "multipart/") {
		return nil, fmt.Errorf("unexpected batch content type: %s", contentType)
	}

	reader := multipart.NewReader(resp.Body, params["boundary"])
	results := make([]map[string]any, 0, len(msgIDs))
	for {
		part, err := reader.NextPart()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		partBytes, err := io.ReadAll(part)
		_ = part.Close()
		if err != nil || len(partBytes) == 0 {
			continue
		}

		respPart, err := http.ReadResponse(bufio.NewReader(bytes.NewReader(partBytes)), &http.Request{Method: "GET"})
		if err != nil {
			continue
		}
		respBody, err := io.ReadAll(respPart.Body)
		_ = respPart.Body.Close()
		if err != nil || respPart.StatusCode >= 400 {
			continue
		}

		var msg map[string]any
		if err := json.Unmarshal(respBody, &msg); err == nil {
			results = append(results, msg)
		}
	}

	return results, nil
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
	// Increment API call counter for A/B testing
	count := atomic.AddInt64(&gmailAPICallCount, 1)
	log.Debug().Int64("api_calls", count).Str("path", path).Msg("gmail API call")

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
	v, _ := m[key].(string)
	return v
}

// dirEntry creates a directory DirEntry
func dirEntry(name string) sources.DirEntry {
	return sources.DirEntry{Name: name, Mode: sources.ModeDir, IsDir: true, Mtime: sources.NowUnix()}
}

// fileEntry creates a file DirEntry with estimated size
func fileEntry(name string, size int64) sources.DirEntry {
	return sources.DirEntry{Name: name, Mode: sources.ModeFile, Size: size, Mtime: sources.NowUnix()}
}

// extractSenderName extracts a clean sender name for folder display
// "Raymond Xu <ray@example.com>" -> "Raymond_Xu"
// "noreply@calendly.com" -> "Calendly"
// "KAYAK <kayak@msg.kayak.com>" -> "KAYAK"
func extractSenderName(from string) string {
	// Try to extract name from "Name <email>" format
	if idx := strings.Index(from, "<"); idx > 0 {
		name := strings.TrimSpace(from[:idx])
		name = strings.Trim(name, `"'`) // Remove quotes if present
		if name != "" {
			return sanitizeFolderName(name)
		}
	}

	// Extract email address from "Name <email>" or use as-is
	email := from
	if idx := strings.Index(from, "<"); idx >= 0 {
		if end := strings.Index(from[idx:], ">"); end > 0 {
			email = from[idx+1 : idx+end]
		}
	}

	// Fall back to domain name from email
	if atIdx := strings.Index(email, "@"); atIdx > 0 {
		domain := email[atIdx+1:]
		parts := strings.Split(domain, ".")
		if len(parts) >= 2 {
			// Use second-to-last part (the main domain name)
			name := parts[len(parts)-2]
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
		time.RFC1123Z,                           // "Mon, 02 Jan 2006 15:04:05 -0700"
		time.RFC1123,                            // "Mon, 02 Jan 2006 15:04:05 MST"
		"Mon, 2 Jan 2006 15:04:05 -0700",        // Single digit day
		"Mon, 2 Jan 2006 15:04:05 MST",          // Single digit day with timezone name
		"2 Jan 2006 15:04:05 -0700",             // No weekday
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

// --- Summary file generators ---

// emailSummary is a simplified email representation for summary files
type emailSummary struct {
	ID      string `json:"id"`
	From    string `json:"from"`
	Subject string `json:"subject"`
	Date    string `json:"date"`
	Snippet string `json:"snippet"`
	Path    string `json:"path"`
}

// toSummary converts a gmailMessage to emailSummary with the given category for path
func (msg *gmailMessage) toSummary(category string) emailSummary {
	return emailSummary{
		ID:      msg.ID,
		From:    msg.From,
		Subject: msg.Subject,
		Date:    parseEmailDate(msg.Date),
		Snippet: msg.Snippet,
		Path:    fmt.Sprintf("messages/%s/%s/%s", category, msg.SenderFolder, msg.SubjectFolder),
	}
}

// generateUnreadJSON creates a summary of all unread emails
func (g *GmailProvider) generateUnreadJSON(ctx context.Context, pctx *sources.ProviderContext) ([]byte, error) {
	return g.generateCategorySummary(ctx, pctx, "unread", 0)
}

// generateRecentJSON creates a summary of the 20 most recent emails
func (g *GmailProvider) generateRecentJSON(ctx context.Context, pctx *sources.ProviderContext) ([]byte, error) {
	data, err := g.generateCategorySummary(ctx, pctx, "inbox", 20)
	if err != nil {
		return nil, err
	}
	// Replace category name for clarity
	var result map[string]any
	json.Unmarshal(data, &result)
	result["category"] = "recent"
	return jsonMarshal(result)
}

// generateCategoryIndexJSON creates a summary of all emails in a category (with senders list)
func (g *GmailProvider) generateCategoryIndexJSON(ctx context.Context, pctx *sources.ProviderContext, category string) ([]byte, error) {
	messages, err := g.getCategoryMessages(ctx, pctx, category)
	if err != nil {
		return nil, err
	}

	senderSet := make(map[string]bool)
	emails := make([]emailSummary, 0, len(messages))
	for i := range messages {
		senderSet[messages[i].SenderFolder] = true
		emails = append(emails, messages[i].toSummary(category))
	}

	senders := make([]string, 0, len(senderSet))
	for s := range senderSet {
		senders = append(senders, s)
	}

	return jsonMarshal(map[string]any{
		"category": category,
		"count":    len(emails),
		"senders":  senders,
		"emails":   emails,
	})
}

// generateCategorySummary creates a JSON summary for a category with optional limit
func (g *GmailProvider) generateCategorySummary(ctx context.Context, pctx *sources.ProviderContext, category string, limit int) ([]byte, error) {
	messages, err := g.getCategoryMessages(ctx, pctx, category)
	if err != nil {
		return nil, err
	}

	if limit > 0 && len(messages) > limit {
		messages = messages[:limit]
	}

	emails := make([]emailSummary, 0, len(messages))
	for i := range messages {
		emails = append(emails, messages[i].toSummary(category))
	}

	return jsonMarshal(map[string]any{
		"category": category,
		"count":    len(emails),
		"emails":   emails,
	})
}
