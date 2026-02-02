package providers

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/beam-cloud/airstore/pkg/sources"
	"github.com/beam-cloud/airstore/pkg/types"
)

const (
	slackAPIBase = "https://slack.com/api"
)

// SlackProvider implements sources.Provider for Slack integration.
// It exposes Slack data as a read-only filesystem under /sources/slack/
// Primary usage is via smart queries: mkdir /sources/slack/team-updates
type SlackProvider struct {
	httpClient *http.Client
}

// NewSlackProvider creates a new Slack source provider
func NewSlackProvider() *SlackProvider {
	return &SlackProvider{
		httpClient: &http.Client{Timeout: 30 * time.Second},
	}
}

func (s *SlackProvider) Name() string {
	return types.ToolSlack.String()
}

// Stat returns file/directory attributes
func (s *SlackProvider) Stat(ctx context.Context, pctx *sources.ProviderContext, path string) (*sources.FileInfo, error) {
	if pctx.Credentials == nil || pctx.Credentials.AccessToken == "" {
		return nil, sources.ErrNotConnected
	}

	// Root directory
	if path == "" {
		return sources.DirInfo(), nil
	}

	return nil, sources.ErrNotFound
}

// ReadDir lists directory contents
func (s *SlackProvider) ReadDir(ctx context.Context, pctx *sources.ProviderContext, path string) ([]sources.DirEntry, error) {
	if pctx.Credentials == nil || pctx.Credentials.AccessToken == "" {
		return nil, sources.ErrNotConnected
	}

	// Root directory - empty, smart queries create the structure
	if path == "" {
		return []sources.DirEntry{}, nil
	}

	return nil, sources.ErrNotFound
}

// Read reads file content
func (s *SlackProvider) Read(ctx context.Context, pctx *sources.ProviderContext, path string, offset, length int64) ([]byte, error) {
	if pctx.Credentials == nil || pctx.Credentials.AccessToken == "" {
		return nil, sources.ErrNotConnected
	}

	return nil, sources.ErrNotFound
}

// Readlink is not supported for Slack
func (s *SlackProvider) Readlink(ctx context.Context, pctx *sources.ProviderContext, path string) (string, error) {
	return "", sources.ErrNotFound
}

// Search executes a Slack search query and returns results
// Uses Slack's search.messages API
func (s *SlackProvider) Search(ctx context.Context, pctx *sources.ProviderContext, query string, limit int) ([]sources.SearchResult, error) {
	if pctx.Credentials == nil || pctx.Credentials.AccessToken == "" {
		return nil, sources.ErrNotConnected
	}

	if limit <= 0 {
		limit = 50
	}
	if limit > 100 {
		limit = 100
	}

	token := pctx.Credentials.AccessToken

	// Search messages
	messages, err := s.searchMessages(ctx, token, query, limit, "")
	if err != nil {
		return nil, err
	}

	results := make([]sources.SearchResult, 0, len(messages))
	for _, msg := range messages {
		ts, _ := msg["ts"].(string)
		text, _ := msg["text"].(string)
		user, _ := msg["user"].(string)
		channel, _ := msg["channel"].(map[string]any)
		channelName := ""
		if channel != nil {
			channelName, _ = channel["name"].(string)
		}

		// Parse timestamp
		mtime := sources.NowUnix()
		if ts != "" {
			if t, err := parseSlackTimestamp(ts); err == nil {
				mtime = t.Unix()
			}
		}

		// Generate filename
		date := time.Unix(mtime, 0).Format("2006-01-02")
		shortID := ts
		if len(shortID) > 10 {
			shortID = shortID[:10]
		}
		filename := fmt.Sprintf("%s_%s_%s.txt", date, sources.SanitizeFilename(channelName), shortID)

		// Preview
		preview := text
		if len(preview) > 100 {
			preview = preview[:100] + "..."
		}

		results = append(results, sources.SearchResult{
			Name:    filename,
			Id:      ts,
			Mode:    sources.ModeFile,
			Size:    int64(len(text)),
			Mtime:   mtime,
			Preview: fmt.Sprintf("#%s @%s: %s", channelName, user, preview),
		})
	}

	return results, nil
}

// ============================================================================
// QueryExecutor implementation - This is the main interface for smart queries
// ============================================================================

// ExecuteQuery runs a Slack search query and returns results with generated filenames.
// This implements the sources.QueryExecutor interface for filesystem queries.
// Users create smart queries like: mkdir /sources/slack/team-updates
func (s *SlackProvider) ExecuteQuery(ctx context.Context, pctx *sources.ProviderContext, spec sources.QuerySpec) (*sources.QueryResponse, error) {
	if pctx.Credentials == nil || pctx.Credentials.AccessToken == "" {
		return nil, sources.ErrNotConnected
	}

	limit := spec.Limit
	if limit <= 0 {
		limit = 50
	}
	if limit > 100 {
		limit = 100
	}

	token := pctx.Credentials.AccessToken

	// Search messages using Slack API
	messages, nextCursor, err := s.searchMessagesWithCursor(ctx, token, spec.Query, limit, spec.PageToken)
	if err != nil {
		return nil, err
	}

	if len(messages) == 0 {
		return &sources.QueryResponse{
			Results:       []sources.QueryResult{},
			NextPageToken: "",
			HasMore:       false,
		}, nil
	}

	filenameFormat := spec.FilenameFormat
	if filenameFormat == "" {
		filenameFormat = sources.DefaultFilenameFormat("slack")
	}

	results := make([]sources.QueryResult, 0, len(messages))
	for _, msg := range messages {
		ts, _ := msg["ts"].(string)
		text, _ := msg["text"].(string)
		user, _ := msg["user"].(string)
		username, _ := msg["username"].(string)
		if username == "" {
			username = user
		}

		// Get channel info
		channelInfo, _ := msg["channel"].(map[string]any)
		channelID := ""
		channelName := ""
		if channelInfo != nil {
			channelID, _ = channelInfo["id"].(string)
			channelName, _ = channelInfo["name"].(string)
		}

		// Parse timestamp
		mtime := sources.NowUnix()
		var msgDate string
		if ts != "" {
			if t, err := parseSlackTimestamp(ts); err == nil {
				mtime = t.Unix()
				msgDate = t.Format("2006-01-02")
			}
		}
		if msgDate == "" {
			msgDate = time.Unix(mtime, 0).Format("2006-01-02")
		}

		// Build metadata
		metadata := map[string]string{
			"id":       ts,
			"channel":  channelName,
			"user":     username,
			"date":     msgDate,
			"text":     truncateText(text, 50),
		}

		// Add channel_id for ReadResult
		if channelID != "" {
			metadata["channel_id"] = channelID
		}

		// Generate filename
		filename := s.FormatFilename(filenameFormat, metadata)

		results = append(results, sources.QueryResult{
			ID:       ts + ":" + channelID, // Include channel_id for reading
			Filename: filename,
			Metadata: metadata,
			Size:     int64(len(text)),
			Mtime:    mtime,
		})
	}

	return &sources.QueryResponse{
		Results:       results,
		NextPageToken: nextCursor,
		HasMore:       nextCursor != "",
	}, nil
}

// ReadResult fetches the content of a Slack message by its timestamp.
// Returns the message formatted as readable text.
// This implements the sources.QueryExecutor interface.
func (s *SlackProvider) ReadResult(ctx context.Context, pctx *sources.ProviderContext, resultID string) ([]byte, error) {
	if pctx.Credentials == nil || pctx.Credentials.AccessToken == "" {
		return nil, sources.ErrNotConnected
	}

	// Parse resultID which is "ts:channel_id"
	parts := strings.SplitN(resultID, ":", 2)
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid result ID format")
	}
	ts, channelID := parts[0], parts[1]

	token := pctx.Credentials.AccessToken

	// Fetch the specific message and its thread replies
	content, err := s.fetchMessageWithThread(ctx, token, channelID, ts)
	if err != nil {
		return nil, err
	}

	return content, nil
}

// FormatFilename generates a filename from metadata using a format template.
// Supported placeholders: {id}, {channel}, {user}, {date}, {text}
// This implements the sources.QueryExecutor interface.
func (s *SlackProvider) FormatFilename(format string, metadata map[string]string) string {
	if format == "" {
		format = "{date}_{channel}_{user}_{id}.txt"
	}

	result := format
	for key, value := range metadata {
		placeholder := "{" + key + "}"
		safeValue := sources.SanitizeFilename(value)
		// Truncate long values (except id)
		if key != "id" && len(safeValue) > 30 {
			safeValue = safeValue[:30]
		}
		result = strings.ReplaceAll(result, placeholder, safeValue)
	}

	// Ensure filename ends with .txt if no extension
	if !strings.Contains(result, ".") {
		result += ".txt"
	}

	// Ensure filename is not empty
	if result == "" || result == ".txt" {
		if id, ok := metadata["id"]; ok {
			shortID := id
			if len(shortID) > 10 {
				shortID = shortID[:10]
			}
			result = shortID + ".txt"
		} else {
			result = "message.txt"
		}
	}

	return result
}

// Compile-time interface checks
var _ sources.Provider = (*SlackProvider)(nil)
var _ sources.QueryExecutor = (*SlackProvider)(nil)

// ============================================================================
// Slack API methods
// ============================================================================

// searchMessages searches for messages using Slack's search.messages API
func (s *SlackProvider) searchMessages(ctx context.Context, token, query string, count int, cursor string) ([]map[string]any, error) {
	messages, _, err := s.searchMessagesWithCursor(ctx, token, query, count, cursor)
	return messages, err
}

// searchMessagesWithCursor searches messages and returns the next cursor for pagination
func (s *SlackProvider) searchMessagesWithCursor(ctx context.Context, token, query string, count int, cursor string) ([]map[string]any, string, error) {
	params := url.Values{
		"query": {query},
		"count": {fmt.Sprintf("%d", count)},
		"sort":  {"timestamp"},
	}
	if cursor != "" {
		params.Set("cursor", cursor)
	}

	var result struct {
		OK       bool   `json:"ok"`
		Error    string `json:"error"`
		Messages struct {
			Matches []map[string]any `json:"matches"`
			Paging  struct {
				Count int `json:"count"`
				Total int `json:"total"`
				Page  int `json:"page"`
				Pages int `json:"pages"`
			} `json:"paging"`
		} `json:"messages"`
		ResponseMetadata struct {
			NextCursor string `json:"next_cursor"`
		} `json:"response_metadata"`
	}

	if err := s.request(ctx, token, "search.messages", params, &result); err != nil {
		return nil, "", err
	}

	if !result.OK {
		return nil, "", fmt.Errorf("slack API error: %s", result.Error)
	}

	// Determine next cursor for pagination
	nextCursor := result.ResponseMetadata.NextCursor
	// Slack search.messages uses page-based pagination, not cursor
	// If we have more pages, construct a page cursor
	if result.Messages.Paging.Page < result.Messages.Paging.Pages {
		nextCursor = fmt.Sprintf("page:%d", result.Messages.Paging.Page+1)
	}

	return result.Messages.Matches, nextCursor, nil
}

// fetchMessageWithThread fetches a message and its thread replies
func (s *SlackProvider) fetchMessageWithThread(ctx context.Context, token, channelID, ts string) ([]byte, error) {
	// First, get the conversation history for the specific message
	params := url.Values{
		"channel":   {channelID},
		"oldest":    {ts},
		"latest":    {ts},
		"inclusive": {"true"},
		"limit":     {"1"},
	}

	var historyResult struct {
		OK       bool             `json:"ok"`
		Error    string           `json:"error"`
		Messages []map[string]any `json:"messages"`
	}

	if err := s.request(ctx, token, "conversations.history", params, &historyResult); err != nil {
		return nil, err
	}

	if !historyResult.OK {
		return nil, fmt.Errorf("slack API error: %s", historyResult.Error)
	}

	if len(historyResult.Messages) == 0 {
		return nil, sources.ErrNotFound
	}

	msg := historyResult.Messages[0]

	// Check if message has thread replies
	replyCount, _ := msg["reply_count"].(float64)
	var threadReplies []map[string]any

	if replyCount > 0 {
		// Fetch thread replies
		threadParams := url.Values{
			"channel": {channelID},
			"ts":      {ts},
			"limit":   {"100"},
		}

		var threadResult struct {
			OK       bool             `json:"ok"`
			Error    string           `json:"error"`
			Messages []map[string]any `json:"messages"`
		}

		if err := s.request(ctx, token, "conversations.replies", threadParams, &threadResult); err == nil && threadResult.OK {
			// Skip first message (it's the parent)
			if len(threadResult.Messages) > 1 {
				threadReplies = threadResult.Messages[1:]
			}
		}
	}

	// Format the message as readable text
	return formatSlackMessage(msg, threadReplies), nil
}

// request makes a GET request to the Slack API
func (s *SlackProvider) request(ctx context.Context, token, method string, params url.Values, result any) error {
	reqURL := slackAPIBase + "/" + method
	if len(params) > 0 {
		reqURL += "?" + params.Encode()
	}

	req, err := http.NewRequestWithContext(ctx, "GET", reqURL, nil)
	if err != nil {
		return err
	}

	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Accept", "application/json")

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("slack API error: %s - %s", resp.Status, string(body))
	}

	return json.NewDecoder(resp.Body).Decode(result)
}

// ============================================================================
// Helper functions
// ============================================================================

// parseSlackTimestamp converts a Slack timestamp (e.g., "1234567890.123456") to time.Time
func parseSlackTimestamp(ts string) (time.Time, error) {
	// Slack timestamps are Unix timestamps with microseconds after the dot
	parts := strings.SplitN(ts, ".", 2)
	if len(parts) == 0 {
		return time.Time{}, fmt.Errorf("invalid timestamp")
	}

	var sec int64
	if _, err := fmt.Sscanf(parts[0], "%d", &sec); err != nil {
		return time.Time{}, err
	}

	return time.Unix(sec, 0), nil
}

// formatSlackMessage formats a Slack message as readable text
func formatSlackMessage(msg map[string]any, threadReplies []map[string]any) []byte {
	var b strings.Builder

	// Message header
	user, _ := msg["user"].(string)
	username, _ := msg["username"].(string)
	if username == "" {
		username = user
	}
	ts, _ := msg["ts"].(string)
	text, _ := msg["text"].(string)

	// Parse timestamp for display
	var timeStr string
	if t, err := parseSlackTimestamp(ts); err == nil {
		timeStr = t.Format("2006-01-02 15:04:05")
	}

	// Main message
	b.WriteString(fmt.Sprintf("From: @%s\n", username))
	b.WriteString(fmt.Sprintf("Date: %s\n", timeStr))
	b.WriteString("\n")
	b.WriteString(text)
	b.WriteString("\n")

	// Thread replies
	if len(threadReplies) > 0 {
		b.WriteString("\n---\n")
		b.WriteString(fmt.Sprintf("Thread (%d replies):\n\n", len(threadReplies)))

		for i, reply := range threadReplies {
			replyUser, _ := reply["user"].(string)
			replyUsername, _ := reply["username"].(string)
			if replyUsername == "" {
				replyUsername = replyUser
			}
			replyText, _ := reply["text"].(string)
			replyTs, _ := reply["ts"].(string)

			var replyTime string
			if t, err := parseSlackTimestamp(replyTs); err == nil {
				replyTime = t.Format("15:04")
			}

			b.WriteString(fmt.Sprintf("[%d] @%s (%s):\n", i+1, replyUsername, replyTime))
			b.WriteString(replyText)
			b.WriteString("\n\n")
		}
	}

	// Reactions
	if reactions, ok := msg["reactions"].([]any); ok && len(reactions) > 0 {
		b.WriteString("\n---\nReactions: ")
		for i, r := range reactions {
			if reaction, ok := r.(map[string]any); ok {
				name, _ := reaction["name"].(string)
				count, _ := reaction["count"].(float64)
				if i > 0 {
					b.WriteString(", ")
				}
				b.WriteString(fmt.Sprintf(":%s: (%d)", name, int(count)))
			}
		}
		b.WriteString("\n")
	}

	return []byte(b.String())
}

// truncateText truncates text to maxLen characters
func truncateText(text string, maxLen int) string {
	if len(text) <= maxLen {
		return text
	}
	return text[:maxLen]
}
