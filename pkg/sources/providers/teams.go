package providers

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/beam-cloud/airstore/pkg/sources"
	"github.com/beam-cloud/airstore/pkg/types"
	"github.com/rs/zerolog/log"
)

const (
	graphAPIBase = "https://graph.microsoft.com/v1.0"
)

// TeamsProvider implements sources.Provider for Microsoft Teams integration.
// It exposes Teams data as a read-only filesystem under /sources/teams/
// Primary usage is via smart queries: mkdir /sources/teams/team-updates
type TeamsProvider struct {
	httpClient *http.Client
}

// NewTeamsProvider creates a new Teams source provider
func NewTeamsProvider() *TeamsProvider {
	return &TeamsProvider{
		httpClient: &http.Client{Timeout: 30 * time.Second},
	}
}

func (t *TeamsProvider) Name() string {
	return types.Teams.String()
}

// DefaultResourceType implements sources.ResourceLister.
func (t *TeamsProvider) DefaultResourceType() string { return "channels" }

// ListResources implements sources.ResourceLister.
func (t *TeamsProvider) ListResources(ctx context.Context, pctx *sources.ProviderContext, resourceType string) ([]sources.Resource, error) {
	if pctx.Credentials == nil || pctx.Credentials.AccessToken == "" {
		return nil, sources.ErrNotConnected
	}
	switch resourceType {
	case "channels":
		token := pctx.Credentials.AccessToken
		teams, err := t.listJoinedTeams(ctx, token)
		if err != nil {
			return nil, err
		}
		var out []sources.Resource
		for _, team := range teams {
			channels, err := t.listChannels(ctx, token, team.ID)
			if err != nil {
				log.Warn().Err(err).Str("team", team.DisplayName).Msg("teams: failed to list channels")
				continue
			}
			for _, ch := range channels {
				out = append(out, sources.Resource{
					ID:   team.DisplayName + "/" + ch.DisplayName,
					Name: team.DisplayName + " > #" + ch.DisplayName,
				})
			}
		}
		return out, nil
	default:
		return nil, fmt.Errorf("unsupported resource type: %s", resourceType)
	}
}

// Stat returns file/directory attributes
func (t *TeamsProvider) Stat(ctx context.Context, pctx *sources.ProviderContext, path string) (*sources.FileInfo, error) {
	if pctx.Credentials == nil || pctx.Credentials.AccessToken == "" {
		return nil, sources.ErrNotConnected
	}
	if path == "" {
		return sources.DirInfo(), nil
	}
	return nil, sources.ErrNotFound
}

// ReadDir lists directory contents
func (t *TeamsProvider) ReadDir(ctx context.Context, pctx *sources.ProviderContext, path string) ([]sources.DirEntry, error) {
	if pctx.Credentials == nil || pctx.Credentials.AccessToken == "" {
		return nil, sources.ErrNotConnected
	}
	if path == "" {
		return []sources.DirEntry{}, nil
	}
	return nil, sources.ErrNotFound
}

// Read reads file content
func (t *TeamsProvider) Read(ctx context.Context, pctx *sources.ProviderContext, path string, offset, length int64) ([]byte, error) {
	if pctx.Credentials == nil || pctx.Credentials.AccessToken == "" {
		return nil, sources.ErrNotConnected
	}
	return nil, sources.ErrNotFound
}

// Readlink is not supported for Teams
func (t *TeamsProvider) Readlink(ctx context.Context, pctx *sources.ProviderContext, path string) (string, error) {
	return "", sources.ErrNotFound
}

// Search executes a Teams search query and returns results
func (t *TeamsProvider) Search(ctx context.Context, pctx *sources.ProviderContext, query string, limit int) ([]sources.SearchResult, error) {
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
	messages, _, err := t.searchMessages(ctx, token, query, limit, 0)
	if err != nil {
		return nil, err
	}

	results := make([]sources.SearchResult, 0, len(messages))
	for _, msg := range messages {
		mtime := sources.NowUnix()
		if msg.CreatedDateTime != "" {
			if parsed, err := time.Parse(time.RFC3339, msg.CreatedDateTime); err == nil {
				mtime = parsed.Unix()
			}
		}

		text := teamsBodyToText(msg.Body)
		date := time.Unix(mtime, 0).Format("2006-01-02")
		shortID := msg.ID
		if len(shortID) > 10 {
			shortID = shortID[:10]
		}
		filename := fmt.Sprintf("%s_%s_%s.txt", date, sources.SanitizeFilename(msg.ChannelName), shortID)

		preview := text
		if len(preview) > 100 {
			preview = preview[:100] + "..."
		}

		results = append(results, sources.SearchResult{
			Name:    filename,
			Id:      msg.ID,
			Mode:    sources.ModeFile,
			Size:    int64(len(text)),
			Mtime:   mtime,
			Preview: fmt.Sprintf("%s > #%s @%s: %s", msg.TeamName, msg.ChannelName, msg.FromName, preview),
		})
	}

	return results, nil
}

// ============================================================================
// QueryExecutor implementation
// ============================================================================

// ExecuteQuery runs a Teams search query and returns results with generated filenames.
func (t *TeamsProvider) ExecuteQuery(ctx context.Context, pctx *sources.ProviderContext, spec sources.QuerySpec) (*sources.QueryResponse, error) {
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

	// For pure channel queries (no text search), use channel messages API directly
	if teamName, channelName, ok := parseTeamsChannelQuery(spec.Query); ok {
		teamID, channelID, err := t.resolveTeamAndChannelIDs(ctx, token, teamName, channelName)
		if err == nil && teamID != "" && channelID != "" {
			log.Debug().Str("team", teamName).Str("channel", channelName).
				Msg("teams: using channel messages API for channel query")
			return t.executeChannelMessages(ctx, token, teamID, teamName, channelID, channelName, limit, spec.PageToken, spec.FilenameFormat)
		}
		log.Warn().Err(err).Str("team", teamName).Str("channel", channelName).Str("query", spec.Query).
			Msg("teams: channel resolution failed, falling back to search")
	}

	// Search messages using Graph search API
	offset := 0
	if spec.PageToken != "" {
		fmt.Sscanf(spec.PageToken, "%d", &offset)
	}

	messages, totalCount, err := t.searchMessages(ctx, token, spec.Query, limit, offset)
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
		filenameFormat = sources.DefaultFilenameFormat("teams")
	}

	results := make([]sources.QueryResult, 0, len(messages))
	for _, msg := range messages {
		mtime := sources.NowUnix()
		var msgDate string
		if msg.CreatedDateTime != "" {
			if parsed, err := time.Parse(time.RFC3339, msg.CreatedDateTime); err == nil {
				mtime = parsed.Unix()
				msgDate = parsed.Format("2006-01-02")
			}
		}
		if msgDate == "" {
			msgDate = time.Unix(mtime, 0).Format("2006-01-02")
		}

		text := teamsBodyToText(msg.Body)
		metadata := map[string]string{
			"id":      msg.ID,
			"team":    msg.TeamName,
			"channel": msg.ChannelName,
			"user":    msg.FromName,
			"date":    msgDate,
			"text":    truncateText(text, 50),
		}
		if msg.TeamID != "" {
			metadata["team_id"] = msg.TeamID
		}
		if msg.ChannelID != "" {
			metadata["channel_id"] = msg.ChannelID
		}

		filename := t.FormatFilename(filenameFormat, metadata)

		results = append(results, sources.QueryResult{
			ID:       buildTeamsResultID(msg.ID, msg.TeamID, msg.ChannelID, ""),
			Filename: filename,
			Metadata: metadata,
			Size:     int64(len(text)),
			Mtime:    mtime,
		})
	}

	var nextPageToken string
	hasMore := false
	nextOffset := offset + len(messages)
	if totalCount > 0 && nextOffset < totalCount {
		nextPageToken = fmt.Sprintf("%d", nextOffset)
		hasMore = true
	}

	return &sources.QueryResponse{
		Results:       results,
		NextPageToken: nextPageToken,
		HasMore:       hasMore,
	}, nil
}

// ReadResult fetches the content of a Teams message by its ID.
func (t *TeamsProvider) ReadResult(ctx context.Context, pctx *sources.ProviderContext, resultID string) ([]byte, error) {
	if pctx.Credentials == nil || pctx.Credentials.AccessToken == "" {
		return nil, sources.ErrNotConnected
	}

	messageID, teamID, channelID, replyToID, err := parseTeamsResultID(resultID)
	if err != nil {
		return nil, fmt.Errorf("invalid result ID format")
	}

	token := pctx.Credentials.AccessToken

	if replyToID != "" {
		// This is a reply — fetch the specific reply
		reply, err := t.fetchReply(ctx, token, teamID, channelID, replyToID, messageID)
		if err != nil {
			return nil, err
		}
		return formatTeamsMessage(reply, nil), nil
	}

	// Fetch the message and its replies
	msg, err := t.fetchMessage(ctx, token, teamID, channelID, messageID)
	if err != nil {
		return nil, err
	}

	replies, _ := t.fetchReplies(ctx, token, teamID, channelID, messageID)
	return formatTeamsMessage(msg, replies), nil
}

// FormatFilename generates a filename from metadata using a format template.
func (t *TeamsProvider) FormatFilename(format string, metadata map[string]string) string {
	if format == "" {
		format = "{date}_{team}_{channel}_{user}_{id}.txt"
	}

	result := format
	for key, value := range metadata {
		placeholder := "{" + key + "}"
		safeValue := sources.SanitizeFilename(value)
		if key != "id" && len(safeValue) > 30 {
			safeValue = safeValue[:30]
		}
		result = strings.ReplaceAll(result, placeholder, safeValue)
	}

	if !strings.Contains(result, ".") {
		result += ".txt"
	}

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

// ============================================================================
// Result ID encoding/decoding
// ============================================================================

func buildTeamsResultID(messageID, teamID, channelID, replyToID string) string {
	resultID := messageID + ":" + teamID + ":" + channelID
	if replyToID != "" {
		return resultID + ":" + replyToID
	}
	return resultID
}

func parseTeamsResultID(resultID string) (messageID, teamID, channelID, replyToID string, err error) {
	parts := strings.SplitN(resultID, ":", 4)
	if len(parts) < 3 {
		return "", "", "", "", fmt.Errorf("invalid result ID format: expected at least 3 parts")
	}

	messageID = parts[0]
	teamID = parts[1]
	channelID = parts[2]
	if len(parts) == 4 {
		replyToID = parts[3]
	}

	return messageID, teamID, channelID, replyToID, nil
}

// Compile-time interface checks
var _ sources.Provider = (*TeamsProvider)(nil)
var _ sources.QueryExecutor = (*TeamsProvider)(nil)
var _ sources.ResourceLister = (*TeamsProvider)(nil)

// ============================================================================
// Microsoft Graph API types
// ============================================================================

type graphTeam struct {
	ID          string `json:"id"`
	DisplayName string `json:"displayName"`
}

type graphChannel struct {
	ID          string `json:"id"`
	DisplayName string `json:"displayName"`
}

type graphMessageBody struct {
	ContentType string `json:"contentType"` // "text" or "html"
	Content     string `json:"content"`
}

type graphMessageFrom struct {
	User *struct {
		ID          string `json:"id"`
		DisplayName string `json:"displayName"`
	} `json:"user"`
}

type graphMessage struct {
	ID              string            `json:"id"`
	CreatedDateTime string            `json:"createdDateTime"`
	Body            graphMessageBody  `json:"body"`
	From            *graphMessageFrom `json:"from"`
	Subject         string            `json:"subject"`
	// Enriched fields (set by provider, not by Graph API)
	TeamID      string `json:"-"`
	TeamName    string `json:"-"`
	ChannelID   string `json:"-"`
	ChannelName string `json:"-"`
	FromName    string `json:"-"`
}

func (m *graphMessage) resolveFromName() {
	if m.From != nil && m.From.User != nil {
		m.FromName = m.From.User.DisplayName
	}
	if m.FromName == "" {
		m.FromName = "unknown"
	}
}

// ============================================================================
// Microsoft Graph API methods
// ============================================================================

func (t *TeamsProvider) listJoinedTeams(ctx context.Context, token string) ([]graphTeam, error) {
	var result struct {
		Value []graphTeam `json:"value"`
	}
	if err := t.graphRequest(ctx, token, "GET", "/me/joinedTeams", nil, &result); err != nil {
		return nil, err
	}
	return result.Value, nil
}

func (t *TeamsProvider) listChannels(ctx context.Context, token, teamID string) ([]graphChannel, error) {
	var result struct {
		Value []graphChannel `json:"value"`
	}
	path := fmt.Sprintf("/teams/%s/channels", teamID)
	if err := t.graphRequest(ctx, token, "GET", path, nil, &result); err != nil {
		return nil, err
	}
	return result.Value, nil
}

func (t *TeamsProvider) resolveTeamAndChannelIDs(ctx context.Context, token, teamName, channelName string) (string, string, error) {
	teams, err := t.listJoinedTeams(ctx, token)
	if err != nil {
		return "", "", err
	}

	var teamID string
	for _, team := range teams {
		if strings.EqualFold(team.DisplayName, teamName) {
			teamID = team.ID
			break
		}
	}
	if teamID == "" {
		return "", "", fmt.Errorf("team not found: %s", teamName)
	}

	channels, err := t.listChannels(ctx, token, teamID)
	if err != nil {
		return "", "", err
	}

	for _, ch := range channels {
		if strings.EqualFold(ch.DisplayName, channelName) {
			return teamID, ch.ID, nil
		}
	}
	return "", "", fmt.Errorf("channel not found: %s in team %s", channelName, teamName)
}

func (t *TeamsProvider) fetchChannelMessages(ctx context.Context, token, teamID, channelID string, limit int, nextLink string) ([]graphMessage, string, error) {
	var result struct {
		Value    []graphMessage `json:"value"`
		NextLink string         `json:"@odata.nextLink"`
	}

	if nextLink != "" {
		// Use the full nextLink URL for pagination
		if err := t.graphRequestURL(ctx, token, "GET", nextLink, nil, &result); err != nil {
			return nil, "", err
		}
	} else {
		path := fmt.Sprintf("/teams/%s/channels/%s/messages?$top=%d", teamID, channelID, limit)
		if err := t.graphRequest(ctx, token, "GET", path, nil, &result); err != nil {
			return nil, "", err
		}
	}

	return result.Value, result.NextLink, nil
}

func (t *TeamsProvider) executeChannelMessages(ctx context.Context, token, teamID, teamName, channelID, channelName string, limit int, pageToken, filenameFormat string) (*sources.QueryResponse, error) {
	messages, nextLink, err := t.fetchChannelMessages(ctx, token, teamID, channelID, limit, pageToken)
	if err != nil {
		return nil, err
	}

	log.Debug().Str("team_id", teamID).Str("channel_id", channelID).Int("raw_count", len(messages)).
		Msg("teams: channel messages API returned messages")

	if filenameFormat == "" {
		filenameFormat = sources.DefaultFilenameFormat("teams")
	}

	results := make([]sources.QueryResult, 0, len(messages))
	for _, msg := range messages {
		msg.resolveFromName()

		// Skip system messages
		if msg.Body.Content == "" && msg.Subject == "" {
			continue
		}

		mtime := sources.NowUnix()
		var msgDate string
		if msg.CreatedDateTime != "" {
			if parsed, err := time.Parse(time.RFC3339, msg.CreatedDateTime); err == nil {
				mtime = parsed.Unix()
				msgDate = parsed.Format("2006-01-02")
			}
		}
		if msgDate == "" {
			msgDate = time.Unix(mtime, 0).Format("2006-01-02")
		}

		text := teamsBodyToText(msg.Body)
		metadata := map[string]string{
			"id":         msg.ID,
			"team":       teamName,
			"team_id":    teamID,
			"channel":    channelName,
			"channel_id": channelID,
			"user":       msg.FromName,
			"date":       msgDate,
			"text":       truncateText(text, 50),
		}

		filename := t.FormatFilename(filenameFormat, metadata)

		results = append(results, sources.QueryResult{
			ID:       buildTeamsResultID(msg.ID, teamID, channelID, ""),
			Filename: filename,
			Metadata: metadata,
			Size:     int64(len(text)),
			Mtime:    mtime,
		})
	}

	return &sources.QueryResponse{
		Results:       results,
		NextPageToken: nextLink,
		HasMore:       nextLink != "",
	}, nil
}

func (t *TeamsProvider) searchMessages(ctx context.Context, token, query string, limit, offset int) ([]graphMessage, int, error) {
	payload := map[string]any{
		"requests": []map[string]any{
			{
				"entityTypes": []string{"chatMessage"},
				"query": map[string]string{
					"queryString": query,
				},
				"from": offset,
				"size": limit,
			},
		},
	}

	var result struct {
		Value []struct {
			HitsContainers []struct {
				Hits []struct {
					Resource graphMessage `json:"resource"`
				} `json:"hits"`
				Total         int  `json:"total"`
				MoreResultsAvailable bool `json:"moreResultsAvailable"`
			} `json:"hitsContainers"`
		} `json:"value"`
	}

	if err := t.graphRequest(ctx, token, "POST", "/search/query", payload, &result); err != nil {
		return nil, 0, err
	}

	var messages []graphMessage
	totalCount := 0
	if len(result.Value) > 0 && len(result.Value[0].HitsContainers) > 0 {
		container := result.Value[0].HitsContainers[0]
		totalCount = container.Total
		for _, hit := range container.Hits {
			msg := hit.Resource
			msg.resolveFromName()
			messages = append(messages, msg)
		}
	}

	return messages, totalCount, nil
}

func (t *TeamsProvider) fetchMessage(ctx context.Context, token, teamID, channelID, messageID string) (*graphMessage, error) {
	path := fmt.Sprintf("/teams/%s/channels/%s/messages/%s", teamID, channelID, messageID)
	var msg graphMessage
	if err := t.graphRequest(ctx, token, "GET", path, nil, &msg); err != nil {
		return nil, err
	}
	msg.resolveFromName()
	return &msg, nil
}

func (t *TeamsProvider) fetchReplies(ctx context.Context, token, teamID, channelID, messageID string) ([]graphMessage, error) {
	path := fmt.Sprintf("/teams/%s/channels/%s/messages/%s/replies", teamID, channelID, messageID)
	var result struct {
		Value []graphMessage `json:"value"`
	}
	if err := t.graphRequest(ctx, token, "GET", path, nil, &result); err != nil {
		return nil, err
	}
	for i := range result.Value {
		result.Value[i].resolveFromName()
	}
	return result.Value, nil
}

func (t *TeamsProvider) fetchReply(ctx context.Context, token, teamID, channelID, messageID, replyID string) (*graphMessage, error) {
	path := fmt.Sprintf("/teams/%s/channels/%s/messages/%s/replies/%s", teamID, channelID, messageID, replyID)
	var msg graphMessage
	if err := t.graphRequest(ctx, token, "GET", path, nil, &msg); err != nil {
		return nil, err
	}
	msg.resolveFromName()
	return &msg, nil
}

// ============================================================================
// HTTP helpers
// ============================================================================

func (t *TeamsProvider) graphRequest(ctx context.Context, token, method, path string, payload any, result any) error {
	reqURL := graphAPIBase + path
	return t.graphRequestURL(ctx, token, method, reqURL, payload, result)
}

func (t *TeamsProvider) graphRequestURL(ctx context.Context, token, method, reqURL string, payload any, result any) error {
	var body io.Reader
	if payload != nil {
		data, err := json.Marshal(payload)
		if err != nil {
			return err
		}
		body = bytes.NewReader(data)
	}

	req, err := http.NewRequestWithContext(ctx, method, reqURL, body)
	if err != nil {
		return err
	}

	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Accept", "application/json")
	if payload != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := t.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("graph API error: %s - %s", resp.Status, string(respBody))
	}

	return json.NewDecoder(resp.Body).Decode(result)
}

// ============================================================================
// Query parsing helpers
// ============================================================================

// parseTeamsChannelQuery detects if a query is a pure channel query like "in:TeamName/ChannelName"
func parseTeamsChannelQuery(query string) (teamName, channelName string, ok bool) {
	tokens := strings.Fields(strings.TrimSpace(query))
	if len(tokens) == 0 {
		return "", "", false
	}
	for _, token := range tokens {
		lower := strings.ToLower(token)
		if strings.HasPrefix(lower, "in:") {
			value := token[len("in:"):]
			value = strings.TrimPrefix(value, "#")
			parts := strings.SplitN(value, "/", 2)
			if len(parts) == 2 {
				teamName = parts[0]
				channelName = parts[1]
			}
		} else {
			// Unrecognized token is a text search term — fall back to search
			return "", "", false
		}
	}
	if teamName == "" || channelName == "" {
		return "", "", false
	}
	return teamName, channelName, true
}

// ============================================================================
// Formatting helpers
// ============================================================================

var htmlTagRegex = regexp.MustCompile(`<[^>]*>`)

// teamsBodyToText converts a Teams message body to plain text.
func teamsBodyToText(body graphMessageBody) string {
	if body.ContentType == "text" {
		return body.Content
	}
	// Strip HTML tags for html content type
	text := body.Content
	// Replace block elements with newlines
	for _, tag := range []string{"<br>", "<br/>", "<br />", "</p>", "</div>", "</li>"} {
		text = strings.ReplaceAll(text, tag, "\n")
	}
	text = htmlTagRegex.ReplaceAllString(text, "")
	text = strings.TrimSpace(text)
	return text
}

// formatTeamsMessage formats a Teams message as readable text
func formatTeamsMessage(msg *graphMessage, replies []graphMessage) []byte {
	var b strings.Builder

	fromName := msg.FromName
	if fromName == "" {
		fromName = "unknown"
	}

	var timeStr string
	if msg.CreatedDateTime != "" {
		if parsed, err := time.Parse(time.RFC3339, msg.CreatedDateTime); err == nil {
			timeStr = parsed.Format("2006-01-02 15:04:05")
		}
	}

	text := teamsBodyToText(msg.Body)

	// Main message
	b.WriteString(fmt.Sprintf("From: @%s\n", fromName))
	b.WriteString(fmt.Sprintf("Date: %s\n", timeStr))
	if msg.Subject != "" {
		b.WriteString(fmt.Sprintf("Subject: %s\n", msg.Subject))
	}
	b.WriteString("\n")
	b.WriteString(text)
	b.WriteString("\n")

	// Thread replies
	if len(replies) > 0 {
		b.WriteString("\n---\n")
		b.WriteString(fmt.Sprintf("Thread (%d replies):\n\n", len(replies)))

		for i, reply := range replies {
			replyFrom := reply.FromName
			if replyFrom == "" {
				replyFrom = "unknown"
			}
			replyText := teamsBodyToText(reply.Body)

			var replyTime string
			if reply.CreatedDateTime != "" {
				if parsed, err := time.Parse(time.RFC3339, reply.CreatedDateTime); err == nil {
					replyTime = parsed.Format("15:04")
				}
			}

			b.WriteString(fmt.Sprintf("[%d] @%s (%s):\n", i+1, replyFrom, replyTime))
			b.WriteString(replyText)
			b.WriteString("\n\n")
		}
	}

	return []byte(b.String())
}
