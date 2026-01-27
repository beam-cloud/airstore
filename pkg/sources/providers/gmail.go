package providers

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/beam-cloud/airstore/pkg/sources"
)

const (
	gmailAPIBase = "https://gmail.googleapis.com/gmail/v1"
)

// GmailProvider implements sources.Provider for Gmail integration.
// It exposes Gmail resources as a read-only filesystem under /sources/gmail/
type GmailProvider struct {
	httpClient *http.Client
}

// NewGmailProvider creates a new Gmail source provider
func NewGmailProvider() *GmailProvider {
	return &GmailProvider{
		httpClient: &http.Client{Timeout: 30 * time.Second},
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
	case "views":
		return g.statViews(ctx, pctx, parts[1:])
	case "messages":
		return g.statMessages(ctx, pctx, parts[1:])
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
			{Name: "views", Mode: sources.ModeDir, IsDir: true, Mtime: sources.NowUnix()},
			{Name: "messages", Mode: sources.ModeDir, IsDir: true, Mtime: sources.NowUnix()},
		}, nil
	}

	parts := strings.Split(path, "/")

	switch parts[0] {
	case "views":
		return g.readdirViews(ctx, pctx, parts[1:])
	case "messages":
		return g.readdirMessages(ctx, pctx, parts[1:])
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
	case "views":
		return g.readViews(ctx, pctx, parts[1:], offset, length)
	case "messages":
		return g.readMessages(ctx, pctx, parts[1:], offset, length)
	default:
		return nil, sources.ErrNotFound
	}
}

// Readlink is not supported for Gmail
func (g *GmailProvider) Readlink(ctx context.Context, pctx *sources.ProviderContext, path string) (string, error) {
	return "", sources.ErrNotFound
}

// --- Views ---
// /views/unread.json - unread messages
// /views/inbox.json - inbox messages
// /views/labels.json - available labels

func (g *GmailProvider) statViews(ctx context.Context, pctx *sources.ProviderContext, parts []string) (*sources.FileInfo, error) {
	if len(parts) == 0 {
		return sources.DirInfo(), nil
	}

	switch parts[0] {
	case "unread.json", "inbox.json", "labels.json":
		data, err := g.getViewData(ctx, pctx, parts[0])
		if err != nil {
			return nil, err
		}
		return sources.FileInfoFromBytes(data), nil
	default:
		return nil, sources.ErrNotFound
	}
}

func (g *GmailProvider) readdirViews(ctx context.Context, pctx *sources.ProviderContext, parts []string) ([]sources.DirEntry, error) {
	if len(parts) == 0 {
		return []sources.DirEntry{
			{Name: "unread.json", Mode: sources.ModeFile, Mtime: sources.NowUnix()},
			{Name: "inbox.json", Mode: sources.ModeFile, Mtime: sources.NowUnix()},
			{Name: "labels.json", Mode: sources.ModeFile, Mtime: sources.NowUnix()},
		}, nil
	}
	return nil, sources.ErrNotDir
}

func (g *GmailProvider) readViews(ctx context.Context, pctx *sources.ProviderContext, parts []string, offset, length int64) ([]byte, error) {
	if len(parts) == 0 {
		return nil, sources.ErrIsDir
	}

	data, err := g.getViewData(ctx, pctx, parts[0])
	if err != nil {
		return nil, err
	}

	return sliceData(data, offset, length), nil
}

func (g *GmailProvider) getViewData(ctx context.Context, pctx *sources.ProviderContext, view string) ([]byte, error) {
	token := pctx.Credentials.AccessToken

	switch view {
	case "unread.json":
		return g.fetchUnread(ctx, token)
	case "inbox.json":
		return g.fetchInbox(ctx, token)
	case "labels.json":
		return g.fetchLabels(ctx, token)
	default:
		return nil, sources.ErrNotFound
	}
}

// --- Messages ---
// /messages/<msgid>/meta.json - message metadata
// /messages/<msgid>/body.txt - plain text body

func (g *GmailProvider) statMessages(ctx context.Context, pctx *sources.ProviderContext, parts []string) (*sources.FileInfo, error) {
	switch len(parts) {
	case 0:
		return sources.DirInfo(), nil
	case 1:
		// /messages/<msgid> - directory
		return sources.DirInfo(), nil
	case 2:
		// /messages/<msgid>/<file>
		msgId, file := parts[0], parts[1]
		data, err := g.getMessageFile(ctx, pctx, msgId, file)
		if err != nil {
			return nil, err
		}
		return sources.FileInfoFromBytes(data), nil
	default:
		return nil, sources.ErrNotFound
	}
}

func (g *GmailProvider) readdirMessages(ctx context.Context, pctx *sources.ProviderContext, parts []string) ([]sources.DirEntry, error) {
	token := pctx.Credentials.AccessToken

	switch len(parts) {
	case 0:
		// List recent message IDs
		messages, err := g.fetchMessageList(ctx, token, 50)
		if err != nil {
			return nil, err
		}

		entries := make([]sources.DirEntry, 0, len(messages))
		for _, msg := range messages {
			if id, ok := msg["id"].(string); ok {
				entries = append(entries, sources.DirEntry{
					Name:  id,
					Mode:  sources.ModeDir,
					IsDir: true,
					Mtime: sources.NowUnix(),
				})
			}
		}
		return entries, nil

	case 1:
		// List files in a message
		return []sources.DirEntry{
			{Name: "meta.json", Mode: sources.ModeFile, Mtime: sources.NowUnix()},
			{Name: "body.txt", Mode: sources.ModeFile, Mtime: sources.NowUnix()},
		}, nil

	default:
		return nil, sources.ErrNotDir
	}
}

func (g *GmailProvider) readMessages(ctx context.Context, pctx *sources.ProviderContext, parts []string, offset, length int64) ([]byte, error) {
	if len(parts) < 2 {
		return nil, sources.ErrIsDir
	}

	msgId, file := parts[0], parts[1]
	data, err := g.getMessageFile(ctx, pctx, msgId, file)
	if err != nil {
		return nil, err
	}

	return sliceData(data, offset, length), nil
}

func (g *GmailProvider) getMessageFile(ctx context.Context, pctx *sources.ProviderContext, msgId, file string) ([]byte, error) {
	token := pctx.Credentials.AccessToken

	switch file {
	case "meta.json":
		return g.fetchMessageMeta(ctx, token, msgId)
	case "body.txt":
		return g.fetchMessageBody(ctx, token, msgId)
	default:
		return nil, sources.ErrNotFound
	}
}

// --- API methods ---

func (g *GmailProvider) fetchUnread(ctx context.Context, token string) ([]byte, error) {
	var result map[string]any
	if err := g.request(ctx, token, "/users/me/messages?q=is:unread&maxResults=50", &result); err != nil {
		return nil, err
	}

	messages := result["messages"]
	if messages == nil {
		messages = []any{}
	}

	return jsonMarshal(map[string]any{
		"unread":  messages,
		"count":   len(messages.([]any)),
		"query":   "is:unread",
	})
}

func (g *GmailProvider) fetchInbox(ctx context.Context, token string) ([]byte, error) {
	var result map[string]any
	if err := g.request(ctx, token, "/users/me/messages?labelIds=INBOX&maxResults=50", &result); err != nil {
		return nil, err
	}

	messages := result["messages"]
	if messages == nil {
		messages = []any{}
	}

	return jsonMarshal(map[string]any{
		"inbox": messages,
		"count": len(messages.([]any)),
	})
}

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

func (g *GmailProvider) fetchMessageList(ctx context.Context, token string, maxResults int) ([]map[string]any, error) {
	var result map[string]any
	path := fmt.Sprintf("/users/me/messages?maxResults=%d", maxResults)
	if err := g.request(ctx, token, path, &result); err != nil {
		return nil, err
	}

	messages, ok := result["messages"].([]any)
	if !ok {
		return nil, nil
	}

	msgList := make([]map[string]any, 0, len(messages))
	for _, m := range messages {
		if msg, ok := m.(map[string]any); ok {
			msgList = append(msgList, msg)
		}
	}
	return msgList, nil
}

func (g *GmailProvider) fetchMessageMeta(ctx context.Context, token, msgId string) ([]byte, error) {
	var result map[string]any
	path := fmt.Sprintf("/users/me/messages/%s?format=metadata", msgId)
	if err := g.request(ctx, token, path, &result); err != nil {
		return nil, err
	}

	// Extract headers
	headers := make(map[string]string)
	if payload, ok := result["payload"].(map[string]any); ok {
		if hdrs, ok := payload["headers"].([]any); ok {
			for _, h := range hdrs {
				if hdr, ok := h.(map[string]any); ok {
					name, _ := hdr["name"].(string)
					value, _ := hdr["value"].(string)
					headers[name] = value
				}
			}
		}
	}

	return jsonMarshal(map[string]any{
		"id":       result["id"],
		"threadId": result["threadId"],
		"labelIds": result["labelIds"],
		"snippet":  result["snippet"],
		"from":     headers["From"],
		"to":       headers["To"],
		"subject":  headers["Subject"],
		"date":     headers["Date"],
	})
}

func (g *GmailProvider) fetchMessageBody(ctx context.Context, token, msgId string) ([]byte, error) {
	var result map[string]any
	path := fmt.Sprintf("/users/me/messages/%s?format=full", msgId)
	if err := g.request(ctx, token, path, &result); err != nil {
		return nil, err
	}

	// Try to extract plain text body
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
			decoded, _ := decodeBase64URL(data)
			return decoded
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
						decoded, _ := decodeBase64URL(data)
						return decoded
					}
				}
			}
		}
	}

	return ""
}

func decodeBase64URL(s string) (string, error) {
	// Gmail uses URL-safe base64
	s = strings.ReplaceAll(s, "-", "+")
	s = strings.ReplaceAll(s, "_", "/")

	// Add padding
	switch len(s) % 4 {
	case 2:
		s += "=="
	case 3:
		s += "="
	}

	data := make([]byte, len(s))
	n, err := decodeBase64(s, data)
	if err != nil {
		return "", err
	}
	return string(data[:n]), nil
}

func decodeBase64(s string, dst []byte) (int, error) {
	// Simple base64 decode (standard library import avoided for simplicity)
	const alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"
	lookup := make(map[byte]int)
	for i := range alphabet {
		lookup[alphabet[i]] = i
	}

	n := 0
	for i := 0; i < len(s); i += 4 {
		var val int
		for j := 0; j < 4 && i+j < len(s); j++ {
			c := s[i+j]
			if c == '=' {
				continue
			}
			if v, ok := lookup[c]; ok {
				val = val<<6 | v
			}
		}
		if i+3 < len(s) && s[i+3] == '=' {
			if s[i+2] == '=' {
				dst[n] = byte(val >> 4)
				n++
			} else {
				dst[n] = byte(val >> 10)
				dst[n+1] = byte(val >> 2)
				n += 2
			}
		} else {
			dst[n] = byte(val >> 16)
			dst[n+1] = byte(val >> 8)
			dst[n+2] = byte(val)
			n += 3
		}
	}
	return n, nil
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
