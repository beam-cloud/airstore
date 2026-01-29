package clients

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strings"
	"sync/atomic"
	"time"

	"github.com/beam-cloud/airstore/pkg/types"
	"github.com/rs/zerolog/log"
)

const (
	GmailAPIBase = "https://gmail.googleapis.com/gmail/v1"
)

// GmailCategories are the supported message categories
var GmailCategories = []string{"unread", "inbox", "starred", "sent", "important"}

// CategoryQueries maps category names to Gmail API query/label
var CategoryQueries = map[string]string{
	"unread":    "q=is:unread",
	"inbox":     "labelIds=INBOX",
	"starred":   "labelIds=STARRED",
	"sent":      "labelIds=SENT",
	"important": "labelIds=IMPORTANT",
}

// API call counter for metrics
var gmailAPICallCount int64

// GetGmailAPICallCount returns the current API call count
func GetGmailAPICallCount() int64 {
	return atomic.LoadInt64(&gmailAPICallCount)
}

// ResetGmailAPICallCount resets the API call counter
func ResetGmailAPICallCount() {
	atomic.StoreInt64(&gmailAPICallCount, 0)
}

// GmailMessage represents a parsed Gmail message
type GmailMessage struct {
	ID            string
	ThreadID      string
	From          string
	To            string
	Subject       string
	Date          string
	Snippet       string
	Labels        []string
	SenderFolder  string // Sanitized sender for folder name
	SubjectFolder string // Sanitized subject with date and ID
}

// GmailClient provides shared Gmail API functionality
type GmailClient struct {
	HTTPClient *http.Client
}

// NewGmailClient creates a new Gmail API client
func NewGmailClient() *GmailClient {
	return &GmailClient{
		HTTPClient: &http.Client{Timeout: 60 * time.Second},
	}
}

// Integration returns the integration name
func (c *GmailClient) Integration() types.ToolName {
	return types.ToolGmail
}

// Request makes a GET request to the Gmail API
func (c *GmailClient) Request(ctx context.Context, token, path string, result any) error {
	count := atomic.AddInt64(&gmailAPICallCount, 1)
	log.Debug().Int64("api_calls", count).Str("path", path).Msg("gmail API call")

	url := GmailAPIBase + path
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return err
	}

	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Accept", "application/json")

	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("gmail API error %d: %s", resp.StatusCode, string(body))
	}

	return json.NewDecoder(resp.Body).Decode(result)
}

// ListMessages lists message IDs with optional query
func (c *GmailClient) ListMessages(ctx context.Context, token, query string, maxResults int) ([]string, error) {
	path := fmt.Sprintf("/users/me/messages?maxResults=%d", maxResults)
	if query != "" {
		path += "&" + query
	}

	var result map[string]any
	if err := c.Request(ctx, token, path, &result); err != nil {
		return nil, err
	}

	rawMessages, _ := result["messages"].([]any)
	ids := make([]string, 0, len(rawMessages))
	for _, m := range rawMessages {
		if msgMap, ok := m.(map[string]any); ok {
			if id := getString(msgMap, "id"); id != "" {
				ids = append(ids, id)
			}
		}
	}
	return ids, nil
}

// GetMessage fetches a single message with metadata
func (c *GmailClient) GetMessage(ctx context.Context, token, msgID, format string) (map[string]any, error) {
	path := fmt.Sprintf("/users/me/messages/%s?format=%s", msgID, format)
	var result map[string]any
	if err := c.Request(ctx, token, path, &result); err != nil {
		return nil, err
	}
	return result, nil
}

// ParseMessage extracts structured data from a Gmail API response
func (c *GmailClient) ParseMessage(result map[string]any) *GmailMessage {
	msg := &GmailMessage{
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
	msg.SenderFolder = ExtractSenderName(msg.From)
	msg.SubjectFolder = FormatSubjectFolder(msg.Subject, msg.Date, msg.ID)

	return msg
}

// ExtractPlainTextBody extracts plain text from a Gmail message payload
func (c *GmailClient) ExtractPlainTextBody(msg map[string]any) string {
	return ExtractPlainTextBody(msg)
}

// ExtractMessageBody extracts the best available text body from a Gmail message,
// trying text/plain first, then falling back to text/html converted to plain text
func (c *GmailClient) ExtractMessageBody(msg map[string]any) string {
	return ExtractMessageBody(msg)
}

// DetectCategory determines the primary category for a message
func (c *GmailClient) DetectCategory(labels []string) string {
	return DetectCategory(labels)
}

// ExtractPlainTextBody extracts plain text from a Gmail message payload
func ExtractPlainTextBody(msg map[string]any) string {
	payload, ok := msg["payload"].(map[string]any)
	if !ok {
		return ""
	}

	// Try to get text/plain recursively
	return extractMimePartRecursive(payload, "text/plain")
}

// ExtractMessageBody extracts the best available text body from a Gmail message,
// trying text/plain first, then falling back to text/html converted to plain text
func ExtractMessageBody(msg map[string]any) string {
	payload, ok := msg["payload"].(map[string]any)
	if !ok {
		return ""
	}

	// Try text/plain first
	if plainText := extractMimePartRecursive(payload, "text/plain"); plainText != "" {
		return plainText
	}

	// Fall back to HTML converted to text
	if htmlText := extractMimePartRecursive(payload, "text/html"); htmlText != "" {
		return StripHTML(htmlText)
	}

	// Check if there's a body directly on the payload
	if body, ok := payload["body"].(map[string]any); ok {
		mimeType, _ := payload["mimeType"].(string)
		if decoded := decodeBodyData(body); decoded != "" {
			if strings.HasPrefix(mimeType, "text/html") {
				return StripHTML(decoded)
			}
			return decoded
		}
	}

	return ""
}

// extractMimePartRecursive recursively searches for a MIME part with the given type
func extractMimePartRecursive(part map[string]any, targetMimeType string) string {
	mimeType, _ := part["mimeType"].(string)

	// Direct match
	if mimeType == targetMimeType {
		if body, ok := part["body"].(map[string]any); ok {
			return decodeBodyData(body)
		}
	}

	// Check for nested parts (multipart/*)
	if parts, ok := part["parts"].([]any); ok {
		// First pass: look for exact match at this level
		for _, p := range parts {
			subPart, ok := p.(map[string]any)
			if !ok {
				continue
			}
			subMimeType, _ := subPart["mimeType"].(string)
			if subMimeType == targetMimeType {
				if body, ok := subPart["body"].(map[string]any); ok {
					if decoded := decodeBodyData(body); decoded != "" {
						return decoded
					}
				}
			}
		}

		// Second pass: recurse into multipart containers
		for _, p := range parts {
			subPart, ok := p.(map[string]any)
			if !ok {
				continue
			}
			subMimeType, _ := subPart["mimeType"].(string)
			if strings.HasPrefix(subMimeType, "multipart/") {
				if result := extractMimePartRecursive(subPart, targetMimeType); result != "" {
					return result
				}
			}
		}

		// Third pass: recurse into any part that has nested parts
		for _, p := range parts {
			subPart, ok := p.(map[string]any)
			if !ok {
				continue
			}
			if _, hasParts := subPart["parts"]; hasParts {
				if result := extractMimePartRecursive(subPart, targetMimeType); result != "" {
					return result
				}
			}
		}
	}

	return ""
}

// decodeBodyData decodes the base64url-encoded body data from a Gmail message part
func decodeBodyData(body map[string]any) string {
	data, ok := body["data"].(string)
	if !ok || data == "" {
		return ""
	}

	// Gmail uses URL-safe base64 encoding, often without padding
	// Try RawURLEncoding first (no padding), then URLEncoding (with padding)
	decoded, err := base64.RawURLEncoding.DecodeString(data)
	if err != nil {
		// Try with standard URL encoding (has padding)
		decoded, err = base64.URLEncoding.DecodeString(data)
		if err != nil {
			// Try adding padding and decode again
			padded := data
			switch len(data) % 4 {
			case 2:
				padded += "=="
			case 3:
				padded += "="
			}
			decoded, _ = base64.URLEncoding.DecodeString(padded)
		}
	}

	return string(decoded)
}

// DetectCategory determines the primary category for a message based on labels
func DetectCategory(labels []string) string {
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

// noReplyPatterns are common patterns for automated/noreply addresses
var noReplyPatterns = []string{
	"noreply", "no-reply", "no_reply", "donotreply", "do-not-reply",
	"notifications", "notification", "mailer", "mailer-daemon",
	"postmaster", "bounce", "auto", "automated", "system",
}

// ExtractSenderName extracts a clean sender name from a From header
// "noreply@calendly.com" -> "Calendly"
// "KAYAK <kayak@msg.kayak.com>" -> "KAYAK"
func ExtractSenderName(from string) string {
	// Try to extract name from "Name <email>" format
	if idx := strings.Index(from, "<"); idx > 0 {
		name := strings.TrimSpace(from[:idx])
		name = strings.Trim(name, "\"'")
		if name != "" && !isGenericSenderName(name) {
			return SanitizeFolderName(name)
		}
	}

	// Extract email address
	email := from
	if idx := strings.Index(from, "<"); idx >= 0 {
		end := strings.Index(from[idx:], ">")
		if end > 0 {
			email = from[idx+1 : idx+end]
		}
	}
	email = strings.TrimSpace(email)

	// Parse the email address
	if atIdx := strings.Index(email, "@"); atIdx > 0 {
		localPart := email[:atIdx]
		domain := email[atIdx+1:]

		// Check if local part is a noreply/automated pattern
		localLower := strings.ToLower(localPart)
		isNoReply := false
		for _, pattern := range noReplyPatterns {
			if strings.Contains(localLower, pattern) {
				isNoReply = true
				break
			}
		}

		// If it's a noreply address, use the domain name
		if isNoReply {
			parts := strings.Split(domain, ".")
			if len(parts) >= 2 {
				name := parts[len(parts)-2]
				if len(name) > 0 {
					return strings.ToUpper(name[:1]) + name[1:]
				}
			}
		}

		// Otherwise, try using the local part if it looks like a name
		if isLikelyPersonName(localPart) {
			return SanitizeFolderName(localPart)
		}

		// Fall back to domain name
		parts := strings.Split(domain, ".")
		if len(parts) >= 2 {
			name := parts[len(parts)-2]
			if len(name) > 0 {
				return strings.ToUpper(name[:1]) + name[1:]
			}
		}
	}

	return SanitizeFolderName(email)
}

// isGenericSenderName checks if a display name is too generic to be useful
func isGenericSenderName(name string) bool {
	lower := strings.ToLower(name)
	genericNames := []string{
		"info", "support", "team", "admin", "contact", "hello",
		"service", "customer", "help", "sales", "billing",
	}
	for _, g := range genericNames {
		if lower == g {
			return true
		}
	}
	return false
}

// isLikelyPersonName checks if a local part looks like a person's name
func isLikelyPersonName(localPart string) bool {
	if len(localPart) < 2 {
		return false
	}

	lower := strings.ToLower(localPart)
	for _, pattern := range noReplyPatterns {
		if strings.Contains(lower, pattern) {
			return false
		}
	}

	hasLetter := false
	hasDigit := false
	for _, c := range localPart {
		if c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' {
			hasLetter = true
		}
		if c >= '0' && c <= '9' {
			hasDigit = true
		}
	}

	if hasDigit && !hasLetter {
		return false
	}

	return hasLetter
}

// FormatSubjectFolder creates a folder name from subject, date, and ID
func FormatSubjectFolder(subject, dateStr, msgID string) string {
	// Parse date
	datePrefix := "unknown"
	dateFormats := []string{
		time.RFC1123Z,
		"Mon, 2 Jan 2006 15:04:05 -0700",
		"Mon, 2 Jan 2006 15:04:05 MST",
		time.RFC3339,
	}
	for _, fmt := range dateFormats {
		if t, err := time.Parse(fmt, dateStr); err == nil {
			datePrefix = t.Format("2006-01-02")
			break
		}
	}

	// Sanitize subject
	cleanSubject := SanitizeFolderName(subject)
	if cleanSubject == "" || cleanSubject == "unknown" {
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

// ParseEmailDate parses various email date formats
func ParseEmailDate(dateStr string) time.Time {
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

var htmlTagRegex = regexp.MustCompile(`<[^>]*>`)

// StripHTML removes HTML tags from a string
func StripHTML(html string) string {
	text := htmlTagRegex.ReplaceAllString(html, " ")
	text = strings.ReplaceAll(text, "&nbsp;", " ")
	text = strings.ReplaceAll(text, "&amp;", "&")
	text = strings.ReplaceAll(text, "&lt;", "<")
	text = strings.ReplaceAll(text, "&gt;", ">")
	// Collapse whitespace
	text = regexp.MustCompile(`\s+`).ReplaceAllString(text, " ")
	return strings.TrimSpace(text)
}
