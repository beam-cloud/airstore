package common

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
)

// S2Config configures the S2 stream client
type S2Config struct {
	// Token is the S2 API token
	Token string

	// Basin is the S2 basin name (e.g., "airstore")
	Basin string

	// Timeout for HTTP requests
	Timeout time.Duration
}

// S2Client provides access to S2 streams for append-only log storage
type S2Client struct {
	config     S2Config
	httpClient *http.Client
}

// NewS2Client creates a new S2 stream client
func NewS2Client(config S2Config) *S2Client {
	if config.Timeout == 0 {
		config.Timeout = 10 * time.Second
	}

	return &S2Client{
		config: config,
		httpClient: &http.Client{
			Timeout: config.Timeout,
		},
	}
}

// Enabled returns true if the S2 client is configured
func (c *S2Client) Enabled() bool {
	return c.config.Token != "" && c.config.Basin != ""
}

// TaskLogEntry represents a log entry for a task
type TaskLogEntry struct {
	TaskID    string         `json:"task_id"`
	Timestamp int64          `json:"timestamp"`
	SeqNum    int64          `json:"seq_num,omitempty"`
	EventID   string         `json:"event_id,omitempty"`
	Stream    string         `json:"stream"` // "stdout" or "stderr"
	Data      string         `json:"data"`
	ChunkType string         `json:"chunk_type,omitempty"`
	Metadata  map[string]any `json:"metadata,omitempty"`
}

// TaskStatusEntry represents a status change for a task
type TaskStatusEntry struct {
	TaskID    string `json:"task_id"`
	Timestamp int64  `json:"timestamp"`
	Status    string `json:"status"`
	ExitCode  *int   `json:"exit_code,omitempty"`
	Error     string `json:"error,omitempty"`
}

// RunEventEntry represents a lifecycle event emitted by the orchestration engine.
type RunEventEntry struct {
	RunID     string         `json:"run_id"`
	EventType string         `json:"event_type"`
	Timestamp int64          `json:"timestamp"`
	Payload   map[string]any `json:"payload,omitempty"`
}

// StreamNames provides consistent stream naming
type StreamNames struct{}

// TaskLogs returns the stream name for a task's logs
func (StreamNames) TaskLogs(taskID string) string {
	return fmt.Sprintf("task.%s.logs", taskID)
}

// TaskStatus returns the stream name for a task's status events
func (StreamNames) TaskStatus(taskID string) string {
	return fmt.Sprintf("task.%s.status", taskID)
}

// RunEvents returns the stream name for orchestration run events.
func (StreamNames) RunEvents(runID string) string {
	return fmt.Sprintf("run.%s.events", runID)
}

// ChannelConversation returns the stream name for a channel conversation between an agent and a sender.
func (StreamNames) ChannelConversation(agentID, senderHash string) string {
	return fmt.Sprintf("channel.%s.%s", agentID, senderHash)
}

// Streams provides access to stream names
var Streams = StreamNames{}

// AppendRecord represents a record to append
type appendRecord struct {
	Body string `json:"body"`
}

type appendRequest struct {
	Records []appendRecord `json:"records"`
}

// Append appends a record to a stream
func (c *S2Client) Append(ctx context.Context, stream string, data interface{}) error {
	if !c.Enabled() {
		return nil // Silently skip if not configured
	}

	body, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("failed to marshal data: %w", err)
	}

	req := appendRequest{
		Records: []appendRecord{{Body: string(body)}},
	}

	reqBody, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("failed to marshal request: %w", err)
	}

	url := c.url(fmt.Sprintf("/streams/%s/records", stream))
	httpReq, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(reqBody))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Authorization", "Bearer "+c.config.Token)

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("S2 error %d: %s", resp.StatusCode, string(body))
	}

	return nil
}

// AppendLog is a convenience method for appending a log entry
func (c *S2Client) AppendLog(ctx context.Context, taskID, stream, data string) error {
	chunkType, displayData, metadata := inferTaskLogChunk(data)
	entry := TaskLogEntry{
		TaskID:    taskID,
		Timestamp: time.Now().UnixMilli(),
		Stream:    stream,
		Data:      displayData,
		ChunkType: chunkType,
		Metadata:  metadata,
	}
	return c.Append(ctx, Streams.TaskLogs(taskID), entry)
}

// AppendStatus is a convenience method for appending a status entry
func (c *S2Client) AppendStatus(ctx context.Context, taskID, status string, exitCode *int, errorMsg string) error {
	entry := TaskStatusEntry{
		TaskID:    taskID,
		Timestamp: time.Now().UnixMilli(),
		Status:    status,
		ExitCode:  exitCode,
		Error:     errorMsg,
	}
	return c.Append(ctx, Streams.TaskStatus(taskID), entry)
}

// AppendRunEvent appends a run event entry to the run event stream.
func (c *S2Client) AppendRunEvent(ctx context.Context, runID, eventType string, payload map[string]any) error {
	entry := RunEventEntry{
		RunID:     runID,
		EventType: eventType,
		Timestamp: time.Now().UnixMilli(),
		Payload:   payload,
	}
	return c.Append(ctx, Streams.RunEvents(runID), entry)
}

// ReadRecord represents a record read from S2
type ReadRecord struct {
	SeqNum    int64  `json:"seq_num"`
	Timestamp int64  `json:"timestamp"`
	Body      string `json:"body"` // S2 returns body as a JSON-encoded string
}

type readResponse struct {
	Records []ReadRecord `json:"records"`
}

// Read reads records from a stream
func (c *S2Client) Read(ctx context.Context, stream string, seqNum int64, count int) ([]ReadRecord, error) {
	if !c.Enabled() {
		return nil, nil
	}

	if count <= 0 {
		count = 1000
	}

	url := c.url(fmt.Sprintf("/streams/%s/records?seq_num=%d&count=%d&clamp=true", stream, seqNum, count))
	httpReq, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	httpReq.Header.Set("Authorization", "Bearer "+c.config.Token)

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound || resp.StatusCode == http.StatusConflict ||
		resp.StatusCode == http.StatusRequestedRangeNotSatisfiable {
		// 404 = stream doesn't exist, 409 = deletion pending, 416 = cursor past end of stream
		return nil, nil
	}

	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("S2 error %d: %s", resp.StatusCode, string(body))
	}

	var result readResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return result.Records, nil
}

// ReadLogs reads log entries for a task.
// Returns the logs, the next sequence number for pagination, and any error.
// Pass nextSeqNum to subsequent calls to fetch logs beyond the first page.
func (c *S2Client) ReadLogs(ctx context.Context, taskID string, seqNum int64) ([]TaskLogEntry, int64, error) {
	records, err := c.Read(ctx, Streams.TaskLogs(taskID), seqNum, 1000)
	if err != nil {
		return nil, seqNum, err
	}

	logs := make([]TaskLogEntry, 0, len(records))
	nextSeqNum := seqNum
	for _, r := range records {
		var entry TaskLogEntry
		// Body is a JSON-encoded string, unmarshal it
		if err := json.Unmarshal([]byte(r.Body), &entry); err == nil {
			entry.SeqNum = r.SeqNum
			if strings.TrimSpace(entry.EventID) == "" {
				entry.EventID = fmt.Sprintf("%s:%d", taskID, r.SeqNum)
			}
			logs = append(logs, entry)
		}
		// Track the next sequence number (last seen + 1)
		if r.SeqNum >= nextSeqNum {
			nextSeqNum = r.SeqNum + 1
		}
	}
	return logs, nextSeqNum, nil
}

// FormatLogs converts log entries to plain text (one line per entry).
// Used by filesystem and CLI for consistent output formatting.
func FormatLogs(logs []TaskLogEntry) string {
	if len(logs) == 0 {
		return ""
	}
	var buf bytes.Buffer
	for _, e := range logs {
		buf.WriteString(e.Data)
		buf.WriteByte('\n')
	}
	return buf.String()
}

// StreamInfo represents a stream returned by the S2 list streams API.
type StreamInfo struct {
	Name string `json:"name"`
}

type listStreamsResponse struct {
	Streams []StreamInfo `json:"streams"`
	HasMore bool         `json:"has_more"`
}

// ListStreams lists streams whose names begin with the given prefix.
func (c *S2Client) ListStreams(ctx context.Context, prefix string) ([]StreamInfo, error) {
	if !c.Enabled() {
		return nil, nil
	}

	url := c.url("/streams") + "?prefix=" + prefix + "&limit=1000"
	httpReq, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	httpReq.Header.Set("Authorization", "Bearer "+c.config.Token)

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("S2 error %d: %s", resp.StatusCode, string(body))
	}

	var result listStreamsResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return result.Streams, nil
}

func (c *S2Client) url(path string) string {
	return fmt.Sprintf("https://%s.b.aws.s2.dev/v1%s", c.config.Basin, path)
}

func inferTaskLogChunk(raw string) (string, string, map[string]any) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return "text", raw, nil
	}

	if !strings.HasPrefix(trimmed, "{") || !strings.HasSuffix(trimmed, "}") {
		return "text", raw, nil
	}

	var payload map[string]any
	if err := json.Unmarshal([]byte(trimmed), &payload); err != nil {
		return "text", raw, nil
	}

	chunkType := strings.TrimSpace(stringFromAny(payload["type"]))
	if chunkType == "" {
		chunkType = "json"
	}

	for _, key := range []string{"text", "delta", "content"} {
		if value := strings.TrimSpace(stringFromAny(payload[key])); value != "" {
			return chunkType, value, payload
		}
	}

	return chunkType, raw, payload
}

func stringFromAny(value any) string {
	if value == nil {
		return ""
	}
	if typed, ok := value.(string); ok {
		return typed
	}
	body, err := json.Marshal(value)
	if err != nil {
		return ""
	}
	return string(body)
}

const redactedPlaceholder = "[REDACTED]"

var (
	sensitiveJSONStringValuePattern = regexp.MustCompile(`(?i)("([^"]*(?:api[_-]?key|secret|token|password|authorization|session_key|private_key|access_key)[^"]*)"\s*:\s*)"(.*?)"`)
	sensitiveAssignmentPattern      = regexp.MustCompile(`(?i)\b([A-Z0-9_]*(?:API[_-]?KEY|SECRET|TOKEN|PASSWORD|AUTHORIZATION|SESSION_KEY|PRIVATE_KEY|ACCESS_KEY)[A-Z0-9_]*)\s*=\s*("[^"]*"|'[^']*'|[^\s,;]+)`)
	bearerTokenPattern              = regexp.MustCompile(`(?i)\bBearer\s+[A-Za-z0-9._~+\-/]+=*\b`)
	anthropicKeyPattern             = regexp.MustCompile(`\bsk-[A-Za-z0-9_-]{8,}\b`)
)

func isSensitiveKey(key string) bool {
	normalized := strings.ToLower(strings.TrimSpace(key))
	if normalized == "" {
		return false
	}
	normalized = strings.ReplaceAll(normalized, "-", "_")
	normalized = strings.ReplaceAll(normalized, ".", "_")

	for _, needle := range []string{
		"api_key",
		"apikey",
		"secret",
		"token",
		"password",
		"authorization",
		"session_key",
		"private_key",
		"access_key",
	} {
		if strings.Contains(normalized, needle) {
			return true
		}
	}
	return false
}

// RedactSensitiveString masks likely secrets in plain text payloads.
func RedactSensitiveString(raw string) string {
	if raw == "" {
		return raw
	}
	redacted := sensitiveJSONStringValuePattern.ReplaceAllString(raw, `${1}"`+redactedPlaceholder+`"`)
	redacted = sensitiveAssignmentPattern.ReplaceAllString(redacted, `${1}=`+redactedPlaceholder)
	redacted = bearerTokenPattern.ReplaceAllString(redacted, `Bearer `+redactedPlaceholder)
	redacted = anthropicKeyPattern.ReplaceAllString(redacted, redactedPlaceholder)
	return redacted
}

// RedactSensitiveValue walks a nested value and masks secret-like content.
func RedactSensitiveValue(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		return RedactSensitiveMap(typed)
	case map[string]string:
		out := make(map[string]string, len(typed))
		for key, val := range typed {
			if isSensitiveKey(key) {
				out[key] = redactedPlaceholder
				continue
			}
			out[key] = RedactSensitiveString(val)
		}
		return out
	case []any:
		out := make([]any, len(typed))
		for idx, item := range typed {
			out[idx] = RedactSensitiveValue(item)
		}
		return out
	case []string:
		out := make([]string, len(typed))
		for idx, item := range typed {
			out[idx] = RedactSensitiveString(item)
		}
		return out
	case string:
		return RedactSensitiveString(typed)
	default:
		return value
	}
}

// RedactSensitiveMap clones and redacts a JSON-like map.
func RedactSensitiveMap(payload map[string]any) map[string]any {
	if payload == nil {
		return nil
	}
	out := make(map[string]any, len(payload))
	for key, value := range payload {
		if isSensitiveKey(key) {
			out[key] = redactedPlaceholder
			continue
		}
		out[key] = RedactSensitiveValue(value)
	}
	return out
}

// RedactSensitiveMaps clones and redacts a list of JSON-like maps.
func RedactSensitiveMaps(items []map[string]any) []map[string]any {
	if len(items) == 0 {
		return items
	}
	out := make([]map[string]any, len(items))
	for idx, item := range items {
		out[idx] = RedactSensitiveMap(item)
	}
	return out
}

// RedactTaskLogEntry clones a log entry with secret-like content redacted.
func RedactTaskLogEntry(entry TaskLogEntry) TaskLogEntry {
	entry.Data = RedactSensitiveString(entry.Data)
	entry.Metadata = RedactSensitiveMap(entry.Metadata)
	return entry
}

// RedactTaskLogEntries clones and redacts a task log slice.
func RedactTaskLogEntries(entries []TaskLogEntry) []TaskLogEntry {
	if len(entries) == 0 {
		return entries
	}
	out := make([]TaskLogEntry, len(entries))
	for idx, entry := range entries {
		out[idx] = RedactTaskLogEntry(entry)
	}
	return out
}
