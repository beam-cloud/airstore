package streams

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
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
	TaskID    string `json:"task_id"`
	Timestamp int64  `json:"timestamp"`
	Stream    string `json:"stream"` // "stdout" or "stderr"
	Data      string `json:"data"`
}

// TaskStatusEntry represents a status change for a task
type TaskStatusEntry struct {
	TaskID    string `json:"task_id"`
	Timestamp int64  `json:"timestamp"`
	Status    string `json:"status"`
	ExitCode  *int   `json:"exit_code,omitempty"`
	Error     string `json:"error,omitempty"`
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
	entry := TaskLogEntry{
		TaskID:    taskID,
		Timestamp: time.Now().UnixMilli(),
		Stream:    stream,
		Data:      data,
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

// ReadRecord represents a record read from S2
type ReadRecord struct {
	SeqNum    int64           `json:"seq_num"`
	Timestamp int64           `json:"timestamp"`
	Body      json.RawMessage `json:"body"`
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

	if resp.StatusCode == http.StatusNotFound {
		return nil, nil // Stream doesn't exist yet
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

// ReadLogs reads log entries for a task
func (c *S2Client) ReadLogs(ctx context.Context, taskID string, seqNum int64) ([]TaskLogEntry, error) {
	records, err := c.Read(ctx, Streams.TaskLogs(taskID), seqNum, 1000)
	if err != nil {
		return nil, err
	}

	logs := make([]TaskLogEntry, 0, len(records))
	for _, r := range records {
		var entry TaskLogEntry
		if err := json.Unmarshal(r.Body, &entry); err == nil {
			logs = append(logs, entry)
		}
	}
	return logs, nil
}

func (c *S2Client) url(path string) string {
	return fmt.Sprintf("https://%s.b.aws.s2.dev/v1%s", c.config.Basin, path)
}
