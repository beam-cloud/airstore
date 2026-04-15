package clients

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const agentmailDefaultBaseURL = "https://api.agentmail.to/v0"

type AgentMailConfig struct {
	APIKey  string `key:"apiKey" json:"api_key"`
	BaseURL string `key:"baseUrl" json:"base_url"`
	Domain  string `key:"domain" json:"domain"`
}

func (c AgentMailConfig) Enabled() bool { return c.APIKey != "" }

type AgentMailClient struct {
	apiKey  string
	baseURL string
	domain  string
	http    *http.Client
}

func NewAgentMailClient(cfg AgentMailConfig) *AgentMailClient {
	base := cfg.BaseURL
	if base == "" {
		base = agentmailDefaultBaseURL
	}
	return &AgentMailClient{
		apiKey:  cfg.APIKey,
		baseURL: base,
		domain:  cfg.Domain,
		http:    &http.Client{Timeout: 15 * time.Second},
	}
}

func (c *AgentMailClient) Domain() string { return c.domain }

type AgentMailInbox struct {
	PodID       string `json:"pod_id"`
	InboxID     string `json:"inbox_id"` // the email address (e.g. "agent@agentmail.to")
	DisplayName string `json:"display_name"`
	ClientID    string `json:"client_id"`
	CreatedAt   string `json:"created_at"`
	UpdatedAt   string `json:"updated_at"`
}

type CreateInboxParams struct {
	Username    string `json:"username,omitempty"`
	Domain      string `json:"domain,omitempty"`
	DisplayName string `json:"display_name,omitempty"`
	ClientID    string `json:"client_id,omitempty"`
}

// CreateOrGetInbox creates an inbox, or returns the existing one if it already exists.
func (c *AgentMailClient) CreateOrGetInbox(ctx context.Context, params CreateInboxParams) (*AgentMailInbox, error) {
	if params.Domain == "" && c.domain != "" {
		params.Domain = c.domain
	}
	var inbox AgentMailInbox
	err := c.do(ctx, http.MethodPost, "/inboxes", params, &inbox)
	if err == nil {
		return &inbox, nil
	}

	// If inbox already exists, look it up by the expected address
	if strings.Contains(err.Error(), "AlreadyExists") && params.Username != "" {
		domain := params.Domain
		if domain == "" {
			domain = "agentmail.to"
		}
		existing, getErr := c.GetInbox(ctx, params.Username+"@"+domain)
		if getErr == nil {
			return existing, nil
		}
	}

	return nil, fmt.Errorf("create inbox: %w", err)
}

func (c *AgentMailClient) GetInbox(ctx context.Context, inboxID string) (*AgentMailInbox, error) {
	var inbox AgentMailInbox
	if err := c.do(ctx, http.MethodGet, "/inboxes/"+inboxID, nil, &inbox); err != nil {
		return nil, fmt.Errorf("get inbox: %w", err)
	}
	return &inbox, nil
}

func (c *AgentMailClient) DeleteInbox(ctx context.Context, inboxID string) error {
	return c.do(ctx, http.MethodDelete, "/inboxes/"+inboxID, nil, nil)
}

type AgentMailMessage struct {
	MessageID string   `json:"message_id"`
	InboxID   string   `json:"inbox_id"`
	ThreadID  string   `json:"thread_id"`
	From      string   `json:"from"`
	To        []string `json:"to"`
	ReplyTo   []string `json:"reply_to"`
	Subject   string   `json:"subject"`
	Text      string   `json:"text"`
	HTML      string   `json:"html"`
	CreatedAt string   `json:"created_at"`
}

type AgentMailThread struct {
	ThreadID string             `json:"thread_id"`
	Messages []AgentMailMessage `json:"messages"`
}

type agentMailListResponse[T any] struct {
	Items     []T    `json:"items"`
	NextToken string `json:"next_token"`
}

type SendMessageParams struct {
	To      string `json:"to"`
	Subject string `json:"subject"`
	Text    string `json:"text"`
}

func (c *AgentMailClient) SendMessage(ctx context.Context, inboxID string, params SendMessageParams) error {
	return c.do(ctx, http.MethodPost, "/inboxes/"+inboxID+"/messages/send", params, nil)
}

// RegisterWebhook idempotently registers a webhook for message.received events.
func (c *AgentMailClient) RegisterWebhook(ctx context.Context, url string) error {
	body := map[string]any{
		"url":         url,
		"event_types": []string{"message.received"},
		"client_id":   "airstore",
	}
	err := c.do(ctx, http.MethodPost, "/webhooks", body, nil)
	if err != nil && strings.Contains(err.Error(), "AlreadyExists") {
		return nil // idempotent
	}
	return err
}

// ListInboxes returns all inboxes for the authenticated account.
func (c *AgentMailClient) ListInboxes(ctx context.Context, limit int, pageToken string) ([]AgentMailInbox, string, error) {
	q := fmt.Sprintf("/inboxes?limit=%d", limit)
	if pageToken != "" {
		q += "&page_token=" + url.QueryEscape(pageToken)
	}
	var resp agentMailListResponse[AgentMailInbox]
	if err := c.do(ctx, http.MethodGet, q, nil, &resp); err != nil {
		return nil, "", fmt.Errorf("list inboxes: %w", err)
	}
	return resp.Items, resp.NextToken, nil
}

// ListMessages returns messages in an inbox, newest first.
func (c *AgentMailClient) ListMessages(ctx context.Context, inboxID string, limit int, pageToken string) ([]AgentMailMessage, string, error) {
	q := fmt.Sprintf("/inboxes/%s/messages?limit=%d", inboxID, limit)
	if pageToken != "" {
		q += "&page_token=" + url.QueryEscape(pageToken)
	}
	var resp agentMailListResponse[AgentMailMessage]
	if err := c.do(ctx, http.MethodGet, q, nil, &resp); err != nil {
		return nil, "", fmt.Errorf("list messages: %w", err)
	}
	return resp.Items, resp.NextToken, nil
}

// GetMessage returns a single message by ID.
func (c *AgentMailClient) GetMessage(ctx context.Context, inboxID, messageID string) (*AgentMailMessage, error) {
	var msg AgentMailMessage
	if err := c.do(ctx, http.MethodGet, fmt.Sprintf("/inboxes/%s/messages/%s", inboxID, messageID), nil, &msg); err != nil {
		return nil, fmt.Errorf("get message: %w", err)
	}
	return &msg, nil
}

// ListThreads returns threads in an inbox.
func (c *AgentMailClient) ListThreads(ctx context.Context, inboxID string, limit int, pageToken string) ([]AgentMailThread, string, error) {
	q := fmt.Sprintf("/inboxes/%s/threads?limit=%d", inboxID, limit)
	if pageToken != "" {
		q += "&page_token=" + url.QueryEscape(pageToken)
	}
	var resp agentMailListResponse[AgentMailThread]
	if err := c.do(ctx, http.MethodGet, q, nil, &resp); err != nil {
		return nil, "", fmt.Errorf("list threads: %w", err)
	}
	return resp.Items, resp.NextToken, nil
}

// GetThread returns a thread with all its messages.
func (c *AgentMailClient) GetThread(ctx context.Context, inboxID, threadID string) (*AgentMailThread, error) {
	var thread AgentMailThread
	if err := c.do(ctx, http.MethodGet, fmt.Sprintf("/inboxes/%s/threads/%s", inboxID, threadID), nil, &thread); err != nil {
		return nil, fmt.Errorf("get thread: %w", err)
	}
	return &thread, nil
}

// ReplyToMessage sends a reply to a specific message.
func (c *AgentMailClient) ReplyToMessage(ctx context.Context, inboxID, messageID, text string) error {
	body := map[string]string{"text": text}
	return c.do(ctx, http.MethodPost, fmt.Sprintf("/inboxes/%s/messages/%s/reply", inboxID, messageID), body, nil)
}

func (c *AgentMailClient) do(ctx context.Context, method, path string, body any, dest any) error {
	var reqBody io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return err
		}
		reqBody = bytes.NewReader(b)
	}

	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, reqBody)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+c.apiKey)
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("agentmail %s %s: read body: %w", method, path, err)
	}

	if resp.StatusCode >= 300 {
		return fmt.Errorf("agentmail %s %s: %d %s", method, path, resp.StatusCode, string(respBody))
	}

	if dest != nil && len(respBody) > 0 {
		return json.Unmarshal(respBody, dest)
	}
	return nil
}
