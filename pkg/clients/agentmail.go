package clients

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
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

type SendMessageParams struct {
	To      string `json:"to"`
	Subject string `json:"subject"`
	Text    string `json:"text"`
}

func (c *AgentMailClient) SendMessage(ctx context.Context, inboxID string, params SendMessageParams) error {
	return c.do(ctx, http.MethodPost, "/inboxes/"+inboxID+"/messages", params, nil)
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

	respBody, _ := io.ReadAll(resp.Body)

	if resp.StatusCode >= 300 {
		return fmt.Errorf("agentmail %s %s: %d %s", method, path, resp.StatusCode, string(respBody))
	}

	if dest != nil && len(respBody) > 0 {
		return json.Unmarshal(respBody, dest)
	}
	return nil
}
