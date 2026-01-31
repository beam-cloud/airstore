package oauth

import (
	"context"
	"encoding/base64"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/beam-cloud/airstore/pkg/types"
	"golang.org/x/oauth2"
)

// Notion OAuth endpoints
var notionEndpoint = oauth2.Endpoint{
	AuthURL:  "https://api.notion.com/v1/oauth/authorize",
	TokenURL: "https://api.notion.com/v1/oauth/token",
}

// notionIntegrationScopes maps integration types to their required Notion OAuth scopes
// Notion doesn't use traditional scopes - access is determined by the pages the user selects
var notionIntegrationScopes = map[string][]string{
	"notion": {}, // Notion uses page-level permissions, not scopes
}

// NotionProvider handles Notion OAuth operations for workspace integrations
// Implements the Provider interface
type NotionProvider struct {
	clientID     string
	clientSecret string
	redirectURL  string
	httpClient   *http.Client
}

// Ensure NotionProvider implements Provider interface
var _ Provider = (*NotionProvider)(nil)

// NewNotionProvider creates a new Notion OAuth provider from config
func NewNotionProvider(cfg types.IntegrationNotionOAuth) *NotionProvider {
	return &NotionProvider{
		clientID:     cfg.ClientID,
		clientSecret: cfg.ClientSecret,
		redirectURL:  cfg.RedirectURL,
		httpClient:   &http.Client{Timeout: 30 * time.Second},
	}
}

// Name returns the provider name
func (n *NotionProvider) Name() string {
	return "notion"
}

// IsConfigured returns true if Notion OAuth is configured
func (n *NotionProvider) IsConfigured() bool {
	return n.clientID != "" && n.clientSecret != "" && n.redirectURL != ""
}

// SupportsIntegration returns true if this provider handles the given integration type
func (n *NotionProvider) SupportsIntegration(integrationType string) bool {
	_, ok := notionIntegrationScopes[integrationType]
	return ok
}

// AuthorizeURL generates the Notion OAuth authorization URL for an integration
func (n *NotionProvider) AuthorizeURL(state, integrationType string) (string, error) {
	if _, ok := notionIntegrationScopes[integrationType]; !ok {
		return "", fmt.Errorf("unsupported integration: %s", integrationType)
	}

	// Notion uses a simpler OAuth flow without traditional scopes
	params := url.Values{
		"client_id":     {n.clientID},
		"redirect_uri":  {n.redirectURL},
		"response_type": {"code"},
		"owner":         {"user"},
		"state":         {state},
	}

	return notionEndpoint.AuthURL + "?" + params.Encode(), nil
}

// Exchange exchanges an authorization code for tokens
func (n *NotionProvider) Exchange(ctx context.Context, code, integrationType string) (*types.IntegrationCredentials, error) {
	if _, ok := notionIntegrationScopes[integrationType]; !ok {
		return nil, fmt.Errorf("unsupported integration: %s", integrationType)
	}

	// Notion requires Basic auth with client_id:client_secret
	data := url.Values{
		"grant_type":   {"authorization_code"},
		"code":         {code},
		"redirect_uri": {n.redirectURL},
	}

	req, err := http.NewRequestWithContext(ctx, "POST", notionEndpoint.TokenURL, strings.NewReader(data.Encode()))
	if err != nil {
		return nil, err
	}

	// Notion uses Basic auth for token exchange
	auth := base64.StdEncoding.EncodeToString([]byte(n.clientID + ":" + n.clientSecret))
	req.Header.Set("Authorization", "Basic "+auth)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")

	resp, err := n.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("exchange failed: status %d", resp.StatusCode)
	}

	var result struct {
		AccessToken   string `json:"access_token"`
		TokenType     string `json:"token_type"`
		BotID         string `json:"bot_id"`
		WorkspaceID   string `json:"workspace_id"`
		WorkspaceName string `json:"workspace_name"`
		WorkspaceIcon string `json:"workspace_icon"`
		Owner         struct {
			Type string `json:"type"`
			User struct {
				ID string `json:"id"`
			} `json:"user"`
		} `json:"owner"`
	}

	if err := decodeJSON(resp.Body, &result); err != nil {
		return nil, fmt.Errorf("parse response: %w", err)
	}

	// Notion tokens don't expire and don't have refresh tokens
	creds := &types.IntegrationCredentials{
		AccessToken: result.AccessToken,
		Extra: map[string]string{
			"bot_id":         result.BotID,
			"workspace_id":   result.WorkspaceID,
			"workspace_name": result.WorkspaceName,
		},
	}

	return creds, nil
}

// Refresh refreshes an access token using a refresh token
// Note: Notion tokens don't expire and don't have refresh tokens
func (n *NotionProvider) Refresh(ctx context.Context, refreshToken string) (*types.IntegrationCredentials, error) {
	// Notion tokens don't expire and don't support refresh
	return nil, fmt.Errorf("notion tokens do not support refresh")
}
