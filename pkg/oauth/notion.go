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

var notionEndpoint = oauth2.Endpoint{
	AuthURL:  "https://api.notion.com/v1/oauth/authorize",
	TokenURL: "https://api.notion.com/v1/oauth/token",
}

var notionIntegrations = []string{"notion"}

// NotionProvider handles Notion OAuth operations.
type NotionProvider struct {
	clientID     string
	clientSecret string
	callbackURL  string
	httpClient   *http.Client
}

var _ Provider = (*NotionProvider)(nil)

func NewNotionProvider(creds types.ProviderOAuthCredentials, callbackURL string) *NotionProvider {
	return &NotionProvider{
		clientID:     creds.ClientID,
		clientSecret: creds.ClientSecret,
		callbackURL:  callbackURL,
		httpClient:   &http.Client{Timeout: 30 * time.Second},
	}
}

func (n *NotionProvider) Name() string {
	return "notion"
}

func (n *NotionProvider) IsConfigured() bool {
	return n.clientID != "" && n.clientSecret != "" && n.callbackURL != ""
}

func (n *NotionProvider) Integrations() []string {
	return notionIntegrations
}

func (n *NotionProvider) AuthorizeURL(state, integrationType string) (string, error) {
	params := url.Values{
		"client_id":     {n.clientID},
		"redirect_uri":  {n.callbackURL},
		"response_type": {"code"},
		"owner":         {"user"},
		"state":         {state},
	}

	return notionEndpoint.AuthURL + "?" + params.Encode(), nil
}

func (n *NotionProvider) Exchange(ctx context.Context, code, integrationType string) (*types.IntegrationCredentials, error) {
	data := url.Values{
		"grant_type":   {"authorization_code"},
		"code":         {code},
		"redirect_uri": {n.callbackURL},
	}

	req, err := http.NewRequestWithContext(ctx, "POST", notionEndpoint.TokenURL, strings.NewReader(data.Encode()))
	if err != nil {
		return nil, err
	}

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
		BotID         string `json:"bot_id"`
		WorkspaceID   string `json:"workspace_id"`
		WorkspaceName string `json:"workspace_name"`
	}

	if err := decodeJSON(resp.Body, &result); err != nil {
		return nil, fmt.Errorf("parse response: %w", err)
	}

	return &types.IntegrationCredentials{
		AccessToken: result.AccessToken,
		Extra: map[string]string{
			"bot_id":         result.BotID,
			"workspace_id":   result.WorkspaceID,
			"workspace_name": result.WorkspaceName,
		},
	}, nil
}

func (n *NotionProvider) Refresh(ctx context.Context, refreshToken string) (*types.IntegrationCredentials, error) {
	return nil, fmt.Errorf("notion tokens do not support refresh")
}
