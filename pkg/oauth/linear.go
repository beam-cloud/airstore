package oauth

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/beam-cloud/airstore/pkg/types"
	"golang.org/x/oauth2"
)

// Linear OAuth endpoints
var linearEndpoint = oauth2.Endpoint{
	AuthURL:  "https://linear.app/oauth/authorize",
	TokenURL: "https://api.linear.app/oauth/token",
}

// linearIntegrationScopes maps integration types to their required Linear OAuth scopes
var linearIntegrationScopes = map[string][]string{
	"linear": {
		"read",
		"write",
		"issues:create",
		"comments:create",
	},
}

// LinearProvider handles Linear OAuth operations for workspace integrations
// Implements the Provider interface
type LinearProvider struct {
	clientID     string
	clientSecret string
	redirectURL  string
	httpClient   *http.Client
}

// Ensure LinearProvider implements Provider interface
var _ Provider = (*LinearProvider)(nil)

// NewLinearProvider creates a new Linear OAuth provider from config
func NewLinearProvider(cfg types.IntegrationLinearOAuth) *LinearProvider {
	return &LinearProvider{
		clientID:     cfg.ClientID,
		clientSecret: cfg.ClientSecret,
		redirectURL:  cfg.RedirectURL,
		httpClient:   &http.Client{Timeout: 30 * time.Second},
	}
}

// Name returns the provider name
func (l *LinearProvider) Name() string {
	return "linear"
}

// IsConfigured returns true if Linear OAuth is configured
func (l *LinearProvider) IsConfigured() bool {
	return l.clientID != "" && l.clientSecret != "" && l.redirectURL != ""
}

// SupportsIntegration returns true if this provider handles the given integration type
func (l *LinearProvider) SupportsIntegration(integrationType string) bool {
	_, ok := linearIntegrationScopes[integrationType]
	return ok
}

// AuthorizeURL generates the Linear OAuth authorization URL for an integration
func (l *LinearProvider) AuthorizeURL(state, integrationType string) (string, error) {
	scopes, ok := linearIntegrationScopes[integrationType]
	if !ok {
		return "", fmt.Errorf("unsupported integration: %s", integrationType)
	}

	// Linear uses comma-separated scopes
	params := url.Values{
		"client_id":     {l.clientID},
		"redirect_uri":  {l.redirectURL},
		"response_type": {"code"},
		"scope":         {strings.Join(scopes, ",")},
		"state":         {state},
		"prompt":        {"consent"},
	}

	return linearEndpoint.AuthURL + "?" + params.Encode(), nil
}

// Exchange exchanges an authorization code for tokens
func (l *LinearProvider) Exchange(ctx context.Context, code, integrationType string) (*types.IntegrationCredentials, error) {
	if _, ok := linearIntegrationScopes[integrationType]; !ok {
		return nil, fmt.Errorf("unsupported integration: %s", integrationType)
	}

	// Linear token exchange
	data := url.Values{
		"client_id":     {l.clientID},
		"client_secret": {l.clientSecret},
		"code":          {code},
		"redirect_uri":  {l.redirectURL},
		"grant_type":    {"authorization_code"},
	}

	req, err := http.NewRequestWithContext(ctx, "POST", linearEndpoint.TokenURL, strings.NewReader(data.Encode()))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")

	resp, err := l.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("exchange failed: status %d", resp.StatusCode)
	}

	var result struct {
		AccessToken  string `json:"access_token"`
		TokenType    string `json:"token_type"`
		ExpiresIn    int    `json:"expires_in"`
		Scope        string `json:"scope"`
	}

	if err := decodeJSON(resp.Body, &result); err != nil {
		return nil, fmt.Errorf("parse response: %w", err)
	}

	creds := &types.IntegrationCredentials{
		AccessToken: result.AccessToken,
	}

	// Linear tokens expire (typically 10 years, but we track it)
	if result.ExpiresIn > 0 {
		expiry := time.Now().Add(time.Duration(result.ExpiresIn) * time.Second)
		creds.ExpiresAt = &expiry
	}

	return creds, nil
}

// Refresh refreshes an access token using a refresh token
// Note: Linear access tokens are long-lived and don't use refresh tokens
func (l *LinearProvider) Refresh(ctx context.Context, refreshToken string) (*types.IntegrationCredentials, error) {
	// Linear doesn't use refresh tokens - tokens are long-lived
	return nil, fmt.Errorf("linear tokens do not support refresh")
}
