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
	"golang.org/x/oauth2/github"
)

// githubIntegrationScopes maps integration types to their required GitHub OAuth scopes
var githubIntegrationScopes = map[string][]string{
	"github": {
		"repo",
		"user:email",
	},
}

// GitHubProvider handles GitHub OAuth operations for workspace integrations
// Implements the Provider interface
type GitHubProvider struct {
	clientID     string
	clientSecret string
	redirectURL  string
	httpClient   *http.Client
}

// Ensure GitHubProvider implements Provider interface
var _ Provider = (*GitHubProvider)(nil)

// NewGitHubProvider creates a new GitHub OAuth provider from config
func NewGitHubProvider(cfg types.IntegrationGitHubOAuth) *GitHubProvider {
	return &GitHubProvider{
		clientID:     cfg.ClientID,
		clientSecret: cfg.ClientSecret,
		redirectURL:  cfg.RedirectURL,
		httpClient:   &http.Client{Timeout: 30 * time.Second},
	}
}

// Name returns the provider name
func (g *GitHubProvider) Name() string {
	return "github"
}

// IsConfigured returns true if GitHub OAuth is configured
func (g *GitHubProvider) IsConfigured() bool {
	return g.clientID != "" && g.clientSecret != "" && g.redirectURL != ""
}

// SupportsIntegration returns true if this provider handles the given integration type
func (g *GitHubProvider) SupportsIntegration(integrationType string) bool {
	_, ok := githubIntegrationScopes[integrationType]
	return ok
}

// AuthorizeURL generates the GitHub OAuth authorization URL for an integration
func (g *GitHubProvider) AuthorizeURL(state, integrationType string) (string, error) {
	scopes, ok := githubIntegrationScopes[integrationType]
	if !ok {
		return "", fmt.Errorf("unsupported integration: %s", integrationType)
	}

	cfg := g.oauthConfig(scopes)

	return cfg.AuthCodeURL(state), nil
}

// Exchange exchanges an authorization code for tokens
func (g *GitHubProvider) Exchange(ctx context.Context, code, integrationType string) (*types.IntegrationCredentials, error) {
	scopes, ok := githubIntegrationScopes[integrationType]
	if !ok {
		return nil, fmt.Errorf("unsupported integration: %s", integrationType)
	}

	cfg := g.oauthConfig(scopes)

	token, err := cfg.Exchange(ctx, code)
	if err != nil {
		return nil, fmt.Errorf("exchange failed: %w", err)
	}

	creds := &types.IntegrationCredentials{
		AccessToken:  token.AccessToken,
		RefreshToken: token.RefreshToken,
	}

	// GitHub tokens don't expire by default, but we handle it if they do
	if !token.Expiry.IsZero() {
		creds.ExpiresAt = &token.Expiry
	}

	return creds, nil
}

// Refresh refreshes an access token using a refresh token
// Note: GitHub personal access tokens don't expire, but GitHub Apps use refresh tokens
func (g *GitHubProvider) Refresh(ctx context.Context, refreshToken string) (*types.IntegrationCredentials, error) {
	if refreshToken == "" {
		return nil, fmt.Errorf("no refresh token")
	}

	// Use token endpoint directly for refresh
	data := url.Values{
		"client_id":     {g.clientID},
		"client_secret": {g.clientSecret},
		"refresh_token": {refreshToken},
		"grant_type":    {"refresh_token"},
	}

	req, err := http.NewRequestWithContext(ctx, "POST", github.Endpoint.TokenURL, strings.NewReader(data.Encode()))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")

	resp, err := g.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("refresh failed: status %d", resp.StatusCode)
	}

	var result struct {
		AccessToken  string `json:"access_token"`
		RefreshToken string `json:"refresh_token"`
		ExpiresIn    int    `json:"expires_in"`
		TokenType    string `json:"token_type"`
	}

	if err := decodeJSON(resp.Body, &result); err != nil {
		return nil, fmt.Errorf("parse response: %w", err)
	}

	creds := &types.IntegrationCredentials{
		AccessToken:  result.AccessToken,
		RefreshToken: result.RefreshToken,
	}

	if result.ExpiresIn > 0 {
		expiry := time.Now().Add(time.Duration(result.ExpiresIn) * time.Second)
		creds.ExpiresAt = &expiry
	}

	return creds, nil
}

func (g *GitHubProvider) oauthConfig(scopes []string) *oauth2.Config {
	return &oauth2.Config{
		ClientID:     g.clientID,
		ClientSecret: g.clientSecret,
		RedirectURL:  g.redirectURL,
		Scopes:       scopes,
		Endpoint:     github.Endpoint,
	}
}
