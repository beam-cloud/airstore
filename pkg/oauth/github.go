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

var githubIntegrationScopes = map[string][]string{
	"github": {
		"repo",
		"user:email",
	},
}

// GitHubProvider handles GitHub OAuth operations.
type GitHubProvider struct {
	clientID     string
	clientSecret string
	redirectURL  string
	httpClient   *http.Client
}

var _ Provider = (*GitHubProvider)(nil)

func NewGitHubProvider(cfg types.IntegrationGitHubOAuth) *GitHubProvider {
	return &GitHubProvider{
		clientID:     cfg.ClientID,
		clientSecret: cfg.ClientSecret,
		redirectURL:  cfg.RedirectURL,
		httpClient:   &http.Client{Timeout: 30 * time.Second},
	}
}

func (g *GitHubProvider) Name() string {
	return "github"
}

func (g *GitHubProvider) IsConfigured() bool {
	return g.clientID != "" && g.clientSecret != "" && g.redirectURL != ""
}

func (g *GitHubProvider) Integrations() []string {
	integrations := make([]string, 0, len(githubIntegrationScopes))
	for k := range githubIntegrationScopes {
		integrations = append(integrations, k)
	}
	return integrations
}

func (g *GitHubProvider) AuthorizeURL(state, integrationType string) (string, error) {
	scopes, ok := githubIntegrationScopes[integrationType]
	if !ok {
		return "", fmt.Errorf("unsupported integration: %s", integrationType)
	}

	cfg := g.oauthConfig(scopes)
	return cfg.AuthCodeURL(state), nil
}

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

	if !token.Expiry.IsZero() {
		creds.ExpiresAt = &token.Expiry
	}

	return creds, nil
}

func (g *GitHubProvider) Refresh(ctx context.Context, refreshToken string) (*types.IntegrationCredentials, error) {
	if refreshToken == "" {
		return nil, fmt.Errorf("no refresh token")
	}

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
