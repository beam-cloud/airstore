package oauth

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/beam-cloud/airstore/pkg/types"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
)

// Integration scopes for Google OAuth
var integrationScopes = map[string][]string{
	"gmail": {
		"https://www.googleapis.com/auth/gmail.readonly",
	},
	"gdrive": {
		"https://www.googleapis.com/auth/drive.readonly",
	},
}

// IsGoogleIntegration returns true if the integration uses Google OAuth
func IsGoogleIntegration(integrationType string) bool {
	_, ok := integrationScopes[integrationType]
	return ok
}

// GoogleClient handles Google OAuth operations for workspace integrations
type GoogleClient struct {
	clientID     string
	clientSecret string
	redirectURL  string
	httpClient   *http.Client
}

// NewGoogleClient creates a new Google OAuth client from config
func NewGoogleClient(cfg types.IntegrationGoogleOAuth) *GoogleClient {
	return &GoogleClient{
		clientID:     cfg.ClientID,
		clientSecret: cfg.ClientSecret,
		redirectURL:  cfg.RedirectURL,
		httpClient:   &http.Client{Timeout: 30 * time.Second},
	}
}

// IsConfigured returns true if Google OAuth is configured
func (g *GoogleClient) IsConfigured() bool {
	return g.clientID != "" && g.clientSecret != "" && g.redirectURL != ""
}

// AuthorizeURL generates the Google OAuth authorization URL for an integration
func (g *GoogleClient) AuthorizeURL(state, integrationType string) (string, error) {
	scopes, ok := integrationScopes[integrationType]
	if !ok {
		return "", fmt.Errorf("unsupported integration: %s", integrationType)
	}

	cfg := g.oauthConfig(scopes)

	// Request offline access to get refresh token, and always prompt for consent
	// to ensure we get a refresh token even if user previously authorized
	return cfg.AuthCodeURL(state,
		oauth2.AccessTypeOffline,
		oauth2.SetAuthURLParam("prompt", "consent select_account"),
	), nil
}

// Exchange exchanges an authorization code for tokens
func (g *GoogleClient) Exchange(ctx context.Context, code, integrationType string) (*types.IntegrationCredentials, error) {
	scopes, ok := integrationScopes[integrationType]
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

// Refresh refreshes an access token using a refresh token
func (g *GoogleClient) Refresh(ctx context.Context, refreshToken string) (*types.IntegrationCredentials, error) {
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

	req, err := http.NewRequestWithContext(ctx, "POST", google.Endpoint.TokenURL, strings.NewReader(data.Encode()))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := g.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("refresh failed: status %d", resp.StatusCode)
	}

	// Parse response manually to handle Google's format
	var result struct {
		AccessToken string `json:"access_token"`
		ExpiresIn   int    `json:"expires_in"`
		TokenType   string `json:"token_type"`
	}

	if err := decodeJSON(resp.Body, &result); err != nil {
		return nil, fmt.Errorf("parse response: %w", err)
	}

	expiry := time.Now().Add(time.Duration(result.ExpiresIn) * time.Second)

	return &types.IntegrationCredentials{
		AccessToken:  result.AccessToken,
		RefreshToken: refreshToken, // Keep the same refresh token
		ExpiresAt:    &expiry,
	}, nil
}

// NeedsRefresh returns true if credentials are expired or about to expire
func NeedsRefresh(creds *types.IntegrationCredentials) bool {
	if creds == nil || creds.RefreshToken == "" {
		return false
	}
	if creds.ExpiresAt == nil {
		return false
	}
	// Refresh if expires within 5 minutes
	return time.Until(*creds.ExpiresAt) < 5*time.Minute
}

func (g *GoogleClient) oauthConfig(scopes []string) *oauth2.Config {
	return &oauth2.Config{
		ClientID:     g.clientID,
		ClientSecret: g.clientSecret,
		RedirectURL:  g.redirectURL,
		Scopes:       scopes,
		Endpoint:     google.Endpoint,
	}
}

// decodeJSON decodes JSON from a reader
func decodeJSON(r io.Reader, v interface{}) error {
	return json.NewDecoder(r).Decode(v)
}
