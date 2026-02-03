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

var googleIntegrationScopes = map[string][]string{
	"gmail": {
		"https://www.googleapis.com/auth/gmail.readonly",
	},
	"gdrive": {
		"https://www.googleapis.com/auth/drive.readonly",
	},
}

// GoogleProvider handles Google OAuth operations.
type GoogleProvider struct {
	clientID     string
	clientSecret string
	callbackURL  string
	httpClient   *http.Client
}

var _ Provider = (*GoogleProvider)(nil)

func NewGoogleProvider(creds types.ProviderOAuthCredentials, callbackURL string) *GoogleProvider {
	return &GoogleProvider{
		clientID:     creds.ClientID,
		clientSecret: creds.ClientSecret,
		callbackURL:  callbackURL,
		httpClient:   &http.Client{Timeout: 30 * time.Second},
	}
}

func (g *GoogleProvider) Name() string {
	return "google"
}

func (g *GoogleProvider) IsConfigured() bool {
	return g.clientID != "" && g.clientSecret != "" && g.callbackURL != ""
}

func (g *GoogleProvider) Integrations() []string {
	integrations := make([]string, 0, len(googleIntegrationScopes))
	for k := range googleIntegrationScopes {
		integrations = append(integrations, k)
	}
	return integrations
}

func (g *GoogleProvider) AuthorizeURL(state, integrationType string) (string, error) {
	scopes, ok := googleIntegrationScopes[integrationType]
	if !ok {
		return "", fmt.Errorf("unsupported integration: %s", integrationType)
	}

	cfg := g.oauthConfig(scopes)

	return cfg.AuthCodeURL(state,
		oauth2.AccessTypeOffline,
		oauth2.SetAuthURLParam("prompt", "consent select_account"),
	), nil
}

func (g *GoogleProvider) Exchange(ctx context.Context, code, integrationType string) (*types.IntegrationCredentials, error) {
	scopes, ok := googleIntegrationScopes[integrationType]
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

func (g *GoogleProvider) Refresh(ctx context.Context, refreshToken string) (*types.IntegrationCredentials, error) {
	if refreshToken == "" {
		return nil, fmt.Errorf("no refresh token")
	}

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

	var result struct {
		AccessToken string `json:"access_token"`
		ExpiresIn   int    `json:"expires_in"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("parse response: %w", err)
	}

	expiry := time.Now().Add(time.Duration(result.ExpiresIn) * time.Second)

	return &types.IntegrationCredentials{
		AccessToken:  result.AccessToken,
		RefreshToken: refreshToken,
		ExpiresAt:    &expiry,
	}, nil
}

func (g *GoogleProvider) oauthConfig(scopes []string) *oauth2.Config {
	return &oauth2.Config{
		ClientID:     g.clientID,
		ClientSecret: g.clientSecret,
		RedirectURL:  g.callbackURL,
		Scopes:       scopes,
		Endpoint:     google.Endpoint,
	}
}

// NeedsRefresh returns true if credentials are expired or about to expire.
func NeedsRefresh(creds *types.IntegrationCredentials) bool {
	if creds == nil || creds.RefreshToken == "" || creds.ExpiresAt == nil {
		return false
	}
	return time.Until(*creds.ExpiresAt) < 5*time.Minute
}

// decodeJSON decodes JSON from a reader.
func decodeJSON(r io.Reader, v interface{}) error {
	return json.NewDecoder(r).Decode(v)
}
