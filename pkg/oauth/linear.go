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

var linearEndpoint = oauth2.Endpoint{
	AuthURL:  "https://linear.app/oauth/authorize",
	TokenURL: "https://api.linear.app/oauth/token",
}

var linearIntegrationScopes = map[string][]string{
	"linear": {
		"read",
		"write",
		"issues:create",
		"comments:create",
	},
}

// LinearProvider handles Linear OAuth operations.
type LinearProvider struct {
	clientID     string
	clientSecret string
	redirectURL  string
	httpClient   *http.Client
}

var _ Provider = (*LinearProvider)(nil)

func NewLinearProvider(cfg types.IntegrationLinearOAuth) *LinearProvider {
	return &LinearProvider{
		clientID:     cfg.ClientID,
		clientSecret: cfg.ClientSecret,
		redirectURL:  cfg.RedirectURL,
		httpClient:   &http.Client{Timeout: 30 * time.Second},
	}
}

func (l *LinearProvider) Name() string {
	return "linear"
}

func (l *LinearProvider) IsConfigured() bool {
	return l.clientID != "" && l.clientSecret != "" && l.redirectURL != ""
}

func (l *LinearProvider) Integrations() []string {
	integrations := make([]string, 0, len(linearIntegrationScopes))
	for k := range linearIntegrationScopes {
		integrations = append(integrations, k)
	}
	return integrations
}

func (l *LinearProvider) AuthorizeURL(state, integrationType string) (string, error) {
	scopes, ok := linearIntegrationScopes[integrationType]
	if !ok {
		return "", fmt.Errorf("unsupported integration: %s", integrationType)
	}

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

func (l *LinearProvider) Exchange(ctx context.Context, code, integrationType string) (*types.IntegrationCredentials, error) {
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
		AccessToken string `json:"access_token"`
		ExpiresIn   int    `json:"expires_in"`
	}

	if err := decodeJSON(resp.Body, &result); err != nil {
		return nil, fmt.Errorf("parse response: %w", err)
	}

	creds := &types.IntegrationCredentials{
		AccessToken: result.AccessToken,
	}

	if result.ExpiresIn > 0 {
		expiry := time.Now().Add(time.Duration(result.ExpiresIn) * time.Second)
		creds.ExpiresAt = &expiry
	}

	return creds, nil
}

func (l *LinearProvider) Refresh(ctx context.Context, refreshToken string) (*types.IntegrationCredentials, error) {
	return nil, fmt.Errorf("linear tokens do not support refresh")
}
