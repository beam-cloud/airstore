package oauth

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/beam-cloud/airstore/pkg/types"
	"golang.org/x/oauth2/slack"
)

var slackIntegrationScopes = map[string][]string{
	"slack": {
		"channels:read",
		"channels:history",
		"files:read",
		"users:read",
		"users:read.email",
	},
}

// SlackProvider handles Slack OAuth operations.
type SlackProvider struct {
	clientID     string
	clientSecret string
	redirectURL  string
	httpClient   *http.Client
}

var _ Provider = (*SlackProvider)(nil)

func NewSlackProvider(cfg types.IntegrationSlackOAuth) *SlackProvider {
	return &SlackProvider{
		clientID:     cfg.ClientID,
		clientSecret: cfg.ClientSecret,
		redirectURL:  cfg.RedirectURL,
		httpClient:   &http.Client{Timeout: 30 * time.Second},
	}
}

func (s *SlackProvider) Name() string {
	return "slack"
}

func (s *SlackProvider) IsConfigured() bool {
	return s.clientID != "" && s.clientSecret != "" && s.redirectURL != ""
}

func (s *SlackProvider) Integrations() []string {
	integrations := make([]string, 0, len(slackIntegrationScopes))
	for k := range slackIntegrationScopes {
		integrations = append(integrations, k)
	}
	return integrations
}

func (s *SlackProvider) AuthorizeURL(state, integrationType string) (string, error) {
	scopes, ok := slackIntegrationScopes[integrationType]
	if !ok {
		return "", fmt.Errorf("unsupported integration: %s", integrationType)
	}

	params := url.Values{
		"client_id":    {s.clientID},
		"redirect_uri": {s.redirectURL},
		"state":        {state},
		"user_scope":   {strings.Join(scopes, ",")},
	}

	return slack.Endpoint.AuthURL + "?" + params.Encode(), nil
}

func (s *SlackProvider) Exchange(ctx context.Context, code, integrationType string) (*types.IntegrationCredentials, error) {
	data := url.Values{
		"client_id":     {s.clientID},
		"client_secret": {s.clientSecret},
		"code":          {code},
		"redirect_uri":  {s.redirectURL},
	}

	req, err := http.NewRequestWithContext(ctx, "POST", slack.Endpoint.TokenURL, strings.NewReader(data.Encode()))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("exchange failed: status %d", resp.StatusCode)
	}

	var result struct {
		OK          bool   `json:"ok"`
		Error       string `json:"error"`
		AccessToken string `json:"access_token"`
		AppID       string `json:"app_id"`
		Team        struct {
			ID   string `json:"id"`
			Name string `json:"name"`
		} `json:"team"`
		AuthedUser struct {
			ID          string `json:"id"`
			AccessToken string `json:"access_token"`
		} `json:"authed_user"`
	}

	if err := decodeJSON(resp.Body, &result); err != nil {
		return nil, fmt.Errorf("parse response: %w", err)
	}

	if !result.OK {
		return nil, fmt.Errorf("slack error: %s", result.Error)
	}

	accessToken := result.AuthedUser.AccessToken
	if accessToken == "" {
		accessToken = result.AccessToken
	}

	return &types.IntegrationCredentials{
		AccessToken: accessToken,
		Extra: map[string]string{
			"team_id":   result.Team.ID,
			"team_name": result.Team.Name,
			"user_id":   result.AuthedUser.ID,
			"app_id":    result.AppID,
		},
	}, nil
}

func (s *SlackProvider) Refresh(ctx context.Context, refreshToken string) (*types.IntegrationCredentials, error) {
	if refreshToken == "" {
		return nil, fmt.Errorf("no refresh token")
	}

	data := url.Values{
		"client_id":     {s.clientID},
		"client_secret": {s.clientSecret},
		"refresh_token": {refreshToken},
		"grant_type":    {"refresh_token"},
	}

	req, err := http.NewRequestWithContext(ctx, "POST", slack.Endpoint.TokenURL, strings.NewReader(data.Encode()))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("refresh failed: status %d", resp.StatusCode)
	}

	var result struct {
		OK           bool   `json:"ok"`
		Error        string `json:"error"`
		AccessToken  string `json:"access_token"`
		RefreshToken string `json:"refresh_token"`
		ExpiresIn    int    `json:"expires_in"`
	}

	if err := decodeJSON(resp.Body, &result); err != nil {
		return nil, fmt.Errorf("parse response: %w", err)
	}

	if !result.OK {
		return nil, fmt.Errorf("slack refresh error: %s", result.Error)
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
