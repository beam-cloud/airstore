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
)

var microsoftEndpoint = oauth2.Endpoint{
	AuthURL:  "https://login.microsoftonline.com/common/oauth2/v2.0/authorize",
	TokenURL: "https://login.microsoftonline.com/common/oauth2/v2.0/token",
}

var microsoftIntegrationScopes = map[string][]string{
	"outlook": {
		"https://graph.microsoft.com/Mail.ReadWrite",
		"https://graph.microsoft.com/Mail.Send",
		"https://graph.microsoft.com/User.Read",
		"offline_access",
	},
	"teams": {
		"https://graph.microsoft.com/Team.ReadBasic.All",
		"https://graph.microsoft.com/Channel.ReadBasic.All",
		"https://graph.microsoft.com/ChannelMessage.Read.All",
		"https://graph.microsoft.com/Chat.Read",
		"https://graph.microsoft.com/ChannelMessage.Send",
		"https://graph.microsoft.com/Chat.ReadWrite",
		"https://graph.microsoft.com/User.Read",
		"offline_access",
	},
}

// MicrosoftProvider handles Microsoft OAuth 2.0 operations.
type MicrosoftProvider struct {
	clientID     string
	clientSecret string
	callbackURL  string
	httpClient   *http.Client
}

var _ Provider = (*MicrosoftProvider)(nil)

func NewMicrosoftProvider(creds types.ProviderOAuthCredentials, callbackURL string) *MicrosoftProvider {
	return &MicrosoftProvider{
		clientID:     creds.ClientID,
		clientSecret: creds.ClientSecret,
		callbackURL:  callbackURL,
		httpClient:   &http.Client{Timeout: 30 * time.Second},
	}
}

func (m *MicrosoftProvider) Name() string {
	return "microsoft"
}

func (m *MicrosoftProvider) IsConfigured() bool {
	return m.clientID != "" && m.clientSecret != "" && m.callbackURL != ""
}

func (m *MicrosoftProvider) Integrations() []string {
	integrations := make([]string, 0, len(microsoftIntegrationScopes))
	for k := range microsoftIntegrationScopes {
		integrations = append(integrations, k)
	}
	return integrations
}

func (m *MicrosoftProvider) AuthorizeURL(state, integrationType string) (string, error) {
	scopes, ok := microsoftIntegrationScopes[integrationType]
	if !ok {
		return "", fmt.Errorf("unsupported integration: %s", integrationType)
	}

	cfg := m.oauthConfig(scopes)

	return cfg.AuthCodeURL(state,
		oauth2.AccessTypeOffline,
		oauth2.SetAuthURLParam("prompt", "consent"),
	), nil
}

func (m *MicrosoftProvider) Exchange(ctx context.Context, code, integrationType string) (*types.IntegrationCredentials, error) {
	scopes, ok := microsoftIntegrationScopes[integrationType]
	if !ok {
		return nil, fmt.Errorf("unsupported integration: %s", integrationType)
	}

	cfg := m.oauthConfig(scopes)

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

	var grantedScopes []string
	if raw := token.Extra("scope"); raw != nil {
		if scopeStr, ok := raw.(string); ok {
			grantedScopes = NormalizeScopes(ParseScopeString(scopeStr))
		}
	}
	if len(grantedScopes) == 0 {
		grantedScopes = scopes
	}

	return AnnotateCredentials(integrationType, creds, grantedScopes), nil
}

func (m *MicrosoftProvider) Refresh(ctx context.Context, refreshToken string) (*types.IntegrationCredentials, error) {
	if refreshToken == "" {
		return nil, fmt.Errorf("no refresh token")
	}

	data := url.Values{
		"client_id":     {m.clientID},
		"client_secret": {m.clientSecret},
		"refresh_token": {refreshToken},
		"grant_type":    {"refresh_token"},
	}

	req, err := http.NewRequestWithContext(ctx, "POST", microsoftEndpoint.TokenURL, strings.NewReader(data.Encode()))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := m.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("refresh failed: status %d: %s", resp.StatusCode, string(body))
	}

	var result struct {
		AccessToken  string `json:"access_token"`
		RefreshToken string `json:"refresh_token"`
		ExpiresIn    int    `json:"expires_in"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("parse response: %w", err)
	}

	expiry := time.Now().Add(time.Duration(result.ExpiresIn) * time.Second)

	// Microsoft may issue a new refresh token on each refresh.
	newRefresh := refreshToken
	if result.RefreshToken != "" {
		newRefresh = result.RefreshToken
	}

	return &types.IntegrationCredentials{
		AccessToken:  result.AccessToken,
		RefreshToken: newRefresh,
		ExpiresAt:    &expiry,
	}, nil
}

func (m *MicrosoftProvider) oauthConfig(scopes []string) *oauth2.Config {
	return &oauth2.Config{
		ClientID:     m.clientID,
		ClientSecret: m.clientSecret,
		RedirectURL:  m.callbackURL,
		Scopes:       scopes,
		Endpoint:     microsoftEndpoint,
	}
}
