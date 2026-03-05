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

var atlassianEndpoint = oauth2.Endpoint{
	AuthURL:  "https://auth.atlassian.com/authorize",
	TokenURL: "https://auth.atlassian.com/oauth/token",
}

var atlassianIntegrationScopes = map[string][]string{
	"confluence": {
		// Classic scopes (v1 API)
		"read:confluence-content.all",
		"read:confluence-space.summary",
		"read:confluence-content.summary",
		// Granular scopes (v2 API)
		"read:page:confluence",
		"read:space:confluence",
		// CQL search
		"search:confluence",
		// Refresh token
		"offline_access",
	},
}

// AtlassianProvider handles Atlassian OAuth 2.0 (3LO) operations.
type AtlassianProvider struct {
	clientID     string
	clientSecret string
	callbackURL  string
	httpClient   *http.Client
}

var _ Provider = (*AtlassianProvider)(nil)

func NewAtlassianProvider(creds types.ProviderOAuthCredentials, callbackURL string) *AtlassianProvider {
	return &AtlassianProvider{
		clientID:     creds.ClientID,
		clientSecret: creds.ClientSecret,
		callbackURL:  callbackURL,
		httpClient:   &http.Client{Timeout: 30 * time.Second},
	}
}

func (a *AtlassianProvider) Name() string {
	return "atlassian"
}

func (a *AtlassianProvider) IsConfigured() bool {
	return a.clientID != "" && a.clientSecret != "" && a.callbackURL != ""
}

func (a *AtlassianProvider) Integrations() []string {
	integrations := make([]string, 0, len(atlassianIntegrationScopes))
	for k := range atlassianIntegrationScopes {
		integrations = append(integrations, k)
	}
	return integrations
}

func (a *AtlassianProvider) AuthorizeURL(state, integrationType string) (string, error) {
	scopes, ok := atlassianIntegrationScopes[integrationType]
	if !ok {
		return "", fmt.Errorf("unsupported integration: %s", integrationType)
	}

	cfg := a.oauthConfig(scopes)

	return cfg.AuthCodeURL(state,
		oauth2.AccessTypeOffline,
		oauth2.SetAuthURLParam("audience", "api.atlassian.com"),
		oauth2.SetAuthURLParam("prompt", "consent"),
	), nil
}

func (a *AtlassianProvider) Exchange(ctx context.Context, code, integrationType string) (*types.IntegrationCredentials, error) {
	scopes, ok := atlassianIntegrationScopes[integrationType]
	if !ok {
		return nil, fmt.Errorf("unsupported integration: %s", integrationType)
	}

	cfg := a.oauthConfig(scopes)

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

	// Discover the Atlassian Cloud ID from the accessible-resources endpoint.
	cloudID, err := a.discoverCloudID(ctx, token.AccessToken)
	if err != nil {
		return nil, fmt.Errorf("cloud ID discovery failed: %w", err)
	}

	if creds.Extra == nil {
		creds.Extra = make(map[string]string)
	}
	creds.Extra["cloud_id"] = cloudID

	return AnnotateCredentials(integrationType, creds, grantedScopes), nil
}

func (a *AtlassianProvider) Refresh(ctx context.Context, refreshToken string) (*types.IntegrationCredentials, error) {
	if refreshToken == "" {
		return nil, fmt.Errorf("no refresh token")
	}

	data := url.Values{
		"client_id":     {a.clientID},
		"client_secret": {a.clientSecret},
		"refresh_token": {refreshToken},
		"grant_type":    {"refresh_token"},
	}

	req, err := http.NewRequestWithContext(ctx, "POST", atlassianEndpoint.TokenURL, strings.NewReader(data.Encode()))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := a.httpClient.Do(req)
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

func (a *AtlassianProvider) oauthConfig(scopes []string) *oauth2.Config {
	return &oauth2.Config{
		ClientID:     a.clientID,
		ClientSecret: a.clientSecret,
		RedirectURL:  a.callbackURL,
		Scopes:       scopes,
		Endpoint:     atlassianEndpoint,
	}
}

// discoverCloudID calls the Atlassian accessible-resources endpoint to find
// the first available Cloud site ID.
func (a *AtlassianProvider) discoverCloudID(ctx context.Context, accessToken string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", "https://api.atlassian.com/oauth/token/accessible-resources", nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("Authorization", "Bearer "+accessToken)
	req.Header.Set("Accept", "application/json")

	resp, err := a.httpClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("accessible-resources: status %d: %s", resp.StatusCode, string(body))
	}

	var resources []struct {
		ID   string `json:"id"`
		Name string `json:"name"`
		URL  string `json:"url"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&resources); err != nil {
		return "", fmt.Errorf("parse accessible-resources: %w", err)
	}

	if len(resources) == 0 {
		return "", fmt.Errorf("no accessible Atlassian Cloud sites found")
	}

	return resources[0].ID, nil
}
