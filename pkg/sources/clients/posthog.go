package clients

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

const defaultPostHogHost = "https://app.posthog.com"

// PostHogClient is an HTTP client for the PostHog REST API.
type PostHogClient struct {
	httpClient *http.Client
	baseURL    string
	apiKey     string
}

// NewPostHogClient creates a new PostHog API client.
// If host is empty, defaults to https://app.posthog.com.
func NewPostHogClient(apiKey, host string) *PostHogClient {
	if host == "" {
		host = defaultPostHogHost
	}
	return &PostHogClient{
		httpClient: &http.Client{Timeout: 30 * time.Second},
		baseURL:    host,
		apiKey:     apiKey,
	}
}

// PostHogProject represents a PostHog project (team).
type PostHogProject struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
}

// PostHogEvent represents a PostHog event.
type PostHogEvent struct {
	ID         string                 `json:"id"`
	Event      string                 `json:"event"`
	Timestamp  string                 `json:"timestamp"`
	Properties map[string]any `json:"properties"`
}

// PostHogFeatureFlag represents a PostHog feature flag.
type PostHogFeatureFlag struct {
	ID     int    `json:"id"`
	Key    string `json:"key"`
	Name   string `json:"name"`
	Active bool   `json:"active"`
}

// PostHogInsight represents a PostHog insight (saved query).
type PostHogInsight struct {
	ID          int    `json:"id"`
	ShortID     string `json:"short_id"`
	Name        string `json:"name"`
	Description string `json:"description"`
}

// PostHogCohort represents a PostHog cohort.
type PostHogCohort struct {
	ID    int    `json:"id"`
	Name  string `json:"name"`
	Count int    `json:"count"`
}

// doRequest executes an authenticated GET request and decodes the response into out.
func (c *PostHogClient) doRequest(ctx context.Context, path string, out any) error {
	url := c.baseURL + path
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+c.apiKey)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("PostHog API error %d: %s", resp.StatusCode, string(body))
	}

	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}
	return nil
}

// ListProjects returns all projects (teams) accessible with the API key.
func (c *PostHogClient) ListProjects(ctx context.Context) ([]PostHogProject, error) {
	var resp struct {
		Results []PostHogProject `json:"results"`
	}
	if err := c.doRequest(ctx, "/api/projects/", &resp); err != nil {
		return nil, err
	}
	return resp.Results, nil
}

// ListEvents returns recent events for a project.
func (c *PostHogClient) ListEvents(ctx context.Context, projectID, limit int) ([]PostHogEvent, error) {
	if limit <= 0 {
		limit = 100
	}
	var resp struct {
		Results []PostHogEvent `json:"results"`
	}
	path := fmt.Sprintf("/api/projects/%d/events/?limit=%d", projectID, limit)
	if err := c.doRequest(ctx, path, &resp); err != nil {
		return nil, err
	}
	return resp.Results, nil
}

// ListFeatureFlags returns all feature flags for a project.
func (c *PostHogClient) ListFeatureFlags(ctx context.Context, projectID int) ([]PostHogFeatureFlag, error) {
	var resp struct {
		Results []PostHogFeatureFlag `json:"results"`
	}
	path := fmt.Sprintf("/api/projects/%d/feature_flags/?limit=200", projectID)
	if err := c.doRequest(ctx, path, &resp); err != nil {
		return nil, err
	}
	return resp.Results, nil
}

// ListInsights returns all insights (saved queries) for a project.
func (c *PostHogClient) ListInsights(ctx context.Context, projectID int) ([]PostHogInsight, error) {
	var resp struct {
		Results []PostHogInsight `json:"results"`
	}
	path := fmt.Sprintf("/api/projects/%d/insights/?limit=200", projectID)
	if err := c.doRequest(ctx, path, &resp); err != nil {
		return nil, err
	}
	return resp.Results, nil
}

// ListCohorts returns all cohorts for a project.
func (c *PostHogClient) ListCohorts(ctx context.Context, projectID int) ([]PostHogCohort, error) {
	var resp struct {
		Results []PostHogCohort `json:"results"`
	}
	path := fmt.Sprintf("/api/projects/%d/cohorts/?limit=200", projectID)
	if err := c.doRequest(ctx, path, &resp); err != nil {
		return nil, err
	}
	return resp.Results, nil
}
