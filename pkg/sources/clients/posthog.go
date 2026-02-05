package clients

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

const defaultPostHogHost = "https://app.posthog.com"

// ErrResourceNotFound is returned when a PostHog resource doesn't exist (HTTP 404).
var ErrResourceNotFound = fmt.Errorf("resource not found")

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
	ID         string         `json:"id"`
	Event      string         `json:"event"`
	Timestamp  string         `json:"timestamp"`
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

// paginatedResponse represents PostHog's paginated API response structure.
type paginatedResponse[T any] struct {
	Results []T     `json:"results"`
	Next    *string `json:"next"`
}

// doRequest executes an authenticated GET request and decodes the response into out.
func (c *PostHogClient) doRequest(ctx context.Context, path string, out any) error {
	return c.doRequestFullURL(ctx, c.baseURL+path, out)
}

// doRequestFullURL executes an authenticated GET request using a full URL (not a path).
// This is needed for pagination since PostHog's "next" field contains complete URLs.
func (c *PostHogClient) doRequestFullURL(ctx context.Context, fullURL string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fullURL, nil)
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
		if resp.StatusCode == http.StatusNotFound {
			return ErrResourceNotFound
		}
		return fmt.Errorf("PostHog API error %d: %s", resp.StatusCode, string(body))
	}

	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}
	return nil
}

// fetchAllPages fetches all pages of a paginated PostHog API endpoint.
func fetchAllPages[T any](ctx context.Context, c *PostHogClient, initialPath string) ([]T, error) {
	var allResults []T

	// First request uses the path-based method
	url := c.baseURL + initialPath
	for url != "" {
		var page paginatedResponse[T]
		if err := c.doRequestFullURL(ctx, url, &page); err != nil {
			return nil, err
		}

		allResults = append(allResults, page.Results...)

		if page.Next == nil {
			break
		}
		// Validate that Next URL belongs to expected host to prevent API key leakage
		if !strings.HasPrefix(*page.Next, c.baseURL) {
			return nil, fmt.Errorf("pagination URL %q does not match expected host %q", *page.Next, c.baseURL)
		}
		url = *page.Next
	}

	return allResults, nil
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
	path := fmt.Sprintf("/api/projects/%d/feature_flags/?limit=200", projectID)
	return fetchAllPages[PostHogFeatureFlag](ctx, c, path)
}

// ListInsights returns all insights (saved queries) for a project.
func (c *PostHogClient) ListInsights(ctx context.Context, projectID int) ([]PostHogInsight, error) {
	path := fmt.Sprintf("/api/projects/%d/insights/?limit=200", projectID)
	return fetchAllPages[PostHogInsight](ctx, c, path)
}

// ListCohorts returns all cohorts for a project.
func (c *PostHogClient) ListCohorts(ctx context.Context, projectID int) ([]PostHogCohort, error) {
	path := fmt.Sprintf("/api/projects/%d/cohorts/?limit=200", projectID)
	return fetchAllPages[PostHogCohort](ctx, c, path)
}

// GetInsightByShortID retrieves an insight by short_id using the API filter.
// Uses: GET /api/projects/{id}/insights/?short_id={shortID}&limit=1
func (c *PostHogClient) GetInsightByShortID(ctx context.Context, projectID int, shortID string) (*PostHogInsight, error) {
	path := fmt.Sprintf("/api/projects/%d/insights/?short_id=%s&limit=1", projectID, shortID)
	var resp paginatedResponse[PostHogInsight]
	if err := c.doRequest(ctx, path, &resp); err != nil {
		return nil, err
	}
	if len(resp.Results) == 0 {
		return nil, ErrResourceNotFound
	}
	return &resp.Results[0], nil
}

// GetCohort retrieves a cohort by numeric ID.
// Uses: GET /api/projects/{id}/cohorts/{cohortID}/
func (c *PostHogClient) GetCohort(ctx context.Context, projectID, cohortID int) (*PostHogCohort, error) {
	path := fmt.Sprintf("/api/projects/%d/cohorts/%d/", projectID, cohortID)
	var cohort PostHogCohort
	if err := c.doRequest(ctx, path, &cohort); err != nil {
		return nil, err
	}
	return &cohort, nil
}
