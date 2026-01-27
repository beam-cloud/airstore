package clients

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/beam-cloud/airstore/pkg/types"
)

// Exa API endpoint
const exaAPI = "https://api.exa.ai"

// Exa command constants - must match YAML definition
const (
	exaCmdSearch   = "search"
	exaCmdResearch = "research"
	exaCmdContents = "contents"
)

// ExaClient implements the Exa neural search tool
type ExaClient struct {
	httpClient *http.Client
	apiKey     string
}

// NewExaClient creates a new Exa client
func NewExaClient(apiKey string) *ExaClient {
	return &ExaClient{
		httpClient: &http.Client{},
		apiKey:     apiKey,
	}
}

// Name returns the tool identifier - implements ToolClient interface
func (e *ExaClient) Name() types.ToolName {
	return types.ToolExa
}

// Execute runs an Exa command - implements ToolClient interface
func (e *ExaClient) Execute(ctx context.Context, command string, args map[string]any, creds *types.IntegrationCredentials, stdout, stderr io.Writer) error {
	// Exa uses API key from constructor, creds can override
	_ = creds // Reserved for future credential override
	var result any
	var err error

	switch command {
	case exaCmdSearch:
		query, _ := args["query"].(string)
		numResults := GetIntArg(args, "num", 10)
		searchType := GetStringArg(args, "type", "auto")
		includeDomains := GetStringArg(args, "include", "")
		excludeDomains := GetStringArg(args, "exclude", "")
		result, err = e.search(ctx, query, numResults, searchType, includeDomains, excludeDomains)

	case exaCmdResearch:
		query, _ := args["query"].(string)
		numResults := GetIntArg(args, "num", 5)
		result, err = e.research(ctx, query, numResults)

	case exaCmdContents:
		url, _ := args["url"].(string)
		maxChars := GetIntArg(args, "max", 10000)
		result, err = e.getContents(ctx, url, maxChars)

	default:
		return fmt.Errorf("unknown command: %s", command)
	}

	if err != nil {
		return err
	}

	// Output as JSON
	enc := json.NewEncoder(stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(result)
}

// SearchResult represents a single search result
type ExaSearchResult struct {
	Title     string  `json:"title"`
	URL       string  `json:"url"`
	Snippet   string  `json:"snippet,omitempty"`
	Score     float64 `json:"score"`
	Published string  `json:"published,omitempty"`
	Author    string  `json:"author,omitempty"`
}

// SearchResponse represents the search response
type ExaSearchResponse struct {
	Query   string            `json:"query"`
	Results []ExaSearchResult `json:"results"`
	Total   int               `json:"total"`
}

func (e *ExaClient) search(ctx context.Context, query string, numResults int, searchType, includeDomains, excludeDomains string) (*ExaSearchResponse, error) {
	if numResults < 1 {
		numResults = 1
	}
	if numResults > 20 {
		numResults = 20
	}

	reqBody := map[string]any{
		"query":      query,
		"numResults": numResults,
		"contents": map[string]any{
			"text": true,
		},
	}

	// Set search type
	if searchType != "auto" {
		reqBody["type"] = searchType
	}

	// Parse domain filters
	if includeDomains != "" {
		domains := strings.Split(includeDomains, ",")
		trimmed := make([]string, 0, len(domains))
		for _, d := range domains {
			trimmed = append(trimmed, strings.TrimSpace(d))
		}
		reqBody["includeDomains"] = trimmed
	}
	if excludeDomains != "" {
		domains := strings.Split(excludeDomains, ",")
		trimmed := make([]string, 0, len(domains))
		for _, d := range domains {
			trimmed = append(trimmed, strings.TrimSpace(d))
		}
		reqBody["excludeDomains"] = trimmed
	}

	body, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", exaAPI+"/search", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-api-key", e.apiKey)

	resp, err := e.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusUnauthorized {
		return nil, fmt.Errorf("invalid API key")
	}
	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("exa API error: %s - %s", resp.Status, string(respBody))
	}

	var apiResp struct {
		Results []struct {
			Title         string  `json:"title"`
			URL           string  `json:"url"`
			Text          string  `json:"text"`
			Score         float64 `json:"score"`
			PublishedDate string  `json:"publishedDate"`
			Author        string  `json:"author"`
		} `json:"results"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&apiResp); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}

	results := make([]ExaSearchResult, 0, len(apiResp.Results))
	for _, r := range apiResp.Results {
		snippet := r.Text
		if len(snippet) > 500 {
			snippet = snippet[:500] + "..."
		}
		results = append(results, ExaSearchResult{
			Title:     r.Title,
			URL:       r.URL,
			Snippet:   snippet,
			Score:     r.Score,
			Published: r.PublishedDate,
			Author:    r.Author,
		})
	}

	return &ExaSearchResponse{
		Query:   query,
		Results: results,
		Total:   len(results),
	}, nil
}

// ResearchFinding represents a single research finding
type ResearchFinding struct {
	Title   string `json:"title"`
	URL     string `json:"url"`
	Summary string `json:"summary"`
	Key     string `json:"key_insight,omitempty"`
}

// ResearchResponse represents the research response
type ResearchResponse struct {
	Topic    string            `json:"topic"`
	Findings []ResearchFinding `json:"findings"`
	Summary  string            `json:"summary"`
}

func (e *ExaClient) research(ctx context.Context, query string, numResults int) (*ResearchResponse, error) {
	if numResults < 1 {
		numResults = 1
	}
	if numResults > 10 {
		numResults = 10
	}

	reqBody := map[string]any{
		"query":      query,
		"numResults": numResults,
		"contents": map[string]any{
			"text": true,
		},
		"type": "neural", // Use neural for better research results
	}

	body, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", exaAPI+"/search", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-api-key", e.apiKey)

	resp, err := e.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("exa API error: %s - %s", resp.Status, string(respBody))
	}

	var apiResp struct {
		Results []struct {
			Title string `json:"title"`
			URL   string `json:"url"`
			Text  string `json:"text"`
		} `json:"results"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&apiResp); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}

	findings := make([]ResearchFinding, 0, len(apiResp.Results))
	for _, r := range apiResp.Results {
		// Truncate text for summary
		summary := r.Text
		if len(summary) > 1000 {
			summary = summary[:1000] + "..."
		}
		findings = append(findings, ResearchFinding{
			Title:   r.Title,
			URL:     r.URL,
			Summary: summary,
		})
	}

	return &ResearchResponse{
		Topic:    query,
		Findings: findings,
		Summary:  fmt.Sprintf("Found %d relevant sources for: %s", len(findings), query),
	}, nil
}

// ContentResponse represents extracted content
type ContentResponse struct {
	URL     string `json:"url"`
	Title   string `json:"title"`
	Content string `json:"content"`
	Length  int    `json:"length"`
}

func (e *ExaClient) getContents(ctx context.Context, urlStr string, maxChars int) (*ContentResponse, error) {
	if maxChars < 100 {
		maxChars = 100
	}
	if maxChars > 50000 {
		maxChars = 50000
	}

	reqBody := map[string]any{
		"ids": []string{urlStr},
		"text": map[string]any{
			"maxCharacters": maxChars,
		},
	}

	body, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", exaAPI+"/contents", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-api-key", e.apiKey)

	resp, err := e.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("exa API error: %s - %s", resp.Status, string(respBody))
	}

	var apiResp struct {
		Results []struct {
			URL   string `json:"url"`
			Title string `json:"title"`
			Text  string `json:"text"`
		} `json:"results"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&apiResp); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}

	if len(apiResp.Results) == 0 {
		return nil, fmt.Errorf("no content found for URL: %s", urlStr)
	}

	result := apiResp.Results[0]
	return &ContentResponse{
		URL:     result.URL,
		Title:   result.Title,
		Content: result.Text,
		Length:  len(result.Text),
	}, nil
}
