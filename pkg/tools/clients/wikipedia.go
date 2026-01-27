package clients

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strings"

	"github.com/beam-cloud/airstore/pkg/types"
)

// Wikipedia API endpoints
const (
	wikipediaRestAPI   = "https://en.wikipedia.org/api/rest_v1"
	wikipediaSearchAPI = "https://en.wikipedia.org/w/api.php"
	userAgent          = "AirstoreTools/1.0 (https://github.com/beam-cloud/airstore)"
)

// Wikipedia command constants - must match YAML definition
const (
	cmdSearch  = "search"
	cmdSummary = "summary"
	cmdArticle = "article"
)

// WikipediaClient implements the Wikipedia tool
type WikipediaClient struct {
	httpClient *http.Client
}

// NewWikipediaClient creates a new Wikipedia client
func NewWikipediaClient() *WikipediaClient {
	return &WikipediaClient{
		httpClient: &http.Client{},
	}
}

// Name returns the tool identifier - implements ToolClient interface
func (w *WikipediaClient) Name() types.ToolName {
	return types.ToolWikipedia
}

// Execute runs a Wikipedia command - implements ToolClient interface
func (w *WikipediaClient) Execute(ctx context.Context, command string, args map[string]any, creds *types.IntegrationCredentials, stdout, stderr io.Writer) error {
	// Wikipedia is free - no credentials needed
	var result any
	var err error

	switch command {
	case cmdSearch:
		query, _ := args["query"].(string)
		limit := 10
		if l, ok := args["limit"].(int); ok {
			limit = l
		}
		result, err = w.search(ctx, query, limit)

	case cmdSummary:
		title, _ := args["title"].(string)
		result, err = w.getSummary(ctx, title)

	case cmdArticle:
		title, _ := args["title"].(string)
		includeHTML := false
		if h, ok := args["html"].(bool); ok {
			includeHTML = h
		}
		result, err = w.getArticle(ctx, title, includeHTML)

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

// SearchResult represents a Wikipedia search result
type SearchResult struct {
	Title       string `json:"title"`
	PageID      int    `json:"page_id"`
	Excerpt     string `json:"excerpt"`
	Description string `json:"description,omitempty"`
}

// SearchResponse is the response from a search command
type SearchResponse struct {
	Results []SearchResult `json:"results"`
	Total   int            `json:"total"`
}

func (w *WikipediaClient) search(ctx context.Context, query string, limit int) (*SearchResponse, error) {
	if limit < 1 {
		limit = 1
	}
	if limit > 50 {
		limit = 50
	}

	params := url.Values{
		"action":   {"query"},
		"format":   {"json"},
		"list":     {"search"},
		"srsearch": {query},
		"srlimit":  {fmt.Sprintf("%d", limit)},
		"srprop":   {"snippet|titlesnippet"},
		"utf8":     {"1"},
	}

	reqURL := wikipediaSearchAPI + "?" + params.Encode()
	req, err := http.NewRequestWithContext(ctx, "GET", reqURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", userAgent)

	resp, err := w.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("wikipedia API error: %s", resp.Status)
	}

	var apiResp struct {
		Query struct {
			SearchInfo struct {
				TotalHits int `json:"totalhits"`
			} `json:"searchinfo"`
			Search []struct {
				Title   string `json:"title"`
				PageID  int    `json:"pageid"`
				Snippet string `json:"snippet"`
			} `json:"search"`
		} `json:"query"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&apiResp); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}

	results := make([]SearchResult, 0, len(apiResp.Query.Search))
	for _, item := range apiResp.Query.Search {
		results = append(results, SearchResult{
			Title:   item.Title,
			PageID:  item.PageID,
			Excerpt: stripHTML(item.Snippet),
		})
	}

	return &SearchResponse{
		Results: results,
		Total:   apiResp.Query.SearchInfo.TotalHits,
	}, nil
}

// SummaryResponse is the response from a get_summary command
type SummaryResponse struct {
	Title        string `json:"title"`
	PageID       int    `json:"page_id"`
	Extract      string `json:"extract"`
	Description  string `json:"description,omitempty"`
	ThumbnailURL string `json:"thumbnail_url,omitempty"`
	URL          string `json:"url"`
}

func (w *WikipediaClient) getSummary(ctx context.Context, title string) (*SummaryResponse, error) {
	normalizedTitle := strings.ReplaceAll(title, " ", "_")
	reqURL := fmt.Sprintf("%s/page/summary/%s", wikipediaRestAPI, url.PathEscape(normalizedTitle))

	req, err := http.NewRequestWithContext(ctx, "GET", reqURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", userAgent)

	resp, err := w.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, fmt.Errorf("article not found: %s", title)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("wikipedia API error: %s", resp.Status)
	}

	var apiResp struct {
		Title       string `json:"title"`
		PageID      int    `json:"pageid"`
		Extract     string `json:"extract"`
		Description string `json:"description"`
		Thumbnail   struct {
			Source string `json:"source"`
		} `json:"thumbnail"`
		ContentURLs struct {
			Desktop struct {
				Page string `json:"page"`
			} `json:"desktop"`
		} `json:"content_urls"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&apiResp); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}

	return &SummaryResponse{
		Title:        apiResp.Title,
		PageID:       apiResp.PageID,
		Extract:      apiResp.Extract,
		Description:  apiResp.Description,
		ThumbnailURL: apiResp.Thumbnail.Source,
		URL:          apiResp.ContentURLs.Desktop.Page,
	}, nil
}

// ArticleResponse is the response from a get_article command
type ArticleResponse struct {
	Title      string   `json:"title"`
	PageID     int      `json:"page_id"`
	Extract    string   `json:"extract"`
	HTML       string   `json:"html,omitempty"`
	Categories []string `json:"categories"`
	URL        string   `json:"url"`
}

func (w *WikipediaClient) getArticle(ctx context.Context, title string, includeHTML bool) (*ArticleResponse, error) {
	normalizedTitle := strings.ReplaceAll(title, " ", "_")

	// Get mobile-sections for better text extraction
	reqURL := fmt.Sprintf("%s/page/mobile-sections/%s", wikipediaRestAPI, url.PathEscape(normalizedTitle))

	req, err := http.NewRequestWithContext(ctx, "GET", reqURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", userAgent)

	resp, err := w.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, fmt.Errorf("article not found: %s", title)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("wikipedia API error: %s", resp.Status)
	}

	var apiResp struct {
		Lead struct {
			ID           int    `json:"id"`
			DisplayTitle string `json:"displaytitle"`
			Sections     []struct {
				Text string `json:"text"`
			} `json:"sections"`
		} `json:"lead"`
		Remaining struct {
			Sections []struct {
				Line string `json:"line"`
				Text string `json:"text"`
			} `json:"sections"`
		} `json:"remaining"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&apiResp); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}

	// Extract text from all sections
	var sections []string

	// Lead sections
	for _, section := range apiResp.Lead.Sections {
		if section.Text != "" {
			sections = append(sections, stripHTML(section.Text))
		}
	}

	// Remaining sections
	for _, section := range apiResp.Remaining.Sections {
		if section.Line != "" {
			sections = append(sections, fmt.Sprintf("\n## %s\n", section.Line))
		}
		if section.Text != "" {
			sections = append(sections, stripHTML(section.Text))
		}
	}

	fullText := strings.Join(sections, "\n")

	// Get categories using Action API
	categories, err := w.getCategories(ctx, title)
	if err != nil {
		// Non-fatal, just log
		categories = []string{}
	}

	result := &ArticleResponse{
		Title:      apiResp.Lead.DisplayTitle,
		PageID:     apiResp.Lead.ID,
		Extract:    fullText,
		Categories: categories,
		URL:        fmt.Sprintf("https://en.wikipedia.org/wiki/%s", url.PathEscape(normalizedTitle)),
	}

	// Optionally include HTML
	if includeHTML {
		html, err := w.getHTML(ctx, normalizedTitle)
		if err == nil {
			result.HTML = html
		}
	}

	return result, nil
}

func (w *WikipediaClient) getCategories(ctx context.Context, title string) ([]string, error) {
	params := url.Values{
		"action":  {"query"},
		"format":  {"json"},
		"titles":  {title},
		"prop":    {"categories"},
		"cllimit": {"50"},
	}

	reqURL := wikipediaSearchAPI + "?" + params.Encode()
	req, err := http.NewRequestWithContext(ctx, "GET", reqURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", userAgent)

	resp, err := w.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var apiResp struct {
		Query struct {
			Pages map[string]struct {
				Categories []struct {
					Title string `json:"title"`
				} `json:"categories"`
			} `json:"pages"`
		} `json:"query"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&apiResp); err != nil {
		return nil, err
	}

	var categories []string
	for _, page := range apiResp.Query.Pages {
		for _, cat := range page.Categories {
			// Remove "Category:" prefix
			name := strings.TrimPrefix(cat.Title, "Category:")
			categories = append(categories, name)
		}
	}

	return categories, nil
}

func (w *WikipediaClient) getHTML(ctx context.Context, normalizedTitle string) (string, error) {
	reqURL := fmt.Sprintf("%s/page/html/%s", wikipediaRestAPI, url.PathEscape(normalizedTitle))

	req, err := http.NewRequestWithContext(ctx, "GET", reqURL, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("User-Agent", userAgent)

	resp, err := w.httpClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("failed to get HTML: %s", resp.Status)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	return string(body), nil
}

// stripHTML removes HTML tags from a string
func stripHTML(s string) string {
	re := regexp.MustCompile(`<[^>]*>`)
	return re.ReplaceAllString(s, "")
}
