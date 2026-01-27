package builtin

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strings"

	"github.com/beam-cloud/airstore/pkg/tools"
)

func init() {
	tools.RegisterTool(&WikipediaTool{httpClient: &http.Client{}})
}

const (
	wikipediaRestAPI   = "https://en.wikipedia.org/api/rest_v1"
	wikipediaSearchAPI = "https://en.wikipedia.org/w/api.php"
	userAgent          = "Airstore/1.0 (https://github.com/beam-cloud/airstore)"
)

// WikipediaTool implements the Tool interface for Wikipedia
type WikipediaTool struct {
	httpClient *http.Client
}

func (w *WikipediaTool) Name() string {
	return "wikipedia"
}

func (w *WikipediaTool) Description() string {
	return "Look up encyclopedic knowledge from Wikipedia"
}

func (w *WikipediaTool) Commands() map[string]*tools.CommandDef {
	pos0 := 0
	return map[string]*tools.CommandDef{
		"search": {
			Description: "Search for Wikipedia articles matching a query",
			Params: []*tools.ParamDef{
				{Name: "query", Type: "string", Required: true, Position: &pos0, Description: "Search query"},
				{Name: "limit", Type: "int", Default: 10, Flag: "--limit", Short: "-n", Description: "Max results (1-50)"},
			},
		},
		"summary": {
			Description: "Get a brief summary of a Wikipedia article",
			Params: []*tools.ParamDef{
				{Name: "title", Type: "string", Required: true, Position: &pos0, Description: "Article title"},
			},
		},
		"article": {
			Description: "Get the full content of a Wikipedia article",
			Params: []*tools.ParamDef{
				{Name: "title", Type: "string", Required: true, Position: &pos0, Description: "Article title"},
				{Name: "html", Type: "bool", Default: false, Flag: "--html", Description: "Include HTML"},
			},
		},
	}
}

func (w *WikipediaTool) Execute(ctx context.Context, _ *tools.ExecutionContext, command string, args map[string]any, stdout, stderr io.Writer) error {
	var result any
	var err error

	switch command {
	case "search":
		query, _ := args["query"].(string)
		limit := 10
		if l, ok := args["limit"].(int); ok {
			limit = l
		}
		result, err = w.search(ctx, query, limit)
	case "summary":
		title, _ := args["title"].(string)
		result, err = w.getSummary(ctx, title)
	case "article":
		title, _ := args["title"].(string)
		html, _ := args["html"].(bool)
		result, err = w.getArticle(ctx, title, html)
	default:
		return fmt.Errorf("unknown command: %s", command)
	}

	if err != nil {
		return err
	}

	enc := json.NewEncoder(stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(result)
}

type searchResult struct {
	Title   string `json:"title"`
	PageID  int    `json:"page_id"`
	Excerpt string `json:"excerpt"`
}

type searchResponse struct {
	Results []searchResult `json:"results"`
	Total   int            `json:"total"`
}

func (w *WikipediaTool) search(ctx context.Context, query string, limit int) (*searchResponse, error) {
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
		"srprop":   {"snippet"},
		"utf8":     {"1"},
	}

	req, err := http.NewRequestWithContext(ctx, "GET", wikipediaSearchAPI+"?"+params.Encode(), nil)
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
		return nil, err
	}

	results := make([]searchResult, 0, len(apiResp.Query.Search))
	for _, item := range apiResp.Query.Search {
		results = append(results, searchResult{
			Title:   item.Title,
			PageID:  item.PageID,
			Excerpt: stripHTML(item.Snippet),
		})
	}

	return &searchResponse{Results: results, Total: apiResp.Query.SearchInfo.TotalHits}, nil
}

type summaryResponse struct {
	Title   string `json:"title"`
	Extract string `json:"extract"`
	URL     string `json:"url"`
}

func (w *WikipediaTool) getSummary(ctx context.Context, title string) (*summaryResponse, error) {
	normalized := strings.ReplaceAll(title, " ", "_")
	reqURL := fmt.Sprintf("%s/page/summary/%s", wikipediaRestAPI, url.PathEscape(normalized))

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
		Extract     string `json:"extract"`
		ContentURLs struct {
			Desktop struct {
				Page string `json:"page"`
			} `json:"desktop"`
		} `json:"content_urls"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&apiResp); err != nil {
		return nil, err
	}

	return &summaryResponse{
		Title:   apiResp.Title,
		Extract: apiResp.Extract,
		URL:     apiResp.ContentURLs.Desktop.Page,
	}, nil
}

type articleResponse struct {
	Title   string `json:"title"`
	Content string `json:"content"`
	HTML    string `json:"html,omitempty"`
	URL     string `json:"url"`
}

func (w *WikipediaTool) getArticle(ctx context.Context, title string, includeHTML bool) (*articleResponse, error) {
	normalized := strings.ReplaceAll(title, " ", "_")

	// Use MediaWiki API for article content (more reliable than REST API)
	params := url.Values{
		"action":      {"query"},
		"format":      {"json"},
		"titles":      {title},
		"prop":        {"extracts|info"},
		"explaintext": {"1"}, // Plain text, not HTML
		"exsectionformat": {"plain"},
		"inprop":      {"url"},
	}
	if includeHTML {
		params.Del("explaintext")
		params.Set("exlimit", "1")
	}

	req, err := http.NewRequestWithContext(ctx, "GET", wikipediaSearchAPI+"?"+params.Encode(), nil)
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
			Pages map[string]struct {
				PageID  int    `json:"pageid"`
				Title   string `json:"title"`
				Extract string `json:"extract"`
				FullURL string `json:"fullurl"`
				Missing bool   `json:"missing,omitempty"`
			} `json:"pages"`
		} `json:"query"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&apiResp); err != nil {
		return nil, err
	}

	// Get the first (and only) page from the response
	for _, page := range apiResp.Query.Pages {
		if page.PageID == 0 {
			return nil, fmt.Errorf("article not found: %s", title)
		}

		result := &articleResponse{
			Title:   page.Title,
			Content: page.Extract,
			URL:     page.FullURL,
		}

		if result.URL == "" {
			result.URL = fmt.Sprintf("https://en.wikipedia.org/wiki/%s", url.PathEscape(normalized))
		}

		if includeHTML {
			// Extract contains HTML when explaintext is not set
			result.HTML = page.Extract
			result.Content = stripHTML(page.Extract)
		}

		return result, nil
	}

	return nil, fmt.Errorf("article not found: %s", title)
}

var htmlTagRe = regexp.MustCompile(`<[^>]*>`)

func stripHTML(s string) string {
	return htmlTagRe.ReplaceAllString(s, "")
}
