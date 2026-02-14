package providers

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/beam-cloud/airstore/pkg/sources"
	"github.com/beam-cloud/airstore/pkg/types"
)

const firecrawlAPI = "https://api.firecrawl.dev/v2"

// Estimated markdown size for a web page. Real size isn't known until scrape
// time (lazy), but reporting 0 breaks tools that check size before reading.
const estimatedPageSize int64 = 64 * 1024

// WebProvider discovers and scrapes web pages via Firecrawl, exposing each
// page as a markdown file through smart queries.
//
// Two modes (selected by the "web_mode" metadata key):
//
//	map    (default) — discover pages on a site via /map, scrape lazily
//	search           — web search via /search, scrape top results
//
// Usage:
//
//	mkdir /sources/web/hennessy-cocktails   # map mode (URL in guidance)
//	mkdir /sources/web/latest-ai-news      # search mode (no URL)
type WebProvider struct {
	apiKey     string
	httpClient *http.Client
}

func NewWebProvider(apiKey string) *WebProvider {
	return &WebProvider{apiKey: apiKey, httpClient: &http.Client{Timeout: 60 * time.Second}}
}

func (w *WebProvider) Name() string { return types.Web.String() }

var (
	_ sources.Provider      = (*WebProvider)(nil)
	_ sources.QueryExecutor = (*WebProvider)(nil)
)

// ---------------------------------------------------------------------------
// Provider interface (stub — smart queries are the only surface)
// ---------------------------------------------------------------------------

func (w *WebProvider) Stat(_ context.Context, _ *sources.ProviderContext, path string) (*sources.FileInfo, error) {
	if w.apiKey == "" {
		return nil, sources.ErrNotConnected
	}
	if path == "" {
		return sources.DirInfo(), nil
	}
	return nil, sources.ErrNotFound
}

func (w *WebProvider) ReadDir(_ context.Context, _ *sources.ProviderContext, _ string) ([]sources.DirEntry, error) {
	return nil, sources.ErrNotFound
}

func (w *WebProvider) Read(_ context.Context, _ *sources.ProviderContext, _ string, _, _ int64) ([]byte, error) {
	return nil, sources.ErrNotFound
}

func (w *WebProvider) Readlink(_ context.Context, _ *sources.ProviderContext, _ string) (string, error) {
	return "", sources.ErrNotFound
}

func (w *WebProvider) Search(_ context.Context, _ *sources.ProviderContext, _ string, _ int) ([]sources.SearchResult, error) {
	return nil, sources.ErrSearchNotSupported
}

// ---------------------------------------------------------------------------
// QueryExecutor
// ---------------------------------------------------------------------------

// ExecuteQuery discovers pages via /map or /search depending on the
// "web_mode" metadata key. Each result is scraped lazily in ReadResult.
func (w *WebProvider) ExecuteQuery(ctx context.Context, _ *sources.ProviderContext, spec sources.QuerySpec) (*sources.QueryResponse, error) {
	if w.apiKey == "" {
		return nil, sources.ErrNotConnected
	}

	mode := spec.Metadata["web_mode"] // "map" or "search"
	limit := clamp(spec.Limit, 1, 500, 100)

	var links []mapLink
	var err error

	switch mode {
	case "search":
		links, err = w.searchWeb(ctx, spec.Query, limit)
	default: // "map" or unset
		if _, e := url.ParseRequestURI(spec.Query); e != nil {
			return nil, fmt.Errorf("invalid URL %q: %w", spec.Query, e)
		}
		var includePaths []string
		if raw := spec.Metadata["include_paths"]; raw != "" {
			json.Unmarshal([]byte(raw), &includePaths) // best-effort
		}
		links, err = w.mapURLs(ctx, spec.Query, limit, includePaths)
	}
	if err != nil {
		return nil, err
	}

	format := spec.FilenameFormat
	if format == "" {
		format = sources.DefaultFilenameFormat("web")
	}

	now := sources.NowUnix()
	today := time.Now().Format("2006-01-02")
	seen := make(map[string]bool, len(links))
	results := make([]sources.QueryResult, 0, len(links))

	for _, link := range links {
		if link.URL == "" || seen[link.URL] {
			continue
		}
		seen[link.URL] = true

		p, _ := url.Parse(link.URL)
		path := ""
		if p != nil {
			path = p.Path
		}

		id := shortHash(link.URL)
		title := link.Title
		if title == "" {
			title = lastPathSegment(path)
		}

		meta := map[string]string{
			"id": id, "url": link.URL, "path": path, "title": title, "date": today,
		}
		if link.Description != "" {
			meta["description"] = link.Description
		}

		results = append(results, sources.QueryResult{
			ID:       link.URL,
			Filename: w.FormatFilename(format, meta),
			Metadata: meta,
			Size:     estimatedPageSize,
			Mtime:    now,
		})
	}
	return &sources.QueryResponse{Results: results}, nil
}

// ReadResult scrapes a single page and returns its markdown content.
func (w *WebProvider) ReadResult(ctx context.Context, _ *sources.ProviderContext, resultID string) ([]byte, error) {
	if w.apiKey == "" {
		return nil, sources.ErrNotConnected
	}
	if _, err := url.ParseRequestURI(resultID); err != nil {
		return nil, fmt.Errorf("invalid URL %q: %w", resultID, err)
	}

	page, err := w.scrape(ctx, resultID)
	if err != nil {
		return nil, err
	}

	var b strings.Builder
	if page.Title != "" {
		fmt.Fprintf(&b, "# %s\n\n", page.Title)
	}
	if page.Description != "" {
		fmt.Fprintf(&b, "*%s*\n\n", page.Description)
	}
	fmt.Fprintf(&b, "> Source: %s\n\n---\n\n%s", resultID, page.Markdown)
	return []byte(b.String()), nil
}

// FormatFilename replaces {id}, {title}, {path}, {date} placeholders.
func (w *WebProvider) FormatFilename(format string, meta map[string]string) string {
	if format == "" {
		format = "{title}_{id}.md"
	}
	for k, v := range meta {
		s := sources.SanitizeFilename(v)
		if k != "id" && len(s) > 40 {
			s = s[:40]
		}
		format = strings.ReplaceAll(format, "{"+k+"}", s)
	}
	if !strings.Contains(format, ".") {
		format += ".md"
	}
	if format == "" || format == ".md" {
		return "page.md"
	}
	return format
}

// ---------------------------------------------------------------------------
// Firecrawl API
// ---------------------------------------------------------------------------

// mapLink represents a discovered page from /map or /search.
type mapLink struct {
	URL         string `json:"url"`
	Title       string `json:"title"`
	Description string `json:"description"`
}

// scrapeResult holds content returned by /scrape.
type scrapeResult struct {
	Markdown    string
	Title       string
	Description string
}

// mapURLs discovers pages on a website via Firecrawl /map.
func (w *WebProvider) mapURLs(ctx context.Context, siteURL string, limit int, includePaths []string) ([]mapLink, error) {
	payload := map[string]any{"url": siteURL, "limit": limit}
	if len(includePaths) > 0 {
		payload["includePaths"] = includePaths
	}
	var resp struct {
		Success bool      `json:"success"`
		Links   []mapLink `json:"links"`
	}
	if err := w.post(ctx, "/map", payload, &resp); err != nil {
		return nil, err
	}
	if !resp.Success {
		return nil, fmt.Errorf("firecrawl /map: success=false")
	}
	return resp.Links, nil
}

// searchWeb finds pages matching a query via Firecrawl /search.
func (w *WebProvider) searchWeb(ctx context.Context, query string, limit int) ([]mapLink, error) {
	payload := map[string]any{"query": query, "limit": limit}
	var resp struct {
		Success bool `json:"success"`
		Data    struct {
			Web []mapLink `json:"web"`
		} `json:"data"`
	}
	if err := w.post(ctx, "/search", payload, &resp); err != nil {
		return nil, err
	}
	if !resp.Success {
		return nil, fmt.Errorf("firecrawl /search: success=false for %q", query)
	}
	return resp.Data.Web, nil
}

// scrape fetches a single page's content via Firecrawl /scrape.
// Images and media tags are excluded since this is a text filesystem.
func (w *WebProvider) scrape(ctx context.Context, pageURL string) (*scrapeResult, error) {
	var resp struct {
		Success bool `json:"success"`
		Data    struct {
			Markdown string `json:"markdown"`
			Metadata struct {
				Title       string `json:"title"`
				Description string `json:"description"`
			} `json:"metadata"`
		} `json:"data"`
	}
	err := w.post(ctx, "/scrape", map[string]any{
		"url":                 pageURL,
		"formats":             []string{"markdown"},
		"onlyMainContent":     true,
		"blockAds":            true,
		"skipTlsVerification": true,
		"removeBase64Images":  true,
		"excludeTags":         []string{"img", "picture", "video", "svg", "figure"},
	}, &resp)
	if err != nil {
		return nil, err
	}
	if !resp.Success {
		return nil, fmt.Errorf("firecrawl /scrape: success=false for %s", pageURL)
	}
	return &scrapeResult{
		Markdown:    resp.Data.Markdown,
		Title:       resp.Data.Metadata.Title,
		Description: resp.Data.Metadata.Description,
	}, nil
}

// post sends an authenticated POST to the Firecrawl API.
func (w *WebProvider) post(ctx context.Context, path string, payload any, dest any) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, firecrawlAPI+path, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+w.apiKey)
	req.Header.Set("Content-Type", "application/json")

	resp, err := w.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		msg, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("firecrawl %s: HTTP %d: %s", path, resp.StatusCode, bytes.TrimSpace(msg))
	}
	return json.NewDecoder(resp.Body).Decode(dest)
}

func shortHash(s string) string {
	h := sha256.Sum256([]byte(s))
	return hex.EncodeToString(h[:4])
}

// lastPathSegment extracts the trailing segment from a URL path.
// "/en-us/cocktails/old-fashioned" → "old-fashioned"
func lastPathSegment(p string) string {
	p = strings.TrimRight(p, "/")
	if i := strings.LastIndex(p, "/"); i >= 0 {
		p = p[i+1:]
	}
	if p == "" {
		return "index"
	}
	// Strip query string before extension so "page.html?v=2" → "page"
	if i := strings.Index(p, "?"); i > 0 {
		p = p[:i]
	}
	if i := strings.LastIndex(p, "."); i > 0 {
		p = p[:i]
	}
	return p
}

func clamp(v, lo, hi, def int) int {
	if v <= 0 {
		return def
	}
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}
