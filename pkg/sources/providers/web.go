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

// WebProvider discovers and scrapes web pages via Firecrawl, exposing each
// page as a markdown file through smart queries.
//
//	mkdir /sources/web/hennessy-cocktails
//	ls   /sources/web/hennessy-cocktails/
//	cat  /sources/web/hennessy-cocktails/old-fashioned.md
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

// ExecuteQuery uses Firecrawl /map to discover pages. Each URL becomes a
// QueryResult; content is fetched lazily in ReadResult.
func (w *WebProvider) ExecuteQuery(ctx context.Context, _ *sources.ProviderContext, spec sources.QuerySpec) (*sources.QueryResponse, error) {
	if w.apiKey == "" {
		return nil, sources.ErrNotConnected
	}
	if _, err := url.ParseRequestURI(spec.Query); err != nil {
		return nil, fmt.Errorf("invalid URL %q: %w", spec.Query, err)
	}

	limit := clamp(spec.Limit, 1, 500, 100)

	var includePaths []string
	if raw, ok := spec.Metadata["include_paths"]; ok && raw != "" {
		json.Unmarshal([]byte(raw), &includePaths) // best-effort
	}

	links, err := w.mapURLs(ctx, spec.Query, limit, includePaths)
	if err != nil {
		return nil, err
	}

	format := spec.FilenameFormat
	if format == "" {
		format = sources.DefaultFilenameFormat("web")
	}

	now := sources.NowUnix()
	today := time.Now().Format("2006-01-02")
	results := make([]sources.QueryResult, 0, len(links))

	for _, link := range links {
		if link.URL == "" {
			continue
		}
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
		results = append(results, sources.QueryResult{
			ID: link.URL, Filename: w.FormatFilename(format, meta), Metadata: meta, Mtime: now,
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

	md, title, err := w.scrape(ctx, resultID)
	if err != nil {
		return nil, err
	}

	var b strings.Builder
	if title != "" {
		fmt.Fprintf(&b, "# %s\n\n", title)
	}
	fmt.Fprintf(&b, "> Source: %s\n\n---\n\n%s", resultID, md)
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

type mapLink struct {
	URL         string `json:"url"`
	Title       string `json:"title"`
	Description string `json:"description"`
}

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

func (w *WebProvider) scrape(ctx context.Context, pageURL string) (markdown, title string, err error) {
	var resp struct {
		Success bool `json:"success"`
		Data    struct {
			Markdown string `json:"markdown"`
			Metadata struct {
				Title string `json:"title"`
			} `json:"metadata"`
		} `json:"data"`
	}
	err = w.post(ctx, "/scrape", map[string]any{
		"url": pageURL, "formats": []string{"markdown"},
		"onlyMainContent": true, "blockAds": true,
		"skipTlsVerification": true, "removeBase64Images": true,
	}, &resp)
	if err != nil {
		return "", "", err
	}
	if !resp.Success {
		return "", "", fmt.Errorf("firecrawl /scrape: success=false for %s", pageURL)
	}
	return resp.Data.Markdown, resp.Data.Metadata.Title, nil
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

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

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
	if i := strings.LastIndex(p, "."); i > 0 {
		p = p[:i]
	}
	if i := strings.Index(p, "?"); i > 0 {
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
