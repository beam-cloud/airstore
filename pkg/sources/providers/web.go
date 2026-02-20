package providers

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	pathpkg "path"
	"sort"
	"strings"
	"time"
	"unicode"

	"github.com/beam-cloud/airstore/pkg/sources"
	"github.com/beam-cloud/airstore/pkg/types"
	"golang.org/x/net/publicsuffix"
)

const firecrawlAPI = "https://api.firecrawl.dev/v2"
const webAuthSnapshotExtraKey = "web_auth_snapshot"

var errCrawlNoLinks = errors.New("firecrawl crawl returned no links")

// Estimated markdown size for a web page. Real size isn't known until scrape
// time (lazy), but reporting 0 breaks tools that check size before reading.
const estimatedPageSize int64 = 64 * 1024

// WebProvider discovers and scrapes web pages via Firecrawl, exposing each
// page as a markdown file through smart queries.
//
// Web modes (selected by the "web_mode" metadata key):
//
//	crawl            — discover pages recursively via /crawl, scrape lazily
//	scrape           — scrape one URL directly
//	search           — web search via /search, scrape top results lazily
//	map              — fast URL discovery via /map, scrape lazily
//
// Usage:
//
//	mkdir /sources/web/hennessy-cocktails  # crawl mode (URL in guidance)
//	mkdir /sources/web/homepage-snapshot   # scrape mode (single URL)
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

// ExecuteQuery discovers pages via /crawl, /map, /search, or single-url scrape
// "web_mode" metadata key. Each result is scraped lazily in ReadResult.
func (w *WebProvider) ExecuteQuery(ctx context.Context, pctx *sources.ProviderContext, spec sources.QuerySpec) (*sources.QueryResponse, error) {
	if w.apiKey == "" {
		return nil, sources.ErrNotConnected
	}

	mode := normalizeWebMode(spec.Metadata["web_mode"])
	limit := clamp(spec.Limit, 1, 500, 100)
	siteURL, siteURLErr := url.ParseRequestURI(spec.Query)
	isURLQuery := siteURLErr == nil
	authHeaders := w.authHeadersForRequest(pctx, spec.Query)
	var includePaths []string
	if raw := spec.Metadata["include_paths"]; raw != "" {
		json.Unmarshal([]byte(raw), &includePaths) // best-effort
	}

	var links []mapLink
	var err error

	// Smart query inference can occasionally emit web_mode=search even when the
	// query is clearly a URL. Force URL queries through crawl mode for predictable
	// crawl semantics.
	if mode == "search" && isURLQuery {
		mode = "crawl"
	}

	switch mode {
	case "search":
		links, err = w.searchWeb(ctx, spec.Query, limit)
	case "scrape":
		if !isURLQuery {
			return nil, fmt.Errorf("invalid URL %q: %w", spec.Query, siteURLErr)
		}
		links = []mapLink{{URL: spec.Query}}
	case "crawl":
		if !isURLQuery {
			return nil, fmt.Errorf("invalid URL %q: %w", spec.Query, siteURLErr)
		}
		links, err = w.crawlURLs(ctx, spec.Query, limit, includePaths, authHeaders)
		// Some crawls complete without discoverable links; try /map before
		// falling back to the seed URL.
		if err != nil && errors.Is(err, errCrawlNoLinks) {
			if mapLinks, mapErr := w.mapURLs(ctx, spec.Query, limit, includePaths); mapErr == nil && len(mapLinks) > 0 {
				links = mapLinks
			} else {
				links = []mapLink{{URL: spec.Query}}
			}
			err = nil
		}
		// Firecrawl crawl responses can include off-site links depending on crawl
		// topology. Keep only same-site URLs for crawl mode.
		links = filterLinksToSite(siteURL, links, len(authHeaders) > 0)
		if len(links) == 0 {
			links = []mapLink{{URL: spec.Query}}
		}
	default: // "map" fast discovery mode
		if !isURLQuery {
			return nil, fmt.Errorf("invalid URL %q: %w", spec.Query, siteURLErr)
		}
		links, err = w.mapURLs(ctx, spec.Query, limit, includePaths)
		// Keep map semantics primary. If /map fails under authenticated traffic,
		// degrade to /crawl as a resilience fallback rather than failing hard.
		if err != nil && len(authHeaders) > 0 {
			if crawlLinks, crawlErr := w.crawlURLs(ctx, spec.Query, limit, includePaths, authHeaders); crawlErr == nil {
				links = crawlLinks
				err = nil
			}
		}
		// Firecrawl map responses can include off-site links depending on crawl
		// topology. Keep only same-site URLs for map mode.
		links = filterLinksToSite(siteURL, links, len(authHeaders) > 0)
		if len(links) == 0 && len(authHeaders) > 0 {
			links = []mapLink{{URL: spec.Query}}
		}
	}
	if err != nil {
		return nil, err
	}
	if isURLQuery && len(authHeaders) > 0 && mode != "search" && mode != "scrape" && len(links) <= 5 {
		// Authenticated SPAs often expose links only after rendering. Use a
		// targeted /scrape links extraction pass to expand sparse discovery.
		links = w.expandSparseAuthenticatedLinks(ctx, spec.Query, siteURL, links, includePaths, limit, authHeaders, mode == "crawl")
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
		slug := webURLSlug(link.URL)
		title := strings.TrimSpace(link.Title)
		if mode == "search" {
			if title == "" || isGenericWebTitle(title) {
				title = slug
			}
		} else {
			title = slug
			// Root URLs frequently normalize to "index". Keep a concrete title
			// when available so results remain distinguishable.
			if title == "index" && strings.TrimSpace(link.Title) != "" {
				title = strings.TrimSpace(link.Title)
			}
		}
		if title == "" {
			title = "page"
		}

		meta := map[string]string{
			"id": id, "url": link.URL, "path": path, "title": title, "slug": slug, "date": today,
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

// filterLinksToSite keeps only links that match the queried host (allowing a
// common www/non-www variant). This prevents off-site leakage in URL modes.
func filterLinksToSite(siteURL *url.URL, links []mapLink, includeRelatedSubdomains bool) []mapLink {
	if siteURL == nil {
		return links
	}
	host := strings.ToLower(siteURL.Hostname())
	if host == "" {
		return links
	}

	allowed := map[string]struct{}{host: {}}
	if strings.HasPrefix(host, "www.") {
		allowed[strings.TrimPrefix(host, "www.")] = struct{}{}
	} else {
		allowed["www."+host] = struct{}{}
	}

	out := make([]mapLink, 0, len(links))
	for _, link := range links {
		u, err := url.Parse(link.URL)
		if err != nil {
			continue
		}
		candidateHost := strings.ToLower(u.Hostname())
		if _, ok := allowed[candidateHost]; ok {
			out = append(out, link)
			continue
		}
		if includeRelatedSubdomains && sameRegistrableDomain(host, candidateHost) {
			out = append(out, link)
		}
	}
	return out
}

func sameRegistrableDomain(hostA, hostB string) bool {
	if hostA == "" || hostB == "" {
		return false
	}
	a, errA := publicsuffix.EffectiveTLDPlusOne(hostA)
	b, errB := publicsuffix.EffectiveTLDPlusOne(hostB)
	if errA != nil || errB != nil {
		return false
	}
	return strings.EqualFold(a, b)
}

// ReadResult scrapes a single page and returns its markdown content.
func (w *WebProvider) ReadResult(ctx context.Context, pctx *sources.ProviderContext, resultID string) ([]byte, error) {
	if w.apiKey == "" {
		return nil, sources.ErrNotConnected
	}
	if _, err := url.ParseRequestURI(resultID); err != nil {
		return nil, fmt.Errorf("invalid URL %q: %w", resultID, err)
	}

	page, err := w.scrape(ctx, resultID, w.authHeadersForRequest(pctx, resultID))
	if err != nil {
		return nil, err
	}

	combined := strings.ToLower(page.Title + " " + page.Markdown)
	if isChromiumBlockPage(combined) {
		return nil, fmt.Errorf("page blocked by browser: %s", resultID)
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

type webAuthSnapshot struct {
	Headers map[string]string `json:"headers"`
	Cookies json.RawMessage   `json:"cookies"`
}

type webAuthCookie struct {
	Name   string `json:"name"`
	Value  string `json:"value"`
	Domain string `json:"domain"`
	Path   string `json:"path"`
}

// mapURLs discovers pages on a website via Firecrawl /map.
func (w *WebProvider) mapURLs(ctx context.Context, siteURL string, limit int, includePaths []string) ([]mapLink, error) {
	payload := map[string]any{
		"url":         siteURL,
		"limit":       limit,
		"ignoreCache": true, // bypass Firecrawl sitemap cache (up to 7 days); we cache on our end
	}
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

// crawlURLs discovers pages on a website via Firecrawl /crawl using
// authenticated scrape options and polling for completion.
func (w *WebProvider) crawlURLs(ctx context.Context, siteURL string, limit int, includePaths []string, headers map[string]string) ([]mapLink, error) {
	payload := map[string]any{
		"url":   siteURL,
		"limit": limit,
		"scrapeOptions": map[string]any{
			"formats":      []string{"markdown"},
			"headers":      headers,
			"storeInCache": false,
		},
	}
	if len(includePaths) > 0 {
		payload["includePaths"] = includePaths
	}
	if len(headers) > 0 {
		// Authenticated app surfaces (dashboards, account portals) are commonly
		// blocked by robots despite being the explicit user target.
		payload["ignoreRobotsTxt"] = true
	}

	var start struct {
		Success bool `json:"success"`
		ID      string
		Data    struct {
			ID string `json:"id"`
		} `json:"data"`
	}
	if err := w.post(ctx, "/crawl", payload, &start); err != nil {
		return nil, err
	}
	if !start.Success {
		return nil, fmt.Errorf("firecrawl /crawl: success=false")
	}

	crawlID := start.ID
	if crawlID == "" {
		crawlID = start.Data.ID
	}
	if crawlID == "" {
		return nil, fmt.Errorf("firecrawl /crawl: missing crawl id")
	}

	const maxPollAttempts = 30
	for i := 0; i < maxPollAttempts; i++ {
		var poll struct {
			Success bool            `json:"success"`
			Status  string          `json:"status"`
			Data    json.RawMessage `json:"data"`
		}
		if err := w.get(ctx, "/crawl/"+crawlID, &poll); err != nil {
			return nil, err
		}

		status := strings.ToLower(strings.TrimSpace(poll.Status))
		dataRaw := poll.Data

		// Some responses nest status/data under top-level "data".
		if len(dataRaw) > 0 {
			var nested struct {
				Status string          `json:"status"`
				Data   json.RawMessage `json:"data"`
				Links  []mapLink       `json:"links"`
			}
			if json.Unmarshal(dataRaw, &nested) == nil {
				if nested.Status != "" {
					status = strings.ToLower(strings.TrimSpace(nested.Status))
				}
				if len(nested.Links) > 0 {
					dataRaw, _ = json.Marshal(nested.Links)
				} else if len(nested.Data) > 0 {
					dataRaw = nested.Data
				}
			}
		}

		switch status {
		case "completed", "done", "success":
			links := decodeCrawlLinks(dataRaw)
			if len(links) == 0 {
				return nil, fmt.Errorf("%w: crawl_id=%s", errCrawlNoLinks, crawlID)
			}
			return links, nil
		case "failed", "error", "cancelled", "canceled":
			return nil, fmt.Errorf("firecrawl /crawl/%s: status=%s", crawlID, status)
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(time.Second):
		}
	}

	return nil, fmt.Errorf("firecrawl /crawl/%s: timed out waiting for completion", crawlID)
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
func (w *WebProvider) scrape(ctx context.Context, pageURL string, headers map[string]string) (*scrapeResult, error) {
	isAuthenticatedRequest := len(headers) > 0
	attempts := []scrapeAttempt{
		{
			Headers:         headers,
			OnlyMainContent: true,
			WaitForRender:   isAuthenticatedRequest,
		},
	}
	if isAuthenticatedRequest {
		retryHeaders := cloneStringMap(headers)
		if fallbackReferer := authFallbackReferer(pageURL); fallbackReferer != "" {
			retryHeaders["Referer"] = fallbackReferer
		}
		attempts = append(attempts, scrapeAttempt{
			Headers:         retryHeaders,
			OnlyMainContent: false,
			WaitForRender:   true,
		})
	}

	var lastErr error
	var lastResult *scrapeResult
	for i, attempt := range attempts {
		result, err := w.scrapeWithAttempt(ctx, pageURL, attempt)
		if err != nil {
			lastErr = err
			continue
		}
		if !isAuthenticatedRequest || !isLikelyAuthInterruptionPage(result) {
			return result, nil
		}

		lastResult = result
		if i == len(attempts)-1 {
			return result, nil
		}
	}

	if lastResult != nil {
		return lastResult, nil
	}
	if lastErr != nil {
		return nil, lastErr
	}
	return nil, fmt.Errorf("firecrawl /scrape: no response for %s", pageURL)
}

type scrapeAttempt struct {
	Headers         map[string]string
	OnlyMainContent bool
	WaitForRender   bool
}

func (w *WebProvider) scrapeWithAttempt(ctx context.Context, pageURL string, attempt scrapeAttempt) (*scrapeResult, error) {
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
	payload := map[string]any{
		"url":                 pageURL,
		"formats":             []string{"markdown"},
		"onlyMainContent":     attempt.OnlyMainContent,
		"blockAds":            true,
		"skipTlsVerification": true,
		"removeBase64Images":  true,
		"excludeTags":         []string{"img", "picture", "video", "svg", "figure"},
		"maxAge":              0, // bypass Firecrawl cache; we cache on our end
	}
	if len(attempt.Headers) > 0 {
		payload["headers"] = attempt.Headers
		payload["storeInCache"] = false
		payload["blockAds"] = false
		if attempt.WaitForRender {
			payload["actions"] = []map[string]any{
				{"type": "wait", "milliseconds": 2200},
			}
		}
	}

	err := w.post(ctx, "/scrape", payload, &resp)
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

func isLikelyAuthInterruptionPage(page *scrapeResult) bool {
	if page == nil {
		return false
	}
	text := strings.ToLower(strings.Join([]string{
		strings.TrimSpace(page.Title),
		strings.TrimSpace(page.Description),
		strings.TrimSpace(page.Markdown),
	}, "\n"))
	if text == "" {
		return false
	}

	outageSignals := []string{
		"we'll be back shortly",
		"site or service you are trying to utilize is not currently working",
		"temporarily unavailable",
		"service unavailable",
		"chase outage",
	}
	for _, signal := range outageSignals {
		if strings.Contains(text, signal) {
			return true
		}
	}

	if isChromiumBlockPage(text) {
		return true
	}

	if strings.Contains(text, "sign in") || strings.Contains(text, "log in") {
		if strings.Contains(text, "password") || strings.Contains(text, "username") || strings.Contains(text, "session expired") {
			return true
		}
	}

	return false
}

// isChromiumBlockPage returns true when Firecrawl's Chromium instance blocked
// the page at the network level (ad blocker, content filter, DNS failure, etc.).
func isChromiumBlockPage(text string) bool {
	signals := []string{
		"err_blocked_by_client",
		"err_blocked_by_response",
		"err_blocked_by_administrator",
		"this page has been blocked by chromium",
		"this site can\u2019t be reached",
		"this site can't be reached",
		"err_connection_refused",
		"err_name_not_resolved",
		"err_cert_authority_invalid",
	}
	for _, s := range signals {
		if strings.Contains(text, s) {
			return true
		}
	}
	return false
}

func (w *WebProvider) scrapeLinks(ctx context.Context, pageURL string, headers map[string]string) ([]mapLink, error) {
	var resp struct {
		Success bool `json:"success"`
		Data    struct {
			Links []string `json:"links"`
		} `json:"data"`
	}
	payload := map[string]any{
		"url":             pageURL,
		"formats":         []string{"links"},
		"onlyMainContent": false,
		"maxAge":          0,
	}
	if len(headers) > 0 {
		payload["headers"] = headers
		payload["storeInCache"] = false
		payload["actions"] = []map[string]any{
			{"type": "wait", "milliseconds": 1500},
		}
	}
	if err := w.post(ctx, "/scrape", payload, &resp); err != nil {
		return nil, err
	}
	if !resp.Success {
		return nil, fmt.Errorf("firecrawl /scrape: success=false for %s", pageURL)
	}
	out := make([]mapLink, 0, len(resp.Data.Links))
	for _, raw := range resp.Data.Links {
		u := strings.TrimSpace(raw)
		if u == "" {
			continue
		}
		out = append(out, mapLink{URL: u})
	}
	return out, nil
}

func (w *WebProvider) expandSparseAuthenticatedLinks(
	ctx context.Context,
	seedURL string,
	siteURL *url.URL,
	baseLinks []mapLink,
	includePaths []string,
	limit int,
	headers map[string]string,
	suppressLowSignal bool,
) []mapLink {
	merged := mergeMapLinks(baseLinks, nil)
	if scrapedSeedLinks, err := w.scrapeLinks(ctx, seedURL, headers); err == nil && len(scrapedSeedLinks) > 0 {
		merged = mergeMapLinks(merged, scrapedSeedLinks)
	}

	merged = filterLinksToSite(siteURL, merged, true)
	if len(includePaths) > 0 {
		merged = filterLinksByIncludePaths(merged, includePaths)
	}

	for _, candidateURL := range pickExpansionCandidates(seedURL, merged, 2) {
		if strings.EqualFold(strings.TrimSpace(candidateURL), strings.TrimSpace(seedURL)) {
			continue
		}
		if scrapedCandidateLinks, err := w.scrapeLinks(ctx, candidateURL, headers); err == nil && len(scrapedCandidateLinks) > 0 {
			merged = mergeMapLinks(merged, scrapedCandidateLinks)
		}
	}

	merged = filterLinksToSite(siteURL, merged, true)
	if len(includePaths) > 0 {
		merged = filterLinksByIncludePaths(merged, includePaths)
	}

	merged = rankWebsiteLinks(merged, seedURL, suppressLowSignal)
	if len(merged) == 0 {
		return []mapLink{{URL: seedURL}}
	}
	if limit > 0 && len(merged) > limit {
		return merged[:limit]
	}
	return merged
}

func filterLinksByIncludePaths(links []mapLink, includePaths []string) []mapLink {
	if len(includePaths) == 0 {
		return links
	}
	patterns := make([]string, 0, len(includePaths))
	for _, rawPattern := range includePaths {
		pattern := strings.TrimSpace(rawPattern)
		if pattern == "" {
			continue
		}
		if !strings.HasPrefix(pattern, "/") {
			pattern = "/" + pattern
		}
		patterns = append(patterns, pattern)
	}
	if len(patterns) == 0 {
		return links
	}

	out := make([]mapLink, 0, len(links))
	for _, link := range links {
		parsed, err := url.Parse(link.URL)
		if err != nil {
			continue
		}
		linkPath := parsed.Path
		if linkPath == "" {
			linkPath = "/"
		}
		if includePathMatches(linkPath, patterns) {
			out = append(out, link)
		}
	}
	return out
}

func includePathMatches(linkPath string, patterns []string) bool {
	for _, pattern := range patterns {
		if linkPath == pattern {
			return true
		}
		if strings.HasSuffix(pattern, "/*") {
			prefix := strings.TrimSuffix(pattern, "*")
			if strings.HasPrefix(linkPath, prefix) || linkPath == strings.TrimSuffix(prefix, "/") {
				return true
			}
		}
		if ok, err := pathpkg.Match(pattern, linkPath); err == nil && ok {
			return true
		}
	}
	return false
}

func pickExpansionCandidates(seedURL string, links []mapLink, max int) []string {
	if max <= 0 {
		return nil
	}
	seedNormalized, _ := normalizeDiscoveredURL(seedURL)

	type scoredCandidate struct {
		URL   string
		Score int
	}
	scored := make([]scoredCandidate, 0, len(links))
	fallback := make([]string, 0, max)
	seen := make(map[string]struct{}, len(links))

	for _, link := range links {
		normalizedURL, ok := normalizeDiscoveredURL(link.URL)
		if !ok || normalizedURL == seedNormalized {
			continue
		}
		if _, exists := seen[normalizedURL]; exists {
			continue
		}
		seen[normalizedURL] = struct{}{}
		if len(fallback) < max {
			fallback = append(fallback, normalizedURL)
		}
		score := websiteURLScore(normalizedURL)
		if score <= 0 {
			continue
		}
		scored = append(scored, scoredCandidate{URL: normalizedURL, Score: score})
	}

	sort.SliceStable(scored, func(i, j int) bool {
		if scored[i].Score == scored[j].Score {
			return scored[i].URL < scored[j].URL
		}
		return scored[i].Score > scored[j].Score
	})

	out := make([]string, 0, max)
	for _, candidate := range scored {
		out = append(out, candidate.URL)
		if len(out) >= max {
			return out
		}
	}
	if len(out) == 0 {
		return fallback
	}
	return out
}

func rankWebsiteLinks(links []mapLink, seedURL string, suppressLowSignal bool) []mapLink {
	type scoredLink struct {
		Link  mapLink
		Score int
		URL   string
	}
	seedNormalized, _ := normalizeDiscoveredURL(seedURL)
	scored := make([]scoredLink, 0, len(links))
	seen := make(map[string]struct{}, len(links))
	hasStrongSignal := false

	for _, link := range links {
		normalizedURL, ok := normalizeDiscoveredURL(link.URL)
		if !ok {
			continue
		}
		if _, exists := seen[normalizedURL]; exists {
			continue
		}
		seen[normalizedURL] = struct{}{}

		if isLikelyNoiseURL(normalizedURL) {
			continue
		}

		link.URL = normalizedURL
		score := websiteURLScore(normalizedURL)
		if score >= 4 {
			hasStrongSignal = true
		}
		scored = append(scored, scoredLink{Link: link, Score: score, URL: normalizedURL})
	}

	sort.SliceStable(scored, func(i, j int) bool {
		if scored[i].Score == scored[j].Score {
			return scored[i].URL < scored[j].URL
		}
		return scored[i].Score > scored[j].Score
	})

	out := make([]mapLink, 0, len(scored))
	for _, candidate := range scored {
		// If we already have high-signal pages, suppress weak utility routes.
		if suppressLowSignal && hasStrongSignal && candidate.Score < 2 && candidate.URL != seedNormalized {
			continue
		}
		out = append(out, candidate.Link)
	}
	if len(out) > 0 {
		return out
	}
	if len(scored) > 0 {
		return []mapLink{scored[0].Link}
	}
	return nil
}

func normalizeDiscoveredURL(rawURL string) (string, bool) {
	parsed, err := url.Parse(strings.TrimSpace(rawURL))
	if err != nil || parsed == nil || parsed.Scheme == "" || parsed.Host == "" {
		return "", false
	}
	parsed.Host = strings.ToLower(parsed.Host)
	if parsed.Path == "" {
		parsed.Path = "/"
	}
	if parsed.Path != "/" {
		parsed.Path = strings.TrimRight(parsed.Path, "/")
	}
	fragment := strings.TrimSpace(parsed.Fragment)
	if fragment == "#" || fragment == "/" {
		fragment = ""
	}
	parsed.Fragment = fragment
	return parsed.String(), true
}

func isLikelyNoiseURL(rawURL string) bool {
	parsed, err := url.Parse(rawURL)
	if err != nil || parsed == nil {
		return true
	}
	path := strings.ToLower(strings.TrimSpace(parsed.Path))
	fragment := strings.ToLower(strings.TrimSpace(parsed.Fragment))

	if (path == "" || path == "/") && (fragment == "" || fragment == "/") {
		return true
	}
	if strings.Contains(path, "/signin") || strings.Contains(path, "/sign-in") ||
		strings.Contains(path, "/logon") || strings.Contains(path, "/logout") {
		return true
	}
	if strings.Contains(path, "/session") && strings.Contains(path, "timeout") {
		return true
	}
	if strings.HasSuffix(path, "/web/auth/dashboard") &&
		(fragment == "" || fragment == "dashboard" || fragment == "/dashboard") {
		return true
	}
	return false
}

func websiteURLScore(rawURL string) int {
	normalized := strings.TrimSpace(rawURL)
	lower := strings.ToLower(normalized)
	score := 0

	weights := []struct {
		token  string
		weight int
	}{
		{"transaction", 5},
		{"account", 4},
		{"balance", 4},
		{"overview", 4},
		{"dashboard", 3},
		{"statement", 3},
		{"history", 2},
		{"activity", 2},
		{"details", 1},
		{"profile", 1},
	}
	for _, entry := range weights {
		if strings.Contains(lower, entry.token) {
			score += entry.weight
		}
	}
	if strings.Contains(lower, "transactiondetails") {
		score += 2
	}
	if strings.Contains(lower, ",chk,") || strings.Contains(lower, ",sav,") {
		score += 2
	}
	if isLikelyNoiseURL(normalized) {
		score -= 4
	}
	if score < 0 {
		return 0
	}
	return score
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

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		msg, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("firecrawl %s: HTTP %d: %s", path, resp.StatusCode, bytes.TrimSpace(msg))
	}
	return json.NewDecoder(resp.Body).Decode(dest)
}

func (w *WebProvider) get(ctx context.Context, path string, dest any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, firecrawlAPI+path, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+w.apiKey)

	resp, err := w.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		msg, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("firecrawl %s: HTTP %d: %s", path, resp.StatusCode, bytes.TrimSpace(msg))
	}
	return json.NewDecoder(resp.Body).Decode(dest)
}

func (w *WebProvider) authHeadersForRequest(pctx *sources.ProviderContext, targetURL string) map[string]string {
	if pctx == nil || pctx.Credentials == nil || pctx.Credentials.Extra == nil {
		return nil
	}

	raw := strings.TrimSpace(pctx.Credentials.Extra[webAuthSnapshotExtraKey])
	if raw == "" {
		return nil
	}

	var snapshot webAuthSnapshot
	if err := json.Unmarshal([]byte(raw), &snapshot); err != nil {
		return nil
	}

	headers := make(map[string]string)
	for k, v := range snapshot.Headers {
		key := http.CanonicalHeaderKey(strings.TrimSpace(k))
		val := strings.TrimSpace(v)
		if key == "" || val == "" {
			continue
		}
		if !isForwardableAuthHeader(key) {
			continue
		}
		headers[key] = val
	}

	cookieHeader := buildCookieHeader(parseSnapshotCookies(snapshot.Cookies), targetURL)
	if cookieHeader != "" {
		if existing := headers["Cookie"]; existing != "" {
			headers["Cookie"] = existing + "; " + cookieHeader
		} else {
			headers["Cookie"] = cookieHeader
		}
	}

	applyAuthHeaderDefaults(headers, targetURL)
	if len(headers) == 0 {
		return nil
	}
	return headers
}

func isForwardableAuthHeader(key string) bool {
	switch key {
	case "Host", "Connection", "Proxy-Connection", "Content-Length", "Accept-Encoding", "Transfer-Encoding", "Upgrade", "Te", "Trailer", "Keep-Alive", "Forwarded", "Via":
		return false
	}
	lower := strings.ToLower(strings.TrimSpace(key))
	if strings.HasPrefix(lower, "sec-") || strings.HasPrefix(lower, "cf-") || strings.HasPrefix(lower, "x-forwarded-") {
		return false
	}
	return true
}

func applyAuthHeaderDefaults(headers map[string]string, targetURL string) {
	target, err := url.Parse(strings.TrimSpace(targetURL))
	if err != nil || target == nil || target.Scheme == "" || target.Host == "" {
		return
	}

	origin := target.Scheme + "://" + target.Host
	fallbackReferer := authFallbackReferer(targetURL)

	if headers["Accept"] == "" {
		headers["Accept"] = "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
	}
	if headers["Accept-Language"] == "" {
		headers["Accept-Language"] = "en-US,en;q=0.9"
	}
	if headers["User-Agent"] == "" {
		headers["User-Agent"] = "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
	}
	headers["Origin"] = origin

	currentReferer := strings.TrimSpace(headers["Referer"])
	targetPath := strings.ToLower(strings.TrimSpace(target.Path))
	if strings.Contains(targetPath, "/web/auth/") {
		if fallbackReferer != "" {
			headers["Referer"] = fallbackReferer
		}
		return
	}

	if currentReferer == "" || !isSameOriginURL(currentReferer, origin) {
		if fallbackReferer != "" {
			headers["Referer"] = fallbackReferer
		}
	}
}

func authFallbackReferer(targetURL string) string {
	target, err := url.Parse(strings.TrimSpace(targetURL))
	if err != nil || target == nil || target.Scheme == "" || target.Host == "" {
		return ""
	}
	origin := target.Scheme + "://" + target.Host
	targetPath := strings.ToLower(strings.TrimSpace(target.Path))
	if strings.Contains(targetPath, "/web/auth/") {
		return origin + "/web/auth/dashboard"
	}
	dir := pathpkg.Dir(target.Path)
	if dir != "." && dir != "/" {
		dir = strings.TrimRight(dir, "/") + "/"
		return origin + dir
	}
	return origin + "/"
}

func isSameOriginURL(rawURL string, origin string) bool {
	parsed, err := url.Parse(strings.TrimSpace(rawURL))
	if err != nil || parsed == nil || parsed.Scheme == "" || parsed.Host == "" {
		return false
	}
	return strings.EqualFold(parsed.Scheme+"://"+parsed.Host, origin)
}

func cloneStringMap(in map[string]string) map[string]string {
	if len(in) == 0 {
		return map[string]string{}
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func parseSnapshotCookies(raw json.RawMessage) []webAuthCookie {
	if len(raw) == 0 {
		return nil
	}

	var cookies []webAuthCookie
	if err := json.Unmarshal(raw, &cookies); err == nil {
		return cookies
	}

	// Compatibility path for {name:value} cookie maps.
	var cookieMap map[string]string
	if err := json.Unmarshal(raw, &cookieMap); err == nil {
		cookies = make([]webAuthCookie, 0, len(cookieMap))
		for name, value := range cookieMap {
			cookies = append(cookies, webAuthCookie{Name: name, Value: value})
		}
		return cookies
	}

	return nil
}

func buildCookieHeader(cookies []webAuthCookie, targetURL string) string {
	if len(cookies) == 0 {
		return ""
	}

	target, _ := url.Parse(targetURL)
	targetHost := ""
	targetPath := "/"
	if target != nil {
		targetHost = strings.ToLower(target.Hostname())
		if target.Path != "" {
			targetPath = target.Path
		}
	}

	parts := make([]string, 0, len(cookies))
	for _, c := range cookies {
		name := strings.TrimSpace(c.Name)
		value := strings.TrimSpace(c.Value)
		if name == "" || value == "" {
			continue
		}
		if targetHost != "" && !cookieDomainMatches(targetHost, c.Domain) {
			continue
		}
		if !cookiePathMatches(targetPath, c.Path) {
			continue
		}
		parts = append(parts, name+"="+value)
	}

	return strings.Join(parts, "; ")
}

func cookieDomainMatches(host, cookieDomain string) bool {
	domain := strings.ToLower(strings.TrimSpace(strings.TrimPrefix(cookieDomain, ".")))
	if domain == "" {
		return true
	}
	return host == domain || strings.HasSuffix(host, "."+domain)
}

func cookiePathMatches(requestPath, cookiePath string) bool {
	if cookiePath == "" || cookiePath == "/" {
		return true
	}
	if !strings.HasPrefix(cookiePath, "/") {
		cookiePath = "/" + cookiePath
	}
	if requestPath == "" {
		requestPath = "/"
	}
	return strings.HasPrefix(requestPath, cookiePath)
}

func decodeCrawlLinks(raw json.RawMessage) []mapLink {
	if len(raw) == 0 {
		return nil
	}

	// Common shape: data is an array of page objects.
	var items []struct {
		URL         string `json:"url"`
		SourceURL   string `json:"sourceURL"`
		Title       string `json:"title"`
		Description string `json:"description"`
		Metadata    struct {
			URL         string `json:"url"`
			SourceURL   string `json:"sourceURL"`
			Title       string `json:"title"`
			Description string `json:"description"`
		} `json:"metadata"`
	}
	if json.Unmarshal(raw, &items) == nil && len(items) > 0 {
		out := make([]mapLink, 0, len(items))
		for _, item := range items {
			title := item.Title
			if title == "" {
				title = item.Metadata.Title
			}
			description := item.Description
			if description == "" {
				description = item.Metadata.Description
			}
			linkURL := item.URL
			if linkURL == "" {
				linkURL = item.SourceURL
			}
			if linkURL == "" {
				linkURL = item.Metadata.URL
			}
			if linkURL == "" {
				linkURL = item.Metadata.SourceURL
			}
			if linkURL == "" {
				continue
			}
			out = append(out, mapLink{
				URL:         linkURL,
				Title:       title,
				Description: description,
			})
		}
		return out
	}

	// Alternate shape: object with nested data/links arrays.
	var wrapped struct {
		Data  json.RawMessage `json:"data"`
		Links []mapLink       `json:"links"`
	}
	if json.Unmarshal(raw, &wrapped) == nil {
		if len(wrapped.Links) > 0 {
			return wrapped.Links
		}
		if len(wrapped.Data) > 0 {
			return decodeCrawlLinks(wrapped.Data)
		}
	}

	return nil
}

func webURLSlug(rawURL string) string {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return "page"
	}

	tokens := make([]string, 0, 8)
	path := strings.Trim(parsed.Path, "/")
	if path == "" {
		tokens = append(tokens, "index")
	} else {
		segments := strings.Split(path, "/")
		for _, segment := range segments {
			token := normalizeSlugToken(segment)
			if token == "" {
				continue
			}
			tokens = append(tokens, token)
			if len(tokens) >= 4 {
				break
			}
		}
	}

	query := parsed.Query()
	if len(query) > 0 {
		keys := make([]string, 0, len(query))
		for key := range query {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			keyToken := normalizeSlugToken(key)
			if keyToken == "" {
				continue
			}
			tokens = append(tokens, keyToken)
			if values := query[key]; len(values) > 0 {
				if valueToken := normalizeSlugValue(values[0]); valueToken != "" {
					tokens = append(tokens, valueToken)
				}
			}
			if len(tokens) >= 8 {
				break
			}
		}
	}

	if len(tokens) == 0 {
		return "page"
	}
	slug := strings.Join(tokens, "_")
	if len(slug) > 80 {
		slug = strings.Trim(slug[:80], "_")
	}
	if slug == "" {
		return "page"
	}
	return slug
}

func normalizeSlugToken(raw string) string {
	unescaped, err := url.QueryUnescape(strings.TrimSpace(raw))
	if err != nil {
		unescaped = strings.TrimSpace(raw)
	}

	// Drop common static extensions from path segments.
	if i := strings.LastIndex(unescaped, "."); i > 0 {
		unescaped = unescaped[:i]
	}

	var b strings.Builder
	prevUnderscore := false
	for _, r := range strings.ToLower(unescaped) {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			b.WriteRune(r)
			prevUnderscore = false
			continue
		}
		if !prevUnderscore && b.Len() > 0 {
			b.WriteRune('_')
			prevUnderscore = true
		}
	}

	token := strings.Trim(b.String(), "_")
	if token == "" || token == "www" {
		return ""
	}
	return token
}

func normalizeSlugValue(raw string) string {
	token := normalizeSlugToken(raw)
	if token == "" {
		return ""
	}
	if len(token) > 24 {
		return ""
	}
	if isLikelySensitiveToken(token) {
		return ""
	}
	return token
}

func isLikelySensitiveToken(token string) bool {
	if token == "" {
		return false
	}
	allowList := map[string]struct{}{
		"yes": {}, "no": {}, "true": {}, "false": {}, "all": {}, "none": {},
		"open": {}, "closed": {}, "active": {}, "inactive": {},
	}
	if _, ok := allowList[token]; ok {
		return false
	}

	allDigits := true
	for _, r := range token {
		if !unicode.IsDigit(r) {
			allDigits = false
			break
		}
	}
	if allDigits && len(token) >= 5 {
		return true
	}

	containsDigit := false
	for _, r := range token {
		if unicode.IsDigit(r) {
			containsDigit = true
			break
		}
	}
	if containsDigit && len(token) >= 16 {
		return true
	}
	return false
}

func isGenericWebTitle(title string) bool {
	token := normalizeSlugToken(title)
	if token == "" {
		return true
	}
	switch token {
	case "home", "homepage", "dashboard", "index", "landing", "overview", "account", "page":
		return true
	}
	return false
}

func shortHash(s string) string {
	h := sha256.Sum256([]byte(s))
	return hex.EncodeToString(h[:4])
}

func normalizeWebMode(mode string) string {
	switch strings.ToLower(strings.TrimSpace(mode)) {
	case "website", "crawl":
		return "crawl"
	case "single_page", "scrape":
		return "scrape"
	case "web_search", "search":
		return "search"
	case "map":
		return "map"
	default:
		return "map"
	}
}

func mergeMapLinks(base, extra []mapLink) []mapLink {
	seen := make(map[string]struct{}, len(base)+len(extra))
	out := make([]mapLink, 0, len(base)+len(extra))
	for _, link := range base {
		u := strings.TrimSpace(link.URL)
		if u == "" {
			continue
		}
		if _, ok := seen[u]; ok {
			continue
		}
		seen[u] = struct{}{}
		out = append(out, link)
	}
	for _, link := range extra {
		u := strings.TrimSpace(link.URL)
		if u == "" {
			continue
		}
		if _, ok := seen[u]; ok {
			continue
		}
		seen[u] = struct{}{}
		out = append(out, link)
	}
	return out
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
