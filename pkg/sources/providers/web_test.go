package providers

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/beam-cloud/airstore/pkg/sources"
	"github.com/beam-cloud/airstore/pkg/types"
)

func TestFilterLinksToSite_RestrictsToSameHost(t *testing.T) {
	site, err := url.Parse("https://docs.airstore.ai/")
	if err != nil {
		t.Fatal(err)
	}

	links := []mapLink{
		{URL: "https://docs.airstore.ai/getting-started"},
		{URL: "https://docs.airstore.ai/reference/api"},
		{URL: "https://airstore.ai/"},
		{URL: "https://example.com/"},
	}

	got := filterLinksToSite(site, links, false)
	if len(got) != 2 {
		t.Fatalf("expected 2 same-host links, got %d", len(got))
	}
	for _, l := range got {
		u, parseErr := url.Parse(l.URL)
		if parseErr != nil {
			t.Fatalf("invalid URL in output: %q", l.URL)
		}
		if u.Hostname() != "docs.airstore.ai" {
			t.Fatalf("unexpected host %q in output", u.Hostname())
		}
	}
}

func TestFilterLinksToSite_AllowsWwwVariant(t *testing.T) {
	site, err := url.Parse("https://airstore.ai/")
	if err != nil {
		t.Fatal(err)
	}

	links := []mapLink{
		{URL: "https://airstore.ai/docs"},
		{URL: "https://www.airstore.ai/blog"},
		{URL: "https://docs.airstore.ai/"},
	}

	got := filterLinksToSite(site, links, false)
	if len(got) != 2 {
		t.Fatalf("expected 2 links on host/www variant, got %d", len(got))
	}
}

func TestFilterLinksToSite_AuthModeAllowsRelatedSubdomains(t *testing.T) {
	site, err := url.Parse("https://chase.com/")
	if err != nil {
		t.Fatal(err)
	}

	links := []mapLink{
		{URL: "https://secure.chase.com/account"},
		{URL: "https://www.chase.com/personal"},
		{URL: "https://example.com/"},
	}

	got := filterLinksToSite(site, links, true)
	if len(got) != 2 {
		t.Fatalf("expected 2 related-domain links in auth mode, got %d", len(got))
	}
}

func TestAuthHeadersForRequest_MergesHeadersAndCookies(t *testing.T) {
	w := NewWebProvider("test-key")
	pctx := &sources.ProviderContext{
		Credentials: &types.IntegrationCredentials{
			Extra: map[string]string{
				webAuthSnapshotExtraKey: `{
					"headers": {"x-auth-token": "abc123"},
					"cookies": [
						{"name": "session", "value": "token", "domain": "example.com", "path": "/"},
						{"name": "ignored", "value": "value", "domain": "other.com"}
					]
				}`,
			},
		},
	}

	headers := w.authHeadersForRequest(pctx, "https://example.com/private")
	if headers["X-Auth-Token"] != "abc123" {
		t.Fatalf("expected X-Auth-Token header, got %#v", headers)
	}
	if !strings.Contains(headers["Cookie"], "session=token") {
		t.Fatalf("expected cookie header to include session=token, got %q", headers["Cookie"])
	}
	if strings.Contains(headers["Cookie"], "ignored=value") {
		t.Fatalf("unexpected cross-domain cookie in header: %q", headers["Cookie"])
	}
}

func TestAuthHeadersForRequest_DropsTransportHeadersAndSetsAuthReferer(t *testing.T) {
	w := NewWebProvider("test-key")
	pctx := &sources.ProviderContext{
		Credentials: &types.IntegrationCredentials{
			Extra: map[string]string{
				webAuthSnapshotExtraKey: `{
					"headers": {
						"host": "bad.example",
						"connection": "keep-alive",
						"x-csrf-token": "csrf"
					},
					"cookies": [{"name":"session","value":"abc","domain":"secure.chase.com"}]
				}`,
			},
		},
	}

	headers := w.authHeadersForRequest(
		pctx,
		"https://secure.chase.com/web/auth/transactionDetails,633924395,CHK,1",
	)
	if headers["Host"] != "" {
		t.Fatalf("expected Host header to be dropped, got %q", headers["Host"])
	}
	if headers["Connection"] != "" {
		t.Fatalf("expected Connection header to be dropped, got %q", headers["Connection"])
	}
	if headers["Origin"] != "https://secure.chase.com" {
		t.Fatalf("unexpected Origin header: %q", headers["Origin"])
	}
	if headers["Referer"] != "https://secure.chase.com/web/auth/dashboard" {
		t.Fatalf("unexpected Referer header: %q", headers["Referer"])
	}
	if headers["X-Csrf-Token"] != "csrf" {
		t.Fatalf("expected csrf header to be preserved, got %#v", headers)
	}
	if !strings.Contains(headers["Cookie"], "session=abc") {
		t.Fatalf("expected cookie header to include session=abc, got %q", headers["Cookie"])
	}
}

func TestExecuteQuery_UsesMapWhenAuthPresentInMapMode(t *testing.T) {
	w := NewWebProvider("test-key")
	mapCalled := false
	crawlCalled := false

	w.httpClient = &http.Client{
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			switch req.URL.Path {
			case "/v2/map":
				mapCalled = true
				return jsonResponse(http.StatusOK, `{
					"success": true,
					"links": [{"url": "https://example.com/page", "title": "Page"}]
				}`), nil
			case "/v2/crawl":
				crawlCalled = true
				return jsonResponse(http.StatusOK, `{"success": true, "id": "crawl-123"}`), nil
			case "/v2/scrape":
				return jsonResponse(http.StatusOK, `{
					"success": true,
					"data": {"links": []}
				}`), nil
			default:
				return jsonResponse(http.StatusNotFound, `{"error":"not found"}`), nil
			}
		}),
	}

	pctx := &sources.ProviderContext{
		Credentials: &types.IntegrationCredentials{
			Extra: map[string]string{
				webAuthSnapshotExtraKey: `{"cookies":[{"name":"session","value":"abc","domain":"example.com"}]}`,
			},
		},
	}

	resp, err := w.ExecuteQuery(context.Background(), pctx, sources.QuerySpec{
		Query: "https://example.com",
		Limit: 10,
		Metadata: map[string]string{
			"web_mode": "map",
		},
	})
	if err != nil {
		t.Fatalf("ExecuteQuery returned error: %v", err)
	}
	if resp == nil || len(resp.Results) != 1 {
		t.Fatalf("expected one query result, got %#v", resp)
	}
	if !mapCalled {
		t.Fatalf("expected /map path for authenticated map mode")
	}
	if crawlCalled {
		t.Fatalf("did not expect /crawl to run when /map succeeds in map mode")
	}
	if resp.Results[0].ID != "https://example.com/page" {
		t.Fatalf("unexpected result ID: %q", resp.Results[0].ID)
	}
}

func TestExecuteQuery_UsesMapWhenNoAuthPresent(t *testing.T) {
	w := NewWebProvider("test-key")
	mapCalled := false
	crawlCalled := false

	w.httpClient = &http.Client{
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			switch req.URL.Path {
			case "/v2/map":
				mapCalled = true
				return jsonResponse(http.StatusOK, `{
					"success": true,
					"links": [
						{"url": "https://example.com/keep", "title": "Keep"},
						{"url": "https://outside.com/drop", "title": "Drop"}
					]
				}`), nil
			case "/v2/crawl":
				crawlCalled = true
				return jsonResponse(http.StatusOK, `{"success": true, "id": "crawl-123"}`), nil
			default:
				return jsonResponse(http.StatusNotFound, `{"error":"not found"}`), nil
			}
		}),
	}

	resp, err := w.ExecuteQuery(context.Background(), &sources.ProviderContext{}, sources.QuerySpec{
		Query: "https://example.com",
		Limit: 10,
		Metadata: map[string]string{
			"web_mode": "map",
		},
	})
	if err != nil {
		t.Fatalf("ExecuteQuery returned error: %v", err)
	}
	if !mapCalled {
		t.Fatalf("expected /map to be called for unauthenticated map mode")
	}
	if crawlCalled {
		t.Fatalf("did not expect /crawl for unauthenticated map mode")
	}
	if resp == nil || len(resp.Results) != 1 {
		t.Fatalf("expected one same-site result after filtering, got %#v", resp)
	}
}

func TestExecuteQuery_UsesCrawlWhenCrawlModeRequested(t *testing.T) {
	w := NewWebProvider("test-key")
	mapCalled := false
	crawlCalled := false
	pollCalled := false

	w.httpClient = &http.Client{
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			switch req.URL.Path {
			case "/v2/map":
				mapCalled = true
				return jsonResponse(http.StatusOK, `{"success": true, "links": []}`), nil
			case "/v2/crawl":
				crawlCalled = true
				return jsonResponse(http.StatusOK, `{"success": true, "id": "crawl-222"}`), nil
			case "/v2/crawl/crawl-222":
				pollCalled = true
				return jsonResponse(http.StatusOK, `{
					"success": true,
					"status": "completed",
					"data": [
						{"url": "https://example.com/keep"},
						{"url": "https://outside.com/drop"}
					]
				}`), nil
			default:
				return jsonResponse(http.StatusNotFound, `{"error":"not found"}`), nil
			}
		}),
	}

	resp, err := w.ExecuteQuery(context.Background(), &sources.ProviderContext{}, sources.QuerySpec{
		Query: "https://example.com",
		Limit: 10,
		Metadata: map[string]string{
			"web_mode": "crawl",
		},
	})
	if err != nil {
		t.Fatalf("ExecuteQuery returned error: %v", err)
	}
	if mapCalled {
		t.Fatalf("did not expect /map for crawl mode")
	}
	if !crawlCalled || !pollCalled {
		t.Fatalf("expected /crawl start+poll calls (start=%v poll=%v)", crawlCalled, pollCalled)
	}
	if resp == nil || len(resp.Results) != 1 {
		t.Fatalf("expected one same-site result after filtering, got %#v", resp)
	}
	if resp.Results[0].ID != "https://example.com/keep" {
		t.Fatalf("unexpected result ID: %q", resp.Results[0].ID)
	}
}

func TestExecuteQuery_ScrapeModeReturnsSingleURLWithoutDiscovery(t *testing.T) {
	w := NewWebProvider("test-key")
	httpCalled := false

	w.httpClient = &http.Client{
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			httpCalled = true
			return jsonResponse(http.StatusNotFound, `{"error":"unexpected call"}`), nil
		}),
	}

	resp, err := w.ExecuteQuery(context.Background(), &sources.ProviderContext{}, sources.QuerySpec{
		Query: "https://example.com/landing",
		Limit: 10,
		Metadata: map[string]string{
			"web_mode": "scrape",
		},
	})
	if err != nil {
		t.Fatalf("ExecuteQuery returned error: %v", err)
	}
	if httpCalled {
		t.Fatalf("did not expect Firecrawl discovery calls in scrape mode")
	}
	if resp == nil || len(resp.Results) != 1 {
		t.Fatalf("expected one scrape-mode result, got %#v", resp)
	}
	if resp.Results[0].ID != "https://example.com/landing" {
		t.Fatalf("unexpected scrape-mode result ID: %q", resp.Results[0].ID)
	}
}

func TestExecuteQuery_IntentWebsiteAliasUsesCrawl(t *testing.T) {
	w := NewWebProvider("test-key")
	crawlCalled := false
	searchCalled := false

	w.httpClient = &http.Client{
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			switch req.URL.Path {
			case "/v2/crawl":
				crawlCalled = true
				return jsonResponse(http.StatusOK, `{"success": true, "id": "crawl-intent"}`), nil
			case "/v2/crawl/crawl-intent":
				return jsonResponse(http.StatusOK, `{
					"success": true,
					"status": "completed",
					"data": [{"url": "https://example.com/transactions"}]
				}`), nil
			case "/v2/search":
				searchCalled = true
				return jsonResponse(http.StatusOK, `{"success": true, "data": {"web": []}}`), nil
			default:
				return jsonResponse(http.StatusNotFound, `{"error":"not found"}`), nil
			}
		}),
	}

	resp, err := w.ExecuteQuery(context.Background(), &sources.ProviderContext{}, sources.QuerySpec{
		Query: "https://example.com",
		Limit: 10,
		Metadata: map[string]string{
			"web_mode": "website",
		},
	})
	if err != nil {
		t.Fatalf("ExecuteQuery returned error: %v", err)
	}
	if !crawlCalled {
		t.Fatalf("expected website intent to execute crawl discovery")
	}
	if searchCalled {
		t.Fatalf("did not expect /search for website intent")
	}
	if resp == nil || len(resp.Results) != 1 {
		t.Fatalf("expected one result, got %#v", resp)
	}
}

func TestExecuteQuery_IntentWebSearchAliasUsesSearch(t *testing.T) {
	w := NewWebProvider("test-key")
	searchCalled := false

	w.httpClient = &http.Client{
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			switch req.URL.Path {
			case "/v2/search":
				searchCalled = true
				return jsonResponse(http.StatusOK, `{
					"success": true,
					"data": {
						"web": [{"url": "https://example.com/rewards", "title": "Rewards"}]
					}
				}`), nil
			default:
				return jsonResponse(http.StatusNotFound, `{"error":"not found"}`), nil
			}
		}),
	}

	resp, err := w.ExecuteQuery(context.Background(), &sources.ProviderContext{}, sources.QuerySpec{
		Query: "latest chase rewards",
		Limit: 10,
		Metadata: map[string]string{
			"web_mode": "web_search",
		},
	})
	if err != nil {
		t.Fatalf("ExecuteQuery returned error: %v", err)
	}
	if !searchCalled {
		t.Fatalf("expected web_search intent to call /search")
	}
	if resp == nil || len(resp.Results) != 1 {
		t.Fatalf("expected one search result, got %#v", resp)
	}
}

func TestExecuteQuery_IntentSinglePageAliasUsesScrapeMode(t *testing.T) {
	w := NewWebProvider("test-key")
	httpCalled := false

	w.httpClient = &http.Client{
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			httpCalled = true
			return jsonResponse(http.StatusNotFound, `{"error":"unexpected call"}`), nil
		}),
	}

	resp, err := w.ExecuteQuery(context.Background(), &sources.ProviderContext{}, sources.QuerySpec{
		Query: "https://example.com/account",
		Limit: 10,
		Metadata: map[string]string{
			"web_mode": "single_page",
		},
	})
	if err != nil {
		t.Fatalf("ExecuteQuery returned error: %v", err)
	}
	if httpCalled {
		t.Fatalf("single_page should not run discovery endpoints")
	}
	if resp == nil || len(resp.Results) != 1 {
		t.Fatalf("expected one single-page result, got %#v", resp)
	}
}

func TestExecuteQuery_AuthCrawlSparseDiscoveryExpandsWithScrapeLinks(t *testing.T) {
	w := NewWebProvider("test-key")
	scrapeCalled := false

	w.httpClient = &http.Client{
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			switch req.URL.Path {
			case "/v2/crawl":
				return jsonResponse(http.StatusOK, `{"success": true, "id": "crawl-sparse"}`), nil
			case "/v2/crawl/crawl-sparse":
				return jsonResponse(http.StatusOK, `{
					"success": true,
					"status": "completed",
					"data": [
						{"url": "https://example.com/dashboard"}
					]
				}`), nil
			case "/v2/scrape":
				scrapeCalled = true
				return jsonResponse(http.StatusOK, `{
					"success": true,
					"data": {
						"links": [
							"https://example.com/transactions?account=no",
							"https://example.com/profile",
							"https://outside.com/drop"
						]
					}
				}`), nil
			default:
				return jsonResponse(http.StatusNotFound, `{"error":"not found"}`), nil
			}
		}),
	}

	pctx := &sources.ProviderContext{
		Credentials: &types.IntegrationCredentials{
			Extra: map[string]string{
				webAuthSnapshotExtraKey: `{"cookies":[{"name":"session","value":"abc","domain":"example.com"}]}`,
			},
		},
	}

	resp, err := w.ExecuteQuery(context.Background(), pctx, sources.QuerySpec{
		Query: "https://example.com/dashboard",
		Limit: 10,
		Metadata: map[string]string{
			"web_mode": "crawl",
		},
	})
	if err != nil {
		t.Fatalf("ExecuteQuery returned error: %v", err)
	}
	if !scrapeCalled {
		t.Fatalf("expected sparse authenticated crawl to trigger scrape link expansion")
	}
	if resp == nil || len(resp.Results) != 2 {
		t.Fatalf("expected 2 prioritized same-site results after scrape expansion, got %#v", resp)
	}
	ids := make(map[string]bool, len(resp.Results))
	for _, r := range resp.Results {
		ids[r.ID] = true
	}
	if !ids["https://example.com/dashboard"] {
		t.Fatalf("expected base dashboard link to remain")
	}
	if !ids["https://example.com/transactions?account=no"] {
		t.Fatalf("expected transactions link from scrape expansion")
	}
	if ids["https://outside.com/drop"] {
		t.Fatalf("did not expect off-site link to survive filtering")
	}
	if ids["https://example.com/profile"] {
		t.Fatalf("did not expect low-signal profile link after prioritization")
	}
}

func TestExecuteQuery_URLBasedModesPreferURLSlugForTitle(t *testing.T) {
	w := NewWebProvider("test-key")

	w.httpClient = &http.Client{
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			switch req.URL.Path {
			case "/v2/map":
				return jsonResponse(http.StatusOK, `{
					"success": true,
					"links": [
						{"url": "https://example.com/transactions?account=no", "title": "Dashboard"}
					]
				}`), nil
			default:
				return jsonResponse(http.StatusNotFound, `{"error":"not found"}`), nil
			}
		}),
	}

	resp, err := w.ExecuteQuery(context.Background(), &sources.ProviderContext{}, sources.QuerySpec{
		Query: "https://example.com",
		Limit: 10,
		Metadata: map[string]string{
			"web_mode": "map",
		},
	})
	if err != nil {
		t.Fatalf("ExecuteQuery returned error: %v", err)
	}
	if resp == nil || len(resp.Results) != 1 {
		t.Fatalf("expected one result, got %#v", resp)
	}
	title := resp.Results[0].Metadata["title"]
	if title != "transactions_account_no" {
		t.Fatalf("expected URL-derived title, got %q", title)
	}
}

func TestExecuteQuery_URLSlugSkipsSensitiveLongNumericValues(t *testing.T) {
	w := NewWebProvider("test-key")

	w.httpClient = &http.Client{
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			switch req.URL.Path {
			case "/v2/map":
				return jsonResponse(http.StatusOK, `{
					"success": true,
					"links": [
						{"url": "https://example.com/transactions?account=1234567890123456"}
					]
				}`), nil
			default:
				return jsonResponse(http.StatusNotFound, `{"error":"not found"}`), nil
			}
		}),
	}

	resp, err := w.ExecuteQuery(context.Background(), &sources.ProviderContext{}, sources.QuerySpec{
		Query: "https://example.com",
		Limit: 10,
		Metadata: map[string]string{
			"web_mode": "map",
		},
	})
	if err != nil {
		t.Fatalf("ExecuteQuery returned error: %v", err)
	}
	if resp == nil || len(resp.Results) != 1 {
		t.Fatalf("expected one result, got %#v", resp)
	}
	title := resp.Results[0].Metadata["title"]
	if strings.Contains(title, "1234567890123456") {
		t.Fatalf("expected sensitive numeric value to be omitted from slug title, got %q", title)
	}
	if !strings.Contains(title, "account") {
		t.Fatalf("expected query key to remain for context, got %q", title)
	}
}

func TestReadResult_AuthenticatedScrapePayloadIncludesHeaders(t *testing.T) {
	w := NewWebProvider("test-key")
	var scrapePayload struct {
		URL          string            `json:"url"`
		Headers      map[string]string `json:"headers"`
		StoreInCache bool              `json:"storeInCache"`
	}

	w.httpClient = &http.Client{
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			if req.URL.Path != "/v2/scrape" {
				return jsonResponse(http.StatusNotFound, `{"error":"not found"}`), nil
			}
			if err := json.NewDecoder(req.Body).Decode(&scrapePayload); err != nil {
				return nil, err
			}
			return jsonResponse(http.StatusOK, `{
				"success": true,
				"data": {
					"markdown": "content",
					"metadata": {"title": "Secure Page", "description": "desc"}
				}
			}`), nil
		}),
	}

	pctx := &sources.ProviderContext{
		Credentials: &types.IntegrationCredentials{
			Extra: map[string]string{
				webAuthSnapshotExtraKey: `{
					"headers": {"X-CSRF-Token": "csrf"},
					"cookies": [{"name":"session","value":"abc","domain":"example.com"}]
				}`,
			},
		},
	}

	data, err := w.ReadResult(context.Background(), pctx, "https://example.com/private")
	if err != nil {
		t.Fatalf("ReadResult returned error: %v", err)
	}
	if !strings.Contains(string(data), "Secure Page") {
		t.Fatalf("expected rendered markdown to include title, got %q", string(data))
	}
	if scrapePayload.URL != "https://example.com/private" {
		t.Fatalf("unexpected scrape URL: %q", scrapePayload.URL)
	}
	if scrapePayload.Headers["X-Csrf-Token"] != "csrf" {
		t.Fatalf("expected X-Csrf-Token header in scrape payload, got %#v", scrapePayload.Headers)
	}
	if !strings.Contains(scrapePayload.Headers["Cookie"], "session=abc") {
		t.Fatalf("expected cookie header in scrape payload, got %#v", scrapePayload.Headers)
	}
	if scrapePayload.StoreInCache {
		t.Fatalf("expected storeInCache=false for authenticated scrape payload")
	}
}

func TestReadResult_AuthenticatedScrapeRetriesOutageInterstitial(t *testing.T) {
	w := NewWebProvider("test-key")
	callCount := 0
	onlyMainByAttempt := make([]bool, 0, 2)
	refererByAttempt := make([]string, 0, 2)
	waitByAttempt := make([]int, 0, 2)

	w.httpClient = &http.Client{
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			if req.URL.Path != "/v2/scrape" {
				return jsonResponse(http.StatusNotFound, `{"error":"not found"}`), nil
			}

			var scrapePayload struct {
				OnlyMainContent bool `json:"onlyMainContent"`
				Headers         map[string]string
				Actions         []struct {
					Type         string `json:"type"`
					Milliseconds int    `json:"milliseconds"`
				} `json:"actions"`
			}
			if err := json.NewDecoder(req.Body).Decode(&scrapePayload); err != nil {
				return nil, err
			}

			callCount++
			onlyMainByAttempt = append(onlyMainByAttempt, scrapePayload.OnlyMainContent)
			refererByAttempt = append(refererByAttempt, scrapePayload.Headers["Referer"])
			if len(scrapePayload.Actions) > 0 {
				waitByAttempt = append(waitByAttempt, scrapePayload.Actions[0].Milliseconds)
			} else {
				waitByAttempt = append(waitByAttempt, 0)
			}

			if callCount == 1 {
				return jsonResponse(http.StatusOK, `{
					"success": true,
					"data": {
						"markdown": "The site or service you are trying to utilize is not currently working. We'll be back shortly.",
						"metadata": {"title": "Chase Outage | Chase.com"}
					}
				}`), nil
			}

			return jsonResponse(http.StatusOK, `{
				"success": true,
				"data": {
					"markdown": "Account balance and transaction activity",
					"metadata": {"title": "Account Activity"}
				}
			}`), nil
		}),
	}

	pctx := &sources.ProviderContext{
		Credentials: &types.IntegrationCredentials{
			Extra: map[string]string{
				webAuthSnapshotExtraKey: `{
					"headers": {"X-CSRF-Token": "csrf"},
					"cookies": [{"name":"session","value":"abc","domain":"secure.chase.com"}]
				}`,
			},
		},
	}

	data, err := w.ReadResult(
		context.Background(),
		pctx,
		"https://secure.chase.com/web/auth/transactionDetails,633924395,CHK,1",
	)
	if err != nil {
		t.Fatalf("ReadResult returned error: %v", err)
	}
	if callCount != 2 {
		t.Fatalf("expected outage interstitial retry to run twice, got %d calls", callCount)
	}
	if len(onlyMainByAttempt) != 2 || !onlyMainByAttempt[0] || onlyMainByAttempt[1] {
		t.Fatalf("expected retry to relax onlyMainContent (got %#v)", onlyMainByAttempt)
	}
	if len(waitByAttempt) != 2 || waitByAttempt[0] <= 0 || waitByAttempt[1] <= 0 {
		t.Fatalf("expected wait actions on authenticated attempts, got %#v", waitByAttempt)
	}
	if len(refererByAttempt) != 2 ||
		refererByAttempt[0] != "https://secure.chase.com/web/auth/dashboard" ||
		refererByAttempt[1] != "https://secure.chase.com/web/auth/dashboard" {
		t.Fatalf("expected dashboard referer for auth transaction scrape, got %#v", refererByAttempt)
	}
	if !strings.Contains(string(data), "Account Activity") {
		t.Fatalf("expected successful retry content, got %q", string(data))
	}
}

func TestExecuteQuery_UsesMetadataSourceURLFromCrawlResults(t *testing.T) {
	w := NewWebProvider("test-key")

	w.httpClient = &http.Client{
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			switch req.URL.Path {
			case "/v2/crawl":
				return jsonResponse(http.StatusOK, `{"success": true, "id": "crawl-456"}`), nil
			case "/v2/crawl/crawl-456":
				return jsonResponse(http.StatusOK, `{
					"success": true,
					"status": "completed",
					"data": [
						{"metadata": {"sourceURL": "https://example.com/private", "title": "Private Page"}}
					]
				}`), nil
			default:
				return jsonResponse(http.StatusNotFound, `{"error":"not found"}`), nil
			}
		}),
	}

	pctx := &sources.ProviderContext{
		Credentials: &types.IntegrationCredentials{
			Extra: map[string]string{
				webAuthSnapshotExtraKey: `{"cookies":[{"name":"session","value":"abc","domain":"example.com"}]}`,
			},
		},
	}

	resp, err := w.ExecuteQuery(context.Background(), pctx, sources.QuerySpec{
		Query: "https://example.com",
		Limit: 10,
		Metadata: map[string]string{
			"web_mode": "map",
		},
	})
	if err != nil {
		t.Fatalf("ExecuteQuery returned error: %v", err)
	}
	if resp == nil || len(resp.Results) != 1 {
		t.Fatalf("expected one query result, got %#v", resp)
	}
	if resp.Results[0].ID != "https://example.com/private" {
		t.Fatalf("unexpected result ID: %q", resp.Results[0].ID)
	}
}

func TestExecuteQuery_AuthMapNoLinksFallsBackToSeedURL(t *testing.T) {
	w := NewWebProvider("test-key")

	w.httpClient = &http.Client{
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			switch req.URL.Path {
			case "/v2/map":
				return jsonResponse(http.StatusOK, `{
					"success": true,
					"links": []
				}`), nil
			case "/v2/scrape":
				return jsonResponse(http.StatusOK, `{
					"success": true,
					"data": {"links": []}
				}`), nil
			default:
				return jsonResponse(http.StatusNotFound, `{"error":"not found"}`), nil
			}
		}),
	}

	pctx := &sources.ProviderContext{
		Credentials: &types.IntegrationCredentials{
			Extra: map[string]string{
				webAuthSnapshotExtraKey: `{"cookies":[{"name":"session","value":"abc","domain":"example.com"}]}`,
			},
		},
	}

	resp, err := w.ExecuteQuery(context.Background(), pctx, sources.QuerySpec{
		Query: "https://example.com/account",
		Limit: 10,
		Metadata: map[string]string{
			"web_mode": "map",
		},
	})
	if err != nil {
		t.Fatalf("ExecuteQuery returned error: %v", err)
	}
	if resp == nil || len(resp.Results) != 1 {
		t.Fatalf("expected one fallback query result, got %#v", resp)
	}
	if resp.Results[0].ID != "https://example.com/account" {
		t.Fatalf("unexpected fallback result ID: %q", resp.Results[0].ID)
	}
}

func TestExecuteQuery_AuthMapUsesMapDiscovery(t *testing.T) {
	w := NewWebProvider("test-key")

	w.httpClient = &http.Client{
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			switch req.URL.Path {
			case "/v2/map":
				return jsonResponse(http.StatusOK, `{
					"success": true,
					"links": [
						{"url": "https://example.com/account"},
						{"url": "https://www.example.com/security"}
					]
				}`), nil
			default:
				return jsonResponse(http.StatusNotFound, `{"error":"not found"}`), nil
			}
		}),
	}

	pctx := &sources.ProviderContext{
		Credentials: &types.IntegrationCredentials{
			Extra: map[string]string{
				webAuthSnapshotExtraKey: `{"cookies":[{"name":"session","value":"abc","domain":"example.com"}]}`,
			},
		},
	}

	resp, err := w.ExecuteQuery(context.Background(), pctx, sources.QuerySpec{
		Query: "https://example.com",
		Limit: 10,
		Metadata: map[string]string{
			"web_mode": "map",
		},
	})
	if err != nil {
		t.Fatalf("ExecuteQuery returned error: %v", err)
	}
	if resp == nil || len(resp.Results) != 2 {
		t.Fatalf("expected map discovery to return 2 results, got %#v", resp)
	}
}

func TestExecuteQuery_AuthMapKeepsRelatedSubdomainLinks(t *testing.T) {
	w := NewWebProvider("test-key")

	w.httpClient = &http.Client{
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			switch req.URL.Path {
			case "/v2/map":
				return jsonResponse(http.StatusOK, `{
					"success": true,
					"links": [
						{"url": "https://secure.example.com/private", "title": "Secure"}
					]
				}`), nil
			case "/v2/scrape":
				return jsonResponse(http.StatusOK, `{
					"success": true,
					"data": {"links": []}
				}`), nil
			default:
				return jsonResponse(http.StatusNotFound, `{"error":"not found"}`), nil
			}
		}),
	}

	pctx := &sources.ProviderContext{
		Credentials: &types.IntegrationCredentials{
			Extra: map[string]string{
				webAuthSnapshotExtraKey: `{"cookies":[{"name":"session","value":"abc","domain":"example.com"}]}`,
			},
		},
	}

	resp, err := w.ExecuteQuery(context.Background(), pctx, sources.QuerySpec{
		Query: "https://example.com/account",
		Limit: 10,
		Metadata: map[string]string{
			"web_mode": "map",
		},
	})
	if err != nil {
		t.Fatalf("ExecuteQuery returned error: %v", err)
	}
	if resp == nil || len(resp.Results) != 1 {
		t.Fatalf("expected one related-domain query result, got %#v", resp)
	}
	if resp.Results[0].ID != "https://secure.example.com/private" {
		t.Fatalf("unexpected related-domain result ID: %q", resp.Results[0].ID)
	}
}

func TestIsChromiumBlockPage(t *testing.T) {
	blocked := []string{
		"www.chase.com is blocked\nThis page has been blocked by Chromium\nERR_BLOCKED_BY_CLIENT",
		"this page has been blocked by chromium err_blocked_by_response",
		"err_blocked_by_administrator",
		"this site can\u2019t be reached err_connection_refused",
	}
	for _, s := range blocked {
		if !isChromiumBlockPage(strings.ToLower(s)) {
			t.Errorf("expected blocked for %q", s)
		}
	}
	ok := []string{
		"Account balance: $1234.56",
		"Chase Transactions overview",
		"",
	}
	for _, s := range ok {
		if isChromiumBlockPage(strings.ToLower(s)) {
			t.Errorf("false positive for %q", s)
		}
	}
}

func TestReadResult_ChromiumBlockPageReturnsError(t *testing.T) {
	w := NewWebProvider("test-key")
	w.httpClient = &http.Client{
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			return jsonResponse(http.StatusOK, `{
				"success": true,
				"data": {
					"markdown": "www.chase.com is blocked\n\nThis page has been blocked by Chromium\n\nERR_BLOCKED_BY_CLIENT\n\nReload",
					"metadata": {"title": "www.chase.com is blocked"}
				}
			}`), nil
		}),
	}

	_, err := w.ReadResult(context.Background(), &sources.ProviderContext{}, "https://www.chase.com/personal/checking")
	if err == nil {
		t.Fatal("expected error for Chromium block page, got nil")
	}
	if !strings.Contains(err.Error(), "blocked by browser") {
		t.Fatalf("unexpected error: %v", err)
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func jsonResponse(statusCode int, body string) *http.Response {
	return &http.Response{
		StatusCode: statusCode,
		Header:     make(http.Header),
		Body:       io.NopCloser(strings.NewReader(body)),
	}
}
