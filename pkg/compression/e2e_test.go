// Package compression — end-to-end tests against a live local gateway.
//
// These exercise the full read path: HTTP API -> gateway -> source -> compression -> response.
// Run with:
//
//	AIRSTORE_E2E=1 go test -v -count=1 -timeout 120s ./pkg/compression/ -run TestE2E
//
// Required env vars:
//
//	AIRSTORE_E2E=1            (gate — skips if unset)
//	AIRSTORE_TOKEN            (workspace member token)
//	AIRSTORE_WORKSPACE_ID     (workspace UUID)
//	AIRSTORE_BASE_URL         (optional, default http://localhost:1994)
//	AIRSTORE_QUERY_PATH       (optional, default /sources/gmail/unread-emails)
package compression

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strings"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// Config — everything from env vars, nothing hardcoded.
// ---------------------------------------------------------------------------

func e2eBaseURL() string {
	if v := os.Getenv("AIRSTORE_BASE_URL"); v != "" {
		return v
	}
	return "http://localhost:1994"
}

func e2eToken(t *testing.T) string {
	t.Helper()
	v := os.Getenv("AIRSTORE_TOKEN")
	if v == "" {
		t.Fatal("AIRSTORE_TOKEN must be set for e2e tests")
	}
	return v
}

func e2eWorkspaceID(t *testing.T) string {
	t.Helper()
	v := os.Getenv("AIRSTORE_WORKSPACE_ID")
	if v == "" {
		t.Fatal("AIRSTORE_WORKSPACE_ID must be set for e2e tests")
	}
	return v
}

func e2eQueryPath() string {
	if v := os.Getenv("AIRSTORE_QUERY_PATH"); v != "" {
		return v
	}
	return "/sources/gmail/unread-emails"
}

func skipUnlessE2E(t *testing.T) {
	t.Helper()
	if os.Getenv("AIRSTORE_E2E") == "" {
		t.Skip("Set AIRSTORE_E2E=1 to run end-to-end tests (requires local gateway + env vars)")
	}
}

// ---------------------------------------------------------------------------
// HTTP helpers
// ---------------------------------------------------------------------------

var httpClient = &http.Client{Timeout: 60 * time.Second}

type listResponse struct {
	Success bool `json:"success"`
	Data    struct {
		Entries []listEntry `json:"entries"`
	} `json:"data"`
}

type listEntry struct {
	Name     string `json:"name"`
	Path     string `json:"path"`
	Size     int    `json:"size"`
	IsFolder bool   `json:"is_folder"`
}

func apiGet(t *testing.T, path string, query map[string]string) *http.Response {
	t.Helper()
	url := e2eBaseURL() + "/api/v1/workspaces/" + e2eWorkspaceID(t) + path
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+e2eToken(t))
	q := req.URL.Query()
	for k, v := range query {
		q.Set(k, v)
	}
	req.URL.RawQuery = q.Encode()
	resp, err := httpClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

func listFiles(t *testing.T) []listEntry {
	t.Helper()
	resp := apiGet(t, "/fs/list", map[string]string{"path": e2eQueryPath()})
	defer resp.Body.Close()
	var lr listResponse
	if err := json.NewDecoder(resp.Body).Decode(&lr); err != nil {
		t.Fatalf("decode list: %v", err)
	}
	if !lr.Success {
		t.Fatal("list was not successful")
	}
	var files []listEntry
	for _, e := range lr.Data.Entries {
		if !e.IsFolder && !strings.HasPrefix(e.Name, ".") {
			files = append(files, e)
		}
	}
	sort.Slice(files, func(i, j int) bool { return files[i].Size > files[j].Size })
	return files
}

type readResult struct {
	Body     string
	Bytes    int
	Tokens   int
	Status   int
	Duration time.Duration
}

func readFile(t *testing.T, path, compression string) readResult {
	t.Helper()
	q := map[string]string{"path": path}
	if compression != "" {
		q["compression"] = compression
	}
	start := time.Now()
	resp := apiGet(t, "/fs/read", q)
	dur := time.Since(start)
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	tokens := EstimateTokens(body)
	if tc := DefaultTokenCounter(); tc != nil {
		tokens = tc.Count(body)
	}
	return readResult{
		Body:     string(body),
		Bytes:    len(body),
		Tokens:   tokens,
		Status:   resp.StatusCode,
		Duration: dur,
	}
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

// TestE2E_ListFiles verifies the gateway serves files from the query path.
func TestE2E_ListFiles(t *testing.T) {
	skipUnlessE2E(t)
	files := listFiles(t)
	if len(files) == 0 {
		t.Fatal("no files in", e2eQueryPath())
	}
	t.Logf("%d files found", len(files))
}

// TestE2E_RawRead verifies uncompressed reads return 200 with content.
func TestE2E_RawRead(t *testing.T) {
	skipUnlessE2E(t)
	files := listFiles(t)
	if len(files) == 0 {
		t.Fatal("no files")
	}

	r := readFile(t, files[0].Path, "")
	if r.Status != 200 {
		t.Fatalf("status %d, body: %.200s", r.Status, r.Body)
	}
	if r.Bytes == 0 {
		t.Fatal("empty response for raw read")
	}
	t.Logf("raw: %d bytes, %d tokens, %dms", r.Bytes, r.Tokens, r.Duration.Milliseconds())
}

// TestE2E_StripReducesTokens reads with strip compression and asserts token reduction.
func TestE2E_StripReducesTokens(t *testing.T) {
	skipUnlessE2E(t)
	files := listFiles(t)
	if len(files) == 0 {
		t.Fatal("no files")
	}

	var totalRaw, totalStrip int

	for _, f := range files {
		raw := readFile(t, f.Path, "")
		if raw.Status != 200 {
			t.Errorf("%s: raw status %d", f.Name, raw.Status)
			continue
		}
		strip := readFile(t, f.Path, "strip")
		if strip.Status != 200 {
			t.Errorf("%s: strip status %d", f.Name, strip.Status)
			continue
		}

		totalRaw += raw.Tokens
		totalStrip += strip.Tokens

		pct := 0.0
		if raw.Tokens > 0 {
			pct = 100.0 * float64(raw.Tokens-strip.Tokens) / float64(raw.Tokens)
		}
		t.Logf("  %-50s  %5d -> %5d tok (%.0f%%)", trunc(f.Name, 50), raw.Tokens, strip.Tokens, pct)

		// Strip should never increase token count
		if strip.Tokens > raw.Tokens {
			t.Errorf("%s: strip (%d tok) > raw (%d tok)", f.Name, strip.Tokens, raw.Tokens)
		}
	}

	if totalRaw > 0 {
		pct := 100.0 * float64(totalRaw-totalStrip) / float64(totalRaw)
		t.Logf("\n  TOTAL: %d -> %d tokens (%.1f%% reduction)", totalRaw, totalStrip, pct)

		// Across a batch of emails, strip should save at least something
		if totalStrip >= totalRaw {
			t.Error("strip should reduce total token count across all files")
		}
	}
}

// TestE2E_CacheHit reads the same file with strip 3 times:
//   - 1st read: cold (compresses, then async-flushes pointer+content to Redis)
//   - 2nd read: should hit Redis cache (identical bytes, potentially faster)
//   - 3rd read: confirms cache is stable
func TestE2E_CacheHit(t *testing.T) {
	skipUnlessE2E(t)
	files := listFiles(t)
	if len(files) == 0 {
		t.Fatal("no files")
	}

	f := files[0] // largest file

	r1 := readFile(t, f.Path, "strip")
	if r1.Status != 200 {
		t.Fatalf("1st read: status %d", r1.Status)
	}

	time.Sleep(1 * time.Second) // let async flusher persist to Redis

	r2 := readFile(t, f.Path, "strip")
	if r2.Status != 200 {
		t.Fatalf("2nd read: status %d", r2.Status)
	}

	r3 := readFile(t, f.Path, "strip")
	if r3.Status != 200 {
		t.Fatalf("3rd read: status %d", r3.Status)
	}

	t.Logf("1st (cold):  %d bytes, %d tokens, %dms", r1.Bytes, r1.Tokens, r1.Duration.Milliseconds())
	t.Logf("2nd (cache): %d bytes, %d tokens, %dms", r2.Bytes, r2.Tokens, r2.Duration.Milliseconds())
	t.Logf("3rd (cache): %d bytes, %d tokens, %dms", r3.Bytes, r3.Tokens, r3.Duration.Milliseconds())

	// Byte-exact identity: cache must return the same content as the cold compress
	if r1.Body != r2.Body {
		t.Errorf("cache content mismatch: 1st (%d bytes) != 2nd (%d bytes)", r1.Bytes, r2.Bytes)
	}
	if r2.Body != r3.Body {
		t.Errorf("cache unstable: 2nd (%d bytes) != 3rd (%d bytes)", r2.Bytes, r3.Bytes)
	}

	// Token counts must be identical (same content → same token count)
	if r1.Tokens != r2.Tokens {
		t.Errorf("token count mismatch: 1st=%d, 2nd=%d", r1.Tokens, r2.Tokens)
	}
}

// TestE2E_StrategyComparison compares raw, strip, passthrough side by side.
func TestE2E_StrategyComparison(t *testing.T) {
	skipUnlessE2E(t)
	files := listFiles(t)
	if len(files) == 0 {
		t.Fatal("no files")
	}

	strategies := []string{"", "strip", "passthrough"}
	labels := []string{"raw", "strip", "passthru"}

	// Header
	t.Logf("\n%-45s %10s %10s %10s", "FILE", labels[0], labels[1], labels[2])
	t.Log(strings.Repeat("-", 80))

	totals := make([]int, len(strategies))

	for _, f := range files {
		cols := make([]string, len(strategies))
		rawTok := 0
		for i, s := range strategies {
			r := readFile(t, f.Path, s)
			if r.Status != 200 {
				cols[i] = "ERR"
				continue
			}
			totals[i] += r.Tokens
			if i == 0 {
				rawTok = r.Tokens
				cols[i] = fmt.Sprintf("%d", r.Tokens)
			} else if rawTok > 0 {
				pct := 100.0 * float64(r.Tokens) / float64(rawTok)
				cols[i] = fmt.Sprintf("%d (%.0f%%)", r.Tokens, pct)
			} else {
				cols[i] = fmt.Sprintf("%d", r.Tokens)
			}
		}
		t.Logf("%-45s %10s %10s %10s", trunc(f.Name, 45), cols[0], cols[1], cols[2])
	}

	// Totals
	t.Log(strings.Repeat("-", 80))
	totalCols := make([]string, len(strategies))
	for i, tot := range totals {
		if i == 0 || totals[0] == 0 {
			totalCols[i] = fmt.Sprintf("%d", tot)
		} else {
			pct := 100.0 * float64(tot) / float64(totals[0])
			totalCols[i] = fmt.Sprintf("%d (%.0f%%)", tot, pct)
		}
	}
	t.Logf("%-45s %10s %10s %10s", "TOTAL", totalCols[0], totalCols[1], totalCols[2])
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func trunc(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return s[:max-3] + "..."
}
