package compression

import (
	"context"
	"strings"
	"testing"

	"github.com/beam-cloud/airstore/pkg/types"
)

func TestStripNullBytes(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{"no nulls", "hello world", "hello world"},
		{"trailing nulls", "hello\x00\x00\x00", "hello"},
		{"interleaved nulls", "h\x00e\x00l\x00l\x00o", "hello"},
		{"all nulls", "\x00\x00\x00", ""},
		{"empty", "", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := string(stripNullBytes([]byte(tt.in)))
			if got != tt.want {
				t.Errorf("got %q, want %q", got, tt.want)
			}
		})
	}
}

func TestSplitAtMarker(t *testing.T) {
	tests := []struct {
		name       string
		in         string
		marker     string
		wantBefore string
		wantAfter  string
	}{
		{
			"normal split",
			"HEADER\n=== BODY ===\ncontent here",
			"=== BODY ===",
			"HEADER\n=== BODY ===\n",
			"content here",
		},
		{
			"marker not found",
			"no marker here",
			"=== BODY ===",
			"",
			"no marker here",
		},
		{
			"marker at end no newline",
			"data\n=== BODY ===",
			"=== BODY ===",
			"data\n=== BODY ===",
			"",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			before, after := splitAtMarker([]byte(tt.in), tt.marker)
			if string(before) != tt.wantBefore {
				t.Errorf("before: got %q, want %q", before, tt.wantBefore)
			}
			if string(after) != tt.wantAfter {
				t.Errorf("after: got %q, want %q", after, tt.wantAfter)
			}
		})
	}
}

func TestReplaceHTMLEntities(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{"nbsp", "hello&nbsp;world", "hello world"},
		{"amp", "a&amp;b", "a&b"},
		{"zwnj stripped", "foo&zwnj;bar", "foobar"},
		{"numeric entity stripped", "foo&#8203;bar", "foobar"},
		{"hex entity stripped", "foo&#x200B;bar", "foobar"},
		{"multiple", "&lt;b&gt;bold&lt;/b&gt;", "<b>bold</b>"},
		{"no entities", "plain text", "plain text"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := string(replaceHTMLEntities([]byte(tt.in)))
			if got != tt.want {
				t.Errorf("got %q, want %q", got, tt.want)
			}
		})
	}
}

func TestStripURLOnlyLines(t *testing.T) {
	tests := []struct {
		name   string
		in     string
		budget int
		want   string
	}{
		{
			"under budget",
			"https://a.com\nhttps://b.com\ntext line",
			5,
			"https://a.com\nhttps://b.com\ntext line",
		},
		{
			"over budget",
			"https://a.com\nhttps://b.com\nhttps://c.com\nhttps://d.com\ntext",
			2,
			"https://a.com\nhttps://b.com\ntext",
		},
		{
			"dedup",
			"https://a.com\nhttps://a.com\ntext",
			5,
			"https://a.com\ntext",
		},
		{
			"mailto removed",
			"text\n  mailto:foo@bar.com\nmore",
			5,
			"text\n\nmore",
		},
		{
			"text with URL not stripped",
			"See https://example.com for details",
			0,
			"See https://example.com for details",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := string(stripURLOnlyLines([]byte(tt.in), tt.budget))
			if got != tt.want {
				t.Errorf("\ngot:  %q\nwant: %q", got, tt.want)
			}
		})
	}
}

func TestDropLines(t *testing.T) {
	junk := []string{"unsubscribe", "view in browser"}
	tests := []struct {
		name string
		in   string
		want string
	}{
		{
			"drops matching lines",
			"hello\nUnsubscribe\nworld\n  VIEW IN BROWSER  \nbye",
			"hello\nworld\nbye",
		},
		{
			"partial match kept",
			"Unsubscribe from this list",
			"Unsubscribe from this list",
		},
		{
			"empty input",
			"",
			"",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := string(dropLines([]byte(tt.in), junk))
			if got != tt.want {
				t.Errorf("\ngot:  %q\nwant: %q", got, tt.want)
			}
		})
	}
}

func TestTruncateAtFooter(t *testing.T) {
	markers := []string{"terms of service", "privacy policy"}

	tests := []struct {
		name    string
		in      string
		minFrac float64
		want    string
	}{
		{
			"truncates at marker",
			"Line one\nLine two\nLine three\nOur Terms of Service apply.\nLegal text.",
			0.2,
			"Line one\nLine two\nLine three\n\n[footer removed]",
		},
		{
			"marker too early",
			"Terms of Service\nActual content follows\nMore stuff\nEven more",
			0.5, // marker is at 0% — before the 50% threshold
			"Terms of Service\nActual content follows\nMore stuff\nEven more",
		},
		{
			"no marker",
			"Just regular content\nNothing special",
			0.0,
			"Just regular content\nNothing special",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := string(truncateAtFooter([]byte(tt.in), markers, tt.minFrac))
			if got != tt.want {
				t.Errorf("\ngot:  %q\nwant: %q", got, tt.want)
			}
		})
	}
}

func TestDedup(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{
			"removes duplicate lines",
			"apple\nbanana\napple\ncherry\nbanana",
			"apple\nbanana\ncherry",
		},
		{
			"keeps blank lines",
			"a\n\nb\n\nc",
			"a\n\nb\n\nc",
		},
		{
			"trims when comparing",
			"  hello  \nhello\n  hello",
			"  hello  ",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := string(dedup([]byte(tt.in)))
			if got != tt.want {
				t.Errorf("\ngot:  %q\nwant: %q", got, tt.want)
			}
		})
	}
}

func TestCleanup(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{
			"strips HTML comments",
			"before<!-- comment -->after",
			"beforeafter",
		},
		{
			"strips HTML tags",
			"<div>hello</div><br/><p>world</p>",
			"helloworld",
		},
		{
			"strips entities",
			"a&zwnj;b&nbsp;c",
			"ab c",
		},
		{
			"collapses whitespace",
			"line one\n\n\n\nline two\n\n\n\n\nline three",
			"line one\n\nline two\n\nline three",
		},
		{
			"deduplicates",
			"foo\nbar\nfoo\nbaz\nbar",
			"foo\nbar\nbaz",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := string(cleanup([]byte(tt.in)))
			if got != tt.want {
				t.Errorf("\ngot:  %q\nwant: %q", got, tt.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Per-source stripper tests
// ---------------------------------------------------------------------------

func TestStripGmail(t *testing.T) {
	longURL := "https://tracking.example.com/" + strings.Repeat("x", 130)

	input := "=== EMAIL MESSAGE ===\n" +
		"From: sender@example.com\n" +
		"Subject: Test Email\n" +
		"=== BODY ===\n" +
		"Hello &zwnj;&zwnj;&zwnj;world.\n" +
		"Content-Type: text/html; charset=utf-8\n" +
		longURL + "\n" +
		"https://first-url.com\n" +
		"https://second-url.com\n" +
		"https://third-url.com\n" +
		"https://fourth-url.com\n" +
		"Click here: " + longURL + "\n" +
		"View In Browser\n" +
		"Unsubscribe\n" +
		"Real content here.\n" +
		"More real content.\n" +
		"On Mon, Jan 1 wrote:\n" +
		"> quoted reply text\n" +
		"This email was sent to you because you opted in.\n" +
		"Legal boilerplate follows.\n"

	got := string(stripGmail([]byte(input)))

	// Header preserved
	assertContains(t, got, "=== EMAIL MESSAGE ===")
	assertContains(t, got, "From: sender@example.com")
	assertContains(t, got, "Subject: Test Email")

	// Real content preserved
	assertContains(t, got, "Hello world.")
	assertContains(t, got, "Real content here.")
	assertContains(t, got, "More real content.")

	// Noise removed
	assertNotContains(t, got, "&zwnj;")
	assertNotContains(t, got, "Content-Type:")
	assertNotContains(t, got, "View In Browser")
	assertNotContains(t, got, "Unsubscribe")
	assertNotContains(t, got, "quoted reply text")
	assertNotContains(t, got, "On Mon, Jan 1 wrote:")

	// URL budget: at most 3 URL-only lines
	urlLineCount := 0
	for _, line := range strings.Split(got, "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "https://") && !strings.Contains(trimmed, " ") {
			urlLineCount++
		}
	}
	if urlLineCount > 3 {
		t.Errorf("URL budget exceeded: found %d URL-only lines, want <= 3", urlLineCount)
	}

	// Footer truncated
	assertNotContains(t, got, "Legal boilerplate")
	assertContains(t, got, "[footer removed]")
}

func TestStripGitHub(t *testing.T) {
	input := "# PR: Fix authentication\n" +
		"**URL:** https://github.com/org/repo/pull/42\n" +
		"**Status:** merged\n" +
		"**Comments:** 3\n" +
		"\n" +
		"Description of the change.\n" +
		"\n" +
		"```diff\n" +
		"index abc123..def456 100644\n" +
		"old mode 100644\n" +
		"new mode 100755\n" +
		"3 files changed, 10 insertions(+), 2 deletions(-)\n" +
		"+  \n" +
		"-  \n" +
		"+ actual code change\n" +
		"Binary files a/image.png and b/image.png differ\n" +
		"```\n"

	got := string(stripGitHub([]byte(input)))

	// Keeps real content
	assertContains(t, got, "# PR: Fix authentication")
	assertContains(t, got, "Description of the change.")
	assertContains(t, got, "+ actual code change")

	// Removes metadata
	assertNotContains(t, got, "**URL:**")
	assertNotContains(t, got, "**Status:**")
	assertNotContains(t, got, "**Comments:**")

	// Removes diff noise
	assertNotContains(t, got, "index abc123")
	assertNotContains(t, got, "old mode")
	assertNotContains(t, got, "new mode")
	assertNotContains(t, got, "files changed")
	assertContains(t, got, "[binary file]")
}

func TestStripSlack(t *testing.T) {
	input := "From: alice\nDate: 2025-01-01\n\n" +
		"hey team, standup notes:\n" +
		"Reactions: thumbsup (3), heart (1)\n" +
		"bob has joined the channel\n" +
		"carol set the channel topic to \"standup\"\n" +
		"dave uploaded a file: report.pdf\n" +
		"I'll fix that (edited)\n" +
		"Thread reply: sounds good!\n"

	got := string(stripSlack([]byte(input)))

	assertContains(t, got, "hey team, standup notes:")
	assertContains(t, got, "I'll fix that")
	assertContains(t, got, "Thread reply: sounds good!")
	assertNotContains(t, got, "Reactions:")
	assertNotContains(t, got, "has joined the channel")
	assertNotContains(t, got, "set the channel topic")
	assertNotContains(t, got, "uploaded a file")
	assertNotContains(t, got, "(edited)")
}

func TestStripNotion(t *testing.T) {
	input := "# My Page\n" +
		"**URL:** https://notion.so/page/abc\n" +
		"**Created:** 2025-01-01\n" +
		"**Last edited:** 2025-01-15\n" +
		"\n" +
		"Page content here.\n"

	got := string(stripNotion([]byte(input)))

	assertContains(t, got, "# My Page")
	assertContains(t, got, "Page content here.")
	assertNotContains(t, got, "**URL:**")
	assertNotContains(t, got, "**Created:**")
	assertNotContains(t, got, "**Last edited:**")
}

func TestStripLinear(t *testing.T) {
	input := "# PROJ-123: Fix bug\n" +
		"| Field | Value |\n" +
		"| --- | --- |\n" +
		"| Status | Done |\n" +
		"| Priority | High |\n" +
		"| Created | 2025-01-01 |\n" +
		"| Updated | 2025-01-15 |\n" +
		"| URL | https://linear.app/... |\n" +
		"| Team | Engineering |\n" +
		"| Project | Backend |\n" +
		"\n" +
		"## Description\n" +
		"The bug was in the auth flow.\n"

	got := string(stripLinear([]byte(input)))

	assertContains(t, got, "# PROJ-123: Fix bug")
	assertContains(t, got, "| Status | Done |")
	assertContains(t, got, "| Priority | High |")
	assertContains(t, got, "The bug was in the auth flow.")

	// Low-value rows removed
	assertNotContains(t, got, "| Created |")
	assertNotContains(t, got, "| Updated |")
	assertNotContains(t, got, "| URL |")
	assertNotContains(t, got, "| Team |")
	assertNotContains(t, got, "| Project |")
}

func TestStripCompressor_Integration(t *testing.T) {
	comp := NewStripCompressor(DefaultConfig())
	ctx := context.Background()

	// Simulated Gmail email with null bytes (FUSE padding) and typical noise
	raw := strings.Repeat("\x00", 1000) +
		"=== EMAIL MESSAGE ===\n" +
		"From: shop@store.com\n" +
		"Subject: Big Sale!\n" +
		"=== BODY ===\n" +
		"Shop now for great deals.\n" +
		"https://tracking.example.com/" + strings.Repeat("x", 150) + "\n" +
		"https://url1.com\nhttps://url2.com\nhttps://url3.com\nhttps://url4.com\nhttps://url5.com\n" +
		"View In Browser\n" +
		"Unsubscribe\n" +
		"&zwnj;&zwnj;&zwnj;\n" +
		"<div style='color:red'>styled</div>\n" +
		"<!-- tracking pixel -->\n" +
		"Real deal content.\n" +
		"More info about products.\n" +
		"This email was sent to you because you signed up.\n" +
		"Legal stuff here.\n" +
		strings.Repeat("\x00", 5000)

	result, err := comp.Compress(ctx, []byte(raw), ContentMeta{Integration: string(types.SourceGmail), Filename: "test.txt"})
	if err != nil {
		t.Fatal(err)
	}

	got := string(result.Data)

	// Null bytes gone
	assertNotContains(t, got, "\x00")

	// Real content preserved
	assertContains(t, got, "From: shop@store.com")
	assertContains(t, got, "Subject: Big Sale!")
	assertContains(t, got, "Shop now for great deals.")
	assertContains(t, got, "Real deal content.")
	assertContains(t, got, "More info about products.")

	// Noise removed
	assertNotContains(t, got, "&zwnj;")
	assertNotContains(t, got, "View In Browser")
	assertNotContains(t, got, "tracking pixel")
	assertNotContains(t, got, "style='color:red'")

	// Token counts make sense
	if result.OriginalTokens <= 0 {
		t.Error("OriginalTokens should be > 0")
	}
	if result.CompressedTokens >= result.OriginalTokens {
		t.Errorf("CompressedTokens (%d) should be < OriginalTokens (%d)", result.CompressedTokens, result.OriginalTokens)
	}
	if result.Strategy != CompressionStrategyStrip {
		t.Errorf("CompressionStrategy: got %q, want %q", result.Strategy, CompressionStrategyStrip)
	}
	if result.Outcome != OutcomeCompressed {
		t.Errorf("Outcome: got %q, want %q", result.Outcome, OutcomeCompressed)
	}

	t.Logf("Tokens: %d -> %d (%.0f%% reduction)",
		result.OriginalTokens, result.CompressedTokens,
		100.0*float64(result.OriginalTokens-result.CompressedTokens)/float64(result.OriginalTokens))
}

// TestStripCompressor_PostHog verifies PostHog JSON passes through unchanged.
func TestStripCompressor_PostHog(t *testing.T) {
	comp := NewStripCompressor(DefaultConfig())
	input := `{
  "id": 42,
  "event": "page_view",
  "properties": {
    "url": "https://example.com/very/long/path/that/might/be/over/one/hundred/and/twenty/characters/if/we/kept/going/and/going/and/going/for/a/while",
    "referrer": "https://google.com"
  }
}
`
	result, err := comp.Compress(context.Background(), []byte(input), ContentMeta{Integration: string(types.SourcePostHog)})
	if err != nil {
		t.Fatal(err)
	}
	// Content must be byte-identical (minus null bytes, which the input doesn't have).
	if string(result.Data) != input {
		t.Errorf("PostHog content was modified.\ngot:  %q\nwant: %q", result.Data, input)
	}
}

// TestStripCompressor_UnknownIntegration verifies the default path works.
func TestStripCompressor_UnknownIntegration(t *testing.T) {
	comp := NewStripCompressor(DefaultConfig())
	ctx := context.Background()

	input := "Some content\n" +
		"https://tracking.example.com/" + strings.Repeat("a", 150) + "\n" +
		"More content\n" +
		"<b>bold</b>\n"

	result, err := comp.Compress(ctx, []byte(input), ContentMeta{Integration: "unknown_source"})
	if err != nil {
		t.Fatal(err)
	}

	got := string(result.Data)
	assertContains(t, got, "Some content")
	assertContains(t, got, "More content")
	assertNotContains(t, got, "<b>")
}

// TestStripWeb verifies web/markdown content stripping.
func TestStripWeb(t *testing.T) {
	comp := NewStripCompressor(DefaultConfig())
	ctx := context.Background()

	input := "# Cocktail Recipe\n\n" +
		"A delicious cocktail.\n\n" +
		"https://example.com/nav/link1\n" +
		"https://example.com/nav/link2\n" +
		"https://example.com/nav/link3\n" +
		"https://example.com/nav/link4\n" +
		"https://example.com/nav/link5\n" +
		"https://example.com/nav/link6\n" +
		"https://example.com/nav/link7\n" +
		"https://example.com/nav/link8\n" +
		"https://example.com/nav/link9\n" +
		"https://example.com/nav/link10\n" +
		"https://example.com/nav/link11\n" +
		"https://example.com/nav/link12\n" +
		"\nIngredients:\n- 2 oz bourbon\n- 1 sugar cube\n"

	result, err := comp.Compress(ctx, []byte(input), ContentMeta{Integration: string(types.SourceWeb)})
	if err != nil {
		t.Fatal(err)
	}

	got := string(result.Data)
	assertContains(t, got, "Cocktail Recipe")
	assertContains(t, got, "delicious cocktail")
	assertContains(t, got, "2 oz bourbon")
	// URLs beyond the budget (10) should be stripped
	assertNotContains(t, got, "link12")
}

// TestStripCompressor_EmptyContent verifies empty input doesn't panic.
func TestStripCompressor_EmptyContent(t *testing.T) {
	comp := NewStripCompressor(DefaultConfig())
	result, err := comp.Compress(context.Background(), nil, ContentMeta{Integration: string(types.SourceGmail)})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Data) != 0 {
		t.Errorf("expected empty output, got %d bytes", len(result.Data))
	}
}

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

func assertContains(t *testing.T, got, substr string) {
	t.Helper()
	if !strings.Contains(got, substr) {
		t.Errorf("output should contain %q but doesn't.\nGot:\n%s", substr, truncForLog(got))
	}
}

func assertNotContains(t *testing.T, got, substr string) {
	t.Helper()
	if strings.Contains(got, substr) {
		t.Errorf("output should NOT contain %q but does.\nGot:\n%s", substr, truncForLog(got))
	}
}

func truncForLog(s string) string {
	if len(s) > 500 {
		return s[:500] + "\n... [truncated]"
	}
	return s
}
