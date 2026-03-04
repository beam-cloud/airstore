package clients

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/beam-cloud/airstore/pkg/types"
)

// ConfluenceClient provides access to the Confluence Cloud REST API v2.
type ConfluenceClient struct {
	HTTPClient *http.Client
}

func NewConfluenceClient() *ConfluenceClient {
	return &ConfluenceClient{
		HTTPClient: &http.Client{Timeout: 60 * time.Second},
	}
}

func (c *ConfluenceClient) Integration() types.IntegrationName {
	return types.Confluence
}

// apiBase returns the base URL for the Confluence REST API v2.
func apiBase(creds *types.IntegrationCredentials) (string, error) {
	cloudID := ""
	if creds.Extra != nil {
		cloudID = creds.Extra["cloud_id"]
	}
	if cloudID == "" {
		return "", fmt.Errorf("no cloud_id in credentials")
	}
	return fmt.Sprintf("https://api.atlassian.com/ex/confluence/%s/wiki/api/v2", cloudID), nil
}

// request performs an authenticated GET request against the Confluence API.
func (c *ConfluenceClient) request(ctx context.Context, creds *types.IntegrationCredentials, path string, result any) error {
	base, err := apiBase(creds)
	if err != nil {
		return err
	}

	reqURL := base + path
	req, err := http.NewRequestWithContext(ctx, "GET", reqURL, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+creds.AccessToken)
	req.Header.Set("Accept", "application/json")

	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		return confluenceAPIError(resp.StatusCode, body)
	}

	return json.NewDecoder(resp.Body).Decode(result)
}

// ConfluenceSpace represents a Confluence space.
type ConfluenceSpace struct {
	ID          string `json:"id"`
	Key         string `json:"key"`
	Name        string `json:"name"`
	Type        string `json:"type"`
	Status      string `json:"status"`
	Description string `json:"description,omitempty"`
	HomepageID  string `json:"homepageId,omitempty"`
}

// ConfluencePage represents a Confluence page or blog post.
type ConfluencePage struct {
	ID         string `json:"id"`
	Title      string `json:"title"`
	SpaceID    string `json:"spaceId"`
	Status     string `json:"status"`
	ParentID   string `json:"parentId,omitempty"`
	ParentType string `json:"parentType,omitempty"`
	AuthorID   string `json:"authorId,omitempty"`
	CreatedAt  string `json:"createdAt,omitempty"`
	Version    *struct {
		Number    int    `json:"number"`
		CreatedAt string `json:"createdAt"`
	} `json:"version,omitempty"`
	Body *struct {
		Storage *struct {
			Value string `json:"value"`
		} `json:"storage,omitempty"`
	} `json:"body,omitempty"`
}

// ConfluenceSearchResult represents a search result from CQL.
type ConfluenceSearchResult struct {
	ID    string `json:"id"`
	Title string `json:"title"`
	Type  string `json:"type"` // "page" or "blogpost"
	Space struct {
		Key  string `json:"key"`
		Name string `json:"name"`
	} `json:"space,omitempty"`
	URL        string `json:"url,omitempty"`
	LastModified string `json:"lastModified,omitempty"`
	Excerpt    string `json:"excerpt,omitempty"`
}

// ListSpaces returns all accessible spaces.
func (c *ConfluenceClient) ListSpaces(ctx context.Context, creds *types.IntegrationCredentials, limit int) ([]ConfluenceSpace, error) {
	if limit <= 0 {
		limit = 250
	}

	params := url.Values{}
	params.Set("limit", strconv.Itoa(limit))
	params.Set("status", "current")

	var result struct {
		Results []ConfluenceSpace `json:"results"`
	}
	if err := c.request(ctx, creds, "/spaces?"+params.Encode(), &result); err != nil {
		return nil, err
	}
	return result.Results, nil
}

// GetSpace returns a single space by key.
func (c *ConfluenceClient) GetSpace(ctx context.Context, creds *types.IntegrationCredentials, spaceID string) (*ConfluenceSpace, error) {
	var space ConfluenceSpace
	if err := c.request(ctx, creds, "/spaces/"+spaceID, &space); err != nil {
		return nil, err
	}
	return &space, nil
}

// ListPages returns pages, optionally filtered by space ID.
func (c *ConfluenceClient) ListPages(ctx context.Context, creds *types.IntegrationCredentials, spaceID string, limit int) ([]ConfluencePage, error) {
	if limit <= 0 {
		limit = 50
	}

	params := url.Values{}
	params.Set("limit", strconv.Itoa(limit))
	params.Set("sort", "-modified-date")
	params.Set("status", "current")

	path := "/pages"
	if spaceID != "" {
		params.Set("space-id", spaceID)
	}

	var result struct {
		Results []ConfluencePage `json:"results"`
	}
	if err := c.request(ctx, creds, path+"?"+params.Encode(), &result); err != nil {
		return nil, err
	}
	return result.Results, nil
}

// GetPage returns a page by ID with its body in storage format.
func (c *ConfluenceClient) GetPage(ctx context.Context, creds *types.IntegrationCredentials, pageID string) (*ConfluencePage, error) {
	params := url.Values{}
	params.Set("body-format", "storage")

	var page ConfluencePage
	if err := c.request(ctx, creds, "/pages/"+pageID+"?"+params.Encode(), &page); err != nil {
		return nil, err
	}
	return &page, nil
}

// GetPageChildren returns child pages of a given page.
func (c *ConfluenceClient) GetPageChildren(ctx context.Context, creds *types.IntegrationCredentials, pageID string, limit int) ([]ConfluencePage, error) {
	if limit <= 0 {
		limit = 50
	}

	params := url.Values{}
	params.Set("limit", strconv.Itoa(limit))

	var result struct {
		Results []ConfluencePage `json:"results"`
	}
	if err := c.request(ctx, creds, "/pages/"+pageID+"/children?"+params.Encode(), &result); err != nil {
		return nil, err
	}
	return result.Results, nil
}

// SearchCQL executes a CQL search query.
func (c *ConfluenceClient) SearchCQL(ctx context.Context, creds *types.IntegrationCredentials, cql string, limit int) ([]ConfluenceSearchResult, string, error) {
	if limit <= 0 {
		limit = 50
	}

	// Use v1 search endpoint (CQL search is still v1 in Confluence Cloud)
	cloudID := ""
	if creds.Extra != nil {
		cloudID = creds.Extra["cloud_id"]
	}
	if cloudID == "" {
		return nil, "", fmt.Errorf("no cloud_id in credentials")
	}

	base := fmt.Sprintf("https://api.atlassian.com/ex/confluence/%s/wiki/rest/api/content/search", cloudID)
	params := url.Values{}
	params.Set("cql", cql)
	params.Set("limit", strconv.Itoa(limit))
	params.Set("expand", "space,version")

	reqURL := base + "?" + params.Encode()
	req, err := http.NewRequestWithContext(ctx, "GET", reqURL, nil)
	if err != nil {
		return nil, "", err
	}
	req.Header.Set("Authorization", "Bearer "+creds.AccessToken)
	req.Header.Set("Accept", "application/json")

	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return nil, "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		return nil, "", confluenceAPIError(resp.StatusCode, body)
	}

	var searchResp struct {
		Results []struct {
			ID    string `json:"id"`
			Title string `json:"title"`
			Type  string `json:"type"`
			Space *struct {
				Key  string `json:"key"`
				Name string `json:"name"`
			} `json:"space,omitempty"`
			Version *struct {
				When string `json:"when"`
			} `json:"version,omitempty"`
			Links struct {
				WebUI string `json:"webui"`
			} `json:"_links"`
			Excerpt string `json:"excerpt,omitempty"`
		} `json:"results"`
		Links struct {
			Next string `json:"next"`
		} `json:"_links"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&searchResp); err != nil {
		return nil, "", fmt.Errorf("decode search response: %w", err)
	}

	results := make([]ConfluenceSearchResult, 0, len(searchResp.Results))
	for _, r := range searchResp.Results {
		sr := ConfluenceSearchResult{
			ID:    r.ID,
			Title: r.Title,
			Type:  r.Type,
			URL:   r.Links.WebUI,
			Excerpt: r.Excerpt,
		}
		if r.Space != nil {
			sr.Space.Key = r.Space.Key
			sr.Space.Name = r.Space.Name
		}
		if r.Version != nil {
			sr.LastModified = r.Version.When
		}
		results = append(results, sr)
	}

	return results, searchResp.Links.Next, nil
}

// GetRecentPages returns recently modified pages across all spaces.
func (c *ConfluenceClient) GetRecentPages(ctx context.Context, creds *types.IntegrationCredentials, limit int) ([]ConfluencePage, error) {
	return c.ListPages(ctx, creds, "", limit)
}

// StorageToMarkdown converts Confluence storage format (XHTML) to Markdown.
func StorageToMarkdown(storage string) string {
	if storage == "" {
		return ""
	}

	s := storage

	// Remove XML processing instructions
	s = regexp.MustCompile(`<\?xml[^?]*\?>`).ReplaceAllString(s, "")

	// Handle Confluence macros before generic HTML conversion
	s = convertMacros(s)

	// Headings
	for i := 6; i >= 1; i-- {
		prefix := strings.Repeat("#", i)
		tag := fmt.Sprintf("h%d", i)
		re := regexp.MustCompile(fmt.Sprintf(`<%s[^>]*>(.*?)</%s>`, tag, tag))
		s = re.ReplaceAllString(s, prefix+" $1\n\n")
	}

	// Paragraphs
	s = regexp.MustCompile(`<p[^>]*>(.*?)</p>`).ReplaceAllString(s, "$1\n\n")

	// Line breaks
	s = regexp.MustCompile(`<br\s*/?\s*>`).ReplaceAllString(s, "\n")

	// Bold
	s = regexp.MustCompile(`<strong[^>]*>(.*?)</strong>`).ReplaceAllString(s, "**$1**")
	s = regexp.MustCompile(`<b[^>]*>(.*?)</b>`).ReplaceAllString(s, "**$1**")

	// Italic
	s = regexp.MustCompile(`<em[^>]*>(.*?)</em>`).ReplaceAllString(s, "*$1*")
	s = regexp.MustCompile(`<i[^>]*>(.*?)</i>`).ReplaceAllString(s, "*$1*")

	// Code inline
	s = regexp.MustCompile(`<code[^>]*>(.*?)</code>`).ReplaceAllString(s, "`$1`")

	// Links
	s = regexp.MustCompile(`<a[^>]*href="([^"]*)"[^>]*>(.*?)</a>`).ReplaceAllString(s, "[$2]($1)")

	// Images
	s = regexp.MustCompile(`<ac:image[^>]*>.*?<ri:url\s+ri:value="([^"]*)"[^>]*/?>.*?</ac:image>`).ReplaceAllString(s, "![]($1)")
	s = regexp.MustCompile(`<img[^>]*src="([^"]*)"[^>]*/?\s*>`).ReplaceAllString(s, "![]($1)")

	// Unordered lists
	s = regexp.MustCompile(`<ul[^>]*>`).ReplaceAllString(s, "\n")
	s = strings.ReplaceAll(s, "</ul>", "\n")

	// Ordered lists
	s = regexp.MustCompile(`<ol[^>]*>`).ReplaceAllString(s, "\n")
	s = strings.ReplaceAll(s, "</ol>", "\n")

	// List items
	s = regexp.MustCompile(`<li[^>]*>(.*?)</li>`).ReplaceAllString(s, "- $1\n")

	// Task lists
	s = regexp.MustCompile(`<ac:task>\s*<ac:task-status>complete</ac:task-status>\s*<ac:task-body>(.*?)</ac:task-body>\s*</ac:task>`).ReplaceAllString(s, "- [x] $1\n")
	s = regexp.MustCompile(`<ac:task>\s*<ac:task-status>incomplete</ac:task-status>\s*<ac:task-body>(.*?)</ac:task-body>\s*</ac:task>`).ReplaceAllString(s, "- [ ] $1\n")

	// Tables
	s = convertTables(s)

	// Horizontal rules
	s = regexp.MustCompile(`<hr\s*/?\s*>`).ReplaceAllString(s, "\n---\n\n")

	// Blockquote
	s = regexp.MustCompile(`<blockquote[^>]*>(.*?)</blockquote>`).ReplaceAllString(s, "> $1\n\n")

	// Pre blocks
	s = regexp.MustCompile(`<pre[^>]*>(.*?)</pre>`).ReplaceAllString(s, "```\n$1\n```\n\n")

	// Strip remaining HTML tags
	s = regexp.MustCompile(`<[^>]+>`).ReplaceAllString(s, "")

	// Decode HTML entities
	s = strings.ReplaceAll(s, "&amp;", "&")
	s = strings.ReplaceAll(s, "&lt;", "<")
	s = strings.ReplaceAll(s, "&gt;", ">")
	s = strings.ReplaceAll(s, "&quot;", "\"")
	s = strings.ReplaceAll(s, "&#39;", "'")
	s = strings.ReplaceAll(s, "&nbsp;", " ")

	// Clean up excessive whitespace
	s = regexp.MustCompile(`\n{3,}`).ReplaceAllString(s, "\n\n")
	s = strings.TrimSpace(s)

	return s + "\n"
}

// convertMacros handles Confluence structured macros.
func convertMacros(s string) string {
	// Code blocks: <ac:structured-macro ac:name="code">...<ac:plain-text-body><![CDATA[...]]></ac:plain-text-body></ac:structured-macro>
	codeBlockRe := regexp.MustCompile(`<ac:structured-macro[^>]*ac:name="code"[^>]*>(?:.*?<ac:parameter ac:name="language">([^<]*)</ac:parameter>)?.*?<ac:plain-text-body><!\[CDATA\[(.*?)\]\]></ac:plain-text-body>\s*</ac:structured-macro>`)
	s = codeBlockRe.ReplaceAllStringFunc(s, func(match string) string {
		lang := codeBlockRe.FindStringSubmatch(match)
		langStr := ""
		if len(lang) > 1 && lang[1] != "" {
			langStr = lang[1]
		}
		body := ""
		if len(lang) > 2 {
			body = lang[2]
		}
		return fmt.Sprintf("```%s\n%s\n```\n\n", langStr, body)
	})

	// Info, note, warning, tip panels
	for _, panelType := range []string{"info", "note", "warning", "tip"} {
		re := regexp.MustCompile(fmt.Sprintf(`<ac:structured-macro[^>]*ac:name="%s"[^>]*>.*?<ac:rich-text-body>(.*?)</ac:rich-text-body>\s*</ac:structured-macro>`, panelType))
		prefix := strings.ToUpper(panelType[:1]) + panelType[1:]
		s = re.ReplaceAllString(s, fmt.Sprintf("> **%s:** $1\n\n", prefix))
	}

	// Panel macro
	panelRe := regexp.MustCompile(`<ac:structured-macro[^>]*ac:name="panel"[^>]*>.*?<ac:rich-text-body>(.*?)</ac:rich-text-body>\s*</ac:structured-macro>`)
	s = panelRe.ReplaceAllString(s, "> $1\n\n")

	// Expand macro (collapse/expand)
	expandRe := regexp.MustCompile(`<ac:structured-macro[^>]*ac:name="expand"[^>]*>(?:.*?<ac:parameter ac:name="title">([^<]*)</ac:parameter>)?.*?<ac:rich-text-body>(.*?)</ac:rich-text-body>\s*</ac:structured-macro>`)
	s = expandRe.ReplaceAllStringFunc(s, func(match string) string {
		parts := expandRe.FindStringSubmatch(match)
		title := "Details"
		if len(parts) > 1 && parts[1] != "" {
			title = parts[1]
		}
		body := ""
		if len(parts) > 2 {
			body = parts[2]
		}
		return fmt.Sprintf("<details>\n<summary>%s</summary>\n\n%s\n</details>\n\n", title, body)
	})

	// TOC macro — just remove it
	s = regexp.MustCompile(`<ac:structured-macro[^>]*ac:name="toc"[^>]*/?\s*>`).ReplaceAllString(s, "")

	// Any remaining unrecognized macros: extract body or show as fenced block
	remainingRe := regexp.MustCompile(`<ac:structured-macro[^>]*ac:name="([^"]*)"[^>]*>(.*?)</ac:structured-macro>`)
	s = remainingRe.ReplaceAllStringFunc(s, func(match string) string {
		parts := remainingRe.FindStringSubmatch(match)
		name := "macro"
		if len(parts) > 1 {
			name = parts[1]
		}
		body := ""
		if len(parts) > 2 {
			body = parts[2]
		}
		// Try to extract rich-text-body or plain-text-body
		richRe := regexp.MustCompile(`<ac:rich-text-body>(.*?)</ac:rich-text-body>`)
		if m := richRe.FindStringSubmatch(body); len(m) > 1 {
			return m[1]
		}
		plainRe := regexp.MustCompile(`<ac:plain-text-body><!\[CDATA\[(.*?)\]\]></ac:plain-text-body>`)
		if m := plainRe.FindStringSubmatch(body); len(m) > 1 {
			return fmt.Sprintf("```%s\n%s\n```\n\n", name, m[1])
		}
		return ""
	})

	// Emoticons
	s = regexp.MustCompile(`<ac:emoticon[^>]*/?\s*>`).ReplaceAllString(s, "")

	return s
}

// convertTables handles basic HTML table to Markdown conversion.
func convertTables(s string) string {
	tableRe := regexp.MustCompile(`(?s)<table[^>]*>(.*?)</table>`)

	return tableRe.ReplaceAllStringFunc(s, func(tableMatch string) string {
		inner := tableRe.FindStringSubmatch(tableMatch)
		if len(inner) < 2 {
			return tableMatch
		}

		rowRe := regexp.MustCompile(`(?s)<tr[^>]*>(.*?)</tr>`)
		cellRe := regexp.MustCompile(`(?s)<t[hd][^>]*>(.*?)</t[hd]>`)

		rows := rowRe.FindAllStringSubmatch(inner[1], -1)
		if len(rows) == 0 {
			return tableMatch
		}

		var md strings.Builder
		md.WriteString("\n")

		for i, row := range rows {
			if len(row) < 2 {
				continue
			}
			cells := cellRe.FindAllStringSubmatch(row[1], -1)
			md.WriteString("|")
			for _, cell := range cells {
				content := ""
				if len(cell) > 1 {
					content = strings.TrimSpace(regexp.MustCompile(`<[^>]+>`).ReplaceAllString(cell[1], ""))
				}
				md.WriteString(" " + content + " |")
			}
			md.WriteString("\n")

			// Add separator after header row
			if i == 0 {
				md.WriteString("|")
				for range cells {
					md.WriteString(" --- |")
				}
				md.WriteString("\n")
			}
		}
		md.WriteString("\n")

		return md.String()
	})
}

func confluenceAPIError(statusCode int, body []byte) error {
	var apiErr struct {
		Message string `json:"message"`
		Errors  []struct {
			Message string `json:"message"`
		} `json:"errors"`
	}
	if json.Unmarshal(body, &apiErr) == nil && apiErr.Message != "" {
		return fmt.Errorf("confluence API: %s (HTTP %d)", apiErr.Message, statusCode)
	}

	snippet := strings.TrimSpace(string(body))
	if len(snippet) > 2048 {
		snippet = snippet[:2048] + "..."
	}
	if snippet != "" {
		return fmt.Errorf("confluence API: HTTP %d - %s", statusCode, snippet)
	}
	return fmt.Errorf("confluence API: HTTP %d", statusCode)
}
