package providers

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/beam-cloud/airstore/pkg/sources"
	"github.com/beam-cloud/airstore/pkg/types"
)

const (
	notionAPIBase    = "https://api.notion.com/v1"
	notionAPIVersion = "2022-06-28"
)

// NotionProvider implements sources.Provider for Notion integration.
// It exposes Notion resources as a read-only filesystem under /sources/notion/
type NotionProvider struct {
	httpClient *http.Client
}

// NewNotionProvider creates a new Notion source provider
func NewNotionProvider() *NotionProvider {
	return &NotionProvider{
		httpClient: &http.Client{Timeout: 30 * time.Second},
	}
}

func (n *NotionProvider) Name() string {
	return types.ToolNotion.String()
}

// Stat returns file/directory attributes
func (n *NotionProvider) Stat(ctx context.Context, pctx *sources.ProviderContext, path string) (*sources.FileInfo, error) {
	if pctx.Credentials == nil || pctx.Credentials.AccessToken == "" {
		return nil, sources.ErrNotConnected
	}

	if path == "" {
		return sources.DirInfo(), nil
	}

	parts := strings.Split(path, "/")

	switch parts[0] {
	case "pages":
		return n.statPages(ctx, pctx, parts[1:])
	case "databases":
		return n.statDatabases(ctx, pctx, parts[1:])
	case "search":
		return n.statSearch(ctx, pctx, parts[1:])
	default:
		return nil, sources.ErrNotFound
	}
}

// ReadDir lists directory contents
func (n *NotionProvider) ReadDir(ctx context.Context, pctx *sources.ProviderContext, path string) ([]sources.DirEntry, error) {
	if pctx.Credentials == nil || pctx.Credentials.AccessToken == "" {
		return nil, sources.ErrNotConnected
	}

	if path == "" {
		return []sources.DirEntry{
			{Name: "pages", Mode: sources.ModeDir, IsDir: true, Mtime: sources.NowUnix()},
			{Name: "databases", Mode: sources.ModeDir, IsDir: true, Mtime: sources.NowUnix()},
			{Name: "search", Mode: sources.ModeDir, IsDir: true, Mtime: sources.NowUnix()},
		}, nil
	}

	parts := strings.Split(path, "/")

	switch parts[0] {
	case "pages":
		return n.readdirPages(ctx, pctx, parts[1:])
	case "databases":
		return n.readdirDatabases(ctx, pctx, parts[1:])
	case "search":
		return n.readdirSearch(ctx, pctx, parts[1:])
	default:
		return nil, sources.ErrNotFound
	}
}

// Read reads file content
func (n *NotionProvider) Read(ctx context.Context, pctx *sources.ProviderContext, path string, offset, length int64) ([]byte, error) {
	if pctx.Credentials == nil || pctx.Credentials.AccessToken == "" {
		return nil, sources.ErrNotConnected
	}

	parts := strings.Split(path, "/")

	switch parts[0] {
	case "pages":
		return n.readPages(ctx, pctx, parts[1:], offset, length)
	case "databases":
		return n.readDatabases(ctx, pctx, parts[1:], offset, length)
	case "search":
		return n.readSearch(ctx, pctx, parts[1:], offset, length)
	default:
		return nil, sources.ErrNotFound
	}
}

// Readlink is not supported for Notion
func (n *NotionProvider) Readlink(ctx context.Context, pctx *sources.ProviderContext, path string) (string, error) {
	return "", sources.ErrNotFound
}

// Search executes a Notion search query and returns results
// The query is a plain text search term
func (n *NotionProvider) Search(ctx context.Context, pctx *sources.ProviderContext, query string, limit int) ([]sources.SearchResult, error) {
	if pctx.Credentials == nil || pctx.Credentials.AccessToken == "" {
		return nil, sources.ErrNotConnected
	}

	if limit <= 0 {
		limit = 50
	}

	token := pctx.Credentials.AccessToken

	// Build search request body
	body := map[string]any{
		"query":     query,
		"page_size": limit,
		"sort": map[string]string{
			"direction": "descending",
			"timestamp": "last_edited_time",
		},
	}

	bodyJSON, _ := json.Marshal(body)

	req, err := http.NewRequestWithContext(ctx, "POST", notionAPIBase+"/search", bytes.NewReader(bodyJSON))
	if err != nil {
		return nil, err
	}

	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Notion-Version", notionAPIVersion)
	req.Header.Set("Content-Type", "application/json")

	resp, err := n.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		respBody, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("notion API error: %s - %s", resp.Status, string(respBody))
	}

	var result map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}

	resultsRaw, _ := result["results"].([]any)
	if len(resultsRaw) == 0 {
		return []sources.SearchResult{}, nil
	}

	results := make([]sources.SearchResult, 0, len(resultsRaw))
	for _, r := range resultsRaw {
		item, ok := r.(map[string]any)
		if !ok {
			continue
		}

		id, _ := item["id"].(string)
		objType, _ := item["object"].(string)
		lastEdited, _ := item["last_edited_time"].(string)

		// Get title from properties
		title := "Untitled"
		if props, ok := item["properties"].(map[string]any); ok {
			if titleProp, ok := props["title"].(map[string]any); ok {
				if titleArr, ok := titleProp["title"].([]any); ok && len(titleArr) > 0 {
					if textObj, ok := titleArr[0].(map[string]any); ok {
						if plainText, ok := textObj["plain_text"].(string); ok {
							title = plainText
						}
					}
				}
			}
			// Try Name property (common for databases)
			if title == "Untitled" {
				if nameProp, ok := props["Name"].(map[string]any); ok {
					if titleArr, ok := nameProp["title"].([]any); ok && len(titleArr) > 0 {
						if textObj, ok := titleArr[0].(map[string]any); ok {
							if plainText, ok := textObj["plain_text"].(string); ok {
								title = plainText
							}
						}
					}
				}
			}
		}

		// Parse modified time
		mtime := sources.NowUnix()
		if t, err := time.Parse(time.RFC3339, lastEdited); err == nil {
			mtime = t.Unix()
		}

		// Generate filename: sanitized title with ID suffix
		safeTitle := sanitizeNotionTitle(title)
		shortID := id
		if len(shortID) > 8 {
			shortID = shortID[:8]
		}
		filename := fmt.Sprintf("%s_%s.json", safeTitle, shortID)

		results = append(results, sources.SearchResult{
			Name:    filename,
			Id:      id,
			Mode:    sources.ModeFile,
			Size:    0,
			Mtime:   mtime,
			Preview: objType + ": " + title,
		})
	}

	return results, nil
}

// sanitizeNotionTitle makes a Notion page title safe for use as a filename
func sanitizeNotionTitle(title string) string {
	// Replace unsafe characters with underscores
	unsafe := []string{"/", "\\", ":", "*", "?", "\"", "<", ">", "|", " "}
	result := title
	for _, char := range unsafe {
		result = strings.ReplaceAll(result, char, "_")
	}
	// Collapse multiple underscores
	for strings.Contains(result, "__") {
		result = strings.ReplaceAll(result, "__", "_")
	}
	result = strings.Trim(result, "_")
	if result == "" {
		result = "untitled"
	}
	// Truncate if too long
	if len(result) > 50 {
		result = result[:50]
	}
	return result
}

// --- Pages ---
// /pages/<pageid>.json - page properties
// /pages/<pageid>.md - page content as markdown

func (n *NotionProvider) statPages(ctx context.Context, pctx *sources.ProviderContext, parts []string) (*sources.FileInfo, error) {
	if len(parts) == 0 {
		return sources.DirInfo(), nil
	}

	file := parts[0]
	if strings.HasSuffix(file, ".json") || strings.HasSuffix(file, ".md") {
		data, err := n.getPageFile(ctx, pctx, file)
		if err != nil {
			return nil, err
		}
		return sources.FileInfoFromBytes(data), nil
	}

	return nil, sources.ErrNotFound
}

func (n *NotionProvider) readdirPages(ctx context.Context, pctx *sources.ProviderContext, parts []string) ([]sources.DirEntry, error) {
	if len(parts) == 0 {
		// List recently edited pages via search
		token := pctx.Credentials.AccessToken
		pages, err := n.searchPages(ctx, token, "", 50)
		if err != nil {
			return nil, err
		}

		entries := make([]sources.DirEntry, 0, len(pages)*2)
		for _, p := range pages {
			if id, ok := p["id"].(string); ok {
				// Remove dashes from UUID for cleaner paths
				cleanId := strings.ReplaceAll(id, "-", "")
				entries = append(entries,
					sources.DirEntry{Name: cleanId + ".json", Mode: sources.ModeFile, Mtime: sources.NowUnix()},
					sources.DirEntry{Name: cleanId + ".md", Mode: sources.ModeFile, Mtime: sources.NowUnix()},
				)
			}
		}
		return entries, nil
	}
	return nil, sources.ErrNotDir
}

func (n *NotionProvider) readPages(ctx context.Context, pctx *sources.ProviderContext, parts []string, offset, length int64) ([]byte, error) {
	if len(parts) == 0 {
		return nil, sources.ErrIsDir
	}

	data, err := n.getPageFile(ctx, pctx, parts[0])
	if err != nil {
		return nil, err
	}

	return sliceData(data, offset, length), nil
}

func (n *NotionProvider) getPageFile(ctx context.Context, pctx *sources.ProviderContext, file string) ([]byte, error) {
	token := pctx.Credentials.AccessToken

	var pageId string
	var format string

	if strings.HasSuffix(file, ".json") {
		pageId = strings.TrimSuffix(file, ".json")
		format = "json"
	} else if strings.HasSuffix(file, ".md") {
		pageId = strings.TrimSuffix(file, ".md")
		format = "md"
	} else {
		return nil, sources.ErrNotFound
	}

	// Add dashes back to UUID if needed
	if len(pageId) == 32 {
		pageId = fmt.Sprintf("%s-%s-%s-%s-%s", pageId[0:8], pageId[8:12], pageId[12:16], pageId[16:20], pageId[20:32])
	}

	switch format {
	case "json":
		return n.fetchPage(ctx, token, pageId)
	case "md":
		return n.fetchPageAsMarkdown(ctx, token, pageId)
	default:
		return nil, sources.ErrNotFound
	}
}

// --- Databases ---
// /databases/<dbid>/schema.json - database schema
// /databases/<dbid>/rows.json - database rows

func (n *NotionProvider) statDatabases(ctx context.Context, pctx *sources.ProviderContext, parts []string) (*sources.FileInfo, error) {
	switch len(parts) {
	case 0:
		return sources.DirInfo(), nil
	case 1:
		// /databases/<dbid> - directory
		return sources.DirInfo(), nil
	case 2:
		// /databases/<dbid>/<file>.json
		dbId, file := parts[0], parts[1]
		data, err := n.getDatabaseFile(ctx, pctx, dbId, file)
		if err != nil {
			return nil, err
		}
		return sources.FileInfoFromBytes(data), nil
	default:
		return nil, sources.ErrNotFound
	}
}

func (n *NotionProvider) readdirDatabases(ctx context.Context, pctx *sources.ProviderContext, parts []string) ([]sources.DirEntry, error) {
	token := pctx.Credentials.AccessToken

	switch len(parts) {
	case 0:
		// List databases via search
		dbs, err := n.searchDatabases(ctx, token, 50)
		if err != nil {
			return nil, err
		}

		entries := make([]sources.DirEntry, 0, len(dbs))
		for _, db := range dbs {
			if id, ok := db["id"].(string); ok {
				cleanId := strings.ReplaceAll(id, "-", "")
				entries = append(entries, sources.DirEntry{
					Name:  cleanId,
					Mode:  sources.ModeDir,
					IsDir: true,
					Mtime: sources.NowUnix(),
				})
			}
		}
		return entries, nil

	case 1:
		// List files in a database
		return []sources.DirEntry{
			{Name: "schema.json", Mode: sources.ModeFile, Mtime: sources.NowUnix()},
			{Name: "rows.json", Mode: sources.ModeFile, Mtime: sources.NowUnix()},
		}, nil

	default:
		return nil, sources.ErrNotDir
	}
}

func (n *NotionProvider) readDatabases(ctx context.Context, pctx *sources.ProviderContext, parts []string, offset, length int64) ([]byte, error) {
	if len(parts) < 2 {
		return nil, sources.ErrIsDir
	}

	dbId, file := parts[0], parts[1]
	data, err := n.getDatabaseFile(ctx, pctx, dbId, file)
	if err != nil {
		return nil, err
	}

	return sliceData(data, offset, length), nil
}

func (n *NotionProvider) getDatabaseFile(ctx context.Context, pctx *sources.ProviderContext, dbId, file string) ([]byte, error) {
	token := pctx.Credentials.AccessToken

	// Add dashes back to UUID if needed
	if len(dbId) == 32 {
		dbId = fmt.Sprintf("%s-%s-%s-%s-%s", dbId[0:8], dbId[8:12], dbId[12:16], dbId[16:20], dbId[20:32])
	}

	switch file {
	case "schema.json":
		return n.fetchDatabaseSchema(ctx, token, dbId)
	case "rows.json":
		return n.fetchDatabaseRows(ctx, token, dbId)
	default:
		return nil, sources.ErrNotFound
	}
}

// --- Search ---
// /search/recent.json - recently edited pages

func (n *NotionProvider) statSearch(ctx context.Context, pctx *sources.ProviderContext, parts []string) (*sources.FileInfo, error) {
	if len(parts) == 0 {
		return sources.DirInfo(), nil
	}

	if parts[0] == "recent.json" {
		data, err := n.getSearchData(ctx, pctx, "recent.json")
		if err != nil {
			return nil, err
		}
		return sources.FileInfoFromBytes(data), nil
	}

	return nil, sources.ErrNotFound
}

func (n *NotionProvider) readdirSearch(ctx context.Context, pctx *sources.ProviderContext, parts []string) ([]sources.DirEntry, error) {
	if len(parts) == 0 {
		return []sources.DirEntry{
			{Name: "recent.json", Mode: sources.ModeFile, Mtime: sources.NowUnix()},
		}, nil
	}
	return nil, sources.ErrNotDir
}

func (n *NotionProvider) readSearch(ctx context.Context, pctx *sources.ProviderContext, parts []string, offset, length int64) ([]byte, error) {
	if len(parts) == 0 {
		return nil, sources.ErrIsDir
	}

	data, err := n.getSearchData(ctx, pctx, parts[0])
	if err != nil {
		return nil, err
	}

	return sliceData(data, offset, length), nil
}

func (n *NotionProvider) getSearchData(ctx context.Context, pctx *sources.ProviderContext, file string) ([]byte, error) {
	token := pctx.Credentials.AccessToken

	switch file {
	case "recent.json":
		return n.fetchRecentPages(ctx, token)
	default:
		return nil, sources.ErrNotFound
	}
}

// --- API methods ---

func (n *NotionProvider) searchPages(ctx context.Context, token, query string, pageSize int) ([]map[string]any, error) {
	body := map[string]any{
		"filter":    map[string]string{"property": "object", "value": "page"},
		"page_size": pageSize,
		"sort": map[string]string{
			"direction": "descending",
			"timestamp": "last_edited_time",
		},
	}
	if query != "" {
		body["query"] = query
	}

	var result map[string]any
	if err := n.postRequest(ctx, token, "/search", body, &result); err != nil {
		return nil, err
	}

	results, ok := result["results"].([]any)
	if !ok {
		return nil, nil
	}

	pages := make([]map[string]any, 0, len(results))
	for _, r := range results {
		if page, ok := r.(map[string]any); ok {
			pages = append(pages, page)
		}
	}
	return pages, nil
}

func (n *NotionProvider) searchDatabases(ctx context.Context, token string, pageSize int) ([]map[string]any, error) {
	body := map[string]any{
		"filter":    map[string]string{"property": "object", "value": "database"},
		"page_size": pageSize,
	}

	var result map[string]any
	if err := n.postRequest(ctx, token, "/search", body, &result); err != nil {
		return nil, err
	}

	results, ok := result["results"].([]any)
	if !ok {
		return nil, nil
	}

	dbs := make([]map[string]any, 0, len(results))
	for _, r := range results {
		if db, ok := r.(map[string]any); ok {
			dbs = append(dbs, db)
		}
	}
	return dbs, nil
}

func (n *NotionProvider) fetchPage(ctx context.Context, token, pageId string) ([]byte, error) {
	var result map[string]any
	if err := n.request(ctx, token, "/pages/"+pageId, &result); err != nil {
		return nil, err
	}

	return jsonMarshal(result)
}

func (n *NotionProvider) fetchPageAsMarkdown(ctx context.Context, token, pageId string) ([]byte, error) {
	// Fetch page properties for title
	var page map[string]any
	if err := n.request(ctx, token, "/pages/"+pageId, &page); err != nil {
		return nil, err
	}

	// Fetch blocks
	var blocksResult map[string]any
	if err := n.request(ctx, token, "/blocks/"+pageId+"/children?page_size=100", &blocksResult); err != nil {
		return nil, err
	}

	// Build markdown
	var md strings.Builder

	// Title
	if props, ok := page["properties"].(map[string]any); ok {
		if title, ok := props["title"].(map[string]any); ok {
			if titleArr, ok := title["title"].([]any); ok && len(titleArr) > 0 {
				if titleObj, ok := titleArr[0].(map[string]any); ok {
					if text, ok := titleObj["plain_text"].(string); ok {
						md.WriteString("# " + text + "\n\n")
					}
				}
			}
		}
	}

	// Blocks to markdown
	if results, ok := blocksResult["results"].([]any); ok {
		for _, r := range results {
			block, ok := r.(map[string]any)
			if !ok {
				continue
			}
			md.WriteString(blockToMarkdown(block))
		}
	}

	return []byte(md.String()), nil
}

func blockToMarkdown(block map[string]any) string {
	blockType, _ := block["type"].(string)

	getText := func(key string) string {
		if content, ok := block[key].(map[string]any); ok {
			if richText, ok := content["rich_text"].([]any); ok {
				var text strings.Builder
				for _, rt := range richText {
					if rtObj, ok := rt.(map[string]any); ok {
						if t, ok := rtObj["plain_text"].(string); ok {
							text.WriteString(t)
						}
					}
				}
				return text.String()
			}
		}
		return ""
	}

	switch blockType {
	case "paragraph":
		return getText("paragraph") + "\n\n"
	case "heading_1":
		return "# " + getText("heading_1") + "\n\n"
	case "heading_2":
		return "## " + getText("heading_2") + "\n\n"
	case "heading_3":
		return "### " + getText("heading_3") + "\n\n"
	case "bulleted_list_item":
		return "- " + getText("bulleted_list_item") + "\n"
	case "numbered_list_item":
		return "1. " + getText("numbered_list_item") + "\n"
	case "code":
		return "```\n" + getText("code") + "\n```\n\n"
	case "quote":
		return "> " + getText("quote") + "\n\n"
	case "divider":
		return "---\n\n"
	default:
		return ""
	}
}

func (n *NotionProvider) fetchDatabaseSchema(ctx context.Context, token, dbId string) ([]byte, error) {
	var result map[string]any
	if err := n.request(ctx, token, "/databases/"+dbId, &result); err != nil {
		return nil, err
	}

	// Extract relevant schema info
	schema := map[string]any{
		"id":               result["id"],
		"title":            result["title"],
		"properties":       result["properties"],
		"created_time":     result["created_time"],
		"last_edited_time": result["last_edited_time"],
	}

	return jsonMarshal(schema)
}

func (n *NotionProvider) fetchDatabaseRows(ctx context.Context, token, dbId string) ([]byte, error) {
	body := map[string]any{
		"page_size": 100,
	}

	var result map[string]any
	if err := n.postRequest(ctx, token, "/databases/"+dbId+"/query", body, &result); err != nil {
		return nil, err
	}

	return jsonMarshal(result)
}

func (n *NotionProvider) fetchRecentPages(ctx context.Context, token string) ([]byte, error) {
	pages, err := n.searchPages(ctx, token, "", 50)
	if err != nil {
		return nil, err
	}

	// Simplify the response
	result := make([]map[string]any, 0, len(pages))
	for _, p := range pages {
		simplified := map[string]any{
			"id":               p["id"],
			"url":              p["url"],
			"created_time":     p["created_time"],
			"last_edited_time": p["last_edited_time"],
		}

		// Extract title
		if props, ok := p["properties"].(map[string]any); ok {
			if title, ok := props["title"].(map[string]any); ok {
				if titleArr, ok := title["title"].([]any); ok && len(titleArr) > 0 {
					if titleObj, ok := titleArr[0].(map[string]any); ok {
						simplified["title"] = titleObj["plain_text"]
					}
				}
			}
		}

		result = append(result, simplified)
	}

	return jsonMarshal(map[string]any{
		"pages": result,
		"count": len(result),
	})
}

func (n *NotionProvider) request(ctx context.Context, token, path string, result any) error {
	url := notionAPIBase + path
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return err
	}

	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Notion-Version", notionAPIVersion)
	req.Header.Set("Accept", "application/json")

	resp, err := n.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		var apiErr struct {
			Message string `json:"message"`
		}
		json.Unmarshal(body, &apiErr)
		if apiErr.Message != "" {
			return fmt.Errorf("notion API: %s", apiErr.Message)
		}
		return fmt.Errorf("notion API: %s", resp.Status)
	}

	return json.NewDecoder(resp.Body).Decode(result)
}

func (n *NotionProvider) postRequest(ctx context.Context, token, path string, body any, result any) error {
	jsonBody, err := json.Marshal(body)
	if err != nil {
		return err
	}

	url := notionAPIBase + path
	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(jsonBody))
	if err != nil {
		return err
	}

	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Notion-Version", notionAPIVersion)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	resp, err := n.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		var apiErr struct {
			Message string `json:"message"`
		}
		json.Unmarshal(body, &apiErr)
		if apiErr.Message != "" {
			return fmt.Errorf("notion API: %s", apiErr.Message)
		}
		return fmt.Errorf("notion API: %s", resp.Status)
	}

	return json.NewDecoder(resp.Body).Decode(result)
}

// ============================================================================
// QueryExecutor implementation
// ============================================================================

// ExecuteQuery runs a Notion search query and returns results with generated filenames.
// This implements the sources.QueryExecutor interface for filesystem queries.
// Supports pagination via spec.PageToken (Notion's start_cursor) for fetching subsequent pages.
func (n *NotionProvider) ExecuteQuery(ctx context.Context, pctx *sources.ProviderContext, spec sources.QuerySpec) (*sources.QueryResponse, error) {
	if pctx.Credentials == nil || pctx.Credentials.AccessToken == "" {
		return nil, sources.ErrNotConnected
	}

	limit := spec.Limit
	if limit <= 0 {
		limit = 50
	}

	// Notion max page_size is 100
	if limit > 100 {
		limit = 100
	}

	token := pctx.Credentials.AccessToken

	// Build search request body
	body := map[string]any{
		"query":     spec.Query,
		"page_size": limit,
		"sort": map[string]string{
			"direction": "descending",
			"timestamp": "last_edited_time",
		},
	}

	// Add start_cursor for pagination if provided
	if spec.PageToken != "" {
		body["start_cursor"] = spec.PageToken
	}

	bodyJSON, _ := json.Marshal(body)

	req, err := http.NewRequestWithContext(ctx, "POST", notionAPIBase+"/search", bytes.NewReader(bodyJSON))
	if err != nil {
		return nil, err
	}

	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Notion-Version", notionAPIVersion)
	req.Header.Set("Content-Type", "application/json")

	resp, err := n.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		respBody, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("notion API error: %s - %s", resp.Status, string(respBody))
	}

	var result map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}

	// Extract pagination info from response
	hasMore, _ := result["has_more"].(bool)
	nextCursor, _ := result["next_cursor"].(string)

	resultsRaw, _ := result["results"].([]any)
	if len(resultsRaw) == 0 {
		return &sources.QueryResponse{
			Results:       []sources.QueryResult{},
			NextPageToken: "",
			HasMore:       false,
		}, nil
	}

	filenameFormat := spec.FilenameFormat
	if filenameFormat == "" {
		filenameFormat = sources.DefaultFilenameFormat("notion")
	}

	results := make([]sources.QueryResult, 0, len(resultsRaw))
	for _, r := range resultsRaw {
		item, ok := r.(map[string]any)
		if !ok {
			continue
		}

		id, _ := item["id"].(string)
		objType, _ := item["object"].(string)
		lastEdited, _ := item["last_edited_time"].(string)
		createdTime, _ := item["created_time"].(string)

		// Get title from properties
		title := "Untitled"
		if props, ok := item["properties"].(map[string]any); ok {
			if titleProp, ok := props["title"].(map[string]any); ok {
				if titleArr, ok := titleProp["title"].([]any); ok && len(titleArr) > 0 {
					if textObj, ok := titleArr[0].(map[string]any); ok {
						if plainText, ok := textObj["plain_text"].(string); ok {
							title = plainText
						}
					}
				}
			}
			// Try Name property (common for databases)
			if title == "Untitled" {
				if nameProp, ok := props["Name"].(map[string]any); ok {
					if titleArr, ok := nameProp["title"].([]any); ok && len(titleArr) > 0 {
						if textObj, ok := titleArr[0].(map[string]any); ok {
							if plainText, ok := textObj["plain_text"].(string); ok {
								title = plainText
							}
						}
					}
				}
			}
		}

		// Parse modified time
		mtime := sources.NowUnix()
		if t, err := time.Parse(time.RFC3339, lastEdited); err == nil {
			mtime = t.Unix()
		}

		// Parse dates for metadata
		modDate := ""
		if t, err := time.Parse(time.RFC3339, lastEdited); err == nil {
			modDate = t.Format("2006-01-02")
		}
		createdDate := ""
		if t, err := time.Parse(time.RFC3339, createdTime); err == nil {
			createdDate = t.Format("2006-01-02")
		}

		// Build metadata map
		metadata := map[string]string{
			"id":      id,
			"title":   title,
			"type":    objType,
			"date":    modDate,
			"created": createdDate,
		}

		// Generate filename
		filename := n.FormatFilename(filenameFormat, metadata)

		results = append(results, sources.QueryResult{
			ID:       id,
			Filename: filename,
			Metadata: metadata,
			Size:     0, // Unknown until read
			Mtime:    mtime,
		})
	}

	return &sources.QueryResponse{
		Results:       results,
		NextPageToken: nextCursor,
		HasMore:       hasMore,
	}, nil
}

// ReadResult fetches the content of a Notion page by its page ID.
// Returns the page content as markdown.
// This implements the sources.QueryExecutor interface.
func (n *NotionProvider) ReadResult(ctx context.Context, pctx *sources.ProviderContext, resultID string) ([]byte, error) {
	if pctx.Credentials == nil || pctx.Credentials.AccessToken == "" {
		return nil, sources.ErrNotConnected
	}

	// Remove dashes if present to normalize, then add them back
	cleanId := strings.ReplaceAll(resultID, "-", "")
	if len(cleanId) == 32 {
		resultID = fmt.Sprintf("%s-%s-%s-%s-%s", cleanId[0:8], cleanId[8:12], cleanId[12:16], cleanId[16:20], cleanId[20:32])
	}

	return n.fetchPageAsMarkdown(ctx, pctx.Credentials.AccessToken, resultID)
}

// FormatFilename generates a filename from metadata using a format template.
// Supported placeholders: {id}, {title}, {type}, {date}, {created}
// This implements the sources.QueryExecutor interface.
func (n *NotionProvider) FormatFilename(format string, metadata map[string]string) string {
	if format == "" {
		format = "{title}_{id}.md"
	}

	result := format
	for key, value := range metadata {
		placeholder := "{" + key + "}"
		// Sanitize the value for filesystem use
		safeValue := sanitizeNotionTitle(value)
		// Truncate long values (except id)
		if key != "id" && len(safeValue) > 50 {
			safeValue = safeValue[:50]
		}
		result = strings.ReplaceAll(result, placeholder, safeValue)
	}

	// Ensure filename is not empty
	if result == "" || result == ".md" {
		if id, ok := metadata["id"]; ok {
			shortID := id
			if len(shortID) > 8 {
				shortID = shortID[:8]
			}
			result = shortID + ".md"
		} else {
			result = "unknown.md"
		}
	}

	return result
}

// Compile-time interface check for QueryExecutor
var _ sources.QueryExecutor = (*NotionProvider)(nil)
