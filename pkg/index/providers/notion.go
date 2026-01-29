package providers

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/beam-cloud/airstore/pkg/index"
	"github.com/beam-cloud/airstore/pkg/types"
	"github.com/rs/zerolog/log"
)

const (
	notionAPIBase    = "https://api.notion.com/v1"
	notionAPIVersion = "2022-06-28"
)

// NotionIndexProvider implements IndexProvider for Notion.
// It syncs pages and databases from Notion into the local index.
type NotionIndexProvider struct {
	httpClient *http.Client
	apiCalls   int64
}

// NewNotionIndexProvider creates a new Notion index provider
func NewNotionIndexProvider() *NotionIndexProvider {
	return &NotionIndexProvider{
		httpClient: &http.Client{Timeout: 60 * time.Second},
	}
}

// Integration returns the integration name
func (n *NotionIndexProvider) Integration() string {
	return types.ToolNotion.String()
}

// SyncInterval returns the recommended sync interval
func (n *NotionIndexProvider) SyncInterval() time.Duration {
	return 60 * time.Second
}

// Sync fetches pages and databases from Notion and updates the index
func (n *NotionIndexProvider) Sync(ctx context.Context, store index.IndexStore, creds *types.IntegrationCredentials, since time.Time) error {
	if creds == nil || creds.AccessToken == "" {
		return fmt.Errorf("no credentials provided")
	}

	token := creds.AccessToken
	log.Info().Time("since", since).Msg("starting notion index sync")

	// Search for all content
	items, err := n.searchAll(ctx, token)
	if err != nil {
		return fmt.Errorf("failed to search notion: %w", err)
	}

	if len(items) == 0 {
		log.Info().Msg("no notion items to sync")
		return nil
	}

	// Filter out items we already have that haven't changed
	var newItems []notionItem
	for _, item := range items {
		existing, err := store.Get(ctx, n.Integration(), item.ID)
		if err == nil && existing != nil {
			// Check if item was modified since we last indexed it
			if !item.LastEditedTime.After(existing.ModTime) {
				continue // Item hasn't changed, skip it
			}
		}
		newItems = append(newItems, item)
	}

	log.Info().
		Int("total_items", len(items)).
		Int("already_indexed", len(items)-len(newItems)).
		Int("new_or_modified", len(newItems)).
		Msg("notion sync - filtering unchanged items")

	if len(newItems) == 0 {
		log.Info().Msg("all notion items already indexed and unchanged")
		return nil
	}

	// Sync items with concurrency limit
	var wg sync.WaitGroup
	semaphore := make(chan struct{}, 5) // Lower concurrency for Notion
	errors := make(chan error, len(newItems))

	for _, item := range newItems {
		wg.Add(1)
		go func(i notionItem) {
			defer wg.Done()
			semaphore <- struct{}{}
			defer func() { <-semaphore }()

			if err := n.syncItem(ctx, store, token, i); err != nil {
				log.Warn().Err(err).Str("id", i.ID).Str("type", i.Type).Msg("failed to sync notion item")
				errors <- err
			}
		}(item)
	}

	wg.Wait()
	close(errors)

	errorCount := 0
	for range errors {
		errorCount++
	}

	log.Info().
		Int("synced", len(newItems)-errorCount).
		Int("errors", errorCount).
		Int64("api_calls", atomic.LoadInt64(&n.apiCalls)).
		Msg("notion sync completed")

	return nil
}

// notionItem represents a Notion page or database
type notionItem struct {
	ID           string
	Type         string // "page" or "database"
	Title        string
	URL          string
	LastEditedTime time.Time
}

// searchAll searches for all accessible content in Notion (paginated)
func (n *NotionIndexProvider) searchAll(ctx context.Context, token string) ([]notionItem, error) {
	var allItems []notionItem
	var startCursor string

	for {
		body := map[string]any{
			"page_size": 100,
			"sort": map[string]string{
				"direction": "descending",
				"timestamp": "last_edited_time",
			},
		}
		if startCursor != "" {
			body["start_cursor"] = startCursor
		}

		var result map[string]any
		if err := n.postRequest(ctx, token, "/search", body, &result); err != nil {
			return nil, err
		}

		rawResults, _ := result["results"].([]any)

		for _, r := range rawResults {
			obj, ok := r.(map[string]any)
			if !ok {
				continue
			}

			item := notionItem{
				ID:   getString(obj, "id"),
				Type: getString(obj, "object"),
				URL:  getString(obj, "url"),
			}

			// Parse last edited time
			if lastEdited, ok := obj["last_edited_time"].(string); ok {
				item.LastEditedTime, _ = time.Parse(time.RFC3339, lastEdited)
			}

			// Extract title
			item.Title = n.extractTitle(obj)

			allItems = append(allItems, item)
		}

		log.Info().
			Int("page_count", len(rawResults)).
			Int("total_so_far", len(allItems)).
			Msg("fetched notion pages")

		// Check for more results
		hasMore, _ := result["has_more"].(bool)
		if !hasMore {
			break
		}

		nextCursor, _ := result["next_cursor"].(string)
		if nextCursor == "" {
			break
		}
		startCursor = nextCursor
	}

	return allItems, nil
}

// extractTitle extracts the title from a Notion object
func (n *NotionIndexProvider) extractTitle(obj map[string]any) string {
	objType := getString(obj, "object")

	// For databases, title is a top-level array
	if objType == "database" {
		if titleArr, ok := obj["title"].([]any); ok && len(titleArr) > 0 {
			if titleObj, ok := titleArr[0].(map[string]any); ok {
				if text, ok := titleObj["plain_text"].(string); ok {
					return text
				}
			}
		}
	}

	// For pages, title is in properties
	if props, ok := obj["properties"].(map[string]any); ok {
		// Try common title property names
		for _, propName := range []string{"title", "Title", "Name", "name"} {
			if prop, ok := props[propName].(map[string]any); ok {
				if titleArr, ok := prop["title"].([]any); ok && len(titleArr) > 0 {
					if titleObj, ok := titleArr[0].(map[string]any); ok {
						if text, ok := titleObj["plain_text"].(string); ok {
							return text
						}
					}
				}
			}
		}
	}

	return "Untitled"
}

// syncItem syncs a single Notion item to the index
func (n *NotionIndexProvider) syncItem(ctx context.Context, store index.IndexStore, token string, item notionItem) error {
	var content string
	var path string

	cleanID := strings.ReplaceAll(item.ID, "-", "")

	switch item.Type {
	case "page":
		// Fetch page content as blocks
		blocks, err := n.getPageBlocks(ctx, token, item.ID)
		if err != nil {
			log.Debug().Err(err).Str("page_id", item.ID).Msg("failed to get page blocks")
		}
		content = n.blocksToText(blocks)
		path = fmt.Sprintf("pages/%s.md", cleanID)

	case "database":
		// For databases, we just index the schema/title
		content = fmt.Sprintf("Database: %s\n", item.Title)
		path = fmt.Sprintf("databases/%s/schema.json", cleanID)
	}

	// Build metadata
	metadata := map[string]any{
		"id":               item.ID,
		"type":             item.Type,
		"title":            item.Title,
		"url":              item.URL,
		"last_edited_time": item.LastEditedTime,
	}
	metadataJSON, _ := json.Marshal(metadata)

	parentPath := "pages"
	if item.Type == "database" {
		parentPath = "databases"
	}

	entry := &index.IndexEntry{
		Integration: types.ToolNotion.String(),
		EntityID:    item.ID,
		Path:        path,
		ParentPath:  parentPath,
		Name:        sanitizeFilename(item.Title),
		Type:        index.EntryTypeFile,
		Size:        int64(len(content)),
		ModTime:     item.LastEditedTime,
		Title:       item.Title,
		Body:        content,
		Metadata:    string(metadataJSON),
	}

	if err := store.Upsert(ctx, entry); err != nil {
		return err
	}

	// Create virtual directory entries for parent paths
	ensureParentDirs(ctx, store, types.ToolNotion.String(), path)

	return nil
}

// getPageBlocks fetches the block children of a page
func (n *NotionIndexProvider) getPageBlocks(ctx context.Context, token, pageID string) ([]map[string]any, error) {
	path := fmt.Sprintf("/blocks/%s/children?page_size=100", pageID)

	var result map[string]any
	if err := n.request(ctx, token, path, &result); err != nil {
		return nil, err
	}

	rawResults, _ := result["results"].([]any)

	blocks := make([]map[string]any, 0, len(rawResults))
	for _, r := range rawResults {
		if block, ok := r.(map[string]any); ok {
			blocks = append(blocks, block)
		}
	}

	return blocks, nil
}

// blocksToText converts Notion blocks to plain text
func (n *NotionIndexProvider) blocksToText(blocks []map[string]any) string {
	var sb strings.Builder

	for _, block := range blocks {
		blockType := getString(block, "type")
		if blockType == "" {
			continue
		}

		// Get the block content
		blockContent, ok := block[blockType].(map[string]any)
		if !ok {
			continue
		}

		// Extract rich text
		text := n.extractRichText(blockContent)
		if text != "" {
			// Add appropriate formatting based on block type
			switch blockType {
			case "heading_1":
				sb.WriteString("# ")
				sb.WriteString(text)
				sb.WriteString("\n\n")
			case "heading_2":
				sb.WriteString("## ")
				sb.WriteString(text)
				sb.WriteString("\n\n")
			case "heading_3":
				sb.WriteString("### ")
				sb.WriteString(text)
				sb.WriteString("\n\n")
			case "bulleted_list_item":
				sb.WriteString("• ")
				sb.WriteString(text)
				sb.WriteString("\n")
			case "numbered_list_item":
				sb.WriteString("- ")
				sb.WriteString(text)
				sb.WriteString("\n")
			case "to_do":
				checked := false
				if c, ok := blockContent["checked"].(bool); ok {
					checked = c
				}
				if checked {
					sb.WriteString("[x] ")
				} else {
					sb.WriteString("[ ] ")
				}
				sb.WriteString(text)
				sb.WriteString("\n")
			case "code":
				sb.WriteString("```\n")
				sb.WriteString(text)
				sb.WriteString("\n```\n\n")
			case "quote":
				sb.WriteString("> ")
				sb.WriteString(text)
				sb.WriteString("\n\n")
			default:
				sb.WriteString(text)
				sb.WriteString("\n\n")
			}
		}
	}

	return strings.TrimSpace(sb.String())
}

// extractRichText extracts plain text from a Notion rich text array
func (n *NotionIndexProvider) extractRichText(blockContent map[string]any) string {
	// Try common rich text field names
	for _, fieldName := range []string{"rich_text", "text", "caption"} {
		if richText, ok := blockContent[fieldName].([]any); ok && len(richText) > 0 {
			var texts []string
			for _, rt := range richText {
				if rtMap, ok := rt.(map[string]any); ok {
					if text, ok := rtMap["plain_text"].(string); ok {
						texts = append(texts, text)
					}
				}
			}
			return strings.Join(texts, "")
		}
	}
	return ""
}

// FetchContent retrieves content for an entity ID
func (n *NotionIndexProvider) FetchContent(ctx context.Context, creds *types.IntegrationCredentials, entityID string) ([]byte, error) {
	if creds == nil || creds.AccessToken == "" {
		return nil, fmt.Errorf("no credentials provided")
	}

	token := creds.AccessToken

	// Get page blocks and convert to text
	blocks, err := n.getPageBlocks(ctx, token, entityID)
	if err != nil {
		return nil, err
	}

	content := n.blocksToText(blocks)
	return []byte(content), nil
}

// request makes a GET request to the Notion API
func (n *NotionIndexProvider) request(ctx context.Context, token, path string, result any) error {
	atomic.AddInt64(&n.apiCalls, 1)

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

	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("notion API error %d: %s", resp.StatusCode, string(body))
	}

	return json.NewDecoder(resp.Body).Decode(result)
}

// postRequest makes a POST request to the Notion API
func (n *NotionIndexProvider) postRequest(ctx context.Context, token, path string, body any, result any) error {
	atomic.AddInt64(&n.apiCalls, 1)

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

	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("notion API error %d: %s", resp.StatusCode, string(body))
	}

	return json.NewDecoder(resp.Body).Decode(result)
}
