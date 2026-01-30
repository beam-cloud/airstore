package clients

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/beam-cloud/airstore/pkg/types"
)

const (
	NotionAPIBase    = "https://api.notion.com/v1"
	NotionAPIVersion = "2022-06-28"
)

// API call counter for metrics
var notionAPICallCount int64

// GetNotionAPICallCount returns the current API call count
func GetNotionAPICallCount() int64 {
	return atomic.LoadInt64(&notionAPICallCount)
}

// ResetNotionAPICallCount resets the API call counter
func ResetNotionAPICallCount() {
	atomic.StoreInt64(&notionAPICallCount, 0)
}

// NotionPage represents a Notion page or database
type NotionPage struct {
	ID             string
	Type           string // "page" or "database"
	Title          string
	URL            string
	LastEditedTime time.Time
}

// NotionBlock represents a Notion block
type NotionBlock struct {
	ID      string
	Type    string
	Content map[string]any
}

// NotionClient provides shared Notion API functionality
type NotionClient struct {
	HTTPClient *http.Client
}

// NewNotionClient creates a new Notion API client
func NewNotionClient() *NotionClient {
	return &NotionClient{
		HTTPClient: &http.Client{Timeout: 60 * time.Second},
	}
}

// Integration returns the integration name
func (c *NotionClient) Integration() types.ToolName {
	return types.ToolNotion
}

// Request makes a GET request to the Notion API
func (c *NotionClient) Request(ctx context.Context, token, path string, result any) error {
	atomic.AddInt64(&notionAPICallCount, 1)

	url := NotionAPIBase + path
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return err
	}

	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Notion-Version", NotionAPIVersion)
	req.Header.Set("Accept", "application/json")

	resp, err := c.HTTPClient.Do(req)
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

// PostRequest makes a POST request to the Notion API
func (c *NotionClient) PostRequest(ctx context.Context, token, path string, body any, result any) error {
	atomic.AddInt64(&notionAPICallCount, 1)

	jsonBody, err := json.Marshal(body)
	if err != nil {
		return err
	}

	url := NotionAPIBase + path
	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(jsonBody))
	if err != nil {
		return err
	}

	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Notion-Version", NotionAPIVersion)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	resp, err := c.HTTPClient.Do(req)
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

// Search searches for all accessible content
func (c *NotionClient) Search(ctx context.Context, token string, query string, maxResults int) ([]*NotionPage, error) {
	body := map[string]any{
		"page_size": maxResults,
		"sort": map[string]string{
			"direction": "descending",
			"timestamp": "last_edited_time",
		},
	}
	if query != "" {
		body["query"] = query
	}

	var result map[string]any
	if err := c.PostRequest(ctx, token, "/search", body, &result); err != nil {
		return nil, err
	}

	rawResults, _ := result["results"].([]any)
	pages := make([]*NotionPage, 0, len(rawResults))

	for _, r := range rawResults {
		obj, ok := r.(map[string]any)
		if !ok {
			continue
		}

		page := c.ParsePage(obj)
		if page != nil {
			pages = append(pages, page)
		}
	}

	return pages, nil
}

// GetPage fetches a single page
func (c *NotionClient) GetPage(ctx context.Context, token, pageID string) (*NotionPage, error) {
	path := fmt.Sprintf("/pages/%s", pageID)

	var result map[string]any
	if err := c.Request(ctx, token, path, &result); err != nil {
		return nil, err
	}

	return c.ParsePage(result), nil
}

// GetPageBlocks fetches the blocks of a page
func (c *NotionClient) GetPageBlocks(ctx context.Context, token, pageID string) ([]*NotionBlock, error) {
	path := fmt.Sprintf("/blocks/%s/children?page_size=100", pageID)

	var result map[string]any
	if err := c.Request(ctx, token, path, &result); err != nil {
		return nil, err
	}

	rawResults, _ := result["results"].([]any)
	blocks := make([]*NotionBlock, 0, len(rawResults))

	for _, r := range rawResults {
		block, ok := r.(map[string]any)
		if !ok {
			continue
		}

		blocks = append(blocks, &NotionBlock{
			ID:      getString(block, "id"),
			Type:    getString(block, "type"),
			Content: block,
		})
	}

	return blocks, nil
}

// ParsePage extracts structured data from a Notion API response
func (c *NotionClient) ParsePage(obj map[string]any) *NotionPage {
	page := &NotionPage{
		ID:   getString(obj, "id"),
		Type: getString(obj, "object"),
		URL:  getString(obj, "url"),
	}

	// Parse last edited time
	if lastEdited, ok := obj["last_edited_time"].(string); ok {
		page.LastEditedTime, _ = time.Parse(time.RFC3339, lastEdited)
	}

	// Extract title
	page.Title = c.ExtractTitle(obj)

	return page
}

// ExtractTitle extracts the title from a Notion object
func (c *NotionClient) ExtractTitle(obj map[string]any) string {
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

// BlocksToText converts Notion blocks to plain text
func (c *NotionClient) BlocksToText(blocks []*NotionBlock) string {
	var sb strings.Builder

	for _, block := range blocks {
		blockType := block.Type
		if blockType == "" {
			continue
		}

		// Get the block content
		blockContent, ok := block.Content[blockType].(map[string]any)
		if !ok {
			continue
		}

		// Extract rich text
		text := c.ExtractRichText(blockContent)
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
				sb.WriteString("- ")
				sb.WriteString(text)
				sb.WriteString("\n")
			case "numbered_list_item":
				sb.WriteString("1. ")
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

// BlockToMarkdown converts a single block to markdown
func (c *NotionClient) BlockToMarkdown(block *NotionBlock) string {
	blockType := block.Type
	if blockType == "" {
		return ""
	}

	blockContent, ok := block.Content[blockType].(map[string]any)
	if !ok {
		return ""
	}

	text := c.ExtractRichText(blockContent)
	if text == "" {
		return ""
	}

	switch blockType {
	case "heading_1":
		return "# " + text
	case "heading_2":
		return "## " + text
	case "heading_3":
		return "### " + text
	case "bulleted_list_item":
		return "- " + text
	case "numbered_list_item":
		return "1. " + text
	case "to_do":
		checked := false
		if c, ok := blockContent["checked"].(bool); ok {
			checked = c
		}
		if checked {
			return "[x] " + text
		}
		return "[ ] " + text
	case "code":
		return "```\n" + text + "\n```"
	case "quote":
		return "> " + text
	default:
		return text
	}
}

// ExtractRichText extracts plain text from a Notion rich text array
func (c *NotionClient) ExtractRichText(blockContent map[string]any) string {
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

// CleanID removes dashes from a Notion ID
func CleanNotionID(id string) string {
	return strings.ReplaceAll(id, "-", "")
}
