package clients

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/beam-cloud/airstore/pkg/types"
)

const (
	notionAPIBase            = "https://api.notion.com/v1"
	notionAPIVersion         = "2022-06-28"
	notionCmdList            = "list"
	notionCmdSearch          = "search"
	notionCmdCreatePage      = "create-page"
	notionCmdAppendParagraph = "append-paragraph"
)

type NotionClient struct {
	api *oauthHTTPClient
}

func NewNotionClient() *NotionClient {
	return &NotionClient{
		api: newOAuthHTTPClient("notion", notionAPIBase, map[string]string{
			"Notion-Version": notionAPIVersion,
		}),
	}
}

func (n *NotionClient) Name() types.IntegrationName {
	return types.Notion
}

func (n *NotionClient) Execute(ctx context.Context, command string, args map[string]any, creds *types.IntegrationCredentials, stdout, _ io.Writer) error {
	return ExecuteOAuthCommand(ctx, "notion", command, args, creds, map[string]OAuthCommandHandler{
		notionCmdList: func(ctx context.Context, token string, args map[string]any) (any, error) {
			limit := normalizeNotionPageSize(GetIntArg(args, "limit", 20))
			return n.searchContent(ctx, token, "", limit)
		},
		notionCmdSearch: func(ctx context.Context, token string, args map[string]any) (any, error) {
			required, err := RequireStringArgs(args, "query")
			if err != nil {
				return nil, err
			}
			limit := normalizeNotionPageSize(GetIntArg(args, "limit", 20))
			return n.searchContent(ctx, token, required["query"], limit)
		},
		notionCmdCreatePage: func(ctx context.Context, token string, args map[string]any) (any, error) {
			required, err := RequireStringArgs(args, "title")
			if err != nil {
				return nil, err
			}
			parentType := strings.ToLower(strings.TrimSpace(GetStringArg(args, "parent_type", "auto")))
			if parentType == "" {
				parentType = "auto"
			}

			databaseID := strings.TrimSpace(GetStringArg(args, "database_id", ""))
			pageID := strings.TrimSpace(GetStringArg(args, "page_id", ""))
			parentID := strings.TrimSpace(GetStringArg(args, "parent_id", ""))
			if parentID == "" {
				switch {
				case databaseID != "":
					parentID = databaseID
					if parentType == "auto" {
						parentType = "database"
					}
				case pageID != "":
					parentID = pageID
					if parentType == "auto" {
						parentType = "page"
					}
				}
			}

			content := strings.TrimSpace(GetStringArg(args, "content", ""))
			return n.createPage(ctx, token, required["title"], parentID, parentType, content)
		},
		notionCmdAppendParagraph: func(ctx context.Context, token string, args map[string]any) (any, error) {
			required, err := RequireStringArgs(args, "block_id", "text")
			if err != nil {
				return nil, err
			}
			return n.appendParagraph(ctx, token, required["block_id"], required["text"])
		},
	}, stdout)
}

func (n *NotionClient) appendParagraph(ctx context.Context, token, blockID, text string) (map[string]any, error) {
	blockID = normalizeNotionID(blockID)
	payload := map[string]any{
		"children": []map[string]any{
			notionParagraphBlock(text),
		},
	}

	var result map[string]any
	path := fmt.Sprintf("/blocks/%s/children", blockID)
	if err := n.api.RequestJSON(ctx, token, "PATCH", path, payload, &result); err != nil {
		return nil, err
	}

	out := map[string]any{
		"block_id": blockID,
		"ok":       true,
	}
	if results, ok := result["results"].([]any); ok {
		out["appended"] = len(results)
	}
	return out, nil
}

func (n *NotionClient) searchContent(ctx context.Context, token, query string, limit int) (map[string]any, error) {
	limit = normalizeNotionPageSize(limit)

	payload := map[string]any{
		"page_size": limit,
		"sort": map[string]string{
			"direction": "descending",
			"timestamp": "last_edited_time",
		},
	}
	if strings.TrimSpace(query) != "" {
		payload["query"] = strings.TrimSpace(query)
	}

	var result map[string]any
	if err := n.api.RequestJSON(ctx, token, "POST", "/search", payload, &result); err != nil {
		return nil, err
	}

	rawResults, _ := result["results"].([]any)
	items := make([]map[string]any, 0, len(rawResults))
	for _, r := range rawResults {
		item, ok := r.(map[string]any)
		if !ok {
			continue
		}
		items = append(items, map[string]any{
			"id":               getString(item, "id"),
			"object":           getString(item, "object"),
			"title":            notionExtractTitle(item),
			"url":              getString(item, "url"),
			"last_edited_time": getString(item, "last_edited_time"),
			"created_time":     getString(item, "created_time"),
		})
	}

	out := map[string]any{
		"results": items,
		"count":   len(items),
		"limit":   limit,
	}
	if strings.TrimSpace(query) != "" {
		out["query"] = strings.TrimSpace(query)
	}
	if hasMore, ok := result["has_more"].(bool); ok {
		out["has_more"] = hasMore
	}
	if nextCursor, ok := result["next_cursor"].(string); ok && strings.TrimSpace(nextCursor) != "" {
		out["next_cursor"] = nextCursor
	}
	return out, nil
}

func (n *NotionClient) createPage(
	ctx context.Context,
	token string,
	title string,
	parentID string,
	parentType string,
	content string,
) (map[string]any, error) {
	parentType = strings.ToLower(strings.TrimSpace(parentType))
	if parentType == "" {
		parentType = "auto"
	}
	switch parentType {
	case "auto", "workspace", "page", "database":
	default:
		return nil, fmt.Errorf("parent_type must be one of: auto, workspace, page, database")
	}

	parentID = normalizeNotionID(parentID)
	title = strings.TrimSpace(title)
	content = strings.TrimSpace(content)

	parent := map[string]any{}
	properties := map[string]any{}
	pageTitle := map[string]any{"title": notionRichText(title)}

	switch parentType {
	case "workspace":
		parent["workspace"] = true
		properties["title"] = pageTitle
	case "page":
		if parentID == "" {
			return nil, fmt.Errorf("parent_id is required when parent_type=page")
		}
		parent["page_id"] = parentID
		properties["title"] = pageTitle
	case "database":
		if parentID == "" {
			return nil, fmt.Errorf("parent_id is required when parent_type=database")
		}
		titleProperty, err := n.databaseTitlePropertyName(ctx, token, parentID)
		if err != nil {
			return nil, err
		}
		parent["database_id"] = parentID
		properties[titleProperty] = pageTitle
	default: // auto
		if parentID == "" {
			parent["workspace"] = true
			properties["title"] = pageTitle
		} else if titleProperty, err := n.databaseTitlePropertyName(ctx, token, parentID); err == nil {
			parent["database_id"] = parentID
			properties[titleProperty] = pageTitle
		} else {
			parent["page_id"] = parentID
			properties["title"] = pageTitle
		}
	}

	payload := map[string]any{
		"parent":     parent,
		"properties": properties,
	}
	if content != "" {
		payload["children"] = []map[string]any{notionParagraphBlock(content)}
	}

	var result map[string]any
	if err := n.api.RequestJSON(ctx, token, "POST", "/pages", payload, &result); err != nil {
		return nil, err
	}

	out := map[string]any{
		"page_id": getString(result, "id"),
		"title":   notionExtractTitle(result),
		"url":     getString(result, "url"),
		"ok":      true,
	}
	if objectType := getString(result, "object"); objectType != "" {
		out["object"] = objectType
	}
	return out, nil
}

func (n *NotionClient) databaseTitlePropertyName(ctx context.Context, token, databaseID string) (string, error) {
	databaseID = normalizeNotionID(databaseID)
	if databaseID == "" {
		return "", fmt.Errorf("database_id is required")
	}

	var result map[string]any
	path := fmt.Sprintf("/databases/%s", databaseID)
	if err := n.api.RequestJSON(ctx, token, "GET", path, nil, &result); err != nil {
		return "", err
	}

	properties, ok := result["properties"].(map[string]any)
	if !ok {
		return "", fmt.Errorf("database %s has no properties", databaseID)
	}
	for propertyName, raw := range properties {
		prop, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		if getString(prop, "type") == "title" {
			return propertyName, nil
		}
	}
	return "", fmt.Errorf("database %s has no title property", databaseID)
}

func notionExtractTitle(item map[string]any) string {
	if item == nil {
		return "Untitled"
	}

	if titleBlocks, ok := item["title"].([]any); ok {
		if title := notionPlainText(titleBlocks); title != "" {
			return title
		}
	}

	if properties, ok := item["properties"].(map[string]any); ok {
		for _, raw := range properties {
			prop, ok := raw.(map[string]any)
			if !ok {
				continue
			}
			if getString(prop, "type") != "title" {
				continue
			}
			if titleBlocks, ok := prop["title"].([]any); ok {
				if title := notionPlainText(titleBlocks); title != "" {
					return title
				}
			}
		}
	}

	return "Untitled"
}

func notionPlainText(richText []any) string {
	if len(richText) == 0 {
		return ""
	}
	var sb strings.Builder
	for _, raw := range richText {
		block, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		if plain := strings.TrimSpace(getString(block, "plain_text")); plain != "" {
			sb.WriteString(plain)
			continue
		}
		text, ok := block["text"].(map[string]any)
		if !ok {
			continue
		}
		sb.WriteString(getString(text, "content"))
	}
	return strings.TrimSpace(sb.String())
}

func notionRichText(content string) []map[string]any {
	return []map[string]any{
		{
			"type": "text",
			"text": map[string]any{
				"content": content,
			},
		},
	}
}

func notionParagraphBlock(text string) map[string]any {
	return map[string]any{
		"object": "block",
		"type":   "paragraph",
		"paragraph": map[string]any{
			"rich_text": notionRichText(text),
		},
	}
}

func normalizeNotionID(id string) string {
	trimmed := strings.TrimSpace(id)
	if trimmed == "" {
		return ""
	}

	compact := strings.ReplaceAll(trimmed, "-", "")
	if len(compact) != 32 {
		return trimmed
	}
	for _, ch := range compact {
		if !isHexChar(ch) {
			return trimmed
		}
	}

	return fmt.Sprintf(
		"%s-%s-%s-%s-%s",
		compact[0:8],
		compact[8:12],
		compact[12:16],
		compact[16:20],
		compact[20:32],
	)
}

func isHexChar(ch rune) bool {
	return (ch >= '0' && ch <= '9') ||
		(ch >= 'a' && ch <= 'f') ||
		(ch >= 'A' && ch <= 'F')
}

func normalizeNotionPageSize(limit int) int {
	if limit <= 0 {
		return 20
	}
	if limit > 100 {
		return 100
	}
	return limit
}
