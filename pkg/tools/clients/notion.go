package clients

import (
	"context"
	"fmt"
	"io"

	"github.com/beam-cloud/airstore/pkg/types"
)

const (
	notionAPIBase            = "https://api.notion.com/v1"
	notionAPIVersion         = "2022-06-28"
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
	payload := map[string]any{
		"children": []map[string]any{
			{
				"object": "block",
				"type":   "paragraph",
				"paragraph": map[string]any{
					"rich_text": []map[string]any{
						{
							"type": "text",
							"text": map[string]any{
								"content": text,
							},
						},
					},
				},
			},
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
