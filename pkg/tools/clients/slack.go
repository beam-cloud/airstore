package clients

import (
	"context"
	"fmt"
	"io"

	"github.com/beam-cloud/airstore/pkg/types"
)

const (
	slackAPIBase        = "https://slack.com/api"
	slackCmdPostMessage = "post-message"
)

type SlackClient struct {
	api *oauthHTTPClient
}

func NewSlackClient() *SlackClient {
	return &SlackClient{
		api: newOAuthHTTPClient("slack", slackAPIBase, nil),
	}
}

func (s *SlackClient) Name() types.IntegrationName {
	return types.Slack
}

func (s *SlackClient) Execute(ctx context.Context, command string, args map[string]any, creds *types.IntegrationCredentials, stdout, _ io.Writer) error {
	return ExecuteOAuthCommand(ctx, "slack", command, args, creds, map[string]OAuthCommandHandler{
		slackCmdPostMessage: func(ctx context.Context, token string, args map[string]any) (any, error) {
			required, err := RequireStringArgs(args, "channel", "text")
			if err != nil {
				return nil, err
			}
			return s.postMessage(ctx, token, required["channel"], required["text"])
		},
	}, stdout)
}

func (s *SlackClient) postMessage(ctx context.Context, token, channel, text string) (map[string]any, error) {
	payload := map[string]any{
		"channel": channel,
		"text":    text,
	}
	var result map[string]any
	if err := s.api.RequestJSON(ctx, token, "POST", "/chat.postMessage", payload, &result); err != nil {
		return nil, err
	}
	if ok, _ := result["ok"].(bool); !ok {
		if apiErr := getString(result, "error"); apiErr != "" {
			return nil, fmt.Errorf("slack API: %s", apiErr)
		}
		return nil, fmt.Errorf("slack API: request failed")
	}
	return map[string]any{
		"channel": getString(result, "channel"),
		"ts":      getString(result, "ts"),
		"ok":      true,
	}, nil
}
