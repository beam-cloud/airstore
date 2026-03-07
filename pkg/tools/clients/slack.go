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
	ch := getString(result, "channel")
	ts := getString(result, "ts")
	out := map[string]any{
		"channel": ch,
		"ts":      ts,
		"ok":      true,
	}
	if ch != "" && ts != "" {
		if link := s.getPermalink(ctx, token, ch, ts); link != "" {
			out["url"] = link
		}
	}
	return out, nil
}

func (s *SlackClient) getPermalink(ctx context.Context, token, channel, ts string) string {
	path := fmt.Sprintf("/chat.getPermalink?channel=%s&message_ts=%s", channel, ts)
	var result map[string]any
	if s.api.RequestJSON(ctx, token, "GET", path, nil, &result) != nil {
		return ""
	}
	if ok, _ := result["ok"].(bool); !ok {
		return ""
	}
	return getString(result, "permalink")
}
