package clients

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/beam-cloud/airstore/pkg/types"
)

const (
	teamsGraphBase     = "https://graph.microsoft.com/v1.0"
	teamsCmdPostMessage = "post-message"
)

type TeamsClient struct {
	api *oauthHTTPClient
}

func NewTeamsClient() *TeamsClient {
	return &TeamsClient{
		api: newOAuthHTTPClient("teams", teamsGraphBase, nil),
	}
}

func (t *TeamsClient) Name() types.IntegrationName {
	return types.Teams
}

func (t *TeamsClient) Execute(ctx context.Context, command string, args map[string]any, creds *types.IntegrationCredentials, stdout, _ io.Writer) error {
	return ExecuteOAuthCommand(ctx, "teams", command, args, creds, map[string]OAuthCommandHandler{
		teamsCmdPostMessage: func(ctx context.Context, token string, args map[string]any) (any, error) {
			required, err := RequireStringArgs(args, "team", "channel", "text")
			if err != nil {
				return nil, err
			}
			return t.postMessage(ctx, token, required["team"], required["channel"], required["text"])
		},
	}, stdout)
}

func (t *TeamsClient) postMessage(ctx context.Context, token, team, channel, text string) (map[string]any, error) {
	// Resolve team and channel IDs
	teamID, channelID, err := t.resolveIDs(ctx, token, team, channel)
	if err != nil {
		return nil, err
	}

	payload := map[string]any{
		"body": map[string]any{
			"content": text,
		},
	}

	path := fmt.Sprintf("/teams/%s/channels/%s/messages", teamID, channelID)
	var result map[string]any
	if err := t.api.RequestJSON(ctx, token, "POST", path, payload, &result); err != nil {
		return nil, err
	}

	messageID := getString(result, "id")
	out := map[string]any{
		"team":       team,
		"channel":    channel,
		"message_id": messageID,
		"ok":         true,
	}

	// Build Teams deep link
	if messageID != "" {
		out["url"] = fmt.Sprintf("https://teams.microsoft.com/l/message/%s/%s", channelID, messageID)
	}

	return out, nil
}

func (t *TeamsClient) resolveIDs(ctx context.Context, token, teamName, channelName string) (string, string, error) {
	// List joined teams
	var teamsResult struct {
		Value []struct {
			ID          string `json:"id"`
			DisplayName string `json:"displayName"`
		} `json:"value"`
	}
	if err := t.api.RequestJSON(ctx, token, "GET", "/me/joinedTeams", nil, &teamsResult); err != nil {
		return "", "", fmt.Errorf("list teams: %w", err)
	}

	var teamID string
	for _, tm := range teamsResult.Value {
		if equalFoldTeams(tm.DisplayName, teamName) || tm.ID == teamName {
			teamID = tm.ID
			break
		}
	}
	if teamID == "" {
		return "", "", fmt.Errorf("team not found: %s", teamName)
	}

	// List channels in team
	var channelsResult struct {
		Value []struct {
			ID          string `json:"id"`
			DisplayName string `json:"displayName"`
		} `json:"value"`
	}
	path := fmt.Sprintf("/teams/%s/channels", teamID)
	if err := t.api.RequestJSON(ctx, token, "GET", path, nil, &channelsResult); err != nil {
		return "", "", fmt.Errorf("list channels: %w", err)
	}

	for _, ch := range channelsResult.Value {
		if equalFoldTeams(ch.DisplayName, channelName) || ch.ID == channelName {
			return teamID, ch.ID, nil
		}
	}
	return "", "", fmt.Errorf("channel not found: %s in team %s", channelName, teamName)
}

func equalFoldTeams(a, b string) bool {
	return strings.EqualFold(a, b)
}
