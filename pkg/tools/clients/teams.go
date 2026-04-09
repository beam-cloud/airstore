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
	teamID, err := t.findByName(ctx, token, "/me/joinedTeams", teamName)
	if err != nil {
		return "", "", fmt.Errorf("team %w", err)
	}

	channelID, err := t.findByName(ctx, token, fmt.Sprintf("/teams/%s/channels", teamID), channelName)
	if err != nil {
		return "", "", fmt.Errorf("channel %w", err)
	}

	return teamID, channelID, nil
}

type graphListEntry struct {
	ID          string `json:"id"`
	DisplayName string `json:"displayName"`
}

type graphListPage struct {
	Value    []graphListEntry `json:"value"`
	NextLink string           `json:"@odata.nextLink"`
}

// findByName pages through a Graph API list endpoint until it finds an entry
// matching name (case-insensitive) or ID, or exhausts all pages.
func (t *TeamsClient) findByName(ctx context.Context, token, path, name string) (string, error) {
	const maxPages = 10
	for i := 0; i < maxPages; i++ {
		var page graphListPage
		if err := t.api.RequestJSON(ctx, token, "GET", path, nil, &page); err != nil {
			return "", err
		}
		for _, entry := range page.Value {
			if strings.EqualFold(entry.DisplayName, name) || entry.ID == name {
				return entry.ID, nil
			}
		}
		if page.NextLink == "" {
			break
		}
		path = page.NextLink
	}
	return "", fmt.Errorf("not found: %s", name)
}
