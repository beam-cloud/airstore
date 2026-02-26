package clients

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/beam-cloud/airstore/pkg/types"
)

const (
	linearAPIBase        = "https://api.linear.app/graphql"
	linearCmdListTeams   = "list-teams"
	linearCmdCreateIssue = "create-issue"
)

type LinearClient struct {
	api *oauthHTTPClient
}

func NewLinearClient() *LinearClient {
	return &LinearClient{
		api: newOAuthHTTPClient("linear", linearAPIBase, nil),
	}
}

func (l *LinearClient) Name() types.IntegrationName {
	return types.Linear
}

func (l *LinearClient) Execute(ctx context.Context, command string, args map[string]any, creds *types.IntegrationCredentials, stdout, _ io.Writer) error {
	return ExecuteOAuthCommand(ctx, "linear", command, args, creds, map[string]OAuthCommandHandler{
		linearCmdListTeams: func(ctx context.Context, token string, args map[string]any) (any, error) {
			teams, err := l.listTeams(ctx, token)
			if err != nil {
				return nil, err
			}
			return map[string]any{
				"teams": teams,
				"count": len(teams),
			}, nil
		},
		linearCmdCreateIssue: func(ctx context.Context, token string, args map[string]any) (any, error) {
			required, err := RequireStringArgs(args, "title")
			if err != nil {
				return nil, err
			}
			description := GetStringArg(args, "description", "")
			teamInput := GetStringArg(args, "team_id", "")
			team, err := l.resolveTeam(ctx, token, teamInput)
			if err != nil {
				return nil, err
			}

			issue, err := l.createIssue(ctx, token, team.ID, required["title"], description)
			if err != nil {
				return nil, err
			}
			issue["team_id"] = team.ID
			issue["team_key"] = team.Key
			issue["team_name"] = team.Name
			return issue, nil
		},
	}, stdout)
}

type linearTeam struct {
	ID   string `json:"id"`
	Key  string `json:"key"`
	Name string `json:"name"`
}

func (l *LinearClient) listTeams(ctx context.Context, token string) ([]linearTeam, error) {
	// Keep this query minimal — Linear may reject unsupported args with HTTP 400.
	const gql = `{ teams { nodes { id key name } } }`

	var data struct {
		Teams struct {
			Nodes []linearTeam `json:"nodes"`
		} `json:"teams"`
	}
	if err := l.graphql(ctx, token, gql, nil, &data); err != nil {
		return nil, err
	}
	return data.Teams.Nodes, nil
}

func (l *LinearClient) resolveTeam(ctx context.Context, token, teamInput string) (linearTeam, error) {
	teams, err := l.listTeams(ctx, token)
	if err != nil {
		return linearTeam{}, err
	}
	if len(teams) == 0 {
		return linearTeam{}, fmt.Errorf("linear API: no teams found for this account")
	}

	trimmed := strings.TrimSpace(teamInput)
	if trimmed == "" {
		return teams[0], nil
	}

	for _, team := range teams {
		if team.ID == trimmed || strings.EqualFold(team.Key, trimmed) || strings.EqualFold(team.Name, trimmed) {
			return team, nil
		}
	}

	available := make([]string, 0, len(teams))
	for i, team := range teams {
		if i >= 8 {
			break
		}
		if team.Key != "" {
			available = append(available, team.Key)
		} else if team.Name != "" {
			available = append(available, team.Name)
		} else {
			available = append(available, team.ID)
		}
	}
	return linearTeam{}, fmt.Errorf("linear API: team_id %q not found (available: %s)", trimmed, strings.Join(available, ", "))
}

func (l *LinearClient) createIssue(ctx context.Context, token, teamID, title, description string) (map[string]any, error) {
	query := `
mutation CreateIssue($input: IssueCreateInput!) {
  issueCreate(input: $input) {
    success
    issue {
      id
      identifier
      title
      url
    }
  }
}`

	input := map[string]any{
		"teamId": teamID,
		"title":  title,
	}
	if description != "" {
		input["description"] = description
	}

	var data struct {
		IssueCreate struct {
			Success bool `json:"success"`
			Issue   struct {
				ID         string `json:"id"`
				Identifier string `json:"identifier"`
				Title      string `json:"title"`
				URL        string `json:"url"`
			} `json:"issue"`
		} `json:"issueCreate"`
	}
	if err := l.graphql(ctx, token, query, map[string]any{"input": input}, &data); err != nil {
		return nil, err
	}
	if !data.IssueCreate.Success {
		return nil, fmt.Errorf("linear API: issue creation failed")
	}

	return map[string]any{
		"id":         data.IssueCreate.Issue.ID,
		"identifier": data.IssueCreate.Issue.Identifier,
		"title":      data.IssueCreate.Issue.Title,
		"url":        data.IssueCreate.Issue.URL,
		"ok":         true,
	}, nil
}

func (l *LinearClient) graphql(ctx context.Context, token, query string, variables map[string]any, out any) error {
	payload := map[string]any{
		"query": query,
	}
	if variables != nil {
		payload["variables"] = variables
	}

	var envelope struct {
		Data   json.RawMessage `json:"data"`
		Errors []struct {
			Message string `json:"message"`
		} `json:"errors"`
	}
	if err := l.api.RequestJSON(ctx, token, "POST", "", payload, &envelope); err != nil {
		return err
	}
	if len(envelope.Errors) > 0 && envelope.Errors[0].Message != "" {
		return fmt.Errorf("linear API: %s", envelope.Errors[0].Message)
	}
	if out == nil || len(envelope.Data) == 0 || string(envelope.Data) == "null" {
		return nil
	}
	return json.Unmarshal(envelope.Data, out)
}
