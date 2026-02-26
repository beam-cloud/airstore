package clients

import (
	"context"
	"fmt"
	"io"

	"github.com/beam-cloud/airstore/pkg/types"
)

const (
	linearAPIBase        = "https://api.linear.app/graphql"
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
		linearCmdCreateIssue: func(ctx context.Context, token string, args map[string]any) (any, error) {
			required, err := RequireStringArgs(args, "team_id", "title")
			if err != nil {
				return nil, err
			}
			description := GetStringArg(args, "description", "")
			return l.createIssue(ctx, token, required["team_id"], required["title"], description)
		},
	}, stdout)
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

	payload := map[string]any{
		"query": query,
		"variables": map[string]any{
			"input": input,
		},
	}

	var response struct {
		Data struct {
			IssueCreate struct {
				Success bool `json:"success"`
				Issue   struct {
					ID         string `json:"id"`
					Identifier string `json:"identifier"`
					Title      string `json:"title"`
					URL        string `json:"url"`
				} `json:"issue"`
			} `json:"issueCreate"`
		} `json:"data"`
		Errors []struct {
			Message string `json:"message"`
		} `json:"errors"`
	}
	if err := l.api.RequestJSON(ctx, token, "POST", "", payload, &response); err != nil {
		return nil, err
	}
	if len(response.Errors) > 0 && response.Errors[0].Message != "" {
		return nil, fmt.Errorf("linear API: %s", response.Errors[0].Message)
	}
	if !response.Data.IssueCreate.Success {
		return nil, fmt.Errorf("linear API: issue creation failed")
	}

	return map[string]any{
		"id":         response.Data.IssueCreate.Issue.ID,
		"identifier": response.Data.IssueCreate.Issue.Identifier,
		"title":      response.Data.IssueCreate.Issue.Title,
		"url":        response.Data.IssueCreate.Issue.URL,
		"ok":         true,
	}, nil
}
