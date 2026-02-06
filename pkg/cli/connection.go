package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os/exec"
	"runtime"
	"strings"
	"time"

	pb "github.com/beam-cloud/airstore/proto"
	"github.com/spf13/cobra"
)

var (
	connToken  string
	connAPIKey string
	connMember string
	connScope  string
)

var connectionCmd = &cobra.Command{
	Use:     "connection",
	Aliases: []string{"conn"},
	Short:   "Manage integration connections",
}

var connectionAddCmd = &cobra.Command{
	Use:   "add <workspace_id> <type>",
	Short: "Add an integration connection",
	Long: `Add an integration connection to a workspace.

Supported integration types:
  github    - GitHub (use --token with personal access token)
  gmail     - Gmail (use --token with OAuth access token)
  notion    - Notion (use --token with integration token)
  gdrive    - Google Drive (use --token with OAuth access token)
  weather   - OpenWeatherMap (use --api-key)
  exa       - Exa AI search (use --api-key)
  posthog   - PostHog analytics (use --api-key with personal API key)

Examples:
  airstore connection add <ws> github --token ghp_xxxxxxxxxxxx
  airstore connection add <ws> notion --token secret_xxxxxxxxxxxx
  airstore connection add <ws> weather --api-key abc123
  airstore connection add <ws> github --token ghp_xxx --member <member_id>  # Personal connection`,
	Args: cobra.ExactArgs(2),
	RunE: func(cmd *cobra.Command, args []string) error {
		workspaceId := args[0]
		integrationType := strings.ToLower(args[1])

		// Validate integration type
		switch integrationType {
		case "github":
			if connToken == "" {
				PrintErrorMsg("GitHub requires --token (personal access token)")
				return nil
			}
			// Validate GitHub PAT format
			if !strings.HasPrefix(connToken, "ghp_") && !strings.HasPrefix(connToken, "github_pat_") {
				PrintWarning("Token doesn't look like a GitHub PAT (expected ghp_* or github_pat_*)")
			}
		case "gmail", "gdrive":
			if connToken == "" {
				PrintErrorMsg(fmt.Sprintf("%s requires --token (OAuth access token)", integrationType))
				return nil
			}
		case "notion":
			if connToken == "" {
				PrintErrorMsg("Notion requires --token (integration token)")
				return nil
			}
			if !strings.HasPrefix(connToken, "secret_") && !strings.HasPrefix(connToken, "ntn_") {
				PrintWarning("Token doesn't look like a Notion integration token (expected secret_* or ntn_*)")
			}
		case "weather", "exa", "posthog":
			if connAPIKey == "" {
				PrintErrorMsg(fmt.Sprintf("%s requires --api-key", integrationType))
				return nil
			}
		default:
			PrintErrorMsg(fmt.Sprintf("Unknown integration type: %s", integrationType))
			PrintHint("Supported: github, gmail, notion, gdrive, weather, exa, posthog")
			return nil
		}

		var client *Client
		var resp *pb.ConnectionResponse

		err := RunSpinnerWithResult("Adding connection...", func() error {
			var err error
			client, err = getClient()
			if err != nil {
				return err
			}

			resp, err = client.Gateway.AddConnection(context.Background(), &pb.AddConnectionRequest{
				WorkspaceId:     workspaceId,
				MemberId:        connMember,
				IntegrationType: integrationType,
				AccessToken:     connToken,
				ApiKey:          connAPIKey,
				Scope:           connScope,
			})
			return err
		})

		if client != nil {
			defer client.Close()
		}

		if err != nil {
			PrintError(err)
			return nil
		}
		if !resp.Ok {
			PrintErrorMsg(resp.Error)
			return nil
		}

		scope := "workspace-shared"
		if !resp.Connection.IsShared {
			scope = "personal"
		}

		PrintSuccess("Connection added")
		PrintNewline()
		PrintKeyValue("Type", resp.Connection.IntegrationType)
		PrintKeyValue("Scope", scope)
		PrintKeyValue("ID", resp.Connection.Id)
		return nil
	},
}

var connectionListCmd = &cobra.Command{
	Use:   "list <workspace_id>",
	Short: "List connections in a workspace",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		client, err := getClient()
		if err != nil {
			PrintError(err)
			return nil
		}
		defer client.Close()

		resp, err := client.Gateway.ListConnections(context.Background(), &pb.ListConnectionsRequest{
			WorkspaceId: args[0],
		})
		if err != nil {
			PrintError(err)
			return nil
		}
		if !resp.Ok {
			PrintErrorMsg(resp.Error)
			return nil
		}

		// JSON output
		if PrintJSON(resp.Connections) {
			return nil
		}

		if len(resp.Connections) == 0 {
			PrintInfo("No connections found")
			PrintHint("Add one with: airstore connection add <workspace_id> <type> --token <token>")
			return nil
		}

		PrintHeader("Connections")

		table := NewTable("ID", "TYPE", "SCOPE", "CREATED")
		for _, c := range resp.Connections {
			scope := "shared"
			if !c.IsShared {
				scope = "personal"
			}
			table.AddRow(c.Id, c.IntegrationType, scope, FormatRelativeTime(c.CreatedAt))
		}
		table.Print()
		PrintNewline()

		return nil
	},
}

var connectionRemoveCmd = &cobra.Command{
	Use:   "remove <connection_id>",
	Short: "Remove a connection",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		var client *Client
		var resp *pb.DeleteResponse

		err := RunSpinnerWithResult("Removing connection...", func() error {
			var err error
			client, err = getClient()
			if err != nil {
				return err
			}

			resp, err = client.Gateway.RemoveConnection(context.Background(), &pb.RemoveConnectionRequest{
				Id: args[0],
			})
			return err
		})

		if client != nil {
			defer client.Close()
		}

		if err != nil {
			PrintError(err)
			return nil
		}
		if !resp.Ok {
			PrintErrorMsg(resp.Error)
			return nil
		}

		PrintSuccessf("Connection %s removed", CodeStyle.Render(args[0]))
		return nil
	},
}

var connectionConnectCmd = &cobra.Command{
	Use:   "connect <integration>",
	Short: "Connect an OAuth integration via browser",
	Long: `Connect an OAuth-based integration by opening a browser for authentication.

The workspace is determined by your token (--token or AIRSTORE_TOKEN).

Supported OAuth integrations:
  gmail     - Gmail (read-only access)
  gdrive    - Google Drive (read-only access)

Examples:
  airstore connection connect gmail
  airstore connection connect gdrive
  airstore connection connect gmail --token <workspace-token>`,
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		integrationType := strings.ToLower(args[0])

		// Validate integration type
		switch integrationType {
		case "gmail", "gdrive":
			// OK - these use OAuth
		default:
			PrintErrorMsg(fmt.Sprintf("%s does not use OAuth", integrationType))
			PrintHint("Use 'connection add' with --token instead")
			return nil
		}

		if authToken == "" {
			PrintErrorMsg("No authentication token provided")
			PrintHint("Set AIRSTORE_TOKEN or use --token <workspace-token>")
			return nil
		}

		// Create OAuth session via HTTP API
		var sessionResp *struct{ SessionID, AuthorizeURL string }

		err := RunSpinnerWithResult("Creating OAuth session...", func() error {
			var err error
			sessionResp, err = createOAuthSession(integrationType)
			return err
		})

		if err != nil {
			PrintErrorMsg(fmt.Sprintf("Failed to create OAuth session: %s", err.Error()))
			return nil
		}

		PrintInfo("Opening browser...")
		PrintNewline()

		// Open browser
		if err := openBrowser(sessionResp.AuthorizeURL); err != nil {
			PrintWarning("Could not open browser automatically")
			fmt.Printf("\n  Please visit: %s\n\n", CodeStyle.Render(sessionResp.AuthorizeURL))
		}

		// Poll for completion
		var result *struct{ Status, Error, ConnectionID string }

		err = RunSpinnerWithResult("Waiting for authorization...", func() error {
			var err error
			result, err = pollOAuthSession(sessionResp.SessionID, 5*time.Minute)
			return err
		})

		if err != nil {
			PrintError(err)
			return nil
		}

		if result.Status == "error" {
			PrintErrorMsg(fmt.Sprintf("Connection failed: %s", result.Error))
			return nil
		}

		PrintSuccessf("Connected %s", integrationType)
		return nil
	},
}

// OAuth API types
type oauthSessionRequest struct {
	IntegrationType string `json:"integration_type"`
}

type oauthSessionResponse struct {
	Success bool `json:"success"`
	Data    struct {
		SessionID    string `json:"session_id"`
		AuthorizeURL string `json:"authorize_url"`
	} `json:"data"`
	Error string `json:"error,omitempty"`
}

type oauthStatusResponse struct {
	Success bool `json:"success"`
	Data    struct {
		Status       string `json:"status"`
		Error        string `json:"error,omitempty"`
		ConnectionID string `json:"connection_id,omitempty"`
	} `json:"data"`
	Error string `json:"error,omitempty"`
}

func createOAuthSession(integrationType string) (*struct{ SessionID, AuthorizeURL string }, error) {
	reqBody, _ := json.Marshal(oauthSessionRequest{IntegrationType: integrationType})

	req, err := http.NewRequest("POST", gatewayHTTPAddr+"/api/v1/oauth/sessions", bytes.NewReader(reqBody))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+authToken)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var result oauthSessionResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}

	if !result.Success {
		return nil, fmt.Errorf("%s", result.Error)
	}

	return &struct{ SessionID, AuthorizeURL string }{
		SessionID:    result.Data.SessionID,
		AuthorizeURL: result.Data.AuthorizeURL,
	}, nil
}

func pollOAuthSession(sessionID string, timeout time.Duration) (*struct{ Status, Error, ConnectionID string }, error) {
	deadline := time.Now().Add(timeout)
	pollInterval := 2 * time.Second

	for time.Now().Before(deadline) {
		req, err := http.NewRequest("GET", gatewayHTTPAddr+"/api/v1/oauth/sessions/"+sessionID, nil)
		if err != nil {
			return nil, err
		}

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			time.Sleep(pollInterval)
			continue
		}

		var result oauthStatusResponse
		json.NewDecoder(resp.Body).Decode(&result)
		resp.Body.Close()

		if !result.Success {
			return nil, fmt.Errorf("%s", result.Error)
		}

		status := result.Data.Status
		if status == "complete" || status == "error" {
			return &struct{ Status, Error, ConnectionID string }{
				Status:       status,
				Error:        result.Data.Error,
				ConnectionID: result.Data.ConnectionID,
			}, nil
		}

		// Still pending
		time.Sleep(pollInterval)
	}

	return nil, fmt.Errorf("timeout waiting for authorization")
}

func openBrowser(url string) error {
	var cmd *exec.Cmd
	switch runtime.GOOS {
	case "darwin":
		cmd = exec.Command("open", url)
	case "linux":
		cmd = exec.Command("xdg-open", url)
	case "windows":
		cmd = exec.Command("rundll32", "url.dll,FileProtocolHandler", url)
	default:
		return fmt.Errorf("unsupported platform")
	}
	return cmd.Start()
}

func init() {
	connectionAddCmd.Flags().StringVar(&connToken, "token", "", "Access token (for OAuth integrations like GitHub)")
	connectionAddCmd.Flags().StringVar(&connAPIKey, "api-key", "", "API key (for API key integrations)")
	connectionAddCmd.Flags().StringVar(&connMember, "member", "", "Member ID (for personal connection, omit for shared)")
	connectionAddCmd.Flags().StringVar(&connScope, "scope", "", "OAuth scopes")

	connectionCmd.AddCommand(connectionAddCmd)
	connectionCmd.AddCommand(connectionListCmd)
	connectionCmd.AddCommand(connectionRemoveCmd)
	connectionCmd.AddCommand(connectionConnectCmd)
}
