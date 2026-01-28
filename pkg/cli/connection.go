package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"text/tabwriter"
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

Examples:
  cli connection add <ws> github --token ghp_xxxxxxxxxxxx
  cli connection add <ws> notion --token secret_xxxxxxxxxxxx
  cli connection add <ws> weather --api-key abc123
  cli connection add <ws> github --token ghp_xxx --member <member_id>  # Personal connection`,
	Args: cobra.ExactArgs(2),
	RunE: func(cmd *cobra.Command, args []string) error {
		workspaceId := args[0]
		integrationType := strings.ToLower(args[1])

		// Validate integration type
		switch integrationType {
		case "github":
			if connToken == "" {
				return fmt.Errorf("github requires --token (personal access token)")
			}
			// Validate GitHub PAT format
			if !strings.HasPrefix(connToken, "ghp_") && !strings.HasPrefix(connToken, "github_pat_") {
				fmt.Println("Warning: Token doesn't look like a GitHub PAT (expected ghp_* or github_pat_*)")
			}
		case "gmail", "gdrive":
			if connToken == "" {
				return fmt.Errorf("%s requires --token (OAuth access token)", integrationType)
			}
		case "notion":
			if connToken == "" {
				return fmt.Errorf("notion requires --token (integration token)")
			}
			if !strings.HasPrefix(connToken, "secret_") && !strings.HasPrefix(connToken, "ntn_") {
				fmt.Println("Warning: Token doesn't look like a Notion integration token (expected secret_* or ntn_*)")
			}
		case "weather", "exa":
			if connAPIKey == "" {
				return fmt.Errorf("%s requires --api-key", integrationType)
			}
		default:
			return fmt.Errorf("unknown integration type: %s\nSupported: github, gmail, notion, gdrive, weather, exa", integrationType)
		}

		client, err := getClient()
		if err != nil {
			return err
		}
		defer client.Close()

		resp, err := client.Gateway.AddConnection(context.Background(), &pb.AddConnectionRequest{
			WorkspaceId:     workspaceId,
			MemberId:        connMember,
			IntegrationType: integrationType,
			AccessToken:     connToken,
			ApiKey:          connAPIKey,
			Scope:           connScope,
		})
		if err != nil {
			return err
		}
		if !resp.Ok {
			return fmt.Errorf("%s", resp.Error)
		}

		scope := "workspace-shared"
		if !resp.Connection.IsShared {
			scope = "personal"
		}
		fmt.Printf("Connected: %s (%s)\n", resp.Connection.IntegrationType, scope)
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
			return err
		}
		defer client.Close()

		resp, err := client.Gateway.ListConnections(context.Background(), &pb.ListConnectionsRequest{
			WorkspaceId: args[0],
		})
		if err != nil {
			return err
		}
		if !resp.Ok {
			return fmt.Errorf("%s", resp.Error)
		}

		if len(resp.Connections) == 0 {
			fmt.Println("No connections found.")
			return nil
		}

		w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
		fmt.Fprintln(w, "ID\tTYPE\tSCOPE\tCREATED")
		for _, c := range resp.Connections {
			scope := "shared"
			if !c.IsShared {
				scope = "personal"
			}
			fmt.Fprintf(w, "%s\t%s\t%s\t%s\n", c.Id, c.IntegrationType, scope, c.CreatedAt)
		}
		w.Flush()
		return nil
	},
}

var connectionRemoveCmd = &cobra.Command{
	Use:   "remove <connection_id>",
	Short: "Remove a connection",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		client, err := getClient()
		if err != nil {
			return err
		}
		defer client.Close()

		resp, err := client.Gateway.RemoveConnection(context.Background(), &pb.RemoveConnectionRequest{
			Id: args[0],
		})
		if err != nil {
			return err
		}
		if !resp.Ok {
			return fmt.Errorf("%s", resp.Error)
		}

		fmt.Printf("Connection %s removed.\n", args[0])
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
  cli connection connect gmail
  cli connection connect gdrive
  cli connection connect gmail --token <workspace-token>`,
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		integrationType := strings.ToLower(args[0])

		// Validate integration type
		switch integrationType {
		case "gmail", "gdrive":
			// OK - these use OAuth
		default:
			return fmt.Errorf("%s does not use OAuth - use 'connection add' with --token instead", integrationType)
		}

		if authToken == "" {
			return fmt.Errorf("set AIRSTORE_TOKEN or use --token <workspace-token>")
		}

		// Create OAuth session via HTTP API
		sessionResp, err := createOAuthSession(integrationType)
		if err != nil {
			return fmt.Errorf("failed to create OAuth session: %w", err)
		}

		fmt.Printf("Opening browser to connect %s...\n", integrationType)

		// Open browser
		if err := openBrowser(sessionResp.AuthorizeURL); err != nil {
			fmt.Printf("Could not open browser automatically.\nPlease visit: %s\n", sessionResp.AuthorizeURL)
		}

		// Poll for completion
		fmt.Println("Waiting for authorization...")
		result, err := pollOAuthSession(sessionResp.SessionID, 5*time.Minute)
		if err != nil {
			return err
		}

		if result.Status == "error" {
			return fmt.Errorf("connection failed: %s", result.Error)
		}

		fmt.Printf("Successfully connected %s!\n", integrationType)
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
