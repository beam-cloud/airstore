package cli

import (
	"context"
	"fmt"
	"os"
	"strings"
	"text/tabwriter"

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

func init() {
	connectionAddCmd.Flags().StringVar(&connToken, "token", "", "Access token (for OAuth integrations like GitHub)")
	connectionAddCmd.Flags().StringVar(&connAPIKey, "api-key", "", "API key (for API key integrations)")
	connectionAddCmd.Flags().StringVar(&connMember, "member", "", "Member ID (for personal connection, omit for shared)")
	connectionAddCmd.Flags().StringVar(&connScope, "scope", "", "OAuth scopes")

	connectionCmd.AddCommand(connectionAddCmd)
	connectionCmd.AddCommand(connectionListCmd)
	connectionCmd.AddCommand(connectionRemoveCmd)
}
