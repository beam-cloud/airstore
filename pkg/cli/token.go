package cli

import (
	"context"
	"fmt"
	"os"
	"text/tabwriter"
	"time"

	pb "github.com/beam-cloud/airstore/proto"
	"github.com/spf13/cobra"
)

var (
	tokenName    string
	tokenExpires string
)

var tokenCmd = &cobra.Command{
	Use:   "token",
	Short: "Manage workspace tokens",
}

var tokenCreateCmd = &cobra.Command{
	Use:   "create <workspace_id> <member_id>",
	Short: "Create an API token for a member",
	Args:  cobra.ExactArgs(2),
	RunE: func(cmd *cobra.Command, args []string) error {
		client, err := getClient()
		if err != nil {
			return err
		}
		defer client.Close()

		var expiresInSeconds int64
		if tokenExpires != "" {
			duration, err := time.ParseDuration(tokenExpires)
			if err != nil {
				return fmt.Errorf("invalid expires duration: %w", err)
			}
			expiresInSeconds = int64(duration.Seconds())
		}

		resp, err := client.Gateway.CreateToken(context.Background(), &pb.CreateTokenRequest{
			WorkspaceId:      args[0],
			MemberId:         args[1],
			Name:             tokenName,
			ExpiresInSeconds: expiresInSeconds,
		})
		if err != nil {
			return err
		}
		if !resp.Ok {
			return fmt.Errorf("%s", resp.Error)
		}

		fmt.Printf("Token: %s\n", resp.Token)
		fmt.Printf("Name:  %s\n", resp.Info.Name)
		if resp.Info.ExpiresAt != "" {
			fmt.Printf("Expires: %s\n", resp.Info.ExpiresAt)
		}
		fmt.Println("\nSave this token! It won't be shown again.")
		return nil
	},
}

var tokenListCmd = &cobra.Command{
	Use:   "list <workspace_id>",
	Short: "List tokens in a workspace",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		client, err := getClient()
		if err != nil {
			return err
		}
		defer client.Close()

		resp, err := client.Gateway.ListTokens(context.Background(), &pb.ListTokensRequest{
			WorkspaceId: args[0],
		})
		if err != nil {
			return err
		}
		if !resp.Ok {
			return fmt.Errorf("%s", resp.Error)
		}

		if len(resp.Tokens) == 0 {
			fmt.Println("No tokens found.")
			return nil
		}

		w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
		fmt.Fprintln(w, "ID\tNAME\tEXPIRES\tLAST USED")
		for _, t := range resp.Tokens {
			expires := "-"
			if t.ExpiresAt != "" {
				expires = t.ExpiresAt
			}
			lastUsed := "-"
			if t.LastUsedAt != "" {
				lastUsed = t.LastUsedAt
			}
			fmt.Fprintf(w, "%s\t%s\t%s\t%s\n", t.Id, t.Name, expires, lastUsed)
		}
		w.Flush()
		return nil
	},
}

var tokenRevokeCmd = &cobra.Command{
	Use:   "revoke <token_id>",
	Short: "Revoke a token",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		client, err := getClient()
		if err != nil {
			return err
		}
		defer client.Close()

		resp, err := client.Gateway.RevokeToken(context.Background(), &pb.RevokeTokenRequest{
			Id: args[0],
		})
		if err != nil {
			return err
		}
		if !resp.Ok {
			return fmt.Errorf("%s", resp.Error)
		}

		fmt.Printf("Token %s revoked.\n", args[0])
		return nil
	},
}

func init() {
	tokenCreateCmd.Flags().StringVar(&tokenName, "name", "API Token", "Token name")
	tokenCreateCmd.Flags().StringVar(&tokenExpires, "expires", "", "Expiration duration (e.g. 24h, 7d)")

	tokenCmd.AddCommand(tokenCreateCmd)
	tokenCmd.AddCommand(tokenListCmd)
	tokenCmd.AddCommand(tokenRevokeCmd)
}
