package cli

import (
	"context"
	"fmt"
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
	Use:   "create <member_id>",
	Short: "Create an API token for a member",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		var client *Client
		var resp *pb.CreateTokenResponse

		var expiresInSeconds int64
		if tokenExpires != "" {
			duration, err := time.ParseDuration(tokenExpires)
			if err != nil {
				PrintErrorMsg(fmt.Sprintf("Invalid expires duration: %s", err.Error()))
				return nil
			}
			expiresInSeconds = int64(duration.Seconds())
		}

		err := RunSpinnerWithResult("Creating token...", func() error {
			var err error
			client, err = getClient()
			if err != nil {
				return err
			}

			resp, err = client.Gateway.CreateToken(context.Background(), &pb.CreateTokenRequest{
				MemberId:         args[0],
				Name:             tokenName,
				ExpiresInSeconds: expiresInSeconds,
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

		PrintSuccess("Token created")
		PrintNewline()

		// Show the token prominently - it's a one-time display
		fmt.Printf("  %s\n", BoldStyle.Render("Token:"))
		fmt.Printf("  %s\n", CodeStyle.Render(resp.Token))
		PrintNewline()

		PrintKeyValue("Name", resp.Info.Name)
		if resp.Info.ExpiresAt != "" {
			PrintKeyValue("Expires", FormatRelativeTime(resp.Info.ExpiresAt))
		}
		PrintNewline()

		PrintWarning("Save this token! It won't be shown again.")
		return nil
	},
}

var tokenListCmd = &cobra.Command{
	Use:   "list",
	Short: "List tokens in your workspace",
	RunE: func(cmd *cobra.Command, args []string) error {
		client, err := getClient()
		if err != nil {
			PrintError(err)
			return nil
		}
		defer client.Close()

		resp, err := client.Gateway.ListTokens(context.Background(), &pb.ListTokensRequest{})
		if err != nil {
			PrintError(err)
			return nil
		}
		if !resp.Ok {
			PrintErrorMsg(resp.Error)
			return nil
		}

		// JSON output
		if PrintJSON(resp.Tokens) {
			return nil
		}

		if len(resp.Tokens) == 0 {
			PrintInfo("No tokens found")
			PrintHint("Create one with: airstore token create <member_id>")
			return nil
		}

		PrintHeader("Tokens")

		table := NewTable("ID", "NAME", "EXPIRES", "LAST USED")
		for _, t := range resp.Tokens {
			expires := "-"
			if t.ExpiresAt != "" {
				expires = FormatRelativeTime(t.ExpiresAt)
			}
			lastUsed := "-"
			if t.LastUsedAt != "" {
				lastUsed = FormatRelativeTime(t.LastUsedAt)
			}
			table.AddRow(t.Id, t.Name, expires, lastUsed)
		}
		table.Print()
		PrintNewline()

		return nil
	},
}

var tokenRevokeCmd = &cobra.Command{
	Use:   "revoke <token_id>",
	Short: "Revoke a token",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		var client *Client
		var resp *pb.DeleteResponse

		err := RunSpinnerWithResult("Revoking token...", func() error {
			var err error
			client, err = getClient()
			if err != nil {
				return err
			}

			resp, err = client.Gateway.RevokeToken(context.Background(), &pb.RevokeTokenRequest{
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

		PrintSuccessf("Token %s revoked", CodeStyle.Render(args[0]))
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
