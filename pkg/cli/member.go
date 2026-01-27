package cli

import (
	"context"
	"fmt"
	"os"
	"text/tabwriter"

	pb "github.com/beam-cloud/airstore/proto"
	"github.com/spf13/cobra"
)

var (
	memberName string
	memberRole string
)

var memberCmd = &cobra.Command{
	Use:   "member",
	Short: "Manage workspace members",
}

var memberAddCmd = &cobra.Command{
	Use:   "add <workspace_id> <email>",
	Short: "Add a member to a workspace",
	Args:  cobra.ExactArgs(2),
	RunE: func(cmd *cobra.Command, args []string) error {
		client, err := getClient()
		if err != nil {
			return err
		}
		defer client.Close()

		resp, err := client.Gateway.AddMember(context.Background(), &pb.AddMemberRequest{
			WorkspaceId: args[0],
			Email:       args[1],
			Name:        memberName,
			Role:        memberRole,
		})
		if err != nil {
			return err
		}
		if !resp.Ok {
			return fmt.Errorf("%s", resp.Error)
		}

		fmt.Printf("Member: %s (%s)\n", resp.Member.Email, resp.Member.Id)
		fmt.Printf("Role:   %s\n", resp.Member.Role)
		return nil
	},
}

var memberListCmd = &cobra.Command{
	Use:   "list <workspace_id>",
	Short: "List members in a workspace",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		client, err := getClient()
		if err != nil {
			return err
		}
		defer client.Close()

		resp, err := client.Gateway.ListMembers(context.Background(), &pb.ListMembersRequest{
			WorkspaceId: args[0],
		})
		if err != nil {
			return err
		}
		if !resp.Ok {
			return fmt.Errorf("%s", resp.Error)
		}

		if len(resp.Members) == 0 {
			fmt.Println("No members found.")
			return nil
		}

		w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
		fmt.Fprintln(w, "ID\tEMAIL\tNAME\tROLE")
		for _, m := range resp.Members {
			fmt.Fprintf(w, "%s\t%s\t%s\t%s\n", m.Id, m.Email, m.Name, m.Role)
		}
		w.Flush()
		return nil
	},
}

var memberRemoveCmd = &cobra.Command{
	Use:   "remove <member_id>",
	Short: "Remove a member",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		client, err := getClient()
		if err != nil {
			return err
		}
		defer client.Close()

		resp, err := client.Gateway.RemoveMember(context.Background(), &pb.RemoveMemberRequest{
			Id: args[0],
		})
		if err != nil {
			return err
		}
		if !resp.Ok {
			return fmt.Errorf("%s", resp.Error)
		}

		fmt.Printf("Member %s removed.\n", args[0])
		return nil
	},
}

func init() {
	memberAddCmd.Flags().StringVar(&memberName, "name", "", "Member display name")
	memberAddCmd.Flags().StringVar(&memberRole, "role", "member", "Role: admin, member, viewer")

	memberCmd.AddCommand(memberAddCmd)
	memberCmd.AddCommand(memberListCmd)
	memberCmd.AddCommand(memberRemoveCmd)
}
