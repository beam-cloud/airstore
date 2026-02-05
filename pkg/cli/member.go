package cli

import (
	"context"

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
	Use:   "add <email>",
	Short: "Add a member to your workspace",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		var client *Client
		var resp *pb.MemberResponse

		err := RunSpinnerWithResult("Adding member...", func() error {
			var err error
			client, err = getClient()
			if err != nil {
				return err
			}

			resp, err = client.Gateway.AddMember(context.Background(), &pb.AddMemberRequest{
				Email: args[0],
				Name:  memberName,
				Role:  memberRole,
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

		PrintSuccess("Member added")
		PrintNewline()
		PrintKeyValue("Email", resp.Member.Email)
		PrintKeyValue("ID", resp.Member.Id)
		PrintKeyValue("Role", resp.Member.Role)
		return nil
	},
}

var memberListCmd = &cobra.Command{
	Use:   "list",
	Short: "List members in your workspace",
	RunE: func(cmd *cobra.Command, args []string) error {
		client, err := getClient()
		if err != nil {
			PrintError(err)
			return nil
		}
		defer client.Close()

		resp, err := client.Gateway.ListMembers(context.Background(), &pb.ListMembersRequest{})
		if err != nil {
			PrintError(err)
			return nil
		}
		if !resp.Ok {
			PrintErrorMsg(resp.Error)
			return nil
		}

		// JSON output
		if PrintJSON(resp.Members) {
			return nil
		}

		if len(resp.Members) == 0 {
			PrintInfo("No members found")
			PrintHint("Add one with: airstore member add <email>")
			return nil
		}

		PrintHeader("Members")

		table := NewTable("ID", "EMAIL", "NAME", "ROLE")
		for _, m := range resp.Members {
			table.AddRow(m.Id, m.Email, m.Name, m.Role)
		}
		table.Print()
		PrintNewline()

		return nil
	},
}

var memberRemoveCmd = &cobra.Command{
	Use:   "remove <member_id>",
	Short: "Remove a member",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		var client *Client
		var resp *pb.DeleteResponse

		err := RunSpinnerWithResult("Removing member...", func() error {
			var err error
			client, err = getClient()
			if err != nil {
				return err
			}

			resp, err = client.Gateway.RemoveMember(context.Background(), &pb.RemoveMemberRequest{
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

		PrintSuccessf("Member %s removed", CodeStyle.Render(args[0]))
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
