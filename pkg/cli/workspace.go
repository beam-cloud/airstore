package cli

import (
	"context"

	pb "github.com/beam-cloud/airstore/proto"
	"github.com/spf13/cobra"
)

var workspaceCmd = &cobra.Command{
	Use:   "workspace",
	Short: "Manage workspaces",
}

var workspaceCreateCmd = &cobra.Command{
	Use:   "create <name>",
	Short: "Create a new workspace",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		var client *Client
		var resp *pb.WorkspaceResponse

		err := RunSpinnerWithResult("Creating workspace...", func() error {
			var err error
			client, err = getClient()
			if err != nil {
				return err
			}

			resp, err = client.Gateway.CreateWorkspace(context.Background(), &pb.CreateWorkspaceRequest{
				Name: args[0],
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

		PrintSuccess("Workspace created")
		PrintNewline()
		PrintKeyValue("Name", resp.Workspace.Name)
		PrintKeyValue("ID", resp.Workspace.Id)
		return nil
	},
}

var workspaceListCmd = &cobra.Command{
	Use:   "list",
	Short: "List all workspaces",
	RunE: func(cmd *cobra.Command, args []string) error {
		client, err := getClient()
		if err != nil {
			PrintError(err)
			return nil
		}
		defer client.Close()

		resp, err := client.Gateway.ListWorkspaces(context.Background(), &pb.ListWorkspacesRequest{})
		if err != nil {
			PrintError(err)
			return nil
		}
		if !resp.Ok {
			PrintErrorMsg(resp.Error)
			return nil
		}

		// JSON output
		if PrintJSON(resp.Workspaces) {
			return nil
		}

		if len(resp.Workspaces) == 0 {
			PrintInfo("No workspaces found")
			PrintHint("Create one with: airstore workspace create <name>")
			return nil
		}

		PrintHeader("Workspaces")

		table := NewTable("ID", "NAME", "CREATED")
		for _, ws := range resp.Workspaces {
			table.AddRow(ws.Id, ws.Name, FormatRelativeTime(ws.CreatedAt))
		}
		table.Print()
		PrintNewline()

		return nil
	},
}

var workspaceGetCmd = &cobra.Command{
	Use:   "get <id>",
	Short: "Get workspace details",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		client, err := getClient()
		if err != nil {
			PrintError(err)
			return nil
		}
		defer client.Close()

		resp, err := client.Gateway.GetWorkspace(context.Background(), &pb.GetWorkspaceRequest{
			Id: args[0],
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
		if PrintJSON(resp.Workspace) {
			return nil
		}

		PrintNewline()
		PrintKeyValue("ID", resp.Workspace.Id)
		PrintKeyValue("Name", resp.Workspace.Name)
		PrintKeyValue("Created", FormatRelativeTime(resp.Workspace.CreatedAt))
		PrintNewline()

		return nil
	},
}

var workspaceDeleteCmd = &cobra.Command{
	Use:   "delete <id>",
	Short: "Delete a workspace",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		var client *Client
		var resp *pb.DeleteResponse

		err := RunSpinnerWithResult("Deleting workspace...", func() error {
			var err error
			client, err = getClient()
			if err != nil {
				return err
			}

			resp, err = client.Gateway.DeleteWorkspace(context.Background(), &pb.DeleteWorkspaceRequest{
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

		PrintSuccessf("Workspace %s deleted", CodeStyle.Render(args[0]))
		return nil
	},
}

func init() {
	workspaceCmd.AddCommand(workspaceCreateCmd)
	workspaceCmd.AddCommand(workspaceListCmd)
	workspaceCmd.AddCommand(workspaceGetCmd)
	workspaceCmd.AddCommand(workspaceDeleteCmd)
}
