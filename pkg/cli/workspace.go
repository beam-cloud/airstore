package cli

import (
	"context"
	"fmt"
	"os"
	"text/tabwriter"

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
		client, err := getClient()
		if err != nil {
			return err
		}
		defer client.Close()

		resp, err := client.Gateway.CreateWorkspace(context.Background(), &pb.CreateWorkspaceRequest{
			Name: args[0],
		})
		if err != nil {
			return err
		}
		if !resp.Ok {
			return fmt.Errorf("%s", resp.Error)
		}

		fmt.Printf("Workspace: %s (%s)\n", resp.Workspace.Name, resp.Workspace.Id)
		return nil
	},
}

var workspaceListCmd = &cobra.Command{
	Use:   "list",
	Short: "List all workspaces",
	RunE: func(cmd *cobra.Command, args []string) error {
		client, err := getClient()
		if err != nil {
			return err
		}
		defer client.Close()

		resp, err := client.Gateway.ListWorkspaces(context.Background(), &pb.ListWorkspacesRequest{})
		if err != nil {
			return err
		}
		if !resp.Ok {
			return fmt.Errorf("%s", resp.Error)
		}

		if len(resp.Workspaces) == 0 {
			fmt.Println("No workspaces found.")
			return nil
		}

		w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
		fmt.Fprintln(w, "ID\tNAME\tCREATED")
		for _, ws := range resp.Workspaces {
			fmt.Fprintf(w, "%s\t%s\t%s\n", ws.Id, ws.Name, ws.CreatedAt)
		}
		w.Flush()
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
			return err
		}
		defer client.Close()

		resp, err := client.Gateway.GetWorkspace(context.Background(), &pb.GetWorkspaceRequest{
			Id: args[0],
		})
		if err != nil {
			return err
		}
		if !resp.Ok {
			return fmt.Errorf("%s", resp.Error)
		}

		fmt.Printf("ID:      %s\n", resp.Workspace.Id)
		fmt.Printf("Name:    %s\n", resp.Workspace.Name)
		fmt.Printf("Created: %s\n", resp.Workspace.CreatedAt)
		return nil
	},
}

var workspaceDeleteCmd = &cobra.Command{
	Use:   "delete <id>",
	Short: "Delete a workspace",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		client, err := getClient()
		if err != nil {
			return err
		}
		defer client.Close()

		resp, err := client.Gateway.DeleteWorkspace(context.Background(), &pb.DeleteWorkspaceRequest{
			Id: args[0],
		})
		if err != nil {
			return err
		}
		if !resp.Ok {
			return fmt.Errorf("%s", resp.Error)
		}

		fmt.Printf("Workspace %s deleted.\n", args[0])
		return nil
	},
}

func init() {
	workspaceCmd.AddCommand(workspaceCreateCmd)
	workspaceCmd.AddCommand(workspaceListCmd)
	workspaceCmd.AddCommand(workspaceGetCmd)
	workspaceCmd.AddCommand(workspaceDeleteCmd)
}
