package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

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

var workspaceSetVisibilityCmd = &cobra.Command{
	Use:   "set-visibility <public|private>",
	Short: "Set workspace visibility",
	Long: `Set whether your workspace is publicly visible.

When public, anyone can read your workspace's content via airstore:// URIs.
When private, only authenticated members can access the workspace.

Examples:
  airstore workspace set-visibility public
  airstore workspace set-visibility private`,
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		visibility := args[0]
		if visibility != "public" && visibility != "private" {
			PrintErrorMsg("visibility must be 'public' or 'private'")
			return nil
		}

		client, err := getClient()
		if err != nil {
			PrintError(err)
			return nil
		}
		defer client.Close()

		resp, err := client.Gateway.GetWorkspace(context.Background(), &pb.GetWorkspaceRequest{})
		if err != nil {
			PrintError(err)
			return nil
		}
		if !resp.Ok {
			PrintErrorMsg(resp.Error)
			return nil
		}

		// Call HTTP API to set visibility
		err = setWorkspaceField(resp.Workspace.Id, "visibility", visibility)
		if err != nil {
			PrintError(err)
			return nil
		}

		PrintSuccessf("Workspace visibility set to %s", CodeStyle.Render(visibility))
		if visibility == "public" {
			PrintInfo("Your workspace content is now publicly readable.")
			PrintHint("Set a slug with: airstore workspace set-slug <name>")
		}
		return nil
	},
}

var workspaceSetSlugCmd = &cobra.Command{
	Use:   "set-slug <slug>",
	Short: "Set workspace vanity slug for public access",
	Long: `Set a vanity slug for your workspace, used in airstore:// URIs and
the public profile page.

Examples:
  airstore workspace set-slug luke
  # Enables: airstore://luke/skills/email-triage
  # Profile: airstore.ai/w/luke`,
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		slug := args[0]

		client, err := getClient()
		if err != nil {
			PrintError(err)
			return nil
		}
		defer client.Close()

		resp, err := client.Gateway.GetWorkspace(context.Background(), &pb.GetWorkspaceRequest{})
		if err != nil {
			PrintError(err)
			return nil
		}
		if !resp.Ok {
			PrintErrorMsg(resp.Error)
			return nil
		}

		err = setWorkspaceField(resp.Workspace.Id, "slug", slug)
		if err != nil {
			PrintError(err)
			return nil
		}

		PrintSuccessf("Workspace slug set to %s", CodeStyle.Render(slug))
		PrintInfo("Public profile: airstore.ai/w/" + slug)
		return nil
	},
}

// setWorkspaceField calls the HTTP API to update a workspace field.
func setWorkspaceField(workspaceId, field, value string) error {
	body, _ := json.Marshal(map[string]string{field: value})

	req, err := http.NewRequest("PUT", gatewayHTTPAddr+"/api/v1/workspaces/"+workspaceId+"/"+field, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if authToken != "" {
		req.Header.Set("Authorization", "Bearer "+authToken)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("server returned %d: %s", resp.StatusCode, string(respBody))
	}
	return nil
}

func init() {
	workspaceCmd.AddCommand(workspaceCreateCmd)
	workspaceCmd.AddCommand(workspaceListCmd)
	workspaceCmd.AddCommand(workspaceGetCmd)
	workspaceCmd.AddCommand(workspaceDeleteCmd)
	workspaceCmd.AddCommand(workspaceSetVisibilityCmd)
	workspaceCmd.AddCommand(workspaceSetSlugCmd)
}
