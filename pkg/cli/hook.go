package cli

import (
	"context"
	"fmt"

	pb "github.com/beam-cloud/airstore/proto"
	"github.com/spf13/cobra"
)

var hookPrompt string

var hookCmd = &cobra.Command{
	Use:   "hook",
	Short: "Manage filesystem hooks",
	Long: `Create, list, and manage hooks that watch paths and trigger tasks.

A hook watches a filesystem path. When something changes there -- a file is
created, modified, or new data arrives from a source -- a task runs with your
prompt. The event type is passed as context to the agent automatically.

Examples:
  airstore hook create --path /skills --prompt "Analyze this file"
  airstore hook create --path /sources/gmail/inbox --prompt "Triage new emails"`,
}

var hookCreateCmd = &cobra.Command{
	Use:   "create --path <path> --prompt <prompt>",
	Short: "Create a hook that watches a path",
	Long: `Create a hook that triggers a task when something changes at a path.

The agent automatically receives context about what happened:
  - "Event: file created or modified at /skills/report.txt"
  - "Event: 3 new results in /sources/gmail/inbox"

Examples:
  airstore hook create --path /skills --prompt "Analyze this file"
  airstore hook create --path /sources/gmail/inbox --prompt "Triage new emails"
  airstore hook create --path /inbox/contracts --prompt "Review and flag issues"`,
	RunE: func(cmd *cobra.Command, args []string) error {
		path, _ := cmd.Flags().GetString("path")
		if path == "" {
			PrintErrorMsg("--path is required")
			return nil
		}
		if hookPrompt == "" {
			PrintErrorMsg("--prompt is required")
			return nil
		}

		var client *Client
		var resp *pb.HookResponse

		err := RunSpinnerWithResult("Creating hook...", func() error {
			var err error
			client, err = getClient()
			if err != nil {
				return err
			}

			resp, err = client.Gateway.CreateHook(context.Background(), &pb.CreateHookRequest{
				Path:   path,
				Prompt: hookPrompt,
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

		PrintSuccess("Hook created")
		PrintNewline()
		PrintKeyValue("ID", resp.Hook.Id)
		PrintKeyValue("Path", resp.Hook.Path)
		if resp.Hook.Prompt != "" {
			PrintKeyValue("Prompt", Truncate(resp.Hook.Prompt, 60))
		}
		PrintNewline()

		return nil
	},
}

var hookListCmd = &cobra.Command{
	Use:   "list",
	Short: "List hooks in your workspace",
	RunE: func(cmd *cobra.Command, args []string) error {
		client, err := getClient()
		if err != nil {
			PrintError(err)
			return nil
		}
		defer client.Close()

		resp, err := client.Gateway.ListHooks(context.Background(), &pb.ListHooksRequest{})
		if err != nil {
			PrintError(err)
			return nil
		}
		if !resp.Ok {
			PrintErrorMsg(resp.Error)
			return nil
		}

		if PrintJSON(resp.Hooks) {
			return nil
		}

		if len(resp.Hooks) == 0 {
			PrintInfo("No hooks found")
			PrintHint("Create one with: airstore hook create --path <path> --prompt \"...\"")
			return nil
		}

		PrintHeader("Hooks")

		table := NewTable("ID", "PATH", "ACTIVE", "CREATED")
		for _, h := range resp.Hooks {
			active := "yes"
			if !h.Active {
				active = "no"
			}
			table.AddRow(h.Id, Truncate(h.Path, 35), active, FormatRelativeTime(h.CreatedAt))
		}
		table.Print()
		PrintNewline()

		return nil
	},
}

var hookGetCmd = &cobra.Command{
	Use:   "get <hook_id>",
	Short: "Get hook details",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		client, err := getClient()
		if err != nil {
			PrintError(err)
			return nil
		}
		defer client.Close()

		resp, err := client.Gateway.GetHook(context.Background(), &pb.GetHookRequest{
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

		if PrintJSON(resp.Hook) {
			return nil
		}

		PrintNewline()
		PrintKeyValue("ID", resp.Hook.Id)
		PrintKeyValue("Path", resp.Hook.Path)
		PrintKeyValue("Active", fmt.Sprintf("%v", resp.Hook.Active))
		if resp.Hook.Prompt != "" {
			PrintKeyValue("Prompt", resp.Hook.Prompt)
		}
		PrintNewline()
		PrintKeyValue("Created", FormatRelativeTime(resp.Hook.CreatedAt))
		PrintKeyValue("Updated", FormatRelativeTime(resp.Hook.UpdatedAt))
		PrintNewline()

		return nil
	},
}

var hookDeleteCmd = &cobra.Command{
	Use:   "delete <hook_id>",
	Short: "Delete a hook",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		var client *Client
		var resp *pb.DeleteResponse

		err := RunSpinnerWithResult("Deleting hook...", func() error {
			var err error
			client, err = getClient()
			if err != nil {
				return err
			}

			resp, err = client.Gateway.DeleteHook(context.Background(), &pb.DeleteHookRequest{
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

		PrintSuccessf("Hook %s deleted", CodeStyle.Render(args[0]))
		return nil
	},
}

var hookPauseCmd = &cobra.Command{
	Use:   "pause <hook_id>",
	Short: "Pause a hook (stop it from firing)",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		return updateHookActive(args[0], false)
	},
}

var hookResumeCmd = &cobra.Command{
	Use:   "resume <hook_id>",
	Short: "Resume a paused hook",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		return updateHookActive(args[0], true)
	},
}

func updateHookActive(hookId string, active bool) error {
	var client *Client
	var resp *pb.HookResponse

	action := "Pausing"
	if active {
		action = "Resuming"
	}

	err := RunSpinnerWithResult(action+" hook...", func() error {
		var err error
		client, err = getClient()
		if err != nil {
			return err
		}

		resp, err = client.Gateway.UpdateHook(context.Background(), &pb.UpdateHookRequest{
			Id:        hookId,
			Active:    active,
			HasActive: true,
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

	if active {
		PrintSuccessf("Hook %s resumed", CodeStyle.Render(hookId))
	} else {
		PrintSuccessf("Hook %s paused", CodeStyle.Render(hookId))
	}
	return nil
}

func init() {
	hookCreateCmd.Flags().String("path", "", "Filesystem path to watch (required)")
	hookCreateCmd.Flags().StringVar(&hookPrompt, "prompt", "", "Task prompt (required)")

	hookCmd.AddCommand(hookCreateCmd)
	hookCmd.AddCommand(hookListCmd)
	hookCmd.AddCommand(hookGetCmd)
	hookCmd.AddCommand(hookDeleteCmd)
	hookCmd.AddCommand(hookPauseCmd)
	hookCmd.AddCommand(hookResumeCmd)
}
