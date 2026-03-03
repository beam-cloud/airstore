package cli

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/beam-cloud/airstore/pkg/skills"
	"github.com/beam-cloud/airstore/pkg/types"
	pb "github.com/beam-cloud/airstore/proto"
	"github.com/spf13/cobra"
)

var hookPrompt string
var hookSkill string

var hookCmd = &cobra.Command{
	Use:   "hook",
	Short: "Manage filesystem hooks",
	Long: `Create, list, and manage hooks that watch paths and trigger tasks.

A hook watches a filesystem path. When something changes there -- a file is
created, modified, or new data arrives from a source -- a task runs using the
configured task template. Optional skills are appended as extra instructions.
The event type is passed as context to the agent automatically.

Examples:
  airstore hook create --path /sources/gmail/inbox --prompt "Review new emails and draft summaries"
  airstore hook create --path /sources/gmail/inbox --prompt "Review PR updates" --skill /skills/pr-review
  airstore hook create --path /inbox/contracts --prompt "Summarize legal risk" --skill ./my-local-skill`,
}

var hookCreateCmd = &cobra.Command{
	Use:   "create --path <path> --prompt <task-template> [--skill <skill>]",
	Short: "Create a hook that watches a path",
	Long: `Create a hook that triggers a task when something changes at a path.

The hook runs the provided task template when files change. Optional skills can
be attached and are appended as extra instructions. Skills can be:
  - Airstore paths: /skills/email-triage (already installed)
  - Local paths: ./my-skill (auto-imported to /skills/)

The agent automatically receives context about what happened:
  - "Event: file created or modified at /skills/report.txt"
  - "Event: 3 new results in /sources/gmail/inbox"

Examples:
  airstore hook create --path /sources/gmail/inbox --prompt "Review and summarize new items"
  airstore hook create --path /sources/gmail/inbox --prompt "Review new PR changes" --skill /skills/pr-review
  airstore hook create --path /inbox/contracts --prompt "Focus on liability" --skill ./my-local-skill`,
	RunE: func(cmd *cobra.Command, args []string) error {
		path, _ := cmd.Flags().GetString("path")
		if path == "" {
			PrintErrorMsg("--path is required")
			return nil
		}
		if strings.TrimSpace(hookPrompt) == "" {
			PrintErrorMsg("--prompt is required")
			return nil
		}

		var client *Client
		var resp *pb.HookResponse
		var skillPath string

		err := RunSpinnerWithResult("Creating hook...", func() error {
			var err error
			client, err = getClient()
			if err != nil {
				return err
			}

			// Handle skill path
			if hookSkill != "" {
				skillPath, err = resolveSkillPath(client, hookSkill)
				if err != nil {
					return fmt.Errorf("skill: %w", err)
				}
			}

			resp, err = client.Gateway.CreateHook(context.Background(), &pb.CreateHookRequest{
				Path:      path,
				Prompt:    hookPrompt,
				SkillPath: skillPath,
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
		if resp.Hook.SkillPath != "" {
			PrintKeyValue("Skill", resp.Hook.SkillPath)
		}
		if resp.Hook.Prompt != "" {
			PrintKeyValue("Prompt", Truncate(resp.Hook.Prompt, 60))
		}
		PrintNewline()

		return nil
	},
}

// resolveSkillPath resolves a skill path to an airstore path.
// For local paths (starting with . or /), it imports the skill first.
// For airstore paths (/Skills/...), it validates the skill exists.
func resolveSkillPath(client *Client, skill string) (string, error) {
	skillsPrefix := types.PathSkills + "/"
	// Check if it's a local path
	if strings.HasPrefix(skill, ".") || (strings.HasPrefix(skill, "/") && !strings.HasPrefix(strings.ToLower(skill), strings.ToLower(skillsPrefix))) {
		return importLocalSkill(client, skill)
	}

	// It's an airstore path - validate it starts with /Skills/ (case-insensitive)
	if !strings.HasPrefix(strings.ToLower(skill), strings.ToLower(skillsPrefix)) {
		return "", fmt.Errorf("skill path must start with %s or be a local path", skillsPrefix)
	}

	// TODO: Validate the skill exists via API
	return skill, nil
}

// importLocalSkill imports a local skill directory to /skills/{name}/
func importLocalSkill(client *Client, localPath string) (string, error) {
	// Resolve the path
	absPath, err := filepath.Abs(localPath)
	if err != nil {
		return "", fmt.Errorf("resolve path: %w", err)
	}

	// Check if directory exists and reject symlink roots.
	info, err := os.Lstat(absPath)
	if err != nil {
		return "", fmt.Errorf("skill not found: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return "", fmt.Errorf("skill directory must not be a symlink")
	}
	if !info.IsDir() {
		return "", fmt.Errorf("skill must be a directory")
	}

	// Check for SKILL.md
	skillMdPath := filepath.Join(absPath, "SKILL.md")
	manifest, _, err := skills.ParseFile(skillMdPath)
	if err != nil {
		return "", fmt.Errorf("parse SKILL.md: %w", err)
	}

	// The skill name from the manifest determines the target path
	targetPath := types.PathSkills + "/" + manifest.Name

	// Upload the skill directory using the existing uploadSkillFiles function
	ctx := context.Background()
	if err := uploadSkillFiles(ctx, client, absPath, manifest.Name); err != nil {
		return "", fmt.Errorf("upload skill: %w", err)
	}

	return targetPath, nil
}

func uploadSkillFiles(ctx context.Context, client *Client, srcDir, skillName string) error {
	return filepath.WalkDir(srcDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		relPath, err := filepath.Rel(srcDir, path)
		if err != nil {
			return err
		}

		// Disallow symlinks so local skill imports cannot read data outside srcDir.
		if d.Type()&fs.ModeSymlink != 0 {
			return fmt.Errorf("skill contains symlink: %s", filepath.ToSlash(relPath))
		}
		if d.IsDir() {
			return nil
		}

		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}

		remotePath := fmt.Sprintf("%s/%s/%s", types.PathSkills, skillName, filepath.ToSlash(relPath))
		resp, err := client.Context.Write(ctx, &pb.ContextWriteRequest{
			Path: remotePath,
			Data: data,
		})
		if err != nil {
			return err
		}
		if !resp.Ok {
			return fmt.Errorf("write %s: %s", remotePath, resp.Error)
		}

		return nil
	})
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
			PrintHint("Create one with: airstore hook create --path <path> --prompt \"<task template>\"")
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
		if resp.Hook.SkillPath != "" {
			PrintKeyValue("Skill", resp.Hook.SkillPath)
		}
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

var hookRunsCmd = &cobra.Command{
	Use:   "runs <hook_id>",
	Short: "Show task runs triggered by a hook",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		client, err := getClient()
		if err != nil {
			PrintError(err)
			return nil
		}
		defer client.Close()

		resp, err := client.Gateway.ListHookRuns(context.Background(), &pb.ListHookRunsRequest{
			HookId: args[0],
		})
		if err != nil {
			PrintError(err)
			return nil
		}
		if !resp.Ok {
			PrintErrorMsg(resp.Error)
			return nil
		}

		if PrintJSON(resp.Runs) {
			return nil
		}

		if len(resp.Runs) == 0 {
			PrintInfo("No runs yet")
			return nil
		}

		PrintHeader("Hook Runs")

		table := NewTable("TASK", "STATUS", "ATTEMPT", "ERROR", "CREATED", "FINISHED")
		for _, r := range resp.Runs {
			attempt := fmt.Sprintf("%d/%d", r.Attempt, r.MaxAttempts)
			errMsg := r.Error
			if len(errMsg) > 30 {
				errMsg = errMsg[:30] + "..."
			}
			finished := "-"
			if r.FinishedAt != "" {
				finished = FormatRelativeTime(r.FinishedAt)
			}
			table.AddRow(r.TaskId, r.Status, attempt, errMsg, FormatRelativeTime(r.CreatedAt), finished)
		}
		table.Print()
		PrintNewline()

		return nil
	},
}

func init() {
	hookCreateCmd.Flags().String("path", "", "Filesystem path to watch (required)")
	hookCreateCmd.Flags().StringVar(&hookSkill, "skill", "", "Skill to run (airstore path or local directory)")
	hookCreateCmd.Flags().StringVar(&hookPrompt, "prompt", "", "Additional prompt context (optional)")

	hookCmd.AddCommand(hookCreateCmd)
	hookCmd.AddCommand(hookListCmd)
	hookCmd.AddCommand(hookGetCmd)
	hookCmd.AddCommand(hookDeleteCmd)
	hookCmd.AddCommand(hookPauseCmd)
	hookCmd.AddCommand(hookResumeCmd)
	hookCmd.AddCommand(hookRunsCmd)
}
