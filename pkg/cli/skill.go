package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/beam-cloud/airstore/pkg/skills"
	"github.com/beam-cloud/airstore/pkg/skills/builtins"
	pb "github.com/beam-cloud/airstore/proto"
	"github.com/spf13/cobra"
)

var skillCmd = &cobra.Command{
	Use:   "skill",
	Short: "Install and manage skills",
	Long: `Install, list, and manage skills that give your agent new capabilities.

A skill is a folder with a SKILL.md file that declares what data sources the
skill needs, when it should trigger, and where it writes output. Installing
a skill automatically connects required sources and creates triggers.

Examples:
  airstore skill install ./email-triage/
  airstore skill list
  airstore skill info email-triage
  airstore skill uninstall email-triage
  airstore skill run email-triage`,
}

var skillInstallCmd = &cobra.Command{
	Use:   "install <name|path>",
	Short: "Install a skill (built-in name or local directory)",
	Long: `Install a skill by name (built-in) or from a local directory with a SKILL.md.

Built-in skills: email-triage, slack-actions, pr-reviewer, issue-triage

Examples:
  airstore skill install email-triage
  airstore skill install ./my-custom-skill/`,
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		nameOrPath := args[0]

		var manifest *skills.SkillManifest
		var content string
		var skillName string
		var builtinFiles map[string][]byte // non-nil if installing a builtin

		// Check if it's a built-in skill first
		if bs, err := builtins.Get(nameOrPath); err == nil {
			manifest = bs.Manifest
			content = bs.Content
			skillName = bs.Name
			builtinFiles, _ = builtins.ExtractFiles(bs.Name)
		} else {
			// Try as a local path
			skillMDPath, err := skills.FindSkillMD(nameOrPath)
			if err != nil {
				PrintErrorMsg(err.Error())
				return nil
			}

			manifest, content, err = skills.ParseFile(skillMDPath)
			if err != nil {
				PrintErrorMsg(err.Error())
				return nil
			}
			skillName = skills.SkillNameFromPath(nameOrPath)
		}
		PrintNewline()
		PrintHeader(fmt.Sprintf("Installing %s", manifest.Name))
		PrintNewline()

		// 2. Connect to gateway
		client, err := getClient()
		if err != nil {
			PrintError(err)
			return nil
		}
		defer client.Close()

		ctx := context.Background()

		// 3. Check required sources
		if len(manifest.Needs) > 0 {
			connResp, err := client.Gateway.ListConnections(ctx, &pb.ListConnectionsRequest{})
			if err != nil {
				PrintError(err)
				return nil
			}
			if !connResp.Ok {
				PrintErrorMsg(connResp.Error)
				return nil
			}

			connected := make(map[string]bool)
			for _, c := range connResp.Connections {
				connected[c.IntegrationType] = true
			}

			for _, need := range manifest.Needs {
				if connected[need] {
					PrintSuccessf("  %s connected", need)
				} else {
					PrintWarning(fmt.Sprintf("  %s not connected", need))
					PrintHint(fmt.Sprintf("Connect it with: airstore connection add %s", need))
					PrintHint("Or connect it in the UI at https://airstore.ai")
				}
			}
			PrintNewline()
		}

		// 4. Upload skill files to /skills/{name}/
		if builtinFiles != nil {
			err = uploadBuiltinSkillFiles(ctx, client, skillName, builtinFiles)
		} else {
			err = uploadSkillFiles(ctx, client, nameOrPath, skillName)
		}
		if err != nil {
			PrintError(fmt.Errorf("uploading skill files: %w", err))
			return nil
		}
		PrintSuccessf("  Skill files uploaded to /skills/%s/", skillName)

		// 5. Create triggers (hooks)
		prompt := skills.BuildPrompt(manifest, content)
		var hookIds []string

		for _, trigger := range manifest.Triggers {
			var resp *pb.HookResponse
			err := RunSpinnerWithResult("  Creating trigger...", func() error {
				var err error
				resp, err = client.Gateway.CreateHook(ctx, &pb.CreateHookRequest{
					Path:   trigger.Path,
					Prompt: prompt,
				})
				return err
			})
			if err != nil {
				PrintError(err)
				return nil
			}
			if !resp.Ok {
				// If hook already exists on this path, that's ok
				if strings.Contains(resp.Error, "already exists") {
					PrintWarning(fmt.Sprintf("  Trigger on %s already exists, skipping", trigger.Path))
					continue
				}
				PrintErrorMsg(resp.Error)
				return nil
			}
			hookIds = append(hookIds, resp.Hook.Id)
			PrintSuccessf("  Trigger: %s on %s", trigger.On, trigger.Path)
		}

		// 6. Create output directories
		for _, writePath := range manifest.Writes {
			_, err := client.Context.Mkdir(ctx, &pb.ContextMkdirRequest{
				Path: writePath,
				Mode: 0755,
			})
			if err != nil {
				// Ignore mkdir errors (directory may already exist)
			}
			PrintSuccessf("  Output: %s", writePath)
		}

		// 7. Write installed metadata to the skill dir in the workspace
		meta := &skills.InstalledSkill{
			Name:        manifest.Name,
			Description: manifest.Description,
			Needs:       manifest.Needs,
			HookIds:     hookIds,
		}
		metaBytes, _ := json.MarshalIndent(meta, "", "  ")
		writeFileToWorkspace(ctx, client, fmt.Sprintf("/skills/%s/%s", skillName, skills.InstalledMetaFile), metaBytes)

		PrintNewline()
		PrintSuccessf("%s installed", manifest.Name)
		PrintNewline()

		if len(manifest.Writes) > 0 {
			PrintHint(fmt.Sprintf("View output: ls ~/airstore%s", manifest.Writes[0]))
		}
		if len(manifest.Triggers) > 0 {
			PrintInfo("Triggers are active. Your agent will run when new data arrives.")
		}
		PrintNewline()

		return nil
	},
}

var skillListCmd = &cobra.Command{
	Use:   "list",
	Short: "List installed skills",
	RunE: func(cmd *cobra.Command, args []string) error {
		client, err := getClient()
		if err != nil {
			PrintError(err)
			return nil
		}
		defer client.Close()

		ctx := context.Background()

		// List /skills/ directory via context service
		listResp, err := client.Context.ReadDir(ctx, &pb.ContextReadDirRequest{
			Path: "/skills",
		})
		if err != nil {
			PrintError(err)
			return nil
		}
		if !listResp.Ok {
			// If /skills doesn't exist yet, that's ok
			PrintInfo("No skills installed")
			PrintHint("Install one with: airstore skill install <path>")
			return nil
		}

		var dirs []*pb.ContextDirEntry
		for _, entry := range listResp.Entries {
			if entry.IsDir {
				dirs = append(dirs, entry)
			}
		}

		if len(dirs) == 0 {
			PrintInfo("No skills installed")
			PrintHint("Install one with: airstore skill install <path>")
			return nil
		}

		// Get connections for status display
		connResp, err := client.Gateway.ListConnections(ctx, &pb.ListConnectionsRequest{})
		connected := make(map[string]bool)
		if err == nil && connResp.Ok {
			for _, c := range connResp.Connections {
				connected[c.IntegrationType] = true
			}
		}

		PrintNewline()
		PrintHeader("Installed Skills")

		table := NewTable("NAME", "DESCRIPTION", "SOURCES", "STATUS")
		for _, entry := range dirs {
			name := entry.Name
			description := ""
			status := "active"
			sourcesStr := ""

			// Try to read installed metadata
			metaResp, err := client.Context.Read(ctx, &pb.ContextReadRequest{
				Path: fmt.Sprintf("/skills/%s/%s", name, skills.InstalledMetaFile),
			})
			if err == nil && metaResp.Ok {
				var meta skills.InstalledSkill
				if json.Unmarshal(metaResp.Data, &meta) == nil {
					description = meta.Description
					sources := make([]string, 0, len(meta.Needs))
					allConnected := true
					for _, need := range meta.Needs {
						if connected[need] {
							sources = append(sources, need)
						} else {
							sources = append(sources, need+" (missing)")
							allConnected = false
						}
					}
					sourcesStr = strings.Join(sources, ", ")
					if !allConnected {
						status = "needs setup"
					}
				}
			}

			table.AddRow(name, Truncate(description, 40), sourcesStr, status)
		}
		table.Print()
		PrintNewline()

		return nil
	},
}

var skillInfoCmd = &cobra.Command{
	Use:   "info <name>",
	Short: "Show details about an installed skill",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		skillName := args[0]

		client, err := getClient()
		if err != nil {
			PrintError(err)
			return nil
		}
		defer client.Close()

		ctx := context.Background()

		// Read SKILL.md
		skillResp, err := client.Context.Read(ctx, &pb.ContextReadRequest{
			Path: fmt.Sprintf("/skills/%s/SKILL.md", skillName),
		})
		if err != nil || !skillResp.Ok {
			PrintErrorMsg(fmt.Sprintf("skill %q not found", skillName))
			return nil
		}

		manifest, err := skills.Parse(skillResp.Data)
		if err != nil {
			PrintErrorMsg(err.Error())
			return nil
		}

		// Read installed metadata
		metaResp, err := client.Context.Read(ctx, &pb.ContextReadRequest{
			Path: fmt.Sprintf("/skills/%s/%s", skillName, skills.InstalledMetaFile),
		})
		var meta *skills.InstalledSkill
		if err == nil && metaResp.Ok {
			var m skills.InstalledSkill
			if json.Unmarshal(metaResp.Data, &m) == nil {
				meta = &m
			}
		}

		// Get connections
		connResp, err := client.Gateway.ListConnections(ctx, &pb.ListConnectionsRequest{})
		connected := make(map[string]bool)
		if err == nil && connResp.Ok {
			for _, c := range connResp.Connections {
				connected[c.IntegrationType] = true
			}
		}

		PrintNewline()
		PrintKeyValue("Name", manifest.Name)
		if manifest.Description != "" {
			PrintKeyValue("Description", manifest.Description)
		}
		PrintNewline()

		if len(manifest.Needs) > 0 {
			PrintHeader("Sources")
			for _, need := range manifest.Needs {
				if connected[need] {
					PrintSuccessf("  %s (connected)", need)
				} else {
					PrintWarning(fmt.Sprintf("  %s (not connected)", need))
				}
			}
			PrintNewline()
		}

		if len(manifest.Triggers) > 0 {
			PrintHeader("Triggers")
			for _, t := range manifest.Triggers {
				fmt.Printf("  %s on %s\n", t.On, t.Path)
			}
			PrintNewline()
		}

		if len(manifest.Writes) > 0 {
			PrintHeader("Output Paths")
			for _, w := range manifest.Writes {
				fmt.Printf("  %s\n", w)
			}
			PrintNewline()
		}

		if meta != nil && len(meta.HookIds) > 0 {
			PrintHeader("Hook IDs")
			for _, id := range meta.HookIds {
				fmt.Printf("  %s\n", id)
			}
			PrintNewline()
		}

		return nil
	},
}

var skillUninstallCmd = &cobra.Command{
	Use:   "uninstall <name>",
	Short: "Uninstall a skill and remove its triggers",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		skillName := args[0]
		keepMemory, _ := cmd.Flags().GetBool("keep-memory")

		client, err := getClient()
		if err != nil {
			PrintError(err)
			return nil
		}
		defer client.Close()

		ctx := context.Background()

		// Read installed metadata to find hooks to delete
		metaResp, err := client.Context.Read(ctx, &pb.ContextReadRequest{
			Path: fmt.Sprintf("/skills/%s/%s", skillName, skills.InstalledMetaFile),
		})
		if err == nil && metaResp.Ok {
			var meta skills.InstalledSkill
			if json.Unmarshal(metaResp.Data, &meta) == nil {
				// Delete hooks created by this skill
				for _, hookId := range meta.HookIds {
					resp, err := client.Gateway.DeleteHook(ctx, &pb.DeleteHookRequest{Id: hookId})
					if err == nil && resp.Ok {
						PrintSuccessf("  Trigger %s removed", hookId)
					}
				}

				// Optionally delete output dirs
				if !keepMemory {
					skillResp, err := client.Context.Read(ctx, &pb.ContextReadRequest{
						Path: fmt.Sprintf("/skills/%s/SKILL.md", skillName),
					})
					if err == nil && skillResp.Ok {
						manifest, err := skills.Parse(skillResp.Data)
						if err == nil {
							for _, w := range manifest.Writes {
								client.Context.Delete(ctx, &pb.ContextDeleteRequest{Path: w, Recursive: true})
							}
						}
					}
				}
			}
		}

		// Delete skill directory
		err = RunSpinnerWithResult("Removing skill...", func() error {
			_, err := client.Context.Delete(ctx, &pb.ContextDeleteRequest{
				Path:      fmt.Sprintf("/skills/%s", skillName),
				Recursive: true,
			})
			return err
		})
		if err != nil {
			PrintError(err)
			return nil
		}

		PrintSuccessf("%s uninstalled", skillName)
		if keepMemory {
			PrintInfo("Memory output was preserved (--keep-memory)")
		}
		return nil
	},
}

var skillRunCmd = &cobra.Command{
	Use:   "run <name>",
	Short: "Manually trigger a skill",
	Long:  `Run a skill immediately by creating a task with its prompt.`,
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		skillName := args[0]

		client, err := getClient()
		if err != nil {
			PrintError(err)
			return nil
		}
		defer client.Close()

		ctx := context.Background()

		// Read SKILL.md for the prompt
		skillResp, err := client.Context.Read(ctx, &pb.ContextReadRequest{
			Path: fmt.Sprintf("/skills/%s/SKILL.md", skillName),
		})
		if err != nil || !skillResp.Ok {
			PrintErrorMsg(fmt.Sprintf("skill %q not found", skillName))
			return nil
		}

		manifest, err := skills.Parse(skillResp.Data)
		if err != nil {
			PrintErrorMsg(err.Error())
			return nil
		}

		prompt := skills.BuildPrompt(manifest, string(skillResp.Data))

		var taskResp *pb.TaskResponse
		err = RunSpinnerWithResult("Running skill...", func() error {
			var err error
			taskResp, err = client.Gateway.CreateTask(ctx, &pb.CreateTaskRequest{
				Prompt: prompt,
			})
			return err
		})
		if err != nil {
			PrintError(err)
			return nil
		}
		if !taskResp.Ok {
			PrintErrorMsg(taskResp.Error)
			return nil
		}

		PrintSuccessf("%s triggered", manifest.Name)
		PrintKeyValue("Task", taskResp.Task.Id)
		PrintHint(fmt.Sprintf("View logs: airstore task logs %s", taskResp.Task.Id))
		return nil
	},
}

// uploadBuiltinSkillFiles uploads embedded built-in skill files to /skills/{name}/ in the workspace.
func uploadBuiltinSkillFiles(ctx context.Context, client *Client, skillName string, files map[string][]byte) error {
	// Create the skill directory first
	client.Context.Mkdir(ctx, &pb.ContextMkdirRequest{
		Path: fmt.Sprintf("/skills/%s", skillName),
		Mode: 0755,
	})

	for relPath, data := range files {
		remotePath := fmt.Sprintf("/skills/%s/%s", skillName, relPath)

		// Create parent dirs if needed
		dir := filepath.Dir(remotePath)
		if dir != fmt.Sprintf("/skills/%s", skillName) {
			client.Context.Mkdir(ctx, &pb.ContextMkdirRequest{Path: dir, Mode: 0755})
		}

		if err := writeFileToWorkspace(ctx, client, remotePath, data); err != nil {
			return fmt.Errorf("writing %s: %w", relPath, err)
		}
	}
	return nil
}

// uploadSkillFiles copies all files from a local directory to /skills/{name}/ in the workspace.
func uploadSkillFiles(ctx context.Context, client *Client, srcDir, skillName string) error {
	// Create the skill directory first
	client.Context.Mkdir(ctx, &pb.ContextMkdirRequest{
		Path: fmt.Sprintf("/skills/%s", skillName),
		Mode: 0755,
	})

	return filepath.WalkDir(srcDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		// Get relative path
		relPath, err := filepath.Rel(srcDir, path)
		if err != nil {
			return err
		}
		if relPath == "." {
			return nil
		}

		remotePath := fmt.Sprintf("/skills/%s/%s", skillName, relPath)

		if d.IsDir() {
			client.Context.Mkdir(ctx, &pb.ContextMkdirRequest{Path: remotePath, Mode: 0755})
			return nil
		}

		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}

		return writeFileToWorkspace(ctx, client, remotePath, data)
	})
}

// writeFileToWorkspace writes content to a path in the workspace using the context service.
func writeFileToWorkspace(ctx context.Context, client *Client, path string, content []byte) error {
	// Create the file first
	client.Context.Create(ctx, &pb.ContextCreateRequest{Path: path, Mode: 0644})

	// Then write content
	_, err := client.Context.Write(ctx, &pb.ContextWriteRequest{
		Path: path,
		Data: content,
	})
	return err
}

var skillCatalogCmd = &cobra.Command{
	Use:   "catalog",
	Short: "List available built-in skills",
	RunE: func(cmd *cobra.Command, args []string) error {
		available, err := builtins.List()
		if err != nil {
			PrintError(err)
			return nil
		}

		PrintNewline()
		PrintHeader("Available Skills")

		table := NewTable("NAME", "DESCRIPTION", "NEEDS")
		for _, bs := range available {
			needs := strings.Join(bs.Manifest.Needs, ", ")
			table.AddRow(bs.Name, bs.Manifest.Description, needs)
		}
		table.Print()
		PrintNewline()
		PrintHint("Install with: airstore skill install <name>")
		PrintNewline()

		return nil
	},
}

func init() {
	skillUninstallCmd.Flags().Bool("keep-memory", false, "Keep memory output when uninstalling")

	skillCmd.AddCommand(skillInstallCmd)
	skillCmd.AddCommand(skillListCmd)
	skillCmd.AddCommand(skillInfoCmd)
	skillCmd.AddCommand(skillUninstallCmd)
	skillCmd.AddCommand(skillRunCmd)
	skillCmd.AddCommand(skillCatalogCmd)
}
