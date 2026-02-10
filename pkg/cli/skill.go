package cli

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/beam-cloud/airstore/pkg/skills"
	"github.com/beam-cloud/airstore/pkg/types"
	pb "github.com/beam-cloud/airstore/proto"
	"github.com/spf13/cobra"
)

var skillCmd = &cobra.Command{
	Use:   "skill",
	Short: "Install and manage skills",
	Long: `Install, list, and manage skills that give your agent new capabilities.

A skill is a folder with a SKILL.md file following the Agent Skills spec
(https://agentskills.io/specification). Skills are passive knowledge that
agents can use. Hooks trigger skills to run automatically.

Skills can be installed from:
  - A GitHub repo path: github.com/beam-cloud/skills/email-triage
  - A local directory: ./my-skill/

Examples:
  airstore skill install github.com/user/repo/skill-name
  airstore skill install ./local-skill/
  airstore skill list
  airstore skill info email-triage
  airstore skill uninstall email-triage`,
}

// SkillSource represents where a skill is being installed from.
type SkillSource int

const (
	SourceLocal SkillSource = iota
	SourceGitHub
)

// ParseSkillSource determines the source type and parses the reference.
func ParseSkillSource(ref string) (source SkillSource, slug, path string, err error) {
	// github.com/owner/repo/path/to/skill
	if strings.HasPrefix(ref, "github.com/") {
		rest := strings.TrimPrefix(ref, "github.com/")
		parts := strings.SplitN(rest, "/", 3)
		if len(parts) < 2 {
			return 0, "", "", fmt.Errorf("invalid GitHub reference: need github.com/owner/repo[/path]")
		}
		// slug = "owner/repo", path = optional subpath
		slug = parts[0] + "/" + parts[1]
		if len(parts) > 2 {
			path = parts[2]
		}
		return SourceGitHub, slug, path, nil
	}

	// Local path
	return SourceLocal, "", ref, nil
}

var skillInstallCmd = &cobra.Command{
	Use:   "install <source>",
	Short: "Install a skill from URL or local path",
	Long: `Install a skill from GitHub or local directory.

Sources:
  github.com/owner/repo/path    Install from a GitHub repository  
  ./path/to/skill               Install from a local directory

Examples:
  airstore skill install github.com/beam-cloud/skills/email-triage
  airstore skill install ./my-custom-skill/`,
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		ref := args[0]

		source, slug, path, err := ParseSkillSource(ref)
		if err != nil {
			PrintErrorMsg(err.Error())
			return nil
		}

		var manifest *skills.SkillManifest
		var skillName string
		var skillFiles map[string][]byte

		switch source {
		case SourceGitHub:
			skillName = filepath.Base(path)
			if skillName == "" || skillName == "." {
				// Use repo name as skill name
				parts := strings.Split(slug, "/")
				skillName = parts[len(parts)-1]
			}
			skillFiles, manifest, _, err = fetchFromGitHub(slug, path)
			if err != nil {
				PrintErrorMsg(err.Error())
				return nil
			}

		case SourceLocal:
			skillMDPath, err := skills.FindSkillMD(path)
			if err != nil {
				PrintErrorMsg(err.Error())
				return nil
			}
			manifest, _, err = skills.ParseFile(skillMDPath)
			if err != nil {
				PrintErrorMsg(err.Error())
				return nil
			}
			skillName = skills.SkillNameFromPath(path)
			// For local, we upload directly from filesystem
		}

		PrintNewline()
		PrintHeader(fmt.Sprintf("Installing %s", manifest.Name))
		PrintNewline()

		// Connect to gateway
		client, err := getClient()
		if err != nil {
			PrintError(err)
			return nil
		}
		defer client.Close()

		ctx := context.Background()

		// Extract Airstore-specific metadata
		am := manifest.AirstoreMetadata()

		// Check required sources
		if len(am.Needs) > 0 {
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

			for _, need := range am.Needs {
				if connected[need] {
					PrintSuccessf("  %s connected", need)
				} else {
					PrintWarning(fmt.Sprintf("  %s not connected", need))
					PrintHint(fmt.Sprintf("Connect it with: airstore connection add %s", need))
				}
			}
			PrintNewline()
		}

		// Upload skill files
		if skillFiles != nil {
			err = uploadSkillFilesFromMap(ctx, client, skillName, skillFiles)
		} else {
			err = uploadSkillFiles(ctx, client, path, skillName)
		}
		if err != nil {
			PrintError(fmt.Errorf("uploading skill files: %w", err))
			return nil
		}
		PrintSuccessf("  Skill files uploaded to %s/%s/", types.PathSkills, skillName)

		// Create output directories
		for _, writePath := range am.Writes {
			_, err := client.Context.Mkdir(ctx, &pb.ContextMkdirRequest{
				Path: writePath,
				Mode: 0755,
			})
			if err != nil {
				// Ignore mkdir errors (directory may already exist)
			}
			PrintSuccessf("  Output: %s", writePath)
		}

		// Write installed metadata
		meta := &skills.InstalledSkill{
			Name:        manifest.Name,
			Description: manifest.Description,
			Needs:       am.Needs,
			Source:      ref,
		}
		metaBytes, _ := json.MarshalIndent(meta, "", "  ")
		writeFileToWorkspace(ctx, client, fmt.Sprintf("%s/%s/%s", types.PathSkills, skillName, skills.InstalledMetaFile), metaBytes)

		PrintNewline()
		PrintSuccessf("%s installed", manifest.Name)
		PrintNewline()

		if len(am.Writes) > 0 {
			PrintHint(fmt.Sprintf("View output: ls ~/airstore%s", am.Writes[0]))
		}
		PrintHint("Create a hook to trigger this skill automatically:")
		PrintHint(fmt.Sprintf("  airstore hook create --path %s/<source> --prompt \"Use the %s skill\"", types.PathSources, skillName))
		PrintNewline()

		return nil
	},
}

// fetchFromGitHub fetches a skill from a GitHub repository.
func fetchFromGitHub(repo, subPath string) (files map[string][]byte, manifest *skills.SkillManifest, content string, err error) {
	// Use GitHub's tarball API to download the repo
	// Format: https://api.github.com/repos/{owner}/{repo}/tarball/{ref}
	tarballURL := fmt.Sprintf("https://api.github.com/repos/%s/tarball/main", repo)

	req, err := http.NewRequest("GET", tarballURL, nil)
	if err != nil {
		return nil, nil, "", err
	}
	req.Header.Set("Accept", "application/vnd.github+json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, nil, "", fmt.Errorf("fetching GitHub repo: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, nil, "", fmt.Errorf("GitHub repo not found: %s", repo)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, nil, "", fmt.Errorf("GitHub returned %d", resp.StatusCode)
	}

	// Parse the gzipped tarball
	gzr, err := gzip.NewReader(resp.Body)
	if err != nil {
		return nil, nil, "", fmt.Errorf("decompressing tarball: %w", err)
	}
	defer gzr.Close()

	tr := tar.NewReader(gzr)
	files = make(map[string][]byte)

	// GitHub tarballs have a top-level directory like "owner-repo-sha/"
	// We need to strip that and optionally filter by subPath
	var topDir string

	for {
		header, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, nil, "", fmt.Errorf("reading tarball: %w", err)
		}

		// Get the path, stripping the top-level directory
		name := header.Name
		parts := strings.SplitN(name, "/", 2)
		if len(parts) < 2 {
			if topDir == "" && strings.HasSuffix(name, "/") {
				topDir = strings.TrimSuffix(name, "/")
			}
			continue
		}

		relativePath := parts[1]

		// If subPath is specified, only include files under that path
		if subPath != "" {
			if !strings.HasPrefix(relativePath, subPath) && !strings.HasPrefix(relativePath, subPath+"/") {
				continue
			}
			// Strip the subPath prefix
			relativePath = strings.TrimPrefix(relativePath, subPath)
			relativePath = strings.TrimPrefix(relativePath, "/")
		}

		if relativePath == "" {
			continue
		}

		// Skip directories
		if header.Typeflag == tar.TypeDir {
			continue
		}

		// Read file content
		data, err := io.ReadAll(tr)
		if err != nil {
			continue
		}

		files[relativePath] = data
	}

	// Parse SKILL.md
	skillMDBytes, ok := files["SKILL.md"]
	if !ok {
		return nil, nil, "", fmt.Errorf("SKILL.md not found in repo")
	}

	manifest, err = skills.Parse(skillMDBytes)
	if err != nil {
		return nil, nil, "", fmt.Errorf("parsing SKILL.md: %w", err)
	}
	content = string(skillMDBytes)

	return files, manifest, content, nil
}

// uploadSkillFilesFromMap uploads skill files from a map to /Skills/{name}/.
func uploadSkillFilesFromMap(ctx context.Context, client *Client, skillName string, files map[string][]byte) error {
	skillDir := fmt.Sprintf("%s/%s", types.PathSkills, skillName)
	// Create the skill directory first
	client.Context.Mkdir(ctx, &pb.ContextMkdirRequest{
		Path: skillDir,
		Mode: 0755,
	})

	for relPath, data := range files {
		remotePath := fmt.Sprintf("%s/%s/%s", types.PathSkills, skillName, relPath)

		// Create parent dirs if needed
		dir := filepath.Dir(remotePath)
		if dir != skillDir {
			client.Context.Mkdir(ctx, &pb.ContextMkdirRequest{Path: dir, Mode: 0755})
		}

		if err := writeFileToWorkspace(ctx, client, remotePath, data); err != nil {
			return fmt.Errorf("writing %s: %w", relPath, err)
		}
	}
	return nil
}

// uploadSkillFiles copies all files from a local directory to /Skills/{name}/ in the workspace.
func uploadSkillFiles(ctx context.Context, client *Client, srcDir, skillName string) error {
	skillDir := fmt.Sprintf("%s/%s", types.PathSkills, skillName)
	// Create the skill directory first
	client.Context.Mkdir(ctx, &pb.ContextMkdirRequest{
		Path: skillDir,
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

		remotePath := fmt.Sprintf("%s/%s/%s", types.PathSkills, skillName, relPath)

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

		// List /Skills/ directory via context service
		listResp, err := client.Context.ReadDir(ctx, &pb.ContextReadDirRequest{
			Path: types.PathSkills,
		})
		if err != nil {
			PrintError(err)
			return nil
		}
		if !listResp.Ok {
			PrintInfo("No skills installed")
			PrintHint("Install one with: airstore skill install <source>")
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
			PrintHint("Install one with: airstore skill install <source>")
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

		table := NewTable("NAME", "DESCRIPTION", "SOURCE", "STATUS")
		for _, entry := range dirs {
			name := entry.Name
			description := ""
			source := "local"
			status := "active"

			// Try to read installed metadata
			metaResp, err := client.Context.Read(ctx, &pb.ContextReadRequest{
				Path: fmt.Sprintf("%s/%s/%s", types.PathSkills, name, skills.InstalledMetaFile),
			})
			if err == nil && metaResp.Ok {
				var meta skills.InstalledSkill
				if json.Unmarshal(metaResp.Data, &meta) == nil {
					description = meta.Description
					if meta.Source != "" {
						source = meta.Source
					}
					allConnected := true
					for _, need := range meta.Needs {
						if !connected[need] {
							allConnected = false
							break
						}
					}
					if !allConnected {
						status = "needs setup"
					}
				}
			}

			table.AddRow(name, Truncate(description, 30), Truncate(source, 35), status)
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
			Path: fmt.Sprintf("%s/%s/SKILL.md", types.PathSkills, skillName),
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
			Path: fmt.Sprintf("%s/%s/%s", types.PathSkills, skillName, skills.InstalledMetaFile),
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

		am := manifest.AirstoreMetadata()

		PrintNewline()
		PrintKeyValue("Name", manifest.Name)
		if manifest.Description != "" {
			PrintKeyValue("Description", manifest.Description)
		}
		if manifest.License != "" {
			PrintKeyValue("License", manifest.License)
		}
		if meta != nil && meta.Source != "" {
			PrintKeyValue("Source", meta.Source)
		}
		PrintNewline()

		if len(am.Needs) > 0 {
			PrintHeader("Required Sources")
			for _, need := range am.Needs {
				if connected[need] {
					PrintSuccessf("  %s (connected)", need)
				} else {
					PrintWarning(fmt.Sprintf("  %s (not connected)", need))
				}
			}
			PrintNewline()
		}

		if len(am.Writes) > 0 {
			PrintHeader("Output Paths")
			for _, w := range am.Writes {
				fmt.Printf("  %s\n", w)
			}
			PrintNewline()
		}

		return nil
	},
}

var skillUninstallCmd = &cobra.Command{
	Use:   "uninstall <name>",
	Short: "Uninstall a skill",
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

		// Optionally delete output dirs
		if !keepMemory {
			skillResp, err := client.Context.Read(ctx, &pb.ContextReadRequest{
				Path: fmt.Sprintf("%s/%s/SKILL.md", types.PathSkills, skillName),
			})
			if err == nil && skillResp.Ok {
				manifest, err := skills.Parse(skillResp.Data)
				if err == nil {
					am := manifest.AirstoreMetadata()
					for _, w := range am.Writes {
						client.Context.Delete(ctx, &pb.ContextDeleteRequest{Path: w, Recursive: true})
					}
				}
			}
		}

		// Delete skill directory
		err = RunSpinnerWithResult("Removing skill...", func() error {
			_, err := client.Context.Delete(ctx, &pb.ContextDeleteRequest{
				Path:      fmt.Sprintf("%s/%s", types.PathSkills, skillName),
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
			Path: fmt.Sprintf("%s/%s/SKILL.md", types.PathSkills, skillName),
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

var skillSearchCmd = &cobra.Command{
	Use:   "search [query]",
	Short: "Search for skills on GitHub",
	Long: `Search for skills shared by the community on GitHub.

Examples:
  airstore skill search email
  airstore skill search github`,
	RunE: func(cmd *cobra.Command, args []string) error {
		PrintNewline()
		PrintInfo("Install skills with:")
		fmt.Println("  airstore skill install github.com/user/repo/skill-name")
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
	skillCmd.AddCommand(skillSearchCmd)
}
