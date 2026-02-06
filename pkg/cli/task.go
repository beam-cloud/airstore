package cli

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/charmbracelet/lipgloss"
	"github.com/spf13/cobra"

	pb "github.com/beam-cloud/airstore/proto"
)

var (
	taskWorkspace  string
	taskImage      string
	taskEntrypoint string
	taskEnv        []string
	taskPrompt     string
	taskFollow     bool
)

var taskCmd = &cobra.Command{
	Use:   "task",
	Short: "Manage tasks",
	Long:  `Create, list, and manage tasks.`,
}

var taskCreateCmd = &cobra.Command{
	Use:   "create",
	Short: "Create a new task",
	RunE: func(cmd *cobra.Command, args []string) error {
		return createTask()
	},
}

var taskListCmd = &cobra.Command{
	Use:   "list",
	Short: "List all tasks",
	RunE: func(cmd *cobra.Command, args []string) error {
		return listTasks()
	},
}

var taskGetCmd = &cobra.Command{
	Use:   "get <id>",
	Short: "Get task details",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		return getTask(args[0])
	},
}

var taskDeleteCmd = &cobra.Command{
	Use:   "delete <id>",
	Short: "Delete a task",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		return deleteTask(args[0])
	},
}

var taskLogsCmd = &cobra.Command{
	Use:   "logs <id>",
	Short: "Get task logs",
	Long:  `Get logs from a running or completed task.`,
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		return getTaskLogs(args[0])
	},
}

var taskRunCmd = &cobra.Command{
	Use:   "run",
	Short: "Run a Claude Code task",
	Long:  `Create and run a Claude Code task with the given prompt. Polls for completion and prints logs.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		return runClaudeCodeTask()
	},
}

func init() {
	taskCreateCmd.Flags().StringVarP(&taskImage, "image", "i", "", "Container image (optional if prompt provided)")
	taskCreateCmd.Flags().StringVarP(&taskPrompt, "prompt", "p", "", "Claude Code prompt (auto-sets image)")
	taskCreateCmd.Flags().StringVarP(&taskEntrypoint, "entrypoint", "e", "", "Entrypoint command (comma-separated)")
	taskCreateCmd.Flags().StringSliceVar(&taskEnv, "env", nil, "Environment variables (KEY=VALUE)")

	taskRunCmd.Flags().StringVarP(&taskPrompt, "prompt", "p", "", "Claude Code prompt (required)")
	taskRunCmd.MarkFlagRequired("prompt")

	taskCmd.AddCommand(taskCreateCmd)
	taskCmd.AddCommand(taskListCmd)
	taskCmd.AddCommand(taskGetCmd)
	taskCmd.AddCommand(taskDeleteCmd)
	taskCmd.AddCommand(taskLogsCmd)
	taskCmd.AddCommand(taskRunCmd)
}

func createTask() error {
	if taskPrompt == "" && taskImage == "" {
		PrintErrorMsg("Either --prompt or --image is required")
		return nil
	}

	var entrypoint []string
	if taskEntrypoint != "" {
		entrypoint = strings.Split(taskEntrypoint, ",")
		for i := range entrypoint {
			entrypoint[i] = strings.TrimSpace(entrypoint[i])
		}
	}

	env := make(map[string]string)
	for _, e := range taskEnv {
		parts := strings.SplitN(e, "=", 2)
		if len(parts) == 2 {
			env[parts[0]] = parts[1]
		}
	}

	var client *Client
	var resp *pb.TaskResponse

	err := RunSpinnerWithResult("Creating task...", func() error {
		var err error
		client, err = getClient()
		if err != nil {
			return err
		}

		resp, err = client.Gateway.CreateTask(context.Background(), &pb.CreateTaskRequest{
			Prompt:     taskPrompt,
			Image:      taskImage,
			Entrypoint: entrypoint,
			Env:        env,
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

	t := resp.Task
	PrintSuccess("Task created")
	PrintNewline()
	PrintKeyValue("ID", t.Id)
	if t.Prompt != "" {
		PrintKeyValue("Prompt", Truncate(t.Prompt, 50))
	}
	PrintKeyValue("Image", Truncate(t.Image, 50))
	PrintKeyValueStyled("Status", t.Status, statusStyle(t.Status))
	PrintNewline()

	return nil
}

func listTasks() error {
	client, err := getClient()
	if err != nil {
		PrintError(err)
		return nil
	}
	defer client.Close()

	resp, err := client.Gateway.ListTasks(context.Background(), &pb.ListTasksRequest{})
	if err != nil {
		PrintError(err)
		return nil
	}
	if !resp.Ok {
		PrintErrorMsg(resp.Error)
		return nil
	}

	if PrintJSON(resp.Tasks) {
		return nil
	}

	if len(resp.Tasks) == 0 {
		PrintInfo("No tasks found")
		PrintHint("Create one with: airstore task create --prompt \"...\"")
		return nil
	}

	PrintHeader("Tasks")

	table := NewTable("ID", "STATUS", "IMAGE", "CREATED", "EXIT")
	for _, t := range resp.Tasks {
		exitCode := "-"
		if t.HasExitCode {
			exitCode = fmt.Sprintf("%d", t.ExitCode)
		}
		image := Truncate(t.Image, 35)
		table.AddRow(t.Id, t.Status, image, FormatRelativeTime(t.CreatedAt), exitCode)
	}
	table.Print()
	PrintNewline()

	return nil
}

func getTask(id string) error {
	client, err := getClient()
	if err != nil {
		PrintError(err)
		return nil
	}
	defer client.Close()

	resp, err := client.Gateway.GetTask(context.Background(), &pb.GetTaskRequest{Id: id})
	if err != nil {
		PrintError(err)
		return nil
	}
	if !resp.Ok {
		PrintErrorMsg(resp.Error)
		return nil
	}

	t := resp.Task
	if PrintJSON(t) {
		return nil
	}

	PrintNewline()
	PrintKeyValue("ID", t.Id)
	PrintKeyValueStyled("Status", t.Status, statusStyle(t.Status))
	PrintKeyValue("Image", t.Image)
	if t.Prompt != "" {
		PrintKeyValue("Prompt", Truncate(t.Prompt, 60))
	}

	if t.HasExitCode {
		exitStyle := SuccessStyle
		if t.ExitCode != 0 {
			exitStyle = ErrorStyle
		}
		PrintKeyValueStyled("Exit Code", fmt.Sprintf("%d", t.ExitCode), exitStyle)
	}

	if t.Error != "" {
		PrintKeyValueStyled("Error", t.Error, ErrorStyle)
	}

	PrintNewline()
	PrintKeyValue("Created", FormatRelativeTime(t.CreatedAt))
	if t.StartedAt != "" {
		PrintKeyValue("Started", FormatRelativeTime(t.StartedAt))
	}
	if t.FinishedAt != "" {
		PrintKeyValue("Finished", FormatRelativeTime(t.FinishedAt))
	}
	PrintNewline()

	return nil
}

func deleteTask(id string) error {
	var client *Client

	err := RunSpinnerWithResult("Deleting task...", func() error {
		var err error
		client, err = getClient()
		if err != nil {
			return err
		}

		resp, err := client.Gateway.DeleteTask(context.Background(), &pb.DeleteTaskRequest{Id: id})
		if err != nil {
			return err
		}
		if !resp.Ok {
			return fmt.Errorf("%s", resp.Error)
		}
		return nil
	})

	if client != nil {
		defer client.Close()
	}

	if err != nil {
		PrintError(err)
		return nil
	}

	PrintSuccessf("Task %s deleted", CodeStyle.Render(id))
	return nil
}

func getTaskLogs(id string) error {
	client, err := getClient()
	if err != nil {
		PrintError(err)
		return nil
	}
	defer client.Close()

	resp, err := client.Gateway.GetTaskLogs(context.Background(), &pb.GetTaskLogsRequest{Id: id})
	if err != nil {
		PrintError(err)
		return nil
	}
	if !resp.Ok {
		PrintErrorMsg(resp.Error)
		return nil
	}

	if len(resp.Logs) == 0 {
		PrintInfo("No logs available")
		return nil
	}

	for _, entry := range resp.Logs {
		fmt.Println(entry.Data)
	}

	return nil
}

// runClaudeCodeTask creates a Claude Code task and polls for logs
func runClaudeCodeTask() error {
	var client *Client
	var taskResp *pb.TaskResponse

	err := RunSpinnerWithResult("Creating task...", func() error {
		var err error
		client, err = getClient()
		if err != nil {
			return err
		}

		taskResp, err = client.Gateway.CreateTask(context.Background(), &pb.CreateTaskRequest{
			Prompt: taskPrompt,
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
	if !taskResp.Ok {
		PrintErrorMsg(taskResp.Error)
		return nil
	}

	t := taskResp.Task
	PrintSuccess("Task created")
	PrintKeyValue("ID", t.Id)
	PrintKeyValue("Prompt", Truncate(t.Prompt, 60))
	PrintNewline()
	fmt.Printf("  %s\n\n", DimStyle.Render("Polling for logs..."))

	// Poll for completion
	lastLogCount := 0
	for {
		time.Sleep(2 * time.Second)

		// Check task status
		statusResp, err := client.Gateway.GetTask(context.Background(), &pb.GetTaskRequest{Id: t.Id})
		if err != nil {
			continue
		}

		// Fetch logs
		logsResp, err := client.Gateway.GetTaskLogs(context.Background(), &pb.GetTaskLogsRequest{Id: t.Id})
		if err == nil && logsResp.Ok {
			for i := lastLogCount; i < len(logsResp.Logs); i++ {
				fmt.Println(logsResp.Logs[i].Data)
			}
			lastLogCount = len(logsResp.Logs)
		}

		// Check if done
		if statusResp != nil && statusResp.Ok {
			status := strings.ToLower(statusResp.Task.Status)
			if status == "complete" || status == "completed" || status == "failed" || status == "cancelled" || status == "error" {
				PrintNewline()
				if status == "complete" || status == "completed" {
					PrintSuccess(fmt.Sprintf("Task %s", status))
				} else {
					PrintErrorMsg(fmt.Sprintf("Task %s", status))
				}
				if statusResp.Task.Error != "" {
					PrintKeyValueStyled("Error", statusResp.Task.Error, ErrorStyle)
				}
				if statusResp.Task.HasExitCode {
					exitStyle := SuccessStyle
					if statusResp.Task.ExitCode != 0 {
						exitStyle = ErrorStyle
					}
					PrintKeyValueStyled("Exit code", fmt.Sprintf("%d", statusResp.Task.ExitCode), exitStyle)
				}
				return nil
			}
		}
	}
}

// statusStyle returns the appropriate style for a task status
func statusStyle(status string) lipgloss.Style {
	switch strings.ToLower(status) {
	case "running":
		return InfoStyle
	case "complete", "completed", "success":
		return SuccessStyle
	case "failed", "error":
		return ErrorStyle
	case "pending", "queued":
		return WarningStyle
	case "cancelled":
		return DimStyle
	default:
		return DimStyle
	}
}
