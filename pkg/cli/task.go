package cli

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/charmbracelet/lipgloss"
	"github.com/spf13/cobra"

	pb "github.com/beam-cloud/airstore/proto"
)

var taskCmd = &cobra.Command{
	Use:   "task",
	Short: "Manage tasks",
	Long:  `List and manage tasks created by hooks.`,
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

func init() {
	taskCmd.AddCommand(taskListCmd)
	taskCmd.AddCommand(taskGetCmd)
	taskCmd.AddCommand(taskDeleteCmd)
	taskCmd.AddCommand(taskLogsCmd)
}

func listTasks() error {
	client, err := getClient()
	if err != nil {
		PrintError(err)
		return nil
	}
	defer client.Close()

	resp, err := client.Tasks.ListTasks(context.Background(), &pb.ListTasksRequest{})
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
		PrintHint("Tasks are created automatically when hooks trigger on file changes")
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

	resp, err := client.Tasks.GetTask(context.Background(), &pb.GetTaskRequest{Id: id})
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

		resp, err := client.Tasks.DeleteTask(context.Background(), &pb.DeleteTaskRequest{Id: id})
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

	resp, err := client.Tasks.GetTaskLogs(context.Background(), &pb.GetTaskLogsRequest{Id: id})
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
		fmt.Fprintln(streamWriter(entry.Stream), entry.Data)
	}

	return nil
}

// streamWriter returns the appropriate output writer for a log stream.
func streamWriter(stream string) *os.File {
	if stream == "stderr" {
		return os.Stderr
	}
	return os.Stdout
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
