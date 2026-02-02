package cli

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/charmbracelet/lipgloss"
	"github.com/spf13/cobra"
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
	Short: "Stream task logs",
	Long:  `Stream logs from a running or completed task. Use -f to follow logs in real-time.`,
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		return streamTaskLogs(args[0], taskFollow)
	},
}

var taskRunCmd = &cobra.Command{
	Use:   "run",
	Short: "Run a Claude Code task",
	Long:  `Create and run a Claude Code task with the given prompt. Streams logs in real-time.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		return runClaudeCodeTask()
	},
}

func init() {
	taskCreateCmd.Flags().StringVarP(&taskWorkspace, "workspace", "w", "", "Workspace name (required)")
	taskCreateCmd.Flags().StringVarP(&taskImage, "image", "i", "", "Container image (optional if prompt provided)")
	taskCreateCmd.Flags().StringVarP(&taskPrompt, "prompt", "p", "", "Claude Code prompt (auto-sets image)")
	taskCreateCmd.Flags().StringVarP(&taskEntrypoint, "entrypoint", "e", "", "Entrypoint command (comma-separated)")
	taskCreateCmd.Flags().StringSliceVar(&taskEnv, "env", nil, "Environment variables (KEY=VALUE)")
	taskCreateCmd.MarkFlagRequired("workspace")

	taskListCmd.Flags().StringVarP(&taskWorkspace, "workspace", "w", "", "Filter by workspace name")

	taskLogsCmd.Flags().BoolVarP(&taskFollow, "follow", "f", false, "Follow logs in real-time")

	taskRunCmd.Flags().StringVarP(&taskWorkspace, "workspace", "w", "", "Workspace name (required)")
	taskRunCmd.Flags().StringVarP(&taskPrompt, "prompt", "p", "", "Claude Code prompt (required)")
	taskRunCmd.MarkFlagRequired("workspace")
	taskRunCmd.MarkFlagRequired("prompt")

	taskCmd.AddCommand(taskCreateCmd)
	taskCmd.AddCommand(taskListCmd)
	taskCmd.AddCommand(taskGetCmd)
	taskCmd.AddCommand(taskDeleteCmd)
	taskCmd.AddCommand(taskLogsCmd)
	taskCmd.AddCommand(taskRunCmd)
}

type taskResponse struct {
	ID          string            `json:"external_id"`
	WorkspaceID string            `json:"workspace_id"`
	Status      string            `json:"status"`
	Prompt      string            `json:"prompt,omitempty"`
	Image       string            `json:"image"`
	Entrypoint  []string          `json:"entrypoint"`
	Env         map[string]string `json:"env"`
	ExitCode    *int              `json:"exit_code,omitempty"`
	Error       string            `json:"error,omitempty"`
	CreatedAt   string            `json:"created_at"`
	StartedAt   string            `json:"started_at,omitempty"`
	FinishedAt  string            `json:"finished_at,omitempty"`
}

type apiResponse struct {
	Success bool            `json:"success"`
	Error   string          `json:"error,omitempty"`
	Data    json.RawMessage `json:"data,omitempty"`
}

type taskWorkspaceResponse struct {
	ID        string `json:"id"`
	Name      string `json:"name"`
	CreatedAt string `json:"created_at"`
}

func httpGatewayAddr() string {
	// Convert gRPC address to HTTP (task API still uses HTTP)
	addr := gatewayAddr
	if strings.HasPrefix(addr, "localhost:") {
		parts := strings.Split(addr, ":")
		if len(parts) == 2 {
			// Assume HTTP API is on port 1994 if gRPC is 1993
			return "http://localhost:1994"
		}
	}
	return "http://" + addr
}

func createTask() error {
	// Validate - either prompt or image must be provided
	if taskPrompt == "" && taskImage == "" {
		PrintErrorMsg("Either --prompt or --image is required")
		return nil
	}

	// Parse entrypoint
	var entrypoint []string
	if taskEntrypoint != "" {
		entrypoint = strings.Split(taskEntrypoint, ",")
		for i := range entrypoint {
			entrypoint[i] = strings.TrimSpace(entrypoint[i])
		}
	}

	// Parse env
	env := make(map[string]string)
	for _, e := range taskEnv {
		parts := strings.SplitN(e, "=", 2)
		if len(parts) == 2 {
			env[parts[0]] = parts[1]
		}
	}

	payload := map[string]interface{}{
		"workspace_name": taskWorkspace,
		"image":          taskImage,
		"prompt":         taskPrompt,
		"entrypoint":     entrypoint,
		"env":            env,
	}
	body, err := json.Marshal(payload)
	if err != nil {
		PrintError(err)
		return nil
	}

	var task taskResponse

	err = RunSpinnerWithResult("Creating task...", func() error {
		resp, err := http.Post(httpGatewayAddr()+"/api/v1/tasks", "application/json", bytes.NewReader(body))
		if err != nil {
			return fmt.Errorf("failed to connect to gateway: %w", err)
		}
		defer resp.Body.Close()

		var result apiResponse
		if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
			return fmt.Errorf("failed to decode response: %w", err)
		}

		if !result.Success {
			return fmt.Errorf("%s", result.Error)
		}

		if err := json.Unmarshal(result.Data, &task); err != nil {
			return fmt.Errorf("failed to parse task: %w", err)
		}

		return nil
	})

	if err != nil {
		PrintError(err)
		return nil
	}

	PrintSuccess("Task created")
	PrintNewline()
	PrintKeyValue("ID", task.ID)
	if task.Prompt != "" {
		PrintKeyValue("Prompt", Truncate(task.Prompt, 50))
	}
	PrintKeyValue("Image", Truncate(task.Image, 50))
	PrintKeyValueStyled("Status", task.Status, statusStyle(task.Status))
	PrintNewline()

	return nil
}

func listTasks() error {
	url := httpGatewayAddr() + "/api/v1/tasks"
	if taskWorkspace != "" {
		// Need to look up workspace ID first
		wsResp, err := http.Get(httpGatewayAddr() + "/api/v1/workspaces")
		if err != nil {
			PrintError(fmt.Errorf("failed to connect to gateway: %w", err))
			return nil
		}
		defer wsResp.Body.Close()

		var wsResult apiResponse
		if err := json.NewDecoder(wsResp.Body).Decode(&wsResult); err != nil {
			PrintError(fmt.Errorf("failed to decode response: %w", err))
			return nil
		}

		var workspaces []taskWorkspaceResponse
		if err := json.Unmarshal(wsResult.Data, &workspaces); err != nil {
			PrintError(fmt.Errorf("failed to parse workspaces: %w", err))
			return nil
		}

		var wsID string
		for _, ws := range workspaces {
			if ws.Name == taskWorkspace {
				wsID = ws.ID
				break
			}
		}
		if wsID == "" {
			PrintErrorMsg(fmt.Sprintf("Workspace not found: %s", taskWorkspace))
			return nil
		}
		url += "?workspace_id=" + wsID
	}

	resp, err := http.Get(url)
	if err != nil {
		PrintError(fmt.Errorf("failed to connect to gateway: %w", err))
		return nil
	}
	defer resp.Body.Close()

	var result apiResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		PrintError(fmt.Errorf("failed to decode response: %w", err))
		return nil
	}

	if !result.Success {
		PrintErrorMsg(result.Error)
		return nil
	}

	var tasks []taskResponse
	if err := json.Unmarshal(result.Data, &tasks); err != nil {
		PrintError(fmt.Errorf("failed to parse tasks: %w", err))
		return nil
	}

	// JSON output
	if PrintJSON(tasks) {
		return nil
	}

	if len(tasks) == 0 {
		PrintInfo("No tasks found")
		PrintHint("Create one with: airstore task create --workspace <name> --prompt \"...\"")
		return nil
	}

	PrintHeader("Tasks")

	table := NewTable("ID", "STATUS", "IMAGE", "CREATED", "EXIT")
	for _, t := range tasks {
		exitCode := "-"
		if t.ExitCode != nil {
			exitCode = fmt.Sprintf("%d", *t.ExitCode)
		}
		image := Truncate(t.Image, 35)
		table.AddRow(t.ID, t.Status, image, FormatRelativeTime(t.CreatedAt), exitCode)
	}
	table.Print()
	PrintNewline()

	return nil
}

func getTask(id string) error {
	resp, err := http.Get(httpGatewayAddr() + "/api/v1/tasks/" + id)
	if err != nil {
		PrintError(fmt.Errorf("failed to connect to gateway: %w", err))
		return nil
	}
	defer resp.Body.Close()

	var result apiResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		PrintError(fmt.Errorf("failed to decode response: %w", err))
		return nil
	}

	if !result.Success {
		PrintErrorMsg(fmt.Sprintf("Task not found: %s", result.Error))
		return nil
	}

	var task taskResponse
	if err := json.Unmarshal(result.Data, &task); err != nil {
		PrintError(fmt.Errorf("failed to parse task: %w", err))
		return nil
	}

	// JSON output
	if PrintJSON(task) {
		return nil
	}

	PrintNewline()
	PrintKeyValue("ID", task.ID)
	PrintKeyValue("Workspace", task.WorkspaceID)
	PrintKeyValueStyled("Status", task.Status, statusStyle(task.Status))
	PrintKeyValue("Image", task.Image)

	if len(task.Entrypoint) > 0 {
		PrintKeyValue("Entrypoint", strings.Join(task.Entrypoint, " "))
	}

	if len(task.Env) > 0 {
		PrintNewline()
		fmt.Printf("  %s\n", DimStyle.Render("Environment:"))
		for k, v := range task.Env {
			fmt.Printf("    %s=%s\n", k, v)
		}
	}

	if task.ExitCode != nil {
		exitStyle := SuccessStyle
		if *task.ExitCode != 0 {
			exitStyle = ErrorStyle
		}
		PrintKeyValueStyled("Exit Code", fmt.Sprintf("%d", *task.ExitCode), exitStyle)
	}

	if task.Error != "" {
		PrintKeyValueStyled("Error", task.Error, ErrorStyle)
	}

	PrintNewline()
	PrintKeyValue("Created", FormatRelativeTime(task.CreatedAt))
	if task.StartedAt != "" {
		PrintKeyValue("Started", FormatRelativeTime(task.StartedAt))
	}
	if task.FinishedAt != "" {
		PrintKeyValue("Finished", FormatRelativeTime(task.FinishedAt))
	}
	PrintNewline()

	return nil
}

func deleteTask(id string) error {
	err := RunSpinnerWithResult("Deleting task...", func() error {
		req, err := http.NewRequest("DELETE", httpGatewayAddr()+"/api/v1/tasks/"+id, nil)
		if err != nil {
			return err
		}

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return fmt.Errorf("failed to connect to gateway: %w", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode == http.StatusNotFound {
			return fmt.Errorf("task not found")
		}

		body, _ := io.ReadAll(resp.Body)
		var result apiResponse
		if err := json.Unmarshal(body, &result); err != nil {
			return fmt.Errorf("failed to decode response: %w", err)
		}

		if !result.Success {
			return fmt.Errorf("%s", result.Error)
		}

		return nil
	})

	if err != nil {
		PrintError(err)
		return nil
	}

	PrintSuccessf("Task %s deleted", CodeStyle.Render(id))
	return nil
}

// streamTaskLogs streams logs from a task via SSE
func streamTaskLogs(id string, follow bool) error {
	url := httpGatewayAddr() + "/api/v1/tasks/" + id + "/logs/stream"

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		PrintError(err)
		return nil
	}
	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set("Cache-Control", "no-cache")

	client := &http.Client{
		Timeout: 0, // No timeout for streaming
	}

	resp, err := client.Do(req)
	if err != nil {
		PrintError(fmt.Errorf("failed to connect to gateway: %w", err))
		return nil
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		PrintErrorMsg("Task not found")
		return nil
	}

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		PrintErrorMsg(string(body))
		return nil
	}

	// Read SSE events
	reader := bufio.NewReader(resp.Body)
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}

		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "data: ") {
			continue
		}

		data := strings.TrimPrefix(line, "data: ")
		var event map[string]interface{}
		if err := json.Unmarshal([]byte(data), &event); err != nil {
			continue
		}

		// Check if it's a log entry or status event
		if logData, ok := event["data"].(string); ok {
			// Log entry
			stream := "stdout"
			if s, ok := event["stream"].(string); ok {
				stream = s
			}
			if stream == "stderr" {
				fmt.Fprintf(os.Stderr, "%s\n", logData)
			} else {
				fmt.Println(logData)
			}
		} else if status, ok := event["status"].(string); ok {
			// Status event
			if status == "complete" || status == "failed" || status == "cancelled" {
				if !follow {
					return nil
				}
				PrintNewline()
				if status == "complete" {
					PrintSuccess(fmt.Sprintf("Task %s", status))
				} else {
					PrintErrorMsg(fmt.Sprintf("Task %s", status))
				}
				if errMsg, ok := event["error"].(string); ok && errMsg != "" {
					PrintKeyValueStyled("Error", errMsg, ErrorStyle)
				}
				if exitCode, ok := event["exit_code"].(float64); ok {
					exitStyle := SuccessStyle
					if int(exitCode) != 0 {
						exitStyle = ErrorStyle
					}
					PrintKeyValueStyled("Exit code", fmt.Sprintf("%d", int(exitCode)), exitStyle)
				}
				return nil
			}
		}
	}
}

// runClaudeCodeTask creates a Claude Code task and streams logs
func runClaudeCodeTask() error {
	// Create the task with the prompt
	payload := map[string]interface{}{
		"workspace_name": taskWorkspace,
		"prompt":         taskPrompt,
	}
	body, err := json.Marshal(payload)
	if err != nil {
		PrintError(err)
		return nil
	}

	var task taskResponse

	err = RunSpinnerWithResult("Creating task...", func() error {
		resp, err := http.Post(httpGatewayAddr()+"/api/v1/tasks", "application/json", bytes.NewReader(body))
		if err != nil {
			return fmt.Errorf("failed to connect to gateway: %w", err)
		}
		defer resp.Body.Close()

		var result apiResponse
		if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
			return fmt.Errorf("failed to decode response: %w", err)
		}

		if !result.Success {
			return fmt.Errorf("%s", result.Error)
		}

		if err := json.Unmarshal(result.Data, &task); err != nil {
			return fmt.Errorf("failed to parse task: %w", err)
		}

		return nil
	})

	if err != nil {
		PrintError(err)
		return nil
	}

	PrintSuccess("Task created")
	PrintKeyValue("ID", task.ID)
	PrintKeyValue("Prompt", Truncate(task.Prompt, 60))
	PrintNewline()
	fmt.Printf("  %s\n\n", DimStyle.Render("Streaming logs..."))

	// Small delay to let the task start
	time.Sleep(500 * time.Millisecond)

	// Stream logs
	return streamTaskLogs(task.ID, true)
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
