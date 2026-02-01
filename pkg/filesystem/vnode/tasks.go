package vnode

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/beam-cloud/airstore/pkg/repository"
	"github.com/beam-cloud/airstore/pkg/types"
	pb "github.com/beam-cloud/airstore/proto"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

const tasksCacheTTL = 5 * time.Second

// TasksVNode provides /tasks directory listing tasks as files.
// Each task appears as a file named {task_id}.task
// Reading the file returns the task logs.
type TasksVNode struct {
	ReadOnlyBase

	// Direct backend access (gateway mode)
	backend   repository.BackendRepository
	taskQueue repository.TaskQueue

	// gRPC gateway access (CLI mode)
	grpcConn *grpc.ClientConn
	token    string

	// Cache for task list
	cacheMu     sync.RWMutex
	cachedTasks []*types.Task
	cacheExpiry time.Time
}

// NewTasksVNode creates a TasksVNode with database access for task listing.
// Use this when the backend and taskQueue are available (e.g., in gateway).
func NewTasksVNode(backend repository.BackendRepository, taskQueue repository.TaskQueue, token string) *TasksVNode {
	return &TasksVNode{
		backend:   backend,
		taskQueue: taskQueue,
		token:     token,
	}
}

// NewTasksVNodeGRPC creates a TasksVNode that fetches tasks via gRPC from the gateway.
// Use this for CLI mounts where we don't have direct DB access.
func NewTasksVNodeGRPC(conn *grpc.ClientConn, token string) *TasksVNode {
	t := &TasksVNode{
		grpcConn: conn,
		token:    token,
	}
	// Pre-warm cache in background
	go t.warmCache()
	return t
}

// warmCache pre-fetches the task list to avoid cold start latency
func (t *TasksVNode) warmCache() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	t.getTasks(ctx)
}

func (t *TasksVNode) Prefix() string { return TasksPath }

// grpcContext adds auth token to context for gRPC calls
func (t *TasksVNode) grpcContext(ctx context.Context) context.Context {
	if t.token != "" {
		md := metadata.Pairs("authorization", "Bearer "+t.token)
		ctx = metadata.NewOutgoingContext(ctx, md)
	}
	return ctx
}

// getWorkspaceId extracts workspace ID from token via backend validation
func (t *TasksVNode) getWorkspaceId(ctx context.Context) (uint, error) {
	if t.backend == nil {
		return 0, fmt.Errorf("no backend configured")
	}

	if t.token == "" {
		return 0, fmt.Errorf("no token provided")
	}

	result, err := t.backend.ValidateToken(ctx, t.token)
	if err != nil {
		return 0, fmt.Errorf("invalid token: %w", err)
	}

	return result.WorkspaceId, nil
}

// getTasks returns cached tasks or fetches fresh data
func (t *TasksVNode) getTasks(ctx context.Context) ([]*types.Task, error) {
	t.cacheMu.RLock()
	if time.Now().Before(t.cacheExpiry) && t.cachedTasks != nil {
		tasks := t.cachedTasks
		t.cacheMu.RUnlock()
		return tasks, nil
	}
	t.cacheMu.RUnlock()

	var tasks []*types.Task
	var err error

	if t.backend != nil {
		// Direct DB access (gateway mode)
		var workspaceId uint
		workspaceId, err = t.getWorkspaceId(ctx)
		if err != nil {
			return nil, err
		}
		tasks, err = t.backend.ListTasks(ctx, workspaceId)
	} else if t.grpcConn != nil {
		// gRPC access (CLI mode)
		tasks, err = t.fetchTasksGRPC(ctx)
	} else {
		return nil, fmt.Errorf("no backend or gateway configured")
	}

	if err != nil {
		return nil, err
	}

	// Update cache
	t.cacheMu.Lock()
	t.cachedTasks = tasks
	t.cacheExpiry = time.Now().Add(tasksCacheTTL)
	t.cacheMu.Unlock()

	return tasks, nil
}

// grpcClient returns a cached gRPC client
func (t *TasksVNode) grpcClient() pb.GatewayServiceClient {
	return pb.NewGatewayServiceClient(t.grpcConn)
}

// fetchTasksGRPC fetches tasks from the gateway via gRPC
func (t *TasksVNode) fetchTasksGRPC(ctx context.Context) ([]*types.Task, error) {
	resp, err := t.grpcClient().ListTasks(t.grpcContext(ctx), &pb.ListTasksRequest{})
	if err != nil {
		return nil, err
	}
	if !resp.Ok {
		return nil, fmt.Errorf("ListTasks failed: %s", resp.Error)
	}

	tasks := make([]*types.Task, len(resp.Tasks))
	for i, pt := range resp.Tasks {
		tasks[i] = pbToTask(pt)
	}
	return tasks, nil
}

// getTaskByName finds a task by its filename (e.g., "abc123.task")
// Uses cached task list first for fast lookups during directory listing.
func (t *TasksVNode) getTaskByName(ctx context.Context, name string) (*types.Task, error) {
	if !strings.HasSuffix(name, ".task") {
		return nil, ErrNotFound
	}
	taskId := strings.TrimSuffix(name, ".task")

	// Check cache first - fast path for Getattr during ls
	t.cacheMu.RLock()
	if t.cachedTasks != nil && time.Now().Before(t.cacheExpiry) {
		for _, task := range t.cachedTasks {
			if task.ExternalId == taskId {
				t.cacheMu.RUnlock()
				return task, nil
			}
		}
	}
	t.cacheMu.RUnlock()

	// Cache miss - fetch directly
	if t.backend != nil {
		task, err := t.backend.GetTask(ctx, taskId)
		if err != nil {
			if _, ok := err.(*types.ErrTaskNotFound); ok {
				return nil, ErrNotFound
			}
			return nil, err
		}
		return task, nil
	}

	if t.grpcConn != nil {
		return t.fetchTaskGRPC(ctx, taskId)
	}

	return nil, ErrNotFound
}

// fetchTaskGRPC fetches a single task from the gateway via gRPC
func (t *TasksVNode) fetchTaskGRPC(ctx context.Context, taskId string) (*types.Task, error) {
	resp, err := t.grpcClient().GetTask(t.grpcContext(ctx), &pb.GetTaskRequest{Id: taskId})
	if err != nil {
		return nil, err
	}
	if !resp.Ok {
		if resp.Error == "task not found" {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("GetTask failed: %s", resp.Error)
	}
	return pbToTask(resp.Task), nil
}

// fetchTaskLogsGRPC fetches task logs via gRPC
func (t *TasksVNode) fetchTaskLogsGRPC(ctx context.Context, taskId string) string {
	resp, err := t.grpcClient().GetTaskLogs(t.grpcContext(ctx), &pb.GetTaskLogsRequest{Id: taskId})
	if err != nil || !resp.Ok {
		return ""
	}

	var sb strings.Builder
	for _, entry := range resp.Logs {
		sb.WriteString(entry.Data)
		sb.WriteByte('\n')
	}
	return sb.String()
}

// pbToTask converts a proto Task to a types.Task
func pbToTask(pt *pb.Task) *types.Task {
	task := &types.Task{
		ExternalId: pt.Id,
		Status:     types.TaskStatus(pt.Status),
		Prompt:     pt.Prompt,
		Image:      pt.Image,
		Error:      pt.Error,
	}
	if pt.HasExitCode {
		exitCode := int(pt.ExitCode)
		task.ExitCode = &exitCode
	}
	if pt.CreatedAt != "" {
		if t, err := time.Parse(time.RFC3339, pt.CreatedAt); err == nil {
			task.CreatedAt = t
		}
	}
	if pt.StartedAt != "" {
		if t, err := time.Parse(time.RFC3339, pt.StartedAt); err == nil {
			task.StartedAt = &t
		}
	}
	if pt.FinishedAt != "" {
		if t, err := time.Parse(time.RFC3339, pt.FinishedAt); err == nil {
			task.FinishedAt = &t
		}
	}
	return task
}

// taskFilename returns the filename for a task
func taskFilename(taskId string) string {
	return taskId + ".task"
}

func (t *TasksVNode) Getattr(path string) (*FileInfo, error) {
	ctx := context.Background()

	if path == TasksPath {
		return NewDirInfo(PathIno(path)), nil
	}

	// Check if it's a task file
	rel := strings.TrimPrefix(path, TasksPath+"/")
	if rel == "" || strings.Contains(rel, "/") {
		return nil, ErrNotFound
	}

	task, err := t.getTaskByName(ctx, rel)
	if err != nil {
		return nil, err
	}

	// Task file - return file info
	info := NewFileInfo(PathIno(path), 0, 0644)
	if task.CreatedAt.Unix() > 0 {
		info.Mtime = task.CreatedAt
		info.Ctime = task.CreatedAt
		info.Atime = task.CreatedAt
	}
	return info, nil
}

func (t *TasksVNode) Readdir(path string) ([]DirEntry, error) {
	ctx := context.Background()

	if path != TasksPath {
		return nil, ErrNotFound
	}

	tasks, err := t.getTasks(ctx)
	if err != nil {
		log.Warn().Err(err).Msg("failed to get tasks for Readdir")
		return []DirEntry{}, nil // Return empty rather than error
	}

	entries := make([]DirEntry, 0, len(tasks))
	for _, task := range tasks {
		name := taskFilename(task.ExternalId)
		mtime := task.CreatedAt.Unix()
		entries = append(entries, DirEntry{
			Name:  name,
			Mode:  syscall.S_IFREG | 0644,
			Ino:   PathIno(TasksPath + "/" + name),
			Size:  0, // Size unknown until read
			Mtime: mtime,
		})
	}

	return entries, nil
}

func (t *TasksVNode) Open(path string, flags int) (FileHandle, error) {
	ctx := context.Background()

	rel := strings.TrimPrefix(path, TasksPath+"/")
	if rel == "" || strings.Contains(rel, "/") {
		return 0, ErrNotFound
	}

	// Verify task exists
	_, err := t.getTaskByName(ctx, rel)
	if err != nil {
		return 0, err
	}

	// Return a handle (we'll fetch logs on Read)
	return FileHandle(PathIno(path)), nil
}

func (t *TasksVNode) Read(path string, buf []byte, off int64, fh FileHandle) (int, error) {
	ctx := context.Background()

	rel := strings.TrimPrefix(path, TasksPath+"/")
	if rel == "" {
		return 0, ErrNotFound
	}

	task, err := t.getTaskByName(ctx, rel)
	if err != nil {
		return 0, err
	}

	// Build task content: task info + logs
	var content strings.Builder
	content.WriteString(fmt.Sprintf("Task: %s\n", task.ExternalId))
	content.WriteString(fmt.Sprintf("Status: %s\n", task.Status))
	if task.Prompt != "" {
		content.WriteString(fmt.Sprintf("Prompt: %s\n", task.Prompt))
	}
	content.WriteString(fmt.Sprintf("Created: %s\n", task.CreatedAt.Format(time.RFC3339)))
	if task.StartedAt != nil {
		content.WriteString(fmt.Sprintf("Started: %s\n", task.StartedAt.Format(time.RFC3339)))
	}
	if task.FinishedAt != nil {
		content.WriteString(fmt.Sprintf("Finished: %s\n", task.FinishedAt.Format(time.RFC3339)))
	}
	if task.ExitCode != nil {
		content.WriteString(fmt.Sprintf("Exit Code: %d\n", *task.ExitCode))
	}
	if task.Error != "" {
		content.WriteString(fmt.Sprintf("Error: %s\n", task.Error))
	}
	content.WriteString("\n--- Output ---\n")

	// Get logs - try taskQueue first (gateway mode), then gRPC (CLI mode)
	if t.taskQueue != nil {
		logs, err := t.taskQueue.GetLogBuffer(ctx, task.ExternalId)
		if err == nil {
			for _, logEntry := range logs {
				content.Write(logEntry)
				content.WriteString("\n")
			}
		}
	} else if t.grpcConn != nil {
		// Fetch logs via gRPC
		logs := t.fetchTaskLogsGRPC(ctx, task.ExternalId)
		content.WriteString(logs)
	}

	data := []byte(content.String())

	// Handle offset
	if off >= int64(len(data)) {
		return 0, nil
	}

	n := copy(buf, data[off:])
	return n, nil
}
