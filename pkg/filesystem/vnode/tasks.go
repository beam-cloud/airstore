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
	"github.com/rs/zerolog/log"
)

const tasksCacheTTL = 10 * time.Second

// TasksVNode provides /tasks directory listing tasks as files.
// Each task appears as a file named {task_id}.task
// Reading the file returns the task logs.
type TasksVNode struct {
	ReadOnlyBase
	backend   repository.BackendRepository
	taskQueue repository.TaskQueue
	token     string

	// Cache for task list to avoid repeated DB calls
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

// NewEmptyTasksVNode creates a TasksVNode without database access.
// This is used for CLI mounts where we don't have direct DB access.
// Tasks will be shown as empty until we implement gRPC-based task listing.
func NewEmptyTasksVNode() *TasksVNode {
	return &TasksVNode{}
}

func (t *TasksVNode) Prefix() string { return TasksPath }

// getWorkspaceId extracts workspace ID from token (simplified - in production, validate token)
func (t *TasksVNode) getWorkspaceId(ctx context.Context) (uint, error) {
	if t.backend == nil {
		return 0, fmt.Errorf("no backend configured")
	}

	// Validate token and get workspace
	if t.token == "" {
		return 0, fmt.Errorf("no token provided")
	}

	result, err := t.backend.ValidateToken(ctx, t.token)
	if err != nil {
		return 0, fmt.Errorf("invalid token: %w", err)
	}

	return result.WorkspaceId, nil
}

// getTasks returns cached tasks or fetches from DB
func (t *TasksVNode) getTasks(ctx context.Context) ([]*types.Task, error) {
	t.cacheMu.RLock()
	if time.Now().Before(t.cacheExpiry) && t.cachedTasks != nil {
		tasks := t.cachedTasks
		t.cacheMu.RUnlock()
		return tasks, nil
	}
	t.cacheMu.RUnlock()

	// Fetch from DB
	workspaceId, err := t.getWorkspaceId(ctx)
	if err != nil {
		return nil, err
	}

	tasks, err := t.backend.ListTasks(ctx, workspaceId)
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

// getTaskByName finds a task by its filename (e.g., "abc123.task")
func (t *TasksVNode) getTaskByName(ctx context.Context, name string) (*types.Task, error) {
	// Extract task ID from filename
	if !strings.HasSuffix(name, ".task") {
		return nil, ErrNotFound
	}
	taskId := strings.TrimSuffix(name, ".task")

	// Get from backend directly (faster than filtering list)
	task, err := t.backend.GetTask(ctx, taskId)
	if err != nil {
		if _, ok := err.(*types.ErrTaskNotFound); ok {
			return nil, ErrNotFound
		}
		return nil, err
	}

	return task, nil
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
	content.WriteString("\n--- Logs ---\n")

	// Get logs from Redis buffer
	if t.taskQueue != nil {
		logs, err := t.taskQueue.GetLogBuffer(ctx, task.ExternalId)
		if err == nil {
			for _, logEntry := range logs {
				// logEntry is JSON, extract data field
				content.Write(logEntry)
				content.WriteString("\n")
			}
		}
	}

	data := []byte(content.String())

	// Handle offset
	if off >= int64(len(data)) {
		return 0, nil
	}

	n := copy(buf, data[off:])
	return n, nil
}
