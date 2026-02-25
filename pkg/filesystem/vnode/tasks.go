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
const taskFileSentinelSize int64 = 10 << 20 // 10MB — must be large enough for FUSE/NFS to issue reads covering all content

// TasksVNode provides /tasks directory listing tasks as files.
// Each task appears as a file named {task_id}.task
// Reading the file returns the task logs.
type TasksVNode struct {
	ReadOnlyBase

	// Direct backend access (gateway mode)
	backend repository.BackendRepository

	// gRPC gateway access (CLI mode)
	grpcConn    *grpc.ClientConn
	token       string
	bearerToken string // precomputed auth header value

	// Cache for task list
	cacheMu     sync.RWMutex
	cachedTasks []*types.AgentTask
	cacheExpiry time.Time

	// Cache for rendered task file sizes (populated after Read)
	sizeCacheMu sync.RWMutex
	sizeCache   map[string]int64
}

// NewTasksVNode creates a TasksVNode with database access for task listing.
// Use this when the backend is available (e.g., in gateway).
func NewTasksVNode(backend repository.BackendRepository, token string) *TasksVNode {
	return &TasksVNode{
		backend:   backend,
		token:     token,
		sizeCache: make(map[string]int64),
	}
}

// NewTasksVNodeGRPC creates a TasksVNode that fetches tasks via gRPC from the gateway.
// Use this for CLI mounts where we don't have direct DB access.
func NewTasksVNodeGRPC(conn *grpc.ClientConn, token string) *TasksVNode {
	t := &TasksVNode{
		grpcConn:    conn,
		token:       token,
		bearerToken: BearerToken(token),
		sizeCache:   make(map[string]int64),
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

// taskFileSize returns the cached rendered size for a task, or the sentinel size if not yet cached.
func (t *TasksVNode) taskFileSize(taskId string) int64 {
	t.sizeCacheMu.RLock()
	if size, ok := t.sizeCache[taskId]; ok {
		t.sizeCacheMu.RUnlock()
		return size
	}
	t.sizeCacheMu.RUnlock()
	return taskFileSentinelSize
}

func (t *TasksVNode) Prefix() string { return TasksPath }

// grpcContext adds auth token to context for gRPC calls
func (t *TasksVNode) grpcContext(ctx context.Context) context.Context {
	if t.bearerToken != "" {
		md := metadata.Pairs("authorization", t.bearerToken)
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
func (t *TasksVNode) getTasks(ctx context.Context) ([]*types.AgentTask, error) {
	t.cacheMu.RLock()
	if time.Now().Before(t.cacheExpiry) && t.cachedTasks != nil {
		tasks := t.cachedTasks
		t.cacheMu.RUnlock()
		return tasks, nil
	}
	t.cacheMu.RUnlock()

	var tasks []*types.AgentTask
	var err error

	if t.backend != nil {
		// Direct DB access (gateway mode)
		var workspaceId uint
		workspaceId, err = t.getWorkspaceId(ctx)
		if err != nil {
			return nil, err
		}
		agentTasks, listErr := t.backend.ListTasks(ctx, workspaceId, 100)
		if listErr != nil {
			return nil, listErr
		}

		tasks = agentTasks
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

// grpcClient returns a cached gRPC client.
func (t *TasksVNode) grpcClient() pb.AgentServiceClient {
	return pb.NewAgentServiceClient(t.grpcConn)
}

// fetchTasksGRPC fetches tasks from the gateway via gRPC
func (t *TasksVNode) fetchTasksGRPC(ctx context.Context) ([]*types.AgentTask, error) {
	resp, err := t.grpcClient().ListTasks(t.grpcContext(ctx), &pb.ListTasksRequest{})
	if err != nil {
		return nil, err
	}
	if !resp.Ok {
		return nil, fmt.Errorf("ListTasks failed: %s", resp.Error)
	}

	tasks := make([]*types.AgentTask, len(resp.Tasks))
	for i, pt := range resp.Tasks {
		tasks[i] = pbToAgentTask(pt)
	}
	return tasks, nil
}

// getTaskByName finds a task by its filename (e.g., "abc123.task")
// Uses cached task list first for fast lookups during directory listing.
func (t *TasksVNode) getTaskByName(ctx context.Context, name string) (*types.AgentTask, error) {
	if isAppleDoublePath(name) {
		return nil, ErrNotFound
	}
	if !strings.HasSuffix(name, ".task") {
		return nil, ErrNotFound
	}
	taskId := strings.TrimSuffix(name, ".task")

	// Check cache first - fast path for Getattr during ls
	t.cacheMu.RLock()
	if t.cachedTasks != nil && time.Now().Before(t.cacheExpiry) {
		for _, task := range t.cachedTasks {
			if task.ID == taskId {
				t.cacheMu.RUnlock()
				return task, nil
			}
		}
	}
	t.cacheMu.RUnlock()

	// Cache miss - fetch directly with workspace validation
	if t.backend != nil {
		// Get workspace ID from token
		workspaceId, err := t.getWorkspaceId(ctx)
		if err != nil {
			return nil, ErrNotFound
		}

		task, err := t.backend.GetTask(ctx, workspaceId, taskId)
		if err != nil {
			if _, ok := err.(*types.ErrAgentTaskNotFound); ok {
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
func (t *TasksVNode) fetchTaskGRPC(ctx context.Context, taskId string) (*types.AgentTask, error) {
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
	return pbToAgentTask(resp.Task), nil
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

// pbToAgentTask converts a proto task to an AgentTask view.
func pbToAgentTask(pt *pb.AgentTask) *types.AgentTask {
	if pt == nil {
		return &types.AgentTask{}
	}
	task := &types.AgentTask{
		ID:             pt.Id,
		AgentID:        optionalString(pt.AgentId),
		Kind:           types.AgentTaskKind(pt.Kind),
		QueueMode:      types.AgentQueueMode(pt.QueueMode),
		State:          types.AgentTaskState(pt.State),
		IdempotencyKey: pt.IdempotencyKey,
		TargetRunID:    optionalString(pt.TargetRunId),
		DroppedReason:  optionalString(pt.DroppedReason),
	}
	if pt.CreatedAt != "" {
		if parsed, err := time.Parse(time.RFC3339, pt.CreatedAt); err == nil {
			task.CreatedAt = parsed
		}
	}
	if pt.UpdatedAt != "" {
		if parsed, err := time.Parse(time.RFC3339, pt.UpdatedAt); err == nil {
			task.UpdatedAt = parsed
		}
	}
	return task
}

func optionalString(value string) *string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return nil
	}
	return &trimmed
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
	if isAppleDoublePath(rel) {
		return NewFileInfo(PathIno(path), 0, 0644), nil
	}

	task, err := t.getTaskByName(ctx, rel)
	if err != nil {
		return nil, err
	}

	// Task file - return file info (use cached size if available)
	info := NewFileInfo(PathIno(path), t.taskFileSize(task.ExternalId), 0644)
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
		name := taskFilename(task.ID)
		mtime := task.CreatedAt.Unix()
		entries = append(entries, DirEntry{
			Name:  name,
			Mode:  syscall.S_IFREG | 0644,
			Ino:   PathIno(TasksPath + "/" + name),
			Size:  t.taskFileSize(task.ExternalId),
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
	if isAppleDoublePath(rel) {
		return FileHandle(PathIno(path)), nil
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
	if isAppleDoublePath(rel) {
		return 0, nil
	}

	task, err := t.getTaskByName(ctx, rel)
	if err != nil {
		return 0, err
	}

	// Build task content: task info + logs
	var content strings.Builder
	content.WriteString(fmt.Sprintf("Task: %s\n", task.ID))
	content.WriteString(fmt.Sprintf("State: %s\n", task.State))
	content.WriteString(fmt.Sprintf("Kind: %s\n", task.Kind))
	content.WriteString(fmt.Sprintf("Queue Mode: %s\n", task.QueueMode))
	if task.TargetRunID != nil {
		content.WriteString(fmt.Sprintf("Run: %s\n", *task.TargetRunID))
	}
	content.WriteString(fmt.Sprintf("Created: %s\n", task.CreatedAt.Format(time.RFC3339)))
	if task.UpdatedAt.Unix() > 0 {
		content.WriteString(fmt.Sprintf("Updated: %s\n", task.UpdatedAt.Format(time.RFC3339)))
	}
	if task.DroppedReason != nil {
		content.WriteString(fmt.Sprintf("Dropped Reason: %s\n", *task.DroppedReason))
	}
	content.WriteString("\n--- Output ---\n")

	// Get logs via gRPC (reads from S2)
	if t.grpcConn != nil {
		logs := t.fetchTaskLogsGRPC(ctx, task.ID)
		content.WriteString(logs)
	} else {
		content.WriteString("(logs available via API)\n")
	}

	data := []byte(content.String())

	// Cache rendered size for terminal tasks (complete/failed/cancelled)
	if task.IsTerminal() {
		t.sizeCacheMu.Lock()
		t.sizeCache[task.ExternalId] = int64(len(data))
		t.sizeCacheMu.Unlock()
	}

	// Handle offset
	if off >= int64(len(data)) {
		return 0, nil
	}

	n := copy(buf, data[off:])
	return n, nil
}
