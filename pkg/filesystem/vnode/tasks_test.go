package vnode

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/beam-cloud/airstore/pkg/repository"
	"github.com/beam-cloud/airstore/pkg/types"
)

// --- Mock Backend ---

type mockTaskBackend struct {
	repository.BackendRepository
	tasks []*types.Task
	token string
}

func (m *mockTaskBackend) ValidateToken(_ context.Context, raw string) (*types.TokenValidationResult, error) {
	if raw == m.token {
		return &types.TokenValidationResult{WorkspaceId: 1}, nil
	}
	return nil, fmt.Errorf("invalid token")
}

func (m *mockTaskBackend) ListTasks(_ context.Context, _ uint) ([]*types.Task, error) {
	return m.tasks, nil
}

func (m *mockTaskBackend) GetTask(_ context.Context, extId string) (*types.Task, error) {
	for _, t := range m.tasks {
		if t.ExternalId == extId {
			return t, nil
		}
	}
	return nil, &types.ErrTaskNotFound{ExternalId: extId}
}

// --- Helpers ---

func newTestTask(id string, status types.TaskStatus) *types.Task {
	t := &types.Task{
		ExternalId:  id,
		WorkspaceId: 1,
		Status:      status,
		CreatedAt:   time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
	}
	if status == types.TaskStatusComplete || status == types.TaskStatusFailed || status == types.TaskStatusCancelled {
		finished := t.CreatedAt.Add(time.Minute)
		t.FinishedAt = &finished
	}
	return t
}

func newTestVNode(tasks ...*types.Task) *TasksVNode {
	return NewTasksVNode(&mockTaskBackend{
		tasks: tasks,
		token: "test-token",
	}, "test-token")
}

// --- Tests ---

func TestTasksVNode_SizeCache_DefaultSentinel(t *testing.T) {
	v := newTestVNode()
	got := v.taskFileSize("unknown")
	if got != taskFileSentinelSize {
		t.Errorf("expected sentinel %d, got %d", taskFileSentinelSize, got)
	}
}

func TestTasksVNode_SizeCache_ReturnsCached(t *testing.T) {
	v := newTestVNode()
	v.sizeCache["abc"] = 42
	got := v.taskFileSize("abc")
	if got != 42 {
		t.Errorf("expected 42, got %d", got)
	}
}

func TestTasksVNode_Getattr_UsesSentinelBeforeRead(t *testing.T) {
	task := newTestTask("task-001", types.TaskStatusComplete)
	v := newTestVNode(task)

	info, err := v.Getattr(TasksPath + "/task-001.task")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if info.Size != taskFileSentinelSize {
		t.Errorf("expected sentinel size %d, got %d", taskFileSentinelSize, info.Size)
	}
}

func TestTasksVNode_Getattr_UsesCachedSizeAfterRead(t *testing.T) {
	task := newTestTask("task-002", types.TaskStatusComplete)
	v := newTestVNode(task)
	v.sizeCache["task-002"] = 500

	info, err := v.Getattr(TasksPath + "/task-002.task")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if info.Size != 500 {
		t.Errorf("expected cached size 500, got %d", info.Size)
	}
}

func TestTasksVNode_Readdir_UsesCachedSize(t *testing.T) {
	task1 := newTestTask("cached-task", types.TaskStatusComplete)
	task2 := newTestTask("uncached-task", types.TaskStatusRunning)
	v := newTestVNode(task1, task2)
	v.sizeCache["cached-task"] = 777

	entries, err := v.Readdir(TasksPath)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(entries))
	}

	sizeByName := make(map[string]int64)
	for _, e := range entries {
		sizeByName[e.Name] = e.Size
	}

	if sizeByName["cached-task.task"] != 777 {
		t.Errorf("expected cached size 777, got %d", sizeByName["cached-task.task"])
	}
	if sizeByName["uncached-task.task"] != taskFileSentinelSize {
		t.Errorf("expected sentinel size %d, got %d", taskFileSentinelSize, sizeByName["uncached-task.task"])
	}
}

func TestTasksVNode_Read_CachesTerminalTaskSize(t *testing.T) {
	task := newTestTask("done-task", types.TaskStatusComplete)
	v := newTestVNode(task)

	buf := make([]byte, 64*1024)
	n, err := v.Read(TasksPath+"/done-task.task", buf, 0, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if n == 0 {
		t.Fatal("expected non-zero read")
	}

	// Verify content looks reasonable
	content := string(buf[:n])
	if !strings.Contains(content, "Task: done-task") {
		t.Errorf("expected task ID in content, got: %s", content[:min(100, len(content))])
	}

	// Check the size cache was populated
	v.sizeCacheMu.RLock()
	cachedSize, ok := v.sizeCache["done-task"]
	v.sizeCacheMu.RUnlock()

	if !ok {
		t.Fatal("expected sizeCache entry for terminal task")
	}
	if cachedSize != int64(n) {
		t.Errorf("expected cached size %d, got %d", n, cachedSize)
	}
}

func TestTasksVNode_Read_DoesNotCacheRunningTask(t *testing.T) {
	task := newTestTask("running-task", types.TaskStatusRunning)
	v := newTestVNode(task)

	buf := make([]byte, 64*1024)
	n, err := v.Read(TasksPath+"/running-task.task", buf, 0, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if n == 0 {
		t.Fatal("expected non-zero read")
	}

	// Check the size cache was NOT populated
	v.sizeCacheMu.RLock()
	_, ok := v.sizeCache["running-task"]
	v.sizeCacheMu.RUnlock()

	if ok {
		t.Error("expected sizeCache to be empty for running task")
	}
}

func TestTasksVNode_Getattr_Dir(t *testing.T) {
	v := newTestVNode()
	info, err := v.Getattr(TasksPath)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if info.Mode&0040000 == 0 {
		t.Error("expected directory mode")
	}
}

func TestTasksVNode_Getattr_NotFound(t *testing.T) {
	v := newTestVNode()
	_, err := v.Getattr(TasksPath + "/nonexistent.task")
	if err != ErrNotFound {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

func TestTasksVNode_Read_SizeConsistentWithGetattr(t *testing.T) {
	task := newTestTask("consistency-task", types.TaskStatusComplete)
	v := newTestVNode(task)

	// Read to populate cache
	buf := make([]byte, 64*1024)
	n, err := v.Read(TasksPath+"/consistency-task.task", buf, 0, 0)
	if err != nil {
		t.Fatalf("Read error: %v", err)
	}

	// Getattr should now return the cached (actual) size
	info, err := v.Getattr(TasksPath + "/consistency-task.task")
	if err != nil {
		t.Fatalf("Getattr error: %v", err)
	}
	if info.Size != int64(n) {
		t.Errorf("Getattr size %d != Read size %d", info.Size, n)
	}
}
