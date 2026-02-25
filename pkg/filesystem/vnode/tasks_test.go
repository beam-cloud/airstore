package vnode

import (
	"context"
	"testing"
	"time"

	"github.com/beam-cloud/airstore/pkg/repository"
	"github.com/beam-cloud/airstore/pkg/types"
	"github.com/stretchr/testify/require"
)

type tasksVNodeBackend struct {
	repository.BackendRepository
	workspaceID uint
	tasks       map[string]*types.AgentTask
}

func (b *tasksVNodeBackend) ValidateToken(_ context.Context, _ string) (*types.TokenValidationResult, error) {
	return &types.TokenValidationResult{WorkspaceId: b.workspaceID}, nil
}

func (b *tasksVNodeBackend) ListTasks(_ context.Context, workspaceID uint, _ int) ([]*types.AgentTask, error) {
	if workspaceID != b.workspaceID {
		return []*types.AgentTask{}, nil
	}
	out := make([]*types.AgentTask, 0, len(b.tasks))
	for _, task := range b.tasks {
		out = append(out, task)
	}
	return out, nil
}

func (b *tasksVNodeBackend) GetTask(_ context.Context, workspaceID uint, taskID string) (*types.AgentTask, error) {
	if workspaceID != b.workspaceID {
		return nil, &types.ErrAgentTaskNotFound{ID: taskID}
	}
	task, ok := b.tasks[taskID]
	if !ok {
		return nil, &types.ErrAgentTaskNotFound{ID: taskID}
	}
	return task, nil
}

func TestTasksVNodeReadUsesAgentTaskView(t *testing.T) {
	createdAt := time.Now().Add(-2 * time.Minute).UTC()
	updatedAt := createdAt.Add(30 * time.Second)
	runID := "run-123"
	task := &types.AgentTask{
		ID:             "task-123",
		WorkspaceID:    42,
		Kind:           types.AgentTaskKindAgentCommand,
		QueueMode:      types.AgentQueueModeQueue,
		State:          types.AgentTaskStateDispatched,
		IdempotencyKey: "idem-123",
		TargetRunID:    &runID,
		CreatedAt:      createdAt,
		UpdatedAt:      updatedAt,
	}

	vnode := NewTasksVNode(&tasksVNodeBackend{
		workspaceID: 42,
		tasks:       map[string]*types.AgentTask{task.ID: task},
	}, "workspace-token")

	entries, err := vnode.Readdir(TasksPath)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, "task-123.task", entries[0].Name)

	buf := make([]byte, 4096)
	n, err := vnode.Read(TasksPath+"/task-123.task", buf, 0, 0)
	require.NoError(t, err)

	content := string(buf[:n])
	require.Contains(t, content, "Task: task-123")
	require.Contains(t, content, "State: dispatched")
	require.Contains(t, content, "Kind: agent_command")
	require.Contains(t, content, "Queue Mode: queue")
	require.Contains(t, content, "Run: run-123")
	require.Contains(t, content, "(logs available via API)")

	require.NotContains(t, content, "Status:")
	require.NotContains(t, content, "Exit Code:")
	require.NotContains(t, content, "Finished:")
}
