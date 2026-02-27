package orchestration

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/beam-cloud/airstore/pkg/types"
)

const (
	dispatchTokenTaskPrefix = "task:"
	dispatchTokenModePrefix = "task-mode:"
	modeTaskStateTTL        = 5 * time.Minute
)

type TaskQueueRouter struct {
	store TaskQueueStore
}

type TaskQueueStore interface {
	UpdateTaskState(ctx context.Context, taskID string, state types.AgentTaskState, dropReason *string, targetRunID *string) error
	PushQueueToken(ctx context.Context, token string) error
	PopQueueToken(ctx context.Context, timeout time.Duration) (string, error)
	GetModeTaskID(ctx context.Context, modeKey string) (string, error)
	SetModeTaskID(ctx context.Context, modeKey string, taskID string, ttl time.Duration) error
	AddModeKey(ctx context.Context, modeKey string) (bool, error)
	RemoveModeKey(ctx context.Context, modeKey string) error
	GetDelModeTaskID(ctx context.Context, modeKey string) (string, error)
}

func NewTaskQueueRouter(store TaskQueueStore) *TaskQueueRouter {
	return &TaskQueueRouter{
		store: store,
	}
}

func (r *TaskQueueRouter) Enqueue(ctx context.Context, task *types.AgentTask, instanceKey string) error {
	if r.store == nil {
		return fmt.Errorf("queue store is required")
	}
	if task == nil {
		return fmt.Errorf("task is required")
	}

	if err := r.store.UpdateTaskState(ctx, task.ID, types.AgentTaskStateQueued, nil, task.TargetRunID); err != nil {
		return err
	}

	if !usesModeQueue(task, instanceKey) {
		token := dispatchTokenTaskPrefix + task.ID
		return r.store.PushQueueToken(ctx, token)
	}

	modeKey := dispatchModeKey(instanceKey, task.QueueMode)
	previousTaskID, err := r.store.GetModeTaskID(ctx, modeKey)
	if err != nil {
		return err
	}
	if previousTaskID != "" && previousTaskID != task.ID {
		reason := types.AgentTaskDropReasonReshapedByQueueMode
		if err := r.store.UpdateTaskState(ctx, previousTaskID, types.AgentTaskStateDropped, &reason, nil); err != nil {
			return err
		}
	}

	if err := r.store.SetModeTaskID(ctx, modeKey, task.ID, modeTaskStateTTL); err != nil {
		return err
	}

	added, err := r.store.AddModeKey(ctx, modeKey)
	if err != nil {
		return err
	}
	if !added {
		return nil
	}
	return r.store.PushQueueToken(ctx, dispatchTokenModePrefix+modeKey)
}

func (r *TaskQueueRouter) Pop(ctx context.Context, timeout time.Duration) (string, error) {
	if r.store == nil {
		return "", fmt.Errorf("queue store is required")
	}
	if timeout <= 0 {
		timeout = 2 * time.Second
	}
	return r.store.PopQueueToken(ctx, timeout)
}

func (r *TaskQueueRouter) ResolveTaskID(ctx context.Context, token string) (string, error) {
	if r.store == nil {
		return "", fmt.Errorf("queue store is required")
	}

	if strings.HasPrefix(token, dispatchTokenTaskPrefix) {
		return strings.TrimPrefix(token, dispatchTokenTaskPrefix), nil
	}

	if strings.HasPrefix(token, dispatchTokenModePrefix) {
		modeKey := strings.TrimPrefix(token, dispatchTokenModePrefix)
		if err := r.store.RemoveModeKey(ctx, modeKey); err != nil {
			return "", err
		}
		return r.store.GetDelModeTaskID(ctx, modeKey)
	}

	return "", fmt.Errorf("unsupported dispatch token: %s", token)
}

func (r *TaskQueueRouter) RequeueTask(ctx context.Context, taskID string) error {
	if r.store == nil {
		return fmt.Errorf("queue store is required")
	}
	if strings.TrimSpace(taskID) == "" {
		return fmt.Errorf("task_id is required")
	}
	return r.store.PushQueueToken(ctx, dispatchTokenTaskPrefix+taskID)
}

func usesModeQueue(task *types.AgentTask, instanceKey string) bool {
	if task == nil || strings.TrimSpace(instanceKey) == "" {
		return false
	}
	switch types.NormalizeRunInputQueueMode(task.QueueMode) {
	case types.AgentQueueModeFollowup, types.AgentQueueModeSteer, types.AgentQueueModeInterrupt:
		return true
	default:
		return false
	}
}

func dispatchModeKey(instanceKey string, mode types.AgentQueueMode) string {
	normalizedMode := types.NormalizeRunInputQueueMode(mode)
	return strings.TrimSpace(instanceKey) + ":" + strings.TrimSpace(string(normalizedMode))
}
