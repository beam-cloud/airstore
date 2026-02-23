package orchestration

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/beam-cloud/airstore/pkg/types"
)

const (
	// Keep "env:" token values for compatibility with existing queued entries.
	dispatchTokenTaskPrefix = "env:"
	dispatchTokenModePrefix = "mode:"
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

	if err := r.store.UpdateTaskState(ctx, task.ID, types.AgentTaskStateQueued, nil, task.TargetRunID); err != nil {
		return err
	}

	switch task.QueueMode {
	case types.AgentQueueModeFollowup, types.AgentQueueModeSteer, types.AgentQueueModeInterrupt:
		return r.enqueueModeKey(ctx, task, instanceKey)
	default:
		token := dispatchTokenTaskPrefix + task.ID
		return r.store.PushQueueToken(ctx, token)
	}
}

func (r *TaskQueueRouter) enqueueModeKey(ctx context.Context, task *types.AgentTask, instanceKey string) error {
	modeKey := fmt.Sprintf("%s:%s", instanceKey, task.QueueMode)
	prevID, err := r.store.GetModeTaskID(ctx, modeKey)
	if err != nil {
		return err
	}
	if prevID != "" && prevID != task.ID {
		reason := types.AgentTaskDropReasonReshapedByQueueMode
		_ = r.store.UpdateTaskState(ctx, prevID, types.AgentTaskStateDropped, &reason, task.TargetRunID)
	}
	if err := r.store.SetModeTaskID(ctx, modeKey, task.ID, 15*time.Minute); err != nil {
		return err
	}

	added, err := r.store.AddModeKey(ctx, modeKey)
	if err != nil {
		return err
	}
	if added {
		token := dispatchTokenModePrefix + modeKey
		if err := r.store.PushQueueToken(ctx, token); err != nil {
			return err
		}
	}
	return nil
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
