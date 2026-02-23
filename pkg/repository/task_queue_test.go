package repository

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/beam-cloud/airstore/pkg/common"
	"github.com/beam-cloud/airstore/pkg/types"
	"github.com/stretchr/testify/require"
)

func newTestTaskQueue(t *testing.T) (*RedisTaskQueue, func()) {
	t.Helper()

	mr, err := miniredis.Run()
	require.NoError(t, err)

	redisClient, err := common.NewRedisClient(types.RedisConfig{
		Mode:  types.RedisModeSingle,
		Addrs: []string{mr.Addr()},
	})
	require.NoError(t, err)

	queue := NewRedisTaskQueue(redisClient, "default")
	cleanup := func() {
		_ = redisClient.Close()
		mr.Close()
	}
	return queue, cleanup
}

func TestRedisTaskQueuePushDelayedMovesIntoQueueAtDueTime(t *testing.T) {
	queue, cleanup := newTestTaskQueue(t)
	defer cleanup()

	task := &types.Task{
		ExternalId:  "task-delayed-1",
		WorkspaceId: 1,
		Status:      types.TaskStatusPending,
		Type:        types.TaskTypeBackground,
		Image:       "ghcr.io/beam/sandbox:latest",
		Entrypoint:  []string{},
		Env:         map[string]string{},
	}

	require.NoError(t, queue.PushDelayed(context.Background(), task, 40*time.Millisecond))

	require.Eventually(t, func() bool {
		if err := queue.moveDueDelayedTasks(context.Background()); err != nil {
			return false
		}
		pending, err := queue.Len(context.Background())
		return err == nil && pending > 0
	}, time.Second, 10*time.Millisecond)

	popped, err := queue.Pop(context.Background(), "worker-1")
	require.NoError(t, err)
	require.NotNil(t, popped)
	require.Equal(t, task.ExternalId, popped.ExternalId)
}

func TestRedisTaskQueuePushDelayedSurvivesQueueReconstruction(t *testing.T) {
	queue, cleanup := newTestTaskQueue(t)
	defer cleanup()

	task := &types.Task{
		ExternalId:  "task-delayed-2",
		WorkspaceId: 1,
		Status:      types.TaskStatusPending,
		Type:        types.TaskTypeBackground,
		Image:       "ghcr.io/beam/sandbox:latest",
		Entrypoint:  []string{},
		Env:         map[string]string{},
	}

	require.NoError(t, queue.PushDelayed(context.Background(), task, 40*time.Millisecond))

	// Simulate service restart by reconstructing the queue instance.
	queueAfterRestart := NewRedisTaskQueue(queue.rdb, "default")
	require.Eventually(t, func() bool {
		if err := queueAfterRestart.moveDueDelayedTasks(context.Background()); err != nil {
			return false
		}
		pending, err := queueAfterRestart.Len(context.Background())
		return err == nil && pending > 0
	}, time.Second, 10*time.Millisecond)

	popped, err := queueAfterRestart.Pop(context.Background(), "worker-2")
	require.NoError(t, err)
	require.NotNil(t, popped)
	require.Equal(t, task.ExternalId, popped.ExternalId)
}

func TestRedisTaskQueuePushDelayedExtendsTaskStateTTL(t *testing.T) {
	queue, cleanup := newTestTaskQueue(t)
	defer cleanup()

	task := &types.Task{
		ExternalId:  "task-delayed-ttl",
		WorkspaceId: 1,
		Status:      types.TaskStatusPending,
		Type:        types.TaskTypeBackground,
		Image:       "ghcr.io/beam/sandbox:latest",
		Entrypoint:  []string{},
		Env:         map[string]string{},
	}

	delay := 48 * time.Hour
	require.NoError(t, queue.PushDelayed(context.Background(), task, delay))

	ttl, err := queue.rdb.TTL(context.Background(), common.Keys.TaskState(task.ExternalId)).Result()
	require.NoError(t, err)
	require.Greater(t, ttl, delay, "task state TTL should outlive the scheduled delay")
}
