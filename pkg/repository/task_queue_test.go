package repository

import (
	"context"
	"encoding/json"
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

	task := &types.RunExecution{
		ExternalId:  "task-delayed-1",
		WorkspaceId: 1,
		Status:      types.RunExecutionStatusPending,
		Type:        types.RunExecutionTypeBackground,
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

	task := &types.RunExecution{
		ExternalId:  "task-delayed-2",
		WorkspaceId: 1,
		Status:      types.RunExecutionStatusPending,
		Type:        types.RunExecutionTypeBackground,
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

	task := &types.RunExecution{
		ExternalId:  "task-delayed-ttl",
		WorkspaceId: 1,
		Status:      types.RunExecutionStatusPending,
		Type:        types.RunExecutionTypeBackground,
		Image:       "ghcr.io/beam/sandbox:latest",
		Entrypoint:  []string{},
		Env:         map[string]string{},
	}

	delay := 48 * time.Hour
	require.NoError(t, queue.PushDelayed(context.Background(), task, delay))

	ttl, err := queue.rdb.TTL(context.Background(), common.Keys.RunExecutionState(task.ExternalId)).Result()
	require.NoError(t, err)
	require.Greater(t, ttl, delay, "task state TTL should outlive the scheduled delay")
}

func TestRedisTaskQueueInFlightCountPrunesOnlyTerminalMembers(t *testing.T) {
	queue, cleanup := newTestTaskQueue(t)
	defer cleanup()

	ctx := context.Background()
	inFlightKey := common.Keys.RunExecutionInFlight("default")

	runningID := "task-running"
	terminalID := "task-terminal"
	missingID := "task-missing"
	require.NoError(t, queue.rdb.SAdd(ctx, inFlightKey, runningID, terminalID, missingID).Err())

	runningState, err := json.Marshal(&types.RunExecutionState{
		ID:        runningID,
		Status:    types.RunExecutionStatusRunning,
		WorkerID:  "worker-1",
		ExitCode:  -1,
		CreatedAt: time.Now().Add(-2 * time.Minute),
		StartedAt: time.Now().Add(-time.Minute),
	})
	require.NoError(t, err)
	require.NoError(t, queue.rdb.Set(ctx, common.Keys.RunExecutionState(runningID), runningState, taskStateTTL).Err())

	terminalState, err := json.Marshal(&types.RunExecutionState{
		ID:         terminalID,
		Status:     types.RunExecutionStatusFailed,
		WorkerID:   "worker-2",
		ExitCode:   -1,
		CreatedAt:  time.Now().Add(-3 * time.Minute),
		StartedAt:  time.Now().Add(-2 * time.Minute),
		FinishedAt: time.Now().Add(-time.Minute),
	})
	require.NoError(t, err)
	require.NoError(t, queue.rdb.Set(ctx, common.Keys.RunExecutionState(terminalID), terminalState, taskStateTTL).Err())

	count, err := queue.InFlightCount(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(2), count)

	members, err := queue.rdb.SMembers(ctx, inFlightKey).Result()
	require.NoError(t, err)
	require.ElementsMatch(t, []string{runningID, missingID}, members)
}

func TestRedisTaskQueueInFlightCountRefreshesRunningStateTTL(t *testing.T) {
	queue, cleanup := newTestTaskQueue(t)
	defer cleanup()

	ctx := context.Background()
	inFlightKey := common.Keys.RunExecutionInFlight("default")
	runningID := "task-running-refresh"
	require.NoError(t, queue.rdb.SAdd(ctx, inFlightKey, runningID).Err())

	runningState, err := json.Marshal(&types.RunExecutionState{
		ID:        runningID,
		Status:    types.RunExecutionStatusRunning,
		WorkerID:  "worker-refresh",
		ExitCode:  -1,
		CreatedAt: time.Now().Add(-2 * time.Minute),
		StartedAt: time.Now().Add(-time.Minute),
	})
	require.NoError(t, err)
	require.NoError(t, queue.rdb.Set(ctx, common.Keys.RunExecutionState(runningID), runningState, time.Second).Err())

	count, err := queue.InFlightCount(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(1), count)

	ttl, err := queue.rdb.TTL(ctx, common.Keys.RunExecutionState(runningID)).Result()
	require.NoError(t, err)
	require.Greater(t, ttl, time.Hour, "expected running state TTL to be refreshed")
}
