package repository

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/beam-cloud/airstore/pkg/common"
	"github.com/beam-cloud/airstore/pkg/types"
	"github.com/redis/go-redis/v9"
)

const (
	defaultQueueName  = "default"
	defaultPopTimeout = 5 * time.Second
	delayedMoveBatch  = 128
	taskStateTTL      = 24 * time.Hour
	taskResultTTL     = 24 * time.Hour
	taskLogBufferTTL  = 24 * time.Hour
	maxInFlightScrub  = 256
)

const moveDueDelayedTasksScript = `
local delayed = KEYS[1]
local queue = KEYS[2]
local now = ARGV[1]
local limit = tonumber(ARGV[2])
local moved = 0
local items = redis.call('ZRANGEBYSCORE', delayed, '-inf', now, 'LIMIT', 0, limit)
for _, item in ipairs(items) do
	if redis.call('ZREM', delayed, item) == 1 then
		redis.call('LPUSH', queue, item)
		moved = moved + 1
	end
end
return moved
`

// RedisTaskQueue implements TaskQueue using Redis
type RedisTaskQueue struct {
	rdb       *common.RedisClient
	queueName string
}

// NewRedisTaskQueue creates a new Redis-based task queue
func NewRedisTaskQueue(rdb *common.RedisClient, queueName string) *RedisTaskQueue {
	if queueName == "" {
		queueName = defaultQueueName
	}
	return &RedisTaskQueue{
		rdb:       rdb,
		queueName: queueName,
	}
}

// Push adds a task to the queue
func (q *RedisTaskQueue) Push(ctx context.Context, task *types.RunExecution) error {
	// Serialize task
	data, err := json.Marshal(task)
	if err != nil {
		return fmt.Errorf("failed to marshal task: %w", err)
	}

	// Store task state
	state := &types.RunExecutionState{
		ID:        task.ExternalId,
		Status:    types.RunExecutionStatusPending,
		ExitCode:  -1,
		CreatedAt: time.Now(),
	}
	stateData, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("failed to marshal state: %w", err)
	}

	// Store state and push to queue atomically via pipeline
	pipe := q.rdb.Pipeline()
	pipe.Set(ctx, common.Keys.RunExecutionState(task.ExternalId), stateData, taskStateTTL)
	pipe.LPush(ctx, common.Keys.RunExecutionQueue(q.queueName), data)
	_, err = pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("failed to push task: %w", err)
	}

	return nil
}

// PushDelayed stores a task for delayed enqueue using a Redis sorted set.
// Delayed tasks survive process restarts and are moved to the main queue by Pop.
func (q *RedisTaskQueue) PushDelayed(ctx context.Context, task *types.RunExecution, delay time.Duration) error {
	if delay <= 0 {
		return q.Push(ctx, task)
	}

	data, err := json.Marshal(task)
	if err != nil {
		return fmt.Errorf("failed to marshal delayed task: %w", err)
	}

	state := &types.RunExecutionState{
		ID:        task.ExternalId,
		Status:    types.RunExecutionStatusPending,
		ExitCode:  -1,
		CreatedAt: time.Now(),
	}
	stateData, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("failed to marshal delayed state: %w", err)
	}

	dueAtMs := time.Now().Add(delay).UnixMilli()
	pipe := q.rdb.Pipeline()
	pipe.Set(ctx, common.Keys.RunExecutionState(task.ExternalId), stateData, taskStateTTLForDelay(delay))
	pipe.ZAdd(ctx, common.Keys.RunExecutionDelayed(q.queueName), redis.Z{
		Score:  float64(dueAtMs),
		Member: data,
	})
	_, err = pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("failed to push delayed task: %w", err)
	}
	return nil
}

func taskStateTTLForDelay(delay time.Duration) time.Duration {
	if delay <= 0 {
		return taskStateTTL
	}

	ttl := taskStateTTL + delay
	// Overflow guard: fall back to no expiration for extremely large delays.
	if ttl < taskStateTTL {
		return 0
	}
	return ttl
}

// Pop blocks until a task is available and returns it
func (q *RedisTaskQueue) Pop(ctx context.Context, workerID string) (*types.RunExecution, error) {
	queueKey := common.Keys.RunExecutionQueue(q.queueName)
	inFlightKey := common.Keys.RunExecutionInFlight(q.queueName)

	if err := q.moveDueDelayedTasks(ctx); err != nil {
		return nil, err
	}

	// BRPOP with timeout - blocks until task available
	result, err := q.rdb.BRPop(ctx, defaultPopTimeout, queueKey).Result()
	if err != nil {
		// Timeout is not an error, just no tasks available
		if err.Error() == "redis: nil" {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to pop task: %w", err)
	}

	if len(result) < 2 {
		return nil, nil
	}

	taskData := result[1]

	// Deserialize task
	var task types.RunExecution
	if err := json.Unmarshal([]byte(taskData), &task); err != nil {
		return nil, fmt.Errorf("failed to unmarshal task: %w", err)
	}

	// Add to in-flight set and update state
	now := time.Now()
	state := &types.RunExecutionState{
		ID:          task.ExternalId,
		Status:      types.RunExecutionStatusRunning,
		WorkerID:    workerID,
		ExitCode:    -1,
		ScheduledAt: now,
		StartedAt:   now,
	}
	stateData, _ := json.Marshal(state)

	pipe := q.rdb.Pipeline()
	pipe.SAdd(ctx, inFlightKey, task.ExternalId)
	pipe.Set(ctx, common.Keys.RunExecutionState(task.ExternalId), stateData, taskStateTTL)
	_, _ = pipe.Exec(ctx)
	// Task was popped - return it even if tracking failed

	return &task, nil
}

func (q *RedisTaskQueue) moveDueDelayedTasks(ctx context.Context) error {
	delayedKey := common.Keys.RunExecutionDelayed(q.queueName)
	queueKey := common.Keys.RunExecutionQueue(q.queueName)

	for {
		moved, err := q.rdb.Eval(
			ctx,
			moveDueDelayedTasksScript,
			[]string{delayedKey, queueKey},
			time.Now().UnixMilli(),
			delayedMoveBatch,
		).Int()
		if err != nil {
			return fmt.Errorf("failed to move delayed tasks: %w", err)
		}
		if moved < delayedMoveBatch {
			return nil
		}
	}
}

// Complete marks a task as complete and stores the result
func (q *RedisTaskQueue) Complete(ctx context.Context, taskID string, result *types.RunExecutionResult) error {
	inFlightKey := common.Keys.RunExecutionInFlight(q.queueName)

	// Update state to complete
	state := &types.RunExecutionState{
		ID:         taskID,
		Status:     types.RunExecutionStatusComplete,
		ExitCode:   result.ExitCode,
		FinishedAt: time.Now(),
	}
	if result.Error != "" {
		state.Status = types.RunExecutionStatusFailed
		state.Error = result.Error
	}

	stateData, _ := json.Marshal(state)
	resultData, _ := json.Marshal(result)

	pipe := q.rdb.Pipeline()
	pipe.SRem(ctx, inFlightKey, taskID)
	pipe.Set(ctx, common.Keys.RunExecutionState(taskID), stateData, taskStateTTL)
	pipe.Set(ctx, common.Keys.RunExecutionResult(taskID), resultData, taskResultTTL)
	_, err := pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("failed to complete task: %w", err)
	}

	return nil
}

// Fail marks a task as failed
func (q *RedisTaskQueue) Fail(ctx context.Context, taskID string, taskErr error) error {
	return q.Complete(ctx, taskID, &types.RunExecutionResult{
		ID:       taskID,
		ExitCode: -1,
		Error:    taskErr.Error(),
	})
}

// GetState returns the current state of a task
func (q *RedisTaskQueue) GetState(ctx context.Context, taskID string) (*types.RunExecutionState, error) {
	data, err := q.rdb.Get(ctx, common.Keys.RunExecutionState(taskID)).Result()
	if err != nil {
		return nil, fmt.Errorf("task not found: %w", err)
	}

	var state types.RunExecutionState
	if err := json.Unmarshal([]byte(data), &state); err != nil {
		return nil, fmt.Errorf("failed to unmarshal state: %w", err)
	}

	return &state, nil
}

// GetResult returns the result of a completed task
func (q *RedisTaskQueue) GetResult(ctx context.Context, taskID string) (*types.RunExecutionResult, error) {
	data, err := q.rdb.Get(ctx, common.Keys.RunExecutionResult(taskID)).Result()
	if err != nil {
		return nil, fmt.Errorf("result not found: %w", err)
	}

	var result types.RunExecutionResult
	if err := json.Unmarshal([]byte(data), &result); err != nil {
		return nil, fmt.Errorf("failed to unmarshal result: %w", err)
	}

	return &result, nil
}

// Len returns the number of pending tasks in the queue
func (q *RedisTaskQueue) Len(ctx context.Context) (int64, error) {
	return q.rdb.LLen(ctx, common.Keys.RunExecutionQueue(q.queueName)).Result()
}

// InFlightCount returns the number of tasks currently being processed
func (q *RedisTaskQueue) InFlightCount(ctx context.Context) (int64, error) {
	inFlightKey := common.Keys.RunExecutionInFlight(q.queueName)
	if err := q.scrubInFlightState(ctx, inFlightKey); err != nil {
		return 0, err
	}
	return q.rdb.SCard(ctx, inFlightKey).Result()
}

func (q *RedisTaskQueue) scrubInFlightState(ctx context.Context, inFlightKey string) error {
	if q == nil || q.rdb == nil {
		return nil
	}
	taskIDs, err := q.rdb.SRandMemberN(ctx, inFlightKey, maxInFlightScrub).Result()
	if err != nil {
		return fmt.Errorf("failed to sample in-flight tasks: %w", err)
	}
	if len(taskIDs) == 0 {
		return nil
	}

	stateKeys := make([]string, 0, len(taskIDs))
	for _, taskID := range taskIDs {
		stateKeys = append(stateKeys, common.Keys.RunExecutionState(taskID))
	}
	stateValues, err := q.rdb.MGet(ctx, stateKeys...).Result()
	if err != nil {
		return fmt.Errorf("failed to fetch in-flight task states: %w", err)
	}

	staleTaskIDs := make([]interface{}, 0)
	nonTerminalStateKeys := make([]string, 0, len(taskIDs))
	for i, raw := range stateValues {
		taskID := taskIDs[i]
		if raw == nil {
			// Missing state can happen when a long-running task's state TTL expires.
			// Keep the in-flight marker until we observe an explicit terminal state.
			continue
		}
		stateRaw, ok := raw.(string)
		if !ok {
			staleTaskIDs = append(staleTaskIDs, taskID)
			continue
		}

		var state types.RunExecutionState
		if err := json.Unmarshal([]byte(stateRaw), &state); err != nil {
			staleTaskIDs = append(staleTaskIDs, taskID)
			continue
		}
		if runExecutionStateTerminal(state.Status) {
			staleTaskIDs = append(staleTaskIDs, taskID)
			continue
		}
		nonTerminalStateKeys = append(nonTerminalStateKeys, stateKeys[i])
	}
	if len(nonTerminalStateKeys) > 0 {
		pipe := q.rdb.Pipeline()
		for _, stateKey := range nonTerminalStateKeys {
			pipe.Expire(ctx, stateKey, taskStateTTL)
		}
		if _, err := pipe.Exec(ctx); err != nil {
			return fmt.Errorf("failed to refresh in-flight task state ttl: %w", err)
		}
	}
	if len(staleTaskIDs) == 0 {
		return nil
	}
	if err := q.rdb.SRem(ctx, inFlightKey, staleTaskIDs...).Err(); err != nil {
		return fmt.Errorf("failed to prune stale in-flight tasks: %w", err)
	}
	return nil
}

func runExecutionStateTerminal(status types.RunExecutionStatus) bool {
	switch status {
	case types.RunExecutionStatusComplete, types.RunExecutionStatusFailed, types.RunExecutionStatusCancelled:
		return true
	default:
		return false
	}
}

// TaskLogEntry represents a log entry for a task
type TaskLogEntry struct {
	TaskID    string `json:"task_id"`
	Timestamp int64  `json:"timestamp"`
	Stream    string `json:"stream"` // "stdout" or "stderr"
	Data      string `json:"data"`
}

// TaskStatusEvent represents a task status change event
type TaskStatusEvent struct {
	TaskID    string                   `json:"task_id"`
	Timestamp int64                    `json:"timestamp"`
	Status    types.RunExecutionStatus `json:"status"`
	ExitCode  *int                     `json:"exit_code,omitempty"`
	Error     string                   `json:"error,omitempty"`
}

// PublishLog publishes a log entry to the task's log channel
func (q *RedisTaskQueue) PublishLog(ctx context.Context, taskID string, stream string, data string) error {
	entry := TaskLogEntry{
		TaskID:    taskID,
		Timestamp: time.Now().UnixMilli(),
		Stream:    stream,
		Data:      data,
	}

	entryData, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("failed to marshal log entry: %w", err)
	}

	channel := common.Keys.RunExecutionLogsChannel(taskID)
	bufferKey := common.Keys.RunExecutionLogsBuffer(taskID)

	// Publish to channel for live subscribers and append to buffer for late joiners
	pipe := q.rdb.Pipeline()
	pipe.Publish(ctx, channel, entryData)
	pipe.RPush(ctx, bufferKey, entryData)
	pipe.Expire(ctx, bufferKey, taskLogBufferTTL)
	_, err = pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("failed to publish log: %w", err)
	}

	return nil
}

// PublishStatus publishes a task status change event
func (q *RedisTaskQueue) PublishStatus(ctx context.Context, taskID string, status types.RunExecutionStatus, exitCode *int, errorMsg string) error {
	event := TaskStatusEvent{
		TaskID:    taskID,
		Timestamp: time.Now().UnixMilli(),
		Status:    status,
		ExitCode:  exitCode,
		Error:     errorMsg,
	}

	eventData, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal status event: %w", err)
	}

	return q.rdb.Publish(ctx, common.Keys.RunExecutionLogsChannel(taskID), eventData).Err()
}

// SubscribeLogs subscribes to a task's log channel and returns a channel of log entries
func (q *RedisTaskQueue) SubscribeLogs(ctx context.Context, taskID string) (<-chan []byte, func(), error) {
	channel := common.Keys.RunExecutionLogsChannel(taskID)
	msgCh, errCh := q.rdb.Subscribe(ctx, channel)

	out := make(chan []byte, 100)
	done := make(chan struct{})

	// Forward messages to output channel
	go func() {
		defer close(out)
		for {
			select {
			case <-ctx.Done():
				return
			case <-done:
				return
			case msg, ok := <-msgCh:
				if !ok {
					return
				}
				select {
				case out <- []byte(msg.Payload):
				case <-ctx.Done():
					return
				case <-done:
					return
				}
			case _, ok := <-errCh:
				if !ok {
					return
				}
				// Log error but continue
			}
		}
	}()

	cleanup := func() {
		close(done)
	}

	return out, cleanup, nil
}

// GetLogBuffer returns buffered logs for a task (for late joiners)
func (q *RedisTaskQueue) GetLogBuffer(ctx context.Context, taskID string) ([][]byte, error) {
	bufferKey := common.Keys.RunExecutionLogsBuffer(taskID)

	result, err := q.rdb.LRange(ctx, bufferKey, 0, -1).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to get log buffer: %w", err)
	}

	logs := make([][]byte, len(result))
	for i, entry := range result {
		logs[i] = []byte(entry)
	}

	return logs, nil
}
