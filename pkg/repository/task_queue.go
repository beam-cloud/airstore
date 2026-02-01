package repository

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/beam-cloud/airstore/pkg/common"
	"github.com/beam-cloud/airstore/pkg/types"
)

const (
	// Redis key prefixes
	taskQueueKey     = "airstore:task_queue:%s"    // per-pool queue
	taskInFlightKey  = "airstore:task_inflight:%s" // tasks being processed
	taskStateKey     = "airstore:task_state:%s"    // task state by ID
	taskResultKey    = "airstore:task_result:%s"   // task result by ID
	taskLogsChannel  = "airstore:task_logs:%s"     // pub/sub channel for task logs
	taskLogsKey      = "airstore:task_logs_buf:%s" // log buffer for task
	defaultQueueName = "default"

	// Default timeout for blocking pop
	defaultPopTimeout = 5 * time.Second

	// Maximum log buffer size per task (1MB)
	maxLogBufferSize = 1 << 20
)

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
func (q *RedisTaskQueue) Push(ctx context.Context, task *types.Task) error {
	// Serialize task
	data, err := json.Marshal(task)
	if err != nil {
		return fmt.Errorf("failed to marshal task: %w", err)
	}

	// Store task state
	state := &types.TaskState{
		ID:        task.ExternalId,
		Status:    types.TaskStatusPending,
		ExitCode:  -1,
		CreatedAt: time.Now(),
	}
	stateData, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("failed to marshal state: %w", err)
	}

	// Store state and push to queue atomically via pipeline
	pipe := q.rdb.Pipeline()
	pipe.Set(ctx, fmt.Sprintf(taskStateKey, task.ExternalId), stateData, 24*time.Hour)
	pipe.LPush(ctx, fmt.Sprintf(taskQueueKey, q.queueName), data)
	_, err = pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("failed to push task: %w", err)
	}

	return nil
}

// Pop blocks until a task is available and returns it
func (q *RedisTaskQueue) Pop(ctx context.Context, workerID string) (*types.Task, error) {
	queueKey := fmt.Sprintf(taskQueueKey, q.queueName)
	inFlightKey := fmt.Sprintf(taskInFlightKey, q.queueName)

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
	var task types.Task
	if err := json.Unmarshal([]byte(taskData), &task); err != nil {
		return nil, fmt.Errorf("failed to unmarshal task: %w", err)
	}

	// Add to in-flight set and update state
	now := time.Now()
	state := &types.TaskState{
		ID:          task.ExternalId,
		Status:      types.TaskStatusRunning,
		WorkerID:    workerID,
		ExitCode:    -1,
		ScheduledAt: now,
		StartedAt:   now,
	}
	stateData, _ := json.Marshal(state)

	pipe := q.rdb.Pipeline()
	pipe.SAdd(ctx, inFlightKey, task.ExternalId)
	pipe.Set(ctx, fmt.Sprintf(taskStateKey, task.ExternalId), stateData, 24*time.Hour)
	_, _ = pipe.Exec(ctx)
	// Task was popped - return it even if tracking failed

	return &task, nil
}

// Complete marks a task as complete and stores the result
func (q *RedisTaskQueue) Complete(ctx context.Context, taskID string, result *types.TaskResult) error {
	inFlightKey := fmt.Sprintf(taskInFlightKey, q.queueName)

	// Update state to complete
	state := &types.TaskState{
		ID:         taskID,
		Status:     types.TaskStatusComplete,
		ExitCode:   result.ExitCode,
		FinishedAt: time.Now(),
	}
	if result.Error != "" {
		state.Status = types.TaskStatusFailed
		state.Error = result.Error
	}

	stateData, _ := json.Marshal(state)
	resultData, _ := json.Marshal(result)

	pipe := q.rdb.Pipeline()
	pipe.SRem(ctx, inFlightKey, taskID)
	pipe.Set(ctx, fmt.Sprintf(taskStateKey, taskID), stateData, 24*time.Hour)
	pipe.Set(ctx, fmt.Sprintf(taskResultKey, taskID), resultData, 24*time.Hour)
	_, err := pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("failed to complete task: %w", err)
	}

	return nil
}

// Fail marks a task as failed
func (q *RedisTaskQueue) Fail(ctx context.Context, taskID string, taskErr error) error {
	return q.Complete(ctx, taskID, &types.TaskResult{
		ID:       taskID,
		ExitCode: -1,
		Error:    taskErr.Error(),
	})
}

// GetState returns the current state of a task
func (q *RedisTaskQueue) GetState(ctx context.Context, taskID string) (*types.TaskState, error) {
	data, err := q.rdb.Get(ctx, fmt.Sprintf(taskStateKey, taskID)).Result()
	if err != nil {
		return nil, fmt.Errorf("task not found: %w", err)
	}

	var state types.TaskState
	if err := json.Unmarshal([]byte(data), &state); err != nil {
		return nil, fmt.Errorf("failed to unmarshal state: %w", err)
	}

	return &state, nil
}

// GetResult returns the result of a completed task
func (q *RedisTaskQueue) GetResult(ctx context.Context, taskID string) (*types.TaskResult, error) {
	data, err := q.rdb.Get(ctx, fmt.Sprintf(taskResultKey, taskID)).Result()
	if err != nil {
		return nil, fmt.Errorf("result not found: %w", err)
	}

	var result types.TaskResult
	if err := json.Unmarshal([]byte(data), &result); err != nil {
		return nil, fmt.Errorf("failed to unmarshal result: %w", err)
	}

	return &result, nil
}

// Len returns the number of pending tasks in the queue
func (q *RedisTaskQueue) Len(ctx context.Context) (int64, error) {
	return q.rdb.LLen(ctx, fmt.Sprintf(taskQueueKey, q.queueName)).Result()
}

// InFlightCount returns the number of tasks currently being processed
func (q *RedisTaskQueue) InFlightCount(ctx context.Context) (int64, error) {
	return q.rdb.SCard(ctx, fmt.Sprintf(taskInFlightKey, q.queueName)).Result()
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
	TaskID    string           `json:"task_id"`
	Timestamp int64            `json:"timestamp"`
	Status    types.TaskStatus `json:"status"`
	ExitCode  *int             `json:"exit_code,omitempty"`
	Error     string           `json:"error,omitempty"`
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

	channel := fmt.Sprintf(taskLogsChannel, taskID)
	bufferKey := fmt.Sprintf(taskLogsKey, taskID)

	// Publish to channel for live subscribers and append to buffer for late joiners
	pipe := q.rdb.Pipeline()
	pipe.Publish(ctx, channel, entryData)
	pipe.RPush(ctx, bufferKey, entryData)
	pipe.Expire(ctx, bufferKey, 24*time.Hour)
	_, err = pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("failed to publish log: %w", err)
	}

	return nil
}

// PublishStatus publishes a task status change event
func (q *RedisTaskQueue) PublishStatus(ctx context.Context, taskID string, status types.TaskStatus, exitCode *int, errorMsg string) error {
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

	channel := fmt.Sprintf(taskLogsChannel, taskID)
	return q.rdb.Publish(ctx, channel, eventData).Err()
}

// SubscribeLogs subscribes to a task's log channel and returns a channel of log entries
func (q *RedisTaskQueue) SubscribeLogs(ctx context.Context, taskID string) (<-chan []byte, func(), error) {
	channel := fmt.Sprintf(taskLogsChannel, taskID)
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
	bufferKey := fmt.Sprintf(taskLogsKey, taskID)

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
