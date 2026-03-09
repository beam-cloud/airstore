package repository

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/beam-cloud/airstore/pkg/common"
	"github.com/beam-cloud/airstore/pkg/types"
	redislib "github.com/redis/go-redis/v9"
)

const redisNilError = "redis: nil"
const orchestrationRedisRequired = "redis is required for orchestration"
const orchestrationStreamMaxLen = 100_000

// OrchestrationStore centralizes Redis-backed orchestration primitives:
// stream dispatch/result channels, instance locks, and run events.
type OrchestrationStore struct {
	backend BackendRepository
	redis   *common.RedisClient
}

func NewOrchestrationStore(backend BackendRepository, redis *common.RedisClient) *OrchestrationStore {
	return &OrchestrationStore{
		backend: backend,
		redis:   redis,
	}
}

func (s *OrchestrationStore) UpdateTaskState(ctx context.Context, taskID string, state types.AgentTaskState, dropReason *string, targetRunID *string) error {
	if s == nil || s.backend == nil {
		return fmt.Errorf("backend is required for task state updates")
	}
	return s.backend.UpdateTaskState(ctx, taskID, state, dropReason, targetRunID)
}

func (s *OrchestrationStore) EnsureTaskDispatchGroup(ctx context.Context) error {
	return s.ensureStreamGroup(
		ctx,
		common.Keys.OrchestrationTaskDispatchStream(),
		common.Keys.OrchestrationTaskDispatchGroup(),
	)
}

func (s *OrchestrationStore) EnsureRunResultGroup(ctx context.Context) error {
	return s.ensureStreamGroup(
		ctx,
		common.Keys.OrchestrationRunResultStream(),
		common.Keys.OrchestrationRunResultGroup(),
	)
}

func (s *OrchestrationStore) PublishTaskDispatch(ctx context.Context, values map[string]any) (string, error) {
	return s.publishToStream(ctx, common.Keys.OrchestrationTaskDispatchStream(), values)
}

func (s *OrchestrationStore) PublishTaskDispatchDLQ(ctx context.Context, values map[string]any) (string, error) {
	return s.publishToStream(ctx, common.Keys.OrchestrationTaskDispatchDLQ(), values)
}

func (s *OrchestrationStore) PublishRunResult(ctx context.Context, values map[string]any) (string, error) {
	return s.publishToStream(ctx, common.Keys.OrchestrationRunResultStream(), values)
}

func (s *OrchestrationStore) PublishRunResultDLQ(ctx context.Context, values map[string]any) (string, error) {
	return s.publishToStream(ctx, common.Keys.OrchestrationRunResultDLQ(), values)
}

func (s *OrchestrationStore) ReadTaskDispatch(
	ctx context.Context,
	consumer string,
	block time.Duration,
	count int64,
) ([]redislib.XMessage, error) {
	return s.readGroup(
		ctx,
		common.Keys.OrchestrationTaskDispatchStream(),
		common.Keys.OrchestrationTaskDispatchGroup(),
		consumer,
		block,
		count,
	)
}

func (s *OrchestrationStore) ReadRunResults(
	ctx context.Context,
	consumer string,
	block time.Duration,
	count int64,
) ([]redislib.XMessage, error) {
	return s.readGroup(
		ctx,
		common.Keys.OrchestrationRunResultStream(),
		common.Keys.OrchestrationRunResultGroup(),
		consumer,
		block,
		count,
	)
}

func (s *OrchestrationStore) ClaimPendingTaskDispatch(
	ctx context.Context,
	consumer string,
	minIdle time.Duration,
	count int64,
) ([]redislib.XMessage, error) {
	return s.claimPending(
		ctx,
		common.Keys.OrchestrationTaskDispatchStream(),
		common.Keys.OrchestrationTaskDispatchGroup(),
		consumer,
		minIdle,
		count,
	)
}

func (s *OrchestrationStore) ClaimPendingRunResults(
	ctx context.Context,
	consumer string,
	minIdle time.Duration,
	count int64,
) ([]redislib.XMessage, error) {
	return s.claimPending(
		ctx,
		common.Keys.OrchestrationRunResultStream(),
		common.Keys.OrchestrationRunResultGroup(),
		consumer,
		minIdle,
		count,
	)
}

func (s *OrchestrationStore) AckTaskDispatch(ctx context.Context, messageIDs ...string) error {
	if len(messageIDs) == 0 {
		return nil
	}
	redis, err := s.queueRedis()
	if err != nil {
		return err
	}
	return redis.XAck(
		ctx,
		common.Keys.OrchestrationTaskDispatchStream(),
		common.Keys.OrchestrationTaskDispatchGroup(),
		messageIDs...,
	).Err()
}

func (s *OrchestrationStore) AckRunResults(ctx context.Context, messageIDs ...string) error {
	if len(messageIDs) == 0 {
		return nil
	}
	redis, err := s.queueRedis()
	if err != nil {
		return err
	}
	return redis.XAck(
		ctx,
		common.Keys.OrchestrationRunResultStream(),
		common.Keys.OrchestrationRunResultGroup(),
		messageIDs...,
	).Err()
}

func (s *OrchestrationStore) ensureStreamGroup(ctx context.Context, stream string, group string) error {
	redis, err := s.queueRedis()
	if err != nil {
		return err
	}
	if err := redis.XGroupCreateMkStream(ctx, stream, group, "0").Err(); err != nil {
		if strings.Contains(strings.ToUpper(err.Error()), "BUSYGROUP") {
			return nil
		}
		return err
	}
	return nil
}

func (s *OrchestrationStore) publishToStream(
	ctx context.Context,
	stream string,
	values map[string]any,
) (string, error) {
	redis, err := s.queueRedis()
	if err != nil {
		return "", err
	}
	if len(values) == 0 {
		return "", fmt.Errorf("stream values are required")
	}
	return redis.XAdd(ctx, &redislib.XAddArgs{
		Stream: stream,
		MaxLen: orchestrationStreamMaxLen,
		Approx: true,
		Values: values,
	}).Result()
}

func (s *OrchestrationStore) readGroup(
	ctx context.Context,
	stream string,
	group string,
	consumer string,
	block time.Duration,
	count int64,
) ([]redislib.XMessage, error) {
	redis, err := s.queueRedis()
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(consumer) == "" {
		return nil, fmt.Errorf("consumer is required")
	}
	if count <= 0 {
		count = 64
	}
	streams, err := redis.XReadGroup(ctx, &redislib.XReadGroupArgs{
		Group:    group,
		Consumer: consumer,
		Streams:  []string{stream, ">"},
		Count:    count,
		Block:    block,
		NoAck:    false,
	}).Result()
	if err != nil {
		if isRedisNil(err) {
			return nil, nil
		}
		return nil, err
	}
	if len(streams) == 0 {
		return nil, nil
	}
	return streams[0].Messages, nil
}

func (s *OrchestrationStore) claimPending(
	ctx context.Context,
	stream string,
	group string,
	consumer string,
	minIdle time.Duration,
	count int64,
) ([]redislib.XMessage, error) {
	redis, err := s.queueRedis()
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(consumer) == "" {
		return nil, fmt.Errorf("consumer is required")
	}
	if minIdle <= 0 {
		minIdle = 10 * time.Second
	}
	if count <= 0 {
		count = 64
	}

	messages, _, err := redis.XAutoClaim(ctx, &redislib.XAutoClaimArgs{
		Stream:   stream,
		Group:    group,
		Consumer: consumer,
		MinIdle:  minIdle,
		Start:    "0-0",
		Count:    count,
	}).Result()
	if err != nil {
		if isRedisNil(err) {
			return nil, nil
		}
		return nil, err
	}
	return messages, nil
}

func (s *OrchestrationStore) WithInstanceLock(ctx context.Context, lockKey string, fn func() error) error {
	if fn == nil {
		return fmt.Errorf("lock function is required")
	}
	if s == nil || s.redis == nil {
		return fn()
	}
	lock := common.NewRedisLock(s.redis)
	if err := lock.Acquire(ctx, lockKey, common.RedisLockOptions{TtlS: 5, Retries: 1}); err != nil {
		return fmt.Errorf("acquire instance lock: %w", err)
	}
	defer func() {
		_ = lock.Release(lockKey)
	}()
	return fn()
}

func (s *OrchestrationStore) PublishRunEvent(ctx context.Context, runID string, body []byte) error {
	if s == nil || s.redis == nil {
		return nil
	}
	pipe := s.redis.Pipeline()
	pipe.Publish(ctx, common.Keys.AgentRunEventsChannel(runID), body)
	pipe.RPush(ctx, common.Keys.AgentRunEventsBuffer(runID), body)
	pipe.Expire(ctx, common.Keys.AgentRunEventsBuffer(runID), 24*time.Hour)
	_, err := pipe.Exec(ctx)
	return err
}

func (s *OrchestrationStore) PublishWorkspaceLive(ctx context.Context, workspaceID uint) error {
	if s == nil || s.redis == nil {
		return nil
	}
	return s.redis.Publish(ctx, common.Keys.WorkspaceLive(workspaceID), []byte("live")).Err()
}

func (s *OrchestrationStore) SubscribeWorkspaceLive(ctx context.Context, workspaceID uint) (<-chan struct{}, func(), error) {
	return s.subscribeSignals(ctx, common.Keys.WorkspaceLive(workspaceID))
}

func (s *OrchestrationStore) PublishTaskLive(ctx context.Context, taskID string) error {
	if s == nil || s.redis == nil {
		return nil
	}
	taskID = strings.TrimSpace(taskID)
	if taskID == "" {
		return fmt.Errorf("task id is required")
	}
	return s.redis.Publish(ctx, common.Keys.TaskLive(taskID), []byte("live")).Err()
}

func (s *OrchestrationStore) SubscribeTaskLive(ctx context.Context, taskID string) (<-chan struct{}, func(), error) {
	taskID = strings.TrimSpace(taskID)
	if taskID == "" {
		return nil, nil, fmt.Errorf("task id is required")
	}
	return s.subscribeSignals(ctx, common.Keys.TaskLive(taskID))
}

func (s *OrchestrationStore) ListRunEvents(ctx context.Context, runID string) ([]string, error) {
	if s == nil || s.redis == nil {
		return []string{}, nil
	}
	return s.redis.LRange(ctx, common.Keys.AgentRunEventsBuffer(runID), 0, -1).Result()
}

func (s *OrchestrationStore) SubscribeRunEvents(ctx context.Context, runID string) (<-chan struct{}, func(), error) {
	runID = strings.TrimSpace(runID)
	if runID == "" {
		return nil, nil, fmt.Errorf("run id is required")
	}
	return s.subscribeSignals(ctx, common.Keys.AgentRunEventsChannel(runID))
}

func (s *OrchestrationStore) subscribeSignals(ctx context.Context, channel string) (<-chan struct{}, func(), error) {
	if s == nil || s.redis == nil {
		return nil, nil, fmt.Errorf(orchestrationRedisRequired)
	}
	channel = strings.TrimSpace(channel)
	if channel == "" {
		return nil, nil, fmt.Errorf("channel is required")
	}
	msgCh, errCh := s.redis.Subscribe(ctx, channel)
	out := make(chan struct{}, 8)
	done := make(chan struct{})
	var once sync.Once

	go func() {
		defer close(out)
		for {
			select {
			case <-ctx.Done():
				return
			case <-done:
				return
			case _, ok := <-msgCh:
				if !ok {
					return
				}
				select {
				case out <- struct{}{}:
				default:
				}
			case _, ok := <-errCh:
				if !ok {
					return
				}
			}
		}
	}()

	cleanup := func() {
		once.Do(func() { close(done) })
	}
	return out, cleanup, nil
}

func isRedisNil(err error) bool {
	return err != nil && (err.Error() == redisNilError || err == redislib.Nil)
}

func (s *OrchestrationStore) queueRedis() (*common.RedisClient, error) {
	if s == nil || s.redis == nil {
		return nil, fmt.Errorf(orchestrationRedisRequired)
	}
	return s.redis, nil
}

func redisStringOrEmpty(value string, err error) (string, error) {
	if err == nil {
		return value, nil
	}
	if isRedisNil(err) {
		return "", nil
	}
	return "", err
}
