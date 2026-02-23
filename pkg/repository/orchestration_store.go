package repository

import (
	"context"
	"fmt"
	"time"

	"github.com/beam-cloud/airstore/pkg/common"
	"github.com/beam-cloud/airstore/pkg/types"
)

const redisNilError = "redis: nil"
const orchestrationQueueRedisRequired = "redis is required for orchestration queue"

// OrchestrationStore centralizes Redis-backed orchestration primitives:
// queue tokens, mode reshaping state, instance locks, and run events.
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

func (s *OrchestrationStore) PushQueueToken(ctx context.Context, token string) error {
	redis, err := s.queueRedis()
	if err != nil {
		return err
	}
	return redis.LPush(ctx, common.Keys.TaskQueue(), token).Err()
}

func (s *OrchestrationStore) PopQueueToken(ctx context.Context, timeout time.Duration) (string, error) {
	redis, err := s.queueRedis()
	if err != nil {
		return "", err
	}
	result, err := redis.BRPop(ctx, timeout, common.Keys.TaskQueue()).Result()
	if err != nil {
		if isRedisNil(err) {
			return "", nil
		}
		return "", err
	}
	if len(result) < 2 {
		return "", nil
	}
	return result[1], nil
}

func (s *OrchestrationStore) GetModeTaskID(ctx context.Context, modeKey string) (string, error) {
	redis, err := s.queueRedis()
	if err != nil {
		return "", err
	}
	id, err := redis.Get(ctx, common.Keys.TaskModeState(modeKey)).Result()
	return redisStringOrEmpty(id, err)
}

func (s *OrchestrationStore) SetModeTaskID(ctx context.Context, modeKey string, taskID string, ttl time.Duration) error {
	redis, err := s.queueRedis()
	if err != nil {
		return err
	}
	return redis.Set(ctx, common.Keys.TaskModeState(modeKey), taskID, ttl).Err()
}

func (s *OrchestrationStore) AddModeKey(ctx context.Context, modeKey string) (bool, error) {
	redis, err := s.queueRedis()
	if err != nil {
		return false, err
	}
	added, err := redis.SAdd(ctx, common.Keys.TaskModeSet(), modeKey).Result()
	return added > 0, err
}

func (s *OrchestrationStore) RemoveModeKey(ctx context.Context, modeKey string) error {
	redis, err := s.queueRedis()
	if err != nil {
		return err
	}
	_, err = redis.SRem(ctx, common.Keys.TaskModeSet(), modeKey).Result()
	return err
}

func (s *OrchestrationStore) GetDelModeTaskID(ctx context.Context, modeKey string) (string, error) {
	redis, err := s.queueRedis()
	if err != nil {
		return "", err
	}
	taskID, err := redis.GetDel(ctx, common.Keys.TaskModeState(modeKey)).Result()
	return redisStringOrEmpty(taskID, err)
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

func (s *OrchestrationStore) ListRunEvents(ctx context.Context, runID string) ([]string, error) {
	if s == nil || s.redis == nil {
		return []string{}, nil
	}
	return s.redis.LRange(ctx, common.Keys.AgentRunEventsBuffer(runID), 0, -1).Result()
}

func isRedisNil(err error) bool {
	return err != nil && err.Error() == redisNilError
}

func (s *OrchestrationStore) queueRedis() (*common.RedisClient, error) {
	if s == nil || s.redis == nil {
		return nil, fmt.Errorf(orchestrationQueueRedisRequired)
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
