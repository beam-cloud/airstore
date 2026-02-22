package repository

import (
	"context"
	"fmt"
	"time"

	"github.com/beam-cloud/airstore/pkg/common"
	"github.com/beam-cloud/airstore/pkg/types"
)

const redisNilError = "redis: nil"

// AgentRunEventStore persists and reads run events for orchestration.
type AgentRunEventStore interface {
	PublishRunEvent(ctx context.Context, runID string, body []byte) error
	ListRunEvents(ctx context.Context, runID string) ([]string, error)
}

// AgentEnvelopeQueueStore backs envelope queueing and queue-mode reshaping state.
type AgentEnvelopeQueueStore struct {
	backend BackendRepository
	redis   *common.RedisClient
}

func NewAgentEnvelopeQueueStore(backend BackendRepository, redis *common.RedisClient) *AgentEnvelopeQueueStore {
	return &AgentEnvelopeQueueStore{
		backend: backend,
		redis:   redis,
	}
}

func (s *AgentEnvelopeQueueStore) UpdateEnvelopeState(ctx context.Context, envelopeID string, state types.AgentEnvelopeState, dropReason *string, targetRunID *string) error {
	return s.backend.UpdateAgentTaskEnvelopeState(ctx, envelopeID, state, dropReason, targetRunID)
}

func (s *AgentEnvelopeQueueStore) PushQueueToken(ctx context.Context, token string) error {
	if s.redis == nil {
		return fmt.Errorf("redis is required for orchestration queue")
	}
	return s.redis.LPush(ctx, common.Keys.AgentEnvelopeQueue(), token).Err()
}

func (s *AgentEnvelopeQueueStore) PopQueueToken(ctx context.Context, timeout time.Duration) (string, error) {
	if s.redis == nil {
		return "", fmt.Errorf("redis is required for orchestration queue")
	}
	result, err := s.redis.BRPop(ctx, timeout, common.Keys.AgentEnvelopeQueue()).Result()
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

func (s *AgentEnvelopeQueueStore) GetModeEnvelopeID(ctx context.Context, modeKey string) (string, error) {
	if s.redis == nil {
		return "", fmt.Errorf("redis is required for orchestration queue")
	}
	id, err := s.redis.Get(ctx, common.Keys.AgentEnvelopeModeState(modeKey)).Result()
	if err != nil {
		if isRedisNil(err) {
			return "", nil
		}
		return "", err
	}
	return id, nil
}

func (s *AgentEnvelopeQueueStore) SetModeEnvelopeID(ctx context.Context, modeKey string, envelopeID string, ttl time.Duration) error {
	if s.redis == nil {
		return fmt.Errorf("redis is required for orchestration queue")
	}
	return s.redis.Set(ctx, common.Keys.AgentEnvelopeModeState(modeKey), envelopeID, ttl).Err()
}

func (s *AgentEnvelopeQueueStore) AddModeKey(ctx context.Context, modeKey string) (bool, error) {
	if s.redis == nil {
		return false, fmt.Errorf("redis is required for orchestration queue")
	}
	added, err := s.redis.SAdd(ctx, common.Keys.AgentEnvelopeModeSet(), modeKey).Result()
	return added > 0, err
}

func (s *AgentEnvelopeQueueStore) RemoveModeKey(ctx context.Context, modeKey string) error {
	if s.redis == nil {
		return fmt.Errorf("redis is required for orchestration queue")
	}
	_, err := s.redis.SRem(ctx, common.Keys.AgentEnvelopeModeSet(), modeKey).Result()
	return err
}

func (s *AgentEnvelopeQueueStore) GetDelModeEnvelopeID(ctx context.Context, modeKey string) (string, error) {
	if s.redis == nil {
		return "", fmt.Errorf("redis is required for orchestration queue")
	}
	envelopeID, err := s.redis.GetDel(ctx, common.Keys.AgentEnvelopeModeState(modeKey)).Result()
	if err != nil {
		if isRedisNil(err) {
			return "", nil
		}
		return "", err
	}
	return envelopeID, nil
}

// AgentInstanceDispatchLocker provides best-effort distributed locking per instance key.
type AgentInstanceDispatchLocker struct {
	redis *common.RedisClient
}

func NewAgentInstanceDispatchLocker(redis *common.RedisClient) *AgentInstanceDispatchLocker {
	return &AgentInstanceDispatchLocker{redis: redis}
}

func (l *AgentInstanceDispatchLocker) WithInstanceLock(ctx context.Context, lockKey string, fn func() error) error {
	if fn == nil {
		return fmt.Errorf("lock function is required")
	}
	if l == nil || l.redis == nil {
		return fn()
	}
	lock := common.NewRedisLock(l.redis)
	if err := lock.Acquire(ctx, lockKey, common.RedisLockOptions{TtlS: 5, Retries: 1}); err != nil {
		return fmt.Errorf("acquire instance lock: %w", err)
	}
	defer func() {
		_ = lock.Release(lockKey)
	}()
	return fn()
}

// RedisAgentRunEventStore is the Redis-backed run-event store implementation.
type RedisAgentRunEventStore struct {
	redis *common.RedisClient
}

func NewAgentRunEventStore(redis *common.RedisClient) AgentRunEventStore {
	if redis == nil {
		return nil
	}
	return &RedisAgentRunEventStore{redis: redis}
}

func (s *RedisAgentRunEventStore) PublishRunEvent(ctx context.Context, runID string, body []byte) error {
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

func (s *RedisAgentRunEventStore) ListRunEvents(ctx context.Context, runID string) ([]string, error) {
	if s == nil || s.redis == nil {
		return []string{}, nil
	}
	return s.redis.LRange(ctx, common.Keys.AgentRunEventsBuffer(runID), 0, -1).Result()
}

func isRedisNil(err error) bool {
	return err != nil && err.Error() == redisNilError
}
