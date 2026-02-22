package repository

import (
	"context"
	"fmt"

	"github.com/beam-cloud/airstore/pkg/common"
)

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
