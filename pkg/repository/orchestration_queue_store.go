package repository

import (
	"context"
	"fmt"
	"time"

	"github.com/beam-cloud/airstore/pkg/common"
	"github.com/beam-cloud/airstore/pkg/types"
)

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
		if err.Error() == "redis: nil" {
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
		if err.Error() == "redis: nil" {
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
		if err.Error() == "redis: nil" {
			return "", nil
		}
		return "", err
	}
	return envelopeID, nil
}
