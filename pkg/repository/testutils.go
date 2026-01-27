package repository

import (
	"github.com/alicebob/miniredis/v2"
	"github.com/beam-cloud/airstore/pkg/common"
	"github.com/beam-cloud/airstore/pkg/types"
)

// NewRedisClientForTest creates a Redis client backed by miniredis for testing
func NewRedisClientForTest() (*common.RedisClient, error) {
	s, err := miniredis.Run()
	if err != nil {
		return nil, err
	}

	rdb, err := common.NewRedisClient(types.RedisConfig{
		Addrs: []string{s.Addr()},
		Mode:  types.RedisModeSingle,
	})
	if err != nil {
		return nil, err
	}

	return rdb, nil
}

// NewWorkerRedisRepositoryForTest creates a WorkerRepository backed by miniredis
func NewWorkerRedisRepositoryForTest(rdb *common.RedisClient) WorkerRepository {
	return NewWorkerRedisRepository(rdb)
}
