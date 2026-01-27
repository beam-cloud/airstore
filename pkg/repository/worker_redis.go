package repository

import (
	"context"
	"fmt"
	"time"

	"github.com/beam-cloud/airstore/pkg/common"
	"github.com/beam-cloud/airstore/pkg/types"
	"github.com/redis/go-redis/v9"
)

// WorkerRedisRepository is a Redis-backed implementation of WorkerRepository
type WorkerRedisRepository struct {
	rdb  *common.RedisClient
	lock *common.RedisLock
}

// NewWorkerRedisRepository creates a new WorkerRedisRepository
func NewWorkerRedisRepository(rdb *common.RedisClient) WorkerRepository {
	return &WorkerRedisRepository{
		rdb:  rdb,
		lock: common.NewRedisLock(rdb),
	}
}

// AddWorker registers a new worker
func (r *WorkerRedisRepository) AddWorker(ctx context.Context, worker *types.Worker) error {
	lockKey := common.Keys.SchedulerWorkerLock(worker.ID)
	if err := r.lock.Acquire(ctx, lockKey, common.RedisLockOptions{TtlS: 10, Retries: 3}); err != nil {
		return fmt.Errorf("failed to acquire lock: %w", err)
	}
	defer r.lock.Release(lockKey)

	stateKey := common.Keys.SchedulerWorkerState(worker.ID)
	indexKey := common.Keys.SchedulerWorkerIndex()

	// Add worker state key to index
	if err := r.rdb.SAdd(ctx, indexKey, stateKey).Err(); err != nil {
		return fmt.Errorf("failed to add worker to index: %w", err)
	}

	// Set worker state
	if err := r.rdb.HSet(ctx, stateKey,
		"id", worker.ID,
		"status", string(worker.Status),
		"pool_name", worker.PoolName,
		"hostname", worker.Hostname,
		"cpu", worker.Cpu,
		"memory", worker.Memory,
		"last_seen_at", worker.LastSeenAt.Unix(),
		"registered_at", worker.RegisteredAt.Unix(),
		"version", worker.Version,
	).Err(); err != nil {
		return fmt.Errorf("failed to set worker state: %w", err)
	}

	// Set TTL on worker state
	if err := r.rdb.Expire(ctx, stateKey, types.WorkerStateTTL).Err(); err != nil {
		return fmt.Errorf("failed to set worker TTL: %w", err)
	}

	return nil
}

// RemoveWorker unregisters a worker
func (r *WorkerRedisRepository) RemoveWorker(ctx context.Context, workerId string) error {
	lockKey := common.Keys.SchedulerWorkerLock(workerId)
	if err := r.lock.Acquire(ctx, lockKey, common.RedisLockOptions{TtlS: 10, Retries: 3}); err != nil {
		return fmt.Errorf("failed to acquire lock: %w", err)
	}
	defer r.lock.Release(lockKey)

	stateKey := common.Keys.SchedulerWorkerState(workerId)
	indexKey := common.Keys.SchedulerWorkerIndex()

	// Remove from index
	if err := r.rdb.SRem(ctx, indexKey, stateKey).Err(); err != nil {
		return fmt.Errorf("failed to remove worker from index: %w", err)
	}

	// Delete worker state
	if err := r.rdb.Del(ctx, stateKey).Err(); err != nil {
		return fmt.Errorf("failed to delete worker state: %w", err)
	}

	return nil
}

// GetWorker retrieves a worker by ID
func (r *WorkerRedisRepository) GetWorker(ctx context.Context, workerId string) (*types.Worker, error) {
	stateKey := common.Keys.SchedulerWorkerState(workerId)
	return r.getWorkerFromKey(ctx, stateKey)
}

// GetAllWorkers retrieves all registered workers
func (r *WorkerRedisRepository) GetAllWorkers(ctx context.Context) ([]*types.Worker, error) {
	indexKey := common.Keys.SchedulerWorkerIndex()

	stateKeys, err := r.rdb.SMembers(ctx, indexKey).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to get worker index: %w", err)
	}

	workers := make([]*types.Worker, 0, len(stateKeys))
	for _, stateKey := range stateKeys {
		worker, err := r.getWorkerFromKey(ctx, stateKey)
		if err != nil {
			// Worker might have expired, remove from index
			r.rdb.SRem(ctx, indexKey, stateKey)
			continue
		}
		workers = append(workers, worker)
	}

	return workers, nil
}

// GetAvailableWorkers retrieves all workers with status "available"
func (r *WorkerRedisRepository) GetAvailableWorkers(ctx context.Context) ([]*types.Worker, error) {
	workers, err := r.GetAllWorkers(ctx)
	if err != nil {
		return nil, err
	}

	available := make([]*types.Worker, 0)
	for _, w := range workers {
		if w.Status == types.WorkerStatusAvailable {
			available = append(available, w)
		}
	}

	return available, nil
}

// UpdateWorkerStatus updates a worker's status
func (r *WorkerRedisRepository) UpdateWorkerStatus(ctx context.Context, workerId string, status types.WorkerStatus) error {
	lockKey := common.Keys.SchedulerWorkerLock(workerId)
	if err := r.lock.Acquire(ctx, lockKey, common.RedisLockOptions{TtlS: 10, Retries: 3}); err != nil {
		return fmt.Errorf("failed to acquire lock: %w", err)
	}
	defer r.lock.Release(lockKey)

	stateKey := common.Keys.SchedulerWorkerState(workerId)

	// Check if worker exists
	exists, err := r.rdb.Exists(ctx, stateKey).Result()
	if err != nil {
		return fmt.Errorf("failed to check worker existence: %w", err)
	}
	if exists == 0 {
		return &types.MetadataNotFoundError{Key: workerId}
	}

	// Update status and last_seen_at
	if err := r.rdb.HSet(ctx, stateKey,
		"status", string(status),
		"last_seen_at", time.Now().Unix(),
	).Err(); err != nil {
		return fmt.Errorf("failed to update worker status: %w", err)
	}

	// Refresh TTL
	if err := r.rdb.Expire(ctx, stateKey, types.WorkerStateTTL).Err(); err != nil {
		return fmt.Errorf("failed to refresh worker TTL: %w", err)
	}

	return nil
}

// SetWorkerKeepAlive refreshes the worker's TTL (heartbeat)
func (r *WorkerRedisRepository) SetWorkerKeepAlive(ctx context.Context, workerId string) error {
	stateKey := common.Keys.SchedulerWorkerState(workerId)

	// Update last_seen_at
	if err := r.rdb.HSet(ctx, stateKey, "last_seen_at", time.Now().Unix()).Err(); err != nil {
		return fmt.Errorf("failed to update last_seen_at: %w", err)
	}

	// Refresh TTL
	if err := r.rdb.Expire(ctx, stateKey, types.WorkerStateTTL).Err(); err != nil {
		return fmt.Errorf("failed to refresh worker TTL: %w", err)
	}

	return nil
}

// getWorkerFromKey retrieves a worker from a Redis hash key
func (r *WorkerRedisRepository) getWorkerFromKey(ctx context.Context, stateKey string) (*types.Worker, error) {
	result, err := r.rdb.HGetAll(ctx, stateKey).Result()
	if err != nil {
		if err == redis.Nil {
			return nil, &types.MetadataNotFoundError{Key: stateKey}
		}
		return nil, fmt.Errorf("failed to get worker state: %w", err)
	}

	if len(result) == 0 {
		return nil, &types.MetadataNotFoundError{Key: stateKey}
	}

	worker := &types.Worker{
		ID:       result["id"],
		Status:   types.WorkerStatus(result["status"]),
		PoolName: result["pool_name"],
		Hostname: result["hostname"],
		Version:  result["version"],
	}

	// Parse numeric fields
	if cpu, err := parseInt64(result["cpu"]); err == nil {
		worker.Cpu = cpu
	}
	if memory, err := parseInt64(result["memory"]); err == nil {
		worker.Memory = memory
	}

	// Parse timestamps
	if lastSeenAt, err := parseInt64(result["last_seen_at"]); err == nil {
		worker.LastSeenAt = time.Unix(lastSeenAt, 0)
	}
	if registeredAt, err := parseInt64(result["registered_at"]); err == nil {
		worker.RegisteredAt = time.Unix(registeredAt, 0)
	}

	return worker, nil
}

// parseInt64 parses a string to int64, returning 0 on error
func parseInt64(s string) (int64, error) {
	if s == "" {
		return 0, nil
	}
	var v int64
	_, err := fmt.Sscanf(s, "%d", &v)
	return v, err
}
