package repository

import (
	"context"
	"fmt"
	"time"

	"github.com/beam-cloud/airstore/pkg/common"
	"github.com/beam-cloud/airstore/pkg/types"
	"github.com/redis/go-redis/v9"
)

// WorkerRedisRepository implements WorkerRepository using Redis.
type WorkerRedisRepository struct {
	rdb  *common.RedisClient
	lock *common.RedisLock
}

func NewWorkerRedisRepository(rdb *common.RedisClient) WorkerRepository {
	return &WorkerRedisRepository{
		rdb:  rdb,
		lock: common.NewRedisLock(rdb),
	}
}

func (r *WorkerRedisRepository) AddWorker(ctx context.Context, w *types.Worker) error {
	lockKey := common.Keys.SchedulerWorkerLock(w.ID)
	if err := r.lock.Acquire(ctx, lockKey, common.RedisLockOptions{TtlS: 10, Retries: 3}); err != nil {
		return fmt.Errorf("lock: %w", err)
	}
	defer r.lock.Release(lockKey)

	stateKey := common.Keys.SchedulerWorkerState(w.ID)
	indexKey := common.Keys.SchedulerWorkerIndex()

	if err := r.rdb.SAdd(ctx, indexKey, stateKey).Err(); err != nil {
		return fmt.Errorf("index: %w", err)
	}

	if err := r.rdb.HSet(ctx, stateKey,
		"id", w.ID,
		"status", string(w.Status),
		"pool_name", w.PoolName,
		"hostname", w.Hostname,
		"cpu", w.Cpu,
		"memory", w.Memory,
		"last_seen_at", w.LastSeenAt.Unix(),
		"registered_at", w.RegisteredAt.Unix(),
		"version", w.Version,
	).Err(); err != nil {
		return fmt.Errorf("state: %w", err)
	}

	return r.rdb.Expire(ctx, stateKey, types.WorkerStateTTL).Err()
}

func (r *WorkerRedisRepository) RemoveWorker(ctx context.Context, id string) error {
	lockKey := common.Keys.SchedulerWorkerLock(id)
	if err := r.lock.Acquire(ctx, lockKey, common.RedisLockOptions{TtlS: 10, Retries: 3}); err != nil {
		return fmt.Errorf("lock: %w", err)
	}
	defer r.lock.Release(lockKey)

	stateKey := common.Keys.SchedulerWorkerState(id)
	indexKey := common.Keys.SchedulerWorkerIndex()

	r.rdb.SRem(ctx, indexKey, stateKey)
	return r.rdb.Del(ctx, stateKey).Err()
}

func (r *WorkerRedisRepository) GetWorker(ctx context.Context, id string) (*types.Worker, error) {
	return r.loadWorker(ctx, common.Keys.SchedulerWorkerState(id))
}

func (r *WorkerRedisRepository) GetAllWorkers(ctx context.Context) ([]*types.Worker, error) {
	indexKey := common.Keys.SchedulerWorkerIndex()
	keys, err := r.rdb.SMembers(ctx, indexKey).Result()
	if err != nil {
		return nil, err
	}

	workers := make([]*types.Worker, 0, len(keys))
	for _, key := range keys {
		w, err := r.loadWorker(ctx, key)
		if err != nil {
			r.rdb.SRem(ctx, indexKey, key) // cleanup stale
			continue
		}
		workers = append(workers, w)
	}
	return workers, nil
}

func (r *WorkerRedisRepository) GetAvailableWorkers(ctx context.Context) ([]*types.Worker, error) {
	all, err := r.GetAllWorkers(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]*types.Worker, 0)
	for _, w := range all {
		if w.Status == types.WorkerStatusAvailable {
			out = append(out, w)
		}
	}
	return out, nil
}

func (r *WorkerRedisRepository) UpdateWorkerStatus(ctx context.Context, id string, status types.WorkerStatus) error {
	lockKey := common.Keys.SchedulerWorkerLock(id)
	if err := r.lock.Acquire(ctx, lockKey, common.RedisLockOptions{TtlS: 10, Retries: 3}); err != nil {
		return fmt.Errorf("lock: %w", err)
	}
	defer r.lock.Release(lockKey)

	stateKey := common.Keys.SchedulerWorkerState(id)
	exists, err := r.rdb.Exists(ctx, stateKey).Result()
	if err != nil {
		return err
	}
	if exists == 0 {
		return &types.MetadataNotFoundError{Key: id}
	}

	if err := r.rdb.HSet(ctx, stateKey,
		"status", string(status),
		"last_seen_at", time.Now().Unix(),
	).Err(); err != nil {
		return err
	}

	return r.rdb.Expire(ctx, stateKey, types.WorkerStateTTL).Err()
}

func (r *WorkerRedisRepository) SetWorkerKeepAlive(ctx context.Context, id string) error {
	stateKey := common.Keys.SchedulerWorkerState(id)
	if err := r.rdb.HSet(ctx, stateKey, "last_seen_at", time.Now().Unix()).Err(); err != nil {
		return err
	}
	return r.rdb.Expire(ctx, stateKey, types.WorkerStateTTL).Err()
}

func (r *WorkerRedisRepository) loadWorker(ctx context.Context, key string) (*types.Worker, error) {
	data, err := r.rdb.HGetAll(ctx, key).Result()
	if err != nil {
		if err == redis.Nil {
			return nil, &types.MetadataNotFoundError{Key: key}
		}
		return nil, err
	}
	if len(data) == 0 {
		return nil, &types.MetadataNotFoundError{Key: key}
	}

	w := &types.Worker{
		ID:       data["id"],
		Status:   types.WorkerStatus(data["status"]),
		PoolName: data["pool_name"],
		Hostname: data["hostname"],
		Version:  data["version"],
	}
	w.Cpu, _ = parseInt64(data["cpu"])
	w.Memory, _ = parseInt64(data["memory"])
	if ts, _ := parseInt64(data["last_seen_at"]); ts > 0 {
		w.LastSeenAt = time.Unix(ts, 0)
	}
	if ts, _ := parseInt64(data["registered_at"]); ts > 0 {
		w.RegisteredAt = time.Unix(ts, 0)
	}
	return w, nil
}

// ----------------------------------------------------------------------------
// IP allocation
// ----------------------------------------------------------------------------


func (r *WorkerRedisRepository) AllocateIP(ctx context.Context, sandboxID, workerID string) (*types.IPAllocation, error) {
	lockKey := common.Keys.NetworkIPLock()
	poolKey := common.Keys.NetworkIPPool()
	mapKey := common.Keys.NetworkIPMap()

	if err := r.lock.Acquire(ctx, lockKey, common.RedisLockOptions{TtlS: 10, Retries: 5}); err != nil {
		return nil, fmt.Errorf("lock: %w", err)
	}
	defer r.lock.Release(lockKey)

	// Idempotent: return existing allocation
	if ip, err := r.rdb.HGet(ctx, mapKey, sandboxID).Result(); err == nil && ip != "" {
		return &types.IPAllocation{
			IP:        ip,
			Gateway:   types.DefaultGateway,
			PrefixLen: types.DefaultPrefixLen,
		}, nil
	}

	// Find available IP in subnet
	allocated, _ := r.rdb.SMembers(ctx, poolKey).Result()
	used := make(map[string]bool, len(allocated))
	for _, ip := range allocated {
		used[ip] = true
	}

	var ip string
	for i := 2; i < 255; i++ { // .0 = network, .1 = gateway, .255 = broadcast
		candidate := fmt.Sprintf("%s.%d", types.DefaultSubnetPrefix, i)
		if !used[candidate] {
			ip = candidate
			break
		}
	}
	if ip == "" {
		return nil, fmt.Errorf("ip pool exhausted")
	}

	pipe := r.rdb.Pipeline()
	pipe.SAdd(ctx, poolKey, ip)
	pipe.HSet(ctx, mapKey, sandboxID, ip)
	if _, err := pipe.Exec(ctx); err != nil {
		return nil, fmt.Errorf("store: %w", err)
	}

	return &types.IPAllocation{
		IP:        ip,
		Gateway:   types.DefaultGateway,
		PrefixLen: types.DefaultPrefixLen,
	}, nil
}

func (r *WorkerRedisRepository) ReleaseIP(ctx context.Context, sandboxID string) error {
	poolKey := common.Keys.NetworkIPPool()
	mapKey := common.Keys.NetworkIPMap()

	ip, err := r.rdb.HGet(ctx, mapKey, sandboxID).Result()
	if err != nil {
		return nil // already released
	}

	pipe := r.rdb.Pipeline()
	pipe.SRem(ctx, poolKey, ip)
	pipe.HDel(ctx, mapKey, sandboxID)
	_, err = pipe.Exec(ctx)
	return err
}

func (r *WorkerRedisRepository) GetSandboxIP(ctx context.Context, sandboxID string) (string, bool) {
	mapKey := common.Keys.NetworkIPMap()
	ip, err := r.rdb.HGet(ctx, mapKey, sandboxID).Result()
	return ip, err == nil && ip != ""
}

func parseInt64(s string) (int64, error) {
	if s == "" {
		return 0, nil
	}
	var v int64
	_, err := fmt.Sscanf(s, "%d", &v)
	return v, err
}
