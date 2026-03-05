package repository

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
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

	pipe := r.rdb.Pipeline()
	pipe.SRem(ctx, indexKey, stateKey)
	pipe.Del(ctx, stateKey)
	_, err := pipe.Exec(ctx)
	return err
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

func (r *WorkerRedisRepository) AllocateIP(ctx context.Context, sandboxID, workerID string) (*types.IPAllocation, error) {
	lockKey := common.Keys.NetworkIPLock()
	poolKey := common.Keys.NetworkIPPool()
	mapKey := common.Keys.NetworkIPMap()

	if err := r.lock.Acquire(ctx, lockKey, common.RedisLockOptions{TtlS: 10, Retries: 5}); err != nil {
		return nil, fmt.Errorf("lock: %w", err)
	}
	defer r.lock.Release(lockKey)

	// Idempotent: return existing allocation
	if encoded, err := r.rdb.HGet(ctx, mapKey, sandboxID).Result(); err == nil && encoded != "" {
		alloc, decodeErr := decodeStoredIPAllocation(encoded)
		if decodeErr == nil {
			return alloc, nil
		}

		// Backward compatibility for legacy values that stored plain IPv4 strings.
		legacyAlloc, legacyErr := newIPAllocation(encoded)
		if legacyErr == nil {
			return legacyAlloc, nil
		}

		return nil, fmt.Errorf(
			"invalid stored ip allocation for sandbox %s (json decode: %v, legacy decode: %v)",
			sandboxID,
			decodeErr,
			legacyErr,
		)
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
		if candidate == types.DefaultGateway {
			continue
		}
		if !used[candidate] {
			ip = candidate
			break
		}
	}
	if ip == "" {
		return nil, fmt.Errorf("ip pool exhausted")
	}

	alloc, err := newIPAllocation(ip)
	if err != nil {
		return nil, fmt.Errorf("create allocation: %w", err)
	}

	encoded, err := encodeStoredIPAllocation(alloc)
	if err != nil {
		return nil, fmt.Errorf("encode allocation: %w", err)
	}

	pipe := r.rdb.Pipeline()
	pipe.SAdd(ctx, poolKey, ip)
	pipe.HSet(ctx, mapKey, sandboxID, encoded)
	if _, err := pipe.Exec(ctx); err != nil {
		return nil, fmt.Errorf("store: %w", err)
	}

	return alloc, nil
}

func (r *WorkerRedisRepository) ReleaseIP(ctx context.Context, sandboxID string) error {
	poolKey := common.Keys.NetworkIPPool()
	mapKey := common.Keys.NetworkIPMap()

	encoded, err := r.rdb.HGet(ctx, mapKey, sandboxID).Result()
	if err != nil {
		return nil // already released
	}

	alloc, err := decodeStoredIPAllocation(encoded)
	if err != nil {
		// Backward compatibility for legacy plain-IPv4 map values.
		alloc, err = newIPAllocation(encoded)
		if err != nil {
			return fmt.Errorf("decode allocation: %w", err)
		}
	}

	pipe := r.rdb.Pipeline()
	pipe.SRem(ctx, poolKey, alloc.IP)
	pipe.HDel(ctx, mapKey, sandboxID)
	_, err = pipe.Exec(ctx)
	return err
}

func (r *WorkerRedisRepository) GetSandboxIP(ctx context.Context, sandboxID string) (string, bool) {
	mapKey := common.Keys.NetworkIPMap()
	encoded, err := r.rdb.HGet(ctx, mapKey, sandboxID).Result()
	if err != nil || encoded == "" {
		return "", false
	}

	if alloc, decodeErr := decodeStoredIPAllocation(encoded); decodeErr == nil {
		return alloc.IP, alloc.IP != ""
	}

	// Legacy map values are plain IPv4 strings.
	return encoded, true
}

type redisIPAllocation struct {
	IP   string `json:"ip"`
	IPv6 string `json:"ipv6"`
}

func encodeStoredIPAllocation(alloc *types.IPAllocation) (string, error) {
	payload := redisIPAllocation{
		IP:   alloc.IP,
		IPv6: alloc.IPv6,
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func decodeStoredIPAllocation(encoded string) (*types.IPAllocation, error) {
	var payload redisIPAllocation
	if err := json.Unmarshal([]byte(encoded), &payload); err != nil {
		return nil, err
	}
	if payload.IP == "" {
		return nil, fmt.Errorf("missing ipv4 address")
	}
	if payload.IPv6 == "" {
		return newIPAllocation(payload.IP)
	}
	return &types.IPAllocation{
		IP:            payload.IP,
		Gateway:       types.DefaultGateway,
		PrefixLen:     types.DefaultPrefixLen,
		IPv6:          payload.IPv6,
		GatewayIPv6:   types.DefaultGatewayIPv6,
		PrefixLenIPv6: types.DefaultPrefixLenIPv6,
	}, nil
}

func newIPAllocation(ipv4 string) (*types.IPAllocation, error) {
	ipv6, err := deriveIPv6Address(ipv4)
	if err != nil {
		return nil, err
	}
	return &types.IPAllocation{
		IP:            ipv4,
		Gateway:       types.DefaultGateway,
		PrefixLen:     types.DefaultPrefixLen,
		IPv6:          ipv6,
		GatewayIPv6:   types.DefaultGatewayIPv6,
		PrefixLenIPv6: types.DefaultPrefixLenIPv6,
	}, nil
}

func deriveIPv6Address(ipv4 string) (string, error) {
	v4 := net.ParseIP(ipv4).To4()
	if v4 == nil {
		return "", fmt.Errorf("invalid ipv4 address: %s", ipv4)
	}

	baseV6 := net.ParseIP(types.DefaultGatewayIPv6).To16()
	if baseV6 == nil {
		return "", fmt.Errorf("invalid default ipv6 gateway: %s", types.DefaultGatewayIPv6)
	}

	derived := append(net.IP(nil), baseV6...)
	// Keep the configured /64 prefix and derive host bits from full IPv4 bytes
	// to guarantee stable, collision-free mapping for the /24 subnet.
	derived[8], derived[9], derived[10], derived[11] = 0, 0, 0, 0
	derived[12], derived[13], derived[14], derived[15] = v4[0], v4[1], v4[2], v4[3]

	if derived.String() == types.DefaultGatewayIPv6 {
		return "", fmt.Errorf("derived ipv6 address collides with gateway: %s", derived.String())
	}

	return derived.String(), nil
}

func parseInt64(s string) (int64, error) {
	if s == "" {
		return 0, nil
	}
	var v int64
	_, err := fmt.Sscanf(s, "%d", &v)
	return v, err
}
