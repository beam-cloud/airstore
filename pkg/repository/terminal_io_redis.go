package repository

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/beam-cloud/airstore/pkg/common"
	"github.com/beam-cloud/airstore/pkg/types"
	"github.com/redis/go-redis/v9"
)

type RedisTerminalIORepository struct {
	rdb *common.RedisClient
}

const sessionCheckpointTTL = 30 * 24 * time.Hour

const renewSessionLeaseScript = `
if redis.call('get', KEYS[1]) == ARGV[1] then
	return redis.call('pexpire', KEYS[1], ARGV[2])
end
return 0
`

const releaseSessionLeaseScript = `
if redis.call('get', KEYS[1]) == ARGV[1] then
	return redis.call('del', KEYS[1])
end
return 0
`

func NewRedisTerminalIORepository(rdb *common.RedisClient) TerminalIORepository {
	return &RedisTerminalIORepository{rdb: rdb}
}

// --- Input wake (pubsub-only, no buffer) ---

func (r *RedisTerminalIORepository) PublishInputWake(ctx context.Context, taskID string) error {
	channel := common.Keys.TerminalInput(taskID)
	return r.rdb.Publish(ctx, channel, []byte("wake")).Err()
}

func (r *RedisTerminalIORepository) SubscribeInputWake(ctx context.Context, taskID string) (<-chan struct{}, func(), error) {
	channel := common.Keys.TerminalInput(taskID)
	msgCh, errCh := r.rdb.Subscribe(ctx, channel)
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

// --- Terminal output ---

func (r *RedisTerminalIORepository) PublishOutput(ctx context.Context, taskID string, data []byte) error {
	return r.rdb.Publish(ctx, common.Keys.TerminalOutput(taskID), data).Err()
}

func (r *RedisTerminalIORepository) SubscribeOutput(ctx context.Context, taskID string) (<-chan []byte, func(), error) {
	return r.subscribeBytes(ctx, common.Keys.TerminalOutput(taskID))
}

// --- Cancel ---

func (r *RedisTerminalIORepository) PublishCancel(ctx context.Context, taskID string) error {
	return r.rdb.Publish(ctx, common.Keys.TerminalCancel(taskID), []byte("cancel")).Err()
}

func (r *RedisTerminalIORepository) SubscribeCancel(ctx context.Context, taskID string) (<-chan struct{}, func(), error) {
	msgCh, errCh := r.rdb.Subscribe(ctx, common.Keys.TerminalCancel(taskID))
	out := make(chan struct{}, 1)
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
				return
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

// --- Session lease ---

func (r *RedisTerminalIORepository) getLeaseOwner(ctx context.Context, key string) (string, error) {
	owner, err := r.rdb.Get(ctx, key).Result()
	if errors.Is(err, redis.Nil) {
		return "", nil
	}
	return owner, err
}

func (r *RedisTerminalIORepository) AcquireSessionLease(ctx context.Context, workspaceID uint, sessionID, ownerID string, ttl time.Duration) (bool, error) {
	key := common.Keys.SessionLease(workspaceID, sessionID)
	if ok, err := r.rdb.SetNX(ctx, key, ownerID, ttl).Result(); err != nil {
		return false, err
	} else if ok {
		return true, nil
	}
	current, err := r.getLeaseOwner(ctx, key)
	if err != nil {
		return false, err
	}
	return current == ownerID, nil
}

func (r *RedisTerminalIORepository) RenewSessionLease(ctx context.Context, workspaceID uint, sessionID, ownerID string, ttl time.Duration) (bool, error) {
	key := common.Keys.SessionLease(workspaceID, sessionID)
	result, err := r.rdb.Eval(
		ctx,
		renewSessionLeaseScript,
		[]string{key},
		ownerID,
		ttl.Milliseconds(),
	).Int64()
	if err != nil {
		return false, err
	}
	return result == 1, nil
}

func (r *RedisTerminalIORepository) ReleaseSessionLease(ctx context.Context, workspaceID uint, sessionID, ownerID string) error {
	key := common.Keys.SessionLease(workspaceID, sessionID)
	_, err := r.rdb.Eval(
		ctx,
		releaseSessionLeaseScript,
		[]string{key},
		ownerID,
	).Int64()
	return err
}

func (r *RedisTerminalIORepository) GetSessionLeaseOwner(ctx context.Context, workspaceID uint, sessionID string) (string, error) {
	return r.getLeaseOwner(ctx, common.Keys.SessionLease(workspaceID, sessionID))
}

// --- Session checkpoint ---

func (r *RedisTerminalIORepository) SetSessionCheckpoint(
	ctx context.Context,
	workspaceID uint,
	sessionID string,
	checkpoint *types.SessionCheckpoint,
	ttl time.Duration,
) error {
	if strings.TrimSpace(sessionID) == "" || checkpoint == nil {
		return nil
	}
	if ttl <= 0 {
		ttl = sessionCheckpointTTL
	}
	payload, err := json.Marshal(checkpoint)
	if err != nil {
		return err
	}
	return r.rdb.Set(ctx, common.Keys.SessionCheckpoint(workspaceID, sessionID), payload, ttl).Err()
}

func (r *RedisTerminalIORepository) GetSessionCheckpoint(ctx context.Context, workspaceID uint, sessionID string) (*types.SessionCheckpoint, error) {
	if strings.TrimSpace(sessionID) == "" {
		return nil, nil
	}
	raw, err := r.rdb.Get(ctx, common.Keys.SessionCheckpoint(workspaceID, sessionID)).Bytes()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, nil
		}
		return nil, err
	}
	var checkpoint types.SessionCheckpoint
	if err := json.Unmarshal(raw, &checkpoint); err != nil {
		return nil, err
	}
	if strings.TrimSpace(checkpoint.RunID) == "" {
		return nil, nil
	}
	return &checkpoint, nil
}

// --- Run interaction state ---

func (r *RedisTerminalIORepository) SetRunInteraction(
	ctx context.Context,
	workspaceID uint,
	runID string,
	interaction types.RunInteraction,
	ttl time.Duration,
) error {
	if strings.TrimSpace(runID) == "" {
		return nil
	}
	if ttl <= 0 {
		ttl = 30 * time.Minute
	}
	interaction.UpdatedAt = time.Now().UnixMilli()
	payload, err := json.Marshal(interaction)
	if err != nil {
		return err
	}
	return r.rdb.Set(ctx, common.Keys.RunInteraction(workspaceID, runID), payload, ttl).Err()
}

func (r *RedisTerminalIORepository) GetRunInteraction(ctx context.Context, workspaceID uint, runID string) (*types.RunInteraction, error) {
	if strings.TrimSpace(runID) == "" {
		return nil, nil
	}
	raw, err := r.rdb.Get(ctx, common.Keys.RunInteraction(workspaceID, runID)).Bytes()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, nil
		}
		return nil, err
	}
	var interaction types.RunInteraction
	if err := json.Unmarshal(raw, &interaction); err != nil {
		return nil, err
	}
	return &interaction, nil
}

// --- Helpers ---

func (r *RedisTerminalIORepository) subscribeBytes(ctx context.Context, channel string) (<-chan []byte, func(), error) {
	msgCh, errCh := r.rdb.Subscribe(ctx, channel)
	out := make(chan []byte, 128)
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
			}
		}
	}()

	cleanup := func() {
		once.Do(func() { close(done) })
	}
	return out, cleanup, nil
}
