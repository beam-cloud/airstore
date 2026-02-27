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
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

// RedisTerminalIORepository implements TerminalIORepository on Redis pub/sub.
type RedisTerminalIORepository struct {
	rdb *common.RedisClient
}

const terminalInputBufferTTL = 24 * time.Hour

func NewRedisTerminalIORepository(rdb *common.RedisClient) TerminalIORepository {
	return &RedisTerminalIORepository{rdb: rdb}
}

// inputEnvelope is the JSON structure stored in the Redis input buffer.
type inputEnvelope struct {
	ID        string `json:"id"`
	Message   string `json:"message"`
	CreatedAt int64  `json:"created_at"`
}

func (r *RedisTerminalIORepository) PublishInput(ctx context.Context, taskID string, data []byte) error {
	if len(data) == 0 {
		return nil
	}
	env := inputEnvelope{
		ID:        uuid.New().String(),
		Message:   string(data),
		CreatedAt: time.Now().UnixMilli(),
	}
	payload, err := json.Marshal(env)
	if err != nil {
		return err
	}

	bufferKey := common.Keys.TerminalInputBuffer(taskID)
	channel := common.Keys.TerminalInput(taskID)

	pipe := r.rdb.Pipeline()
	pipe.RPush(ctx, bufferKey, payload)
	pipe.Expire(ctx, bufferKey, terminalInputBufferTTL)
	pipe.Publish(ctx, channel, []byte("input"))
	_, err = pipe.Exec(ctx)
	return err
}

// extractMessage unwraps the message text from a buffer entry.
// Handles both the structured JSON envelope and legacy raw-text format.
func extractMessage(raw []byte) []byte {
	var env inputEnvelope
	if json.Unmarshal(raw, &env) == nil && env.Message != "" {
		return []byte(env.Message)
	}
	return raw
}

func (r *RedisTerminalIORepository) SubscribeInput(ctx context.Context, taskID string) (<-chan []byte, func(), error) {
	channel := common.Keys.TerminalInput(taskID)
	bufferKey := common.Keys.TerminalInputBuffer(taskID)

	msgCh, errCh := r.rdb.Subscribe(ctx, channel)
	out := make(chan []byte, 128)
	done := make(chan struct{})
	var once sync.Once

	emitBufferedInput := func() bool {
		data, err := r.rdb.LPop(ctx, bufferKey).Bytes()
		if err != nil {
			if errors.Is(err, redis.Nil) {
				return true
			}
			return false
		}
		if len(data) == 0 {
			return true
		}
		msg := extractMessage(data)
		select {
		case out <- msg:
		case <-ctx.Done():
			return false
		case <-done:
			return false
		}
		return true
	}

	go func() {
		defer close(out)
		if ok := emitBufferedInput(); !ok {
			return
		}

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
				if ok := emitBufferedInput(); !ok {
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

func (r *RedisTerminalIORepository) ListPendingInputs(ctx context.Context, taskID string) ([]types.PendingInput, error) {
	bufferKey := common.Keys.TerminalInputBuffer(taskID)
	entries, err := r.rdb.LRange(ctx, bufferKey, 0, -1).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, nil
		}
		return nil, err
	}
	result := make([]types.PendingInput, 0, len(entries))
	for _, raw := range entries {
		var env inputEnvelope
		if json.Unmarshal([]byte(raw), &env) == nil && env.ID != "" {
			result = append(result, types.PendingInput{
				ID:        env.ID,
				Message:   env.Message,
				CreatedAt: env.CreatedAt,
			})
		}
	}
	return result, nil
}

func (r *RedisTerminalIORepository) PublishOutput(ctx context.Context, taskID string, data []byte) error {
	return r.rdb.Publish(ctx, common.Keys.TerminalOutput(taskID), data).Err()
}

func (r *RedisTerminalIORepository) SubscribeOutput(ctx context.Context, taskID string) (<-chan []byte, func(), error) {
	return r.subscribeBytes(ctx, common.Keys.TerminalOutput(taskID))
}

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

// getLeaseOwner returns the current owner of a session lease key, or "" if
// the lease does not exist. Redis Nil errors are treated as "no owner".
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
	current, _ := r.getLeaseOwner(ctx, key)
	return current == ownerID, nil
}

func (r *RedisTerminalIORepository) RenewSessionLease(ctx context.Context, workspaceID uint, sessionID, ownerID string, ttl time.Duration) (bool, error) {
	key := common.Keys.SessionLease(workspaceID, sessionID)
	current, err := r.getLeaseOwner(ctx, key)
	if err != nil || current != ownerID {
		return false, err
	}
	return r.rdb.Expire(ctx, key, ttl).Result()
}

func (r *RedisTerminalIORepository) ReleaseSessionLease(ctx context.Context, workspaceID uint, sessionID, ownerID string) error {
	key := common.Keys.SessionLease(workspaceID, sessionID)
	current, err := r.getLeaseOwner(ctx, key)
	if err != nil || current != ownerID {
		return err
	}
	return r.rdb.Del(ctx, key).Err()
}

func (r *RedisTerminalIORepository) GetSessionLeaseOwner(ctx context.Context, workspaceID uint, sessionID string) (string, error) {
	return r.getLeaseOwner(ctx, common.Keys.SessionLease(workspaceID, sessionID))
}

func (r *RedisTerminalIORepository) SetRunInteraction(
	ctx context.Context,
	workspaceID uint,
	runID string,
	state types.RunInteractionState,
	activeExecutionID string,
	ttl time.Duration,
) error {
	if strings.TrimSpace(runID) == "" {
		return nil
	}
	if ttl <= 0 {
		ttl = 30 * time.Minute
	}
	payload, err := json.Marshal(types.RunInteraction{
		State:             state,
		ActiveExecutionID: strings.TrimSpace(activeExecutionID),
		UpdatedAt:         time.Now().UnixMilli(),
	})
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

func (r *RedisTerminalIORepository) ClearRunInteraction(ctx context.Context, workspaceID uint, runID string) error {
	if strings.TrimSpace(runID) == "" {
		return nil
	}
	return r.rdb.Del(ctx, common.Keys.RunInteraction(workspaceID, runID)).Err()
}

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
