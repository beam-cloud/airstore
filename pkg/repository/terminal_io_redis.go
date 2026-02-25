package repository

import (
	"context"
	"sync"

	"github.com/beam-cloud/airstore/pkg/common"
)

// RedisTerminalIORepository implements TerminalIORepository on Redis pub/sub.
type RedisTerminalIORepository struct {
	rdb *common.RedisClient
}

func NewRedisTerminalIORepository(rdb *common.RedisClient) TerminalIORepository {
	return &RedisTerminalIORepository{rdb: rdb}
}

func (r *RedisTerminalIORepository) PublishInput(ctx context.Context, taskID string, data []byte) error {
	return r.rdb.Publish(ctx, common.Keys.TerminalInput(taskID), data).Err()
}

func (r *RedisTerminalIORepository) SubscribeInput(ctx context.Context, taskID string) (<-chan []byte, func(), error) {
	return r.subscribeBytes(ctx, common.Keys.TerminalInput(taskID))
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
