package repository

import (
	"context"
	"fmt"
	"sync"

	"github.com/beam-cloud/airstore/pkg/common"
)

const (
	terminalInputChannel  = "airstore:terminal:%s:input"
	terminalOutputChannel = "airstore:terminal:%s:output"
	terminalCancelChannel = "airstore:terminal:%s:cancel"
)

// RedisTerminalIORepository implements TerminalIORepository on Redis pub/sub.
type RedisTerminalIORepository struct {
	rdb *common.RedisClient
}

func NewRedisTerminalIORepository(rdb *common.RedisClient) TerminalIORepository {
	return &RedisTerminalIORepository{rdb: rdb}
}

func (r *RedisTerminalIORepository) PublishInput(ctx context.Context, taskID string, data []byte) error {
	return r.rdb.Publish(ctx, fmt.Sprintf(terminalInputChannel, taskID), data).Err()
}

func (r *RedisTerminalIORepository) SubscribeInput(ctx context.Context, taskID string) (<-chan []byte, func(), error) {
	return r.subscribeBytes(ctx, fmt.Sprintf(terminalInputChannel, taskID))
}

func (r *RedisTerminalIORepository) PublishOutput(ctx context.Context, taskID string, data []byte) error {
	return r.rdb.Publish(ctx, fmt.Sprintf(terminalOutputChannel, taskID), data).Err()
}

func (r *RedisTerminalIORepository) SubscribeOutput(ctx context.Context, taskID string) (<-chan []byte, func(), error) {
	return r.subscribeBytes(ctx, fmt.Sprintf(terminalOutputChannel, taskID))
}

func (r *RedisTerminalIORepository) PublishCancel(ctx context.Context, taskID string) error {
	return r.rdb.Publish(ctx, fmt.Sprintf(terminalCancelChannel, taskID), []byte("cancel")).Err()
}

func (r *RedisTerminalIORepository) SubscribeCancel(ctx context.Context, taskID string) (<-chan struct{}, func(), error) {
	msgCh, errCh := r.rdb.Subscribe(ctx, fmt.Sprintf(terminalCancelChannel, taskID))
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
