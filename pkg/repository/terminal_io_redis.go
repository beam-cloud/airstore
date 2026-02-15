package repository

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/beam-cloud/airstore/pkg/common"
)

const (
	terminalInputChannel  = "airstore:terminal:%s:input"
	terminalOutputChannel = "airstore:terminal:%s:output"
	terminalResizeChannel = "airstore:terminal:%s:resize"
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

func (r *RedisTerminalIORepository) PublishResize(ctx context.Context, taskID string, cols int, rows int) error {
	data, err := json.Marshal(TerminalResizeEvent{Cols: cols, Rows: rows})
	if err != nil {
		return fmt.Errorf("failed to marshal terminal resize event: %w", err)
	}
	return r.rdb.Publish(ctx, fmt.Sprintf(terminalResizeChannel, taskID), data).Err()
}

func (r *RedisTerminalIORepository) SubscribeResize(ctx context.Context, taskID string) (<-chan TerminalResizeEvent, func(), error) {
	msgCh, errCh := r.rdb.Subscribe(ctx, fmt.Sprintf(terminalResizeChannel, taskID))
	out := make(chan TerminalResizeEvent, 32)
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
				var event TerminalResizeEvent
				if err := json.Unmarshal([]byte(msg.Payload), &event); err != nil {
					continue
				}
				select {
				case out <- event:
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
