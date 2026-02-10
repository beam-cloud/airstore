package common

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog/log"
)

const (
	EventBusChannel = "airstore:events"
)

type EventType string

const (
	EventCacheInvalidate EventType = "cache.invalidate"
)

type Event struct {
	Type EventType      `json:"type"`
	Data map[string]any `json:"data,omitempty"`
}

type EventBus struct {
	rdb      *RedisClient
	channel  string
	handlers map[EventType][]func(Event)
	mu       sync.RWMutex
	ctx      context.Context
}

func NewEventBus(ctx context.Context, rdb *RedisClient) *EventBus {
	return &EventBus{
		rdb:      rdb,
		channel:  EventBusChannel,
		handlers: make(map[EventType][]func(Event)),
		ctx:      ctx,
	}
}

func (eb *EventBus) On(t EventType, fn func(Event)) {
	eb.mu.Lock()
	eb.handlers[t] = append(eb.handlers[t], fn)
	eb.mu.Unlock()
}

func (eb *EventBus) Emit(e Event) {
	if eb.rdb == nil {
		eb.dispatch(e)
		return
	}
	data, err := json.Marshal(e)
	if err != nil {
		return
	}
	eb.rdb.Publish(eb.ctx, eb.channel, data)
}

func (eb *EventBus) dispatch(e Event) {
	eb.mu.RLock()
	handlers := eb.handlers[e.Type]
	eb.mu.RUnlock()
	for _, fn := range handlers {
		fn(e)
	}
}

func (eb *EventBus) Start() {
	if eb.rdb == nil {
		<-eb.ctx.Done()
		return
	}
	log.Info().Str("channel", eb.channel).Msg("eventbus started")
	eb.listen()
}

func (eb *EventBus) listen() {
	const (
		minBackoff   = time.Second
		maxBackoff   = 30 * time.Second
		resetAfter   = 60 * time.Second // reset backoff after a stable connection
	)

	backoff := minBackoff

	for {
		if eb.ctx.Err() != nil {
			return
		}

		start := time.Now()
		msgs, errs := eb.rdb.Subscribe(eb.ctx, eb.channel)
		eb.recv(msgs, errs)

		// If the subscription was stable for a while, reset backoff
		if time.Since(start) >= resetAfter {
			backoff = minBackoff
		}

		// Back off before reconnecting to avoid tight loop on persistent errors
		log.Warn().Str("channel", eb.channel).Dur("backoff", backoff).Msg("eventbus subscription ended, reconnecting")
		select {
		case <-eb.ctx.Done():
			return
		case <-time.After(backoff):
		}

		backoff = min(backoff*2, maxBackoff)
	}
}

func (eb *EventBus) recv(msgs <-chan *redis.Message, errs <-chan error) {
	for {
		select {
		case <-eb.ctx.Done():
			return
		case <-errs:
			return
		case msg, ok := <-msgs:
			if !ok {
				return
			}
			var e Event
			if json.Unmarshal([]byte(msg.Payload), &e) == nil {
				eb.dispatch(e)
			}
		}
	}
}
