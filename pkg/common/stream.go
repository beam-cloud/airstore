package common

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog/log"
)

// EventStream provides reliable, exactly-once event delivery using Redis Streams.
// Unlike EventBus (pub/sub, fire-and-forget to all replicas), EventStream uses
// consumer groups so each event is processed by exactly one consumer.
//
// Inspired by Kafka consumer groups, Kubernetes work queues.
type EventStream struct {
	rdb      *RedisClient
	stream   string
	group    string
	consumer string // unique per gateway replica
}

// NewEventStream creates a stream producer/consumer.
// stream: the Redis Stream key (e.g., common.Keys.HookStream())
// group: consumer group name (same across all replicas)
// consumer: unique per replica (e.g., hostname)
func NewEventStream(rdb *RedisClient, stream, group, consumer string) *EventStream {
	return &EventStream{
		rdb:      rdb,
		stream:   stream,
		group:    group,
		consumer: consumer,
	}
}

// Emit appends an event to the stream. O(1). Non-blocking.
// Called by StorageService / SourceService on the hot path.
func (s *EventStream) Emit(ctx context.Context, data map[string]any) error {
	return s.rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: s.stream,
		MaxLen: 10000, // cap stream length, trim old events
		Approx: true,  // ~ efficient trimming
		Values: data,
	}).Err()
}

// Consume reads events in a loop. Each event is delivered to exactly one consumer
// in the group. Blocks when idle. Acknowledges after handler returns without error.
// Run this in a goroutine per gateway replica.
func (s *EventStream) Consume(ctx context.Context, handler func(id string, data map[string]any)) {
	// Create consumer group (idempotent -- first replica wins, rest are no-ops)
	if err := s.rdb.XGroupCreateMkStream(ctx, s.stream, s.group, "0").Err(); err != nil {
		// "BUSYGROUP Consumer Group name already exists" is expected
		if err.Error() != "BUSYGROUP Consumer Group name already exists" {
			log.Warn().Err(err).Str("stream", s.stream).Str("group", s.group).Msg("stream: group create")
		}
	}

	log.Info().
		Str("stream", s.stream).
		Str("group", s.group).
		Str("consumer", s.consumer).
		Msg("stream consumer started")

	for {
		if ctx.Err() != nil {
			return
		}

		entries, err := s.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    s.group,
			Consumer: s.consumer,
			Streams:  []string{s.stream, ">"},
			Count:    10,
			Block:    5 * time.Second,
		}).Result()

		if err != nil {
			// Block timeout returns redis.Nil, not an error
			if err == redis.Nil {
				continue
			}
			// Context cancelled during shutdown
			if ctx.Err() != nil {
				return
			}
			log.Warn().Err(err).Str("stream", s.stream).Msg("stream: read error")
			time.Sleep(time.Second) // backoff on unexpected errors
			continue
		}

		if len(entries) == 0 || len(entries[0].Messages) == 0 {
			continue
		}

		for _, msg := range entries[0].Messages {
			// Convert redis StringInterfaceMap values to map[string]any
			data := make(map[string]any, len(msg.Values))
			for k, v := range msg.Values {
				data[k] = v
			}
			handler(msg.ID, data)
			if err := s.rdb.XAck(ctx, s.stream, s.group, msg.ID).Err(); err != nil {
				log.Warn().Err(err).Str("stream", s.stream).Str("id", msg.ID).Msg("stream: ack failed")
			}
		}
	}
}
