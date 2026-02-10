package common

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog/log"
)

// EventEmitter is the interface for emitting hook events.
// Implemented by EventStream (Redis) and LocalEventEmitter (in-process).
type EventEmitter interface {
	Emit(ctx context.Context, data map[string]any) error
}

const (
	streamMaxLen       = 10000         // cap stream length, trim old events
	streamReadCount    = 10            // messages per read batch
	streamBlockTimeout = 5 * time.Second
	reclaimInterval    = 30 * time.Second // how often to check for stuck events
	reclaimMinIdle     = 60 * time.Second // events pending longer than this are reclaimed
	reclaimBatch       = 25               // max events to reclaim per cycle
)

// EventStream provides reliable, exactly-once event delivery using Redis Streams.
// Unlike EventBus (pub/sub, fire-and-forget to all replicas), EventStream uses
// consumer groups so each event is processed by exactly one consumer.
//
// Includes a reclaim loop that rescues events stuck in pending state (e.g., from
// a crashed consumer) using XPENDING + XCLAIM.
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
		MaxLen: streamMaxLen,
		Approx: true,
		Values: data,
	}).Err()
}

// Consume reads events in a loop. Each event is delivered to exactly one consumer
// in the group. Blocks when idle. Acknowledges after handler returns without error.
// Also runs a periodic reclaim loop to rescue events stuck in pending state.
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

	// Start the reclaim loop in background
	go s.reclaimLoop(ctx, handler)

	for {
		if ctx.Err() != nil {
			return
		}

		entries, err := s.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    s.group,
			Consumer: s.consumer,
			Streams:  []string{s.stream, ">"},
			Count:    streamReadCount,
			Block:    streamBlockTimeout,
		}).Result()

		if err != nil {
			if err == redis.Nil {
				continue
			}
			if ctx.Err() != nil {
				return
			}
			log.Warn().Err(err).Str("stream", s.stream).Msg("stream: read error")
			time.Sleep(time.Second)
			continue
		}

		if len(entries) == 0 || len(entries[0].Messages) == 0 {
			continue
		}

		for _, msg := range entries[0].Messages {
			s.processAndAck(ctx, msg, handler)
		}
	}
}

// processAndAck dispatches a message to the handler and ACKs it.
func (s *EventStream) processAndAck(ctx context.Context, msg redis.XMessage, handler func(id string, data map[string]any)) {
	data := make(map[string]any, len(msg.Values))
	for k, v := range msg.Values {
		data[k] = v
	}
	handler(msg.ID, data)
	if err := s.rdb.XAck(ctx, s.stream, s.group, msg.ID).Err(); err != nil {
		log.Warn().Err(err).Str("stream", s.stream).Str("id", msg.ID).Msg("stream: ack failed")
	}
}

// reclaimLoop periodically checks for events stuck in pending state (from crashed
// consumers) and reclaims them to this consumer for re-processing.
func (s *EventStream) reclaimLoop(ctx context.Context, handler func(id string, data map[string]any)) {
	ticker := time.NewTicker(reclaimInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.reclaimPending(ctx, handler)
		}
	}
}

// reclaimPending uses XPENDING + XCLAIM to steal events that have been pending
// for longer than reclaimMinIdle. This handles the case where a consumer crashes
// after reading an event but before ACKing it.
func (s *EventStream) reclaimPending(ctx context.Context, handler func(id string, data map[string]any)) {
	// Get pending entries across all consumers in the group
	pending, err := s.rdb.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream: s.stream,
		Group:  s.group,
		Idle:   reclaimMinIdle,
		Start:  "-",
		End:    "+",
		Count:  int64(reclaimBatch),
	}).Result()
	if err != nil {
		if err != redis.Nil && ctx.Err() == nil {
			log.Warn().Err(err).Str("stream", s.stream).Msg("stream: xpending failed")
		}
		return
	}

	if len(pending) == 0 {
		return
	}

	// Collect message IDs to claim
	ids := make([]string, len(pending))
	for i, p := range pending {
		ids[i] = p.ID
	}

	// Claim the stuck messages for this consumer
	messages, err := s.rdb.XClaim(ctx, &redis.XClaimArgs{
		Stream:   s.stream,
		Group:    s.group,
		Consumer: s.consumer,
		MinIdle:  reclaimMinIdle,
		Messages: ids,
	}).Result()
	if err != nil {
		if ctx.Err() == nil {
			log.Warn().Err(err).Str("stream", s.stream).Int("count", len(ids)).Msg("stream: xclaim failed")
		}
		return
	}

	if len(messages) > 0 {
		log.Info().Str("stream", s.stream).Int("reclaimed", len(messages)).Msg("stream: reclaimed pending events")
	}

	for _, msg := range messages {
		s.processAndAck(ctx, msg, handler)
	}
}

// LocalEventEmitter calls the handler directly in-process. No Redis required.
// Used in local mode where there's only one gateway instance.
type LocalEventEmitter struct {
	handler func(id string, data map[string]any)
	seq     uint64
}

func NewLocalEventEmitter() *LocalEventEmitter {
	return &LocalEventEmitter{}
}

// SetHandler sets the function called on each Emit. Must be called before Emit.
func (e *LocalEventEmitter) SetHandler(handler func(id string, data map[string]any)) {
	e.handler = handler
}

func (e *LocalEventEmitter) Emit(_ context.Context, data map[string]any) error {
	if e.handler == nil {
		return nil
	}
	id := atomic.AddUint64(&e.seq, 1)
	go e.handler(fmt.Sprintf("local-%d", id), data)
	return nil
}
