package filesystem

import (
	"context"
	"strings"
	"sync"
	"time"

	pb "github.com/beam-cloud/airstore/proto"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc/metadata"
)

const (
	defaultAccessCollectorBuffer       = 4096
	defaultAccessCollectorBatchSize    = 128
	defaultAccessCollectorFlushEvery   = 2 * time.Second
	defaultAccessCollectorFlushTimeout = 5 * time.Second
)

// AccessCollectorConfig configures client-side batched access log ingestion.
type AccessCollectorConfig struct {
	BufferSize    int
	BatchSize     int
	FlushInterval time.Duration
	AuthToken     string // optional bearer token for AccessLogService auth
}

// AccessCollector buffers logical read events and periodically flushes them to
// the gateway AccessLogService. Record() is non-blocking; events are dropped if
// the local buffer is full.
type AccessCollector struct {
	client pb.AccessLogServiceClient

	bufferSize    int
	batchSize     int
	flushInterval time.Duration
	authHeader    string

	ch   chan *pb.AccessLogEvent
	done chan struct{}

	closeOnce sync.Once
	wg        sync.WaitGroup
}

func NewAccessCollector(client pb.AccessLogServiceClient, cfg AccessCollectorConfig) *AccessCollector {
	if client == nil {
		return nil
	}
	if cfg.BufferSize <= 0 {
		cfg.BufferSize = defaultAccessCollectorBuffer
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = defaultAccessCollectorBatchSize
	}
	if cfg.FlushInterval <= 0 {
		cfg.FlushInterval = defaultAccessCollectorFlushEvery
	}
	authHeader := strings.TrimSpace(cfg.AuthToken)
	if authHeader != "" && !strings.HasPrefix(strings.ToLower(authHeader), "bearer ") {
		authHeader = "Bearer " + authHeader
	}

	c := &AccessCollector{
		client:        client,
		bufferSize:    cfg.BufferSize,
		batchSize:     cfg.BatchSize,
		flushInterval: cfg.FlushInterval,
		authHeader:    authHeader,
		ch:            make(chan *pb.AccessLogEvent, cfg.BufferSize),
		done:          make(chan struct{}),
	}
	c.wg.Add(1)
	go c.loop()
	return c
}

func (c *AccessCollector) Record(event *pb.AccessLogEvent) {
	if c == nil || event == nil {
		return
	}
	select {
	case c.ch <- event:
	default:
		log.Warn().Str("path", event.Path).Msg("access collector buffer full, dropping event")
	}
}

func (c *AccessCollector) Close() {
	if c == nil {
		return
	}
	c.closeOnce.Do(func() { close(c.done) })
	c.wg.Wait()
}

func (c *AccessCollector) loop() {
	defer c.wg.Done()

	ticker := time.NewTicker(c.flushInterval)
	defer ticker.Stop()

	batch := make([]*pb.AccessLogEvent, 0, c.batchSize)
	flush := func() {
		if len(batch) == 0 {
			return
		}

		ctx, cancel := context.WithTimeout(context.Background(), defaultAccessCollectorFlushTimeout)
		if c.authHeader != "" {
			ctx = metadata.AppendToOutgoingContext(ctx, "authorization", c.authHeader)
		}
		resp, err := c.client.IngestAccessEvents(ctx, &pb.IngestAccessEventsRequest{Events: batch})
		cancel()
		if err != nil {
			log.Warn().Err(err).Int("events", len(batch)).Msg("failed to flush access events")
		} else if resp == nil || !resp.Ok {
			errMsg := ""
			if resp != nil {
				errMsg = resp.Error
			}
			log.Warn().Str("error", errMsg).Int("events", len(batch)).Msg("access event ingest rejected by gateway")
		}
		batch = batch[:0]
	}

	for {
		select {
		case ev := <-c.ch:
			batch = append(batch, ev)
			if len(batch) >= c.batchSize {
				flush()
			}
		case <-ticker.C:
			flush()
		case <-c.done:
			for {
				select {
				case ev := <-c.ch:
					batch = append(batch, ev)
					if len(batch) >= c.batchSize {
						flush()
					}
				default:
					flush()
					return
				}
			}
		}
	}
}
