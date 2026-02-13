package filesystem

import (
	"context"
	"strings"
	"sync"
	"time"

	pb "github.com/beam-cloud/airstore/proto"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

const (
	defaultAccessBuffer       = 4096
	defaultAccessBatchSize    = 128
	defaultAccessFlushEvery   = 2 * time.Second
	defaultAccessFlushTimeout = 5 * time.Second
)

// MountAccessUnaryInterceptor attaches mount telemetry metadata to every unary
// RPC so access events can be grouped by session and marked as mount-origin.
func MountAccessUnaryInterceptor(session string) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		ctx = metadata.AppendToOutgoingContext(ctx, "x-airstore-session", session, "x-airstore-access-origin", "fuse")
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}

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

	mu        sync.RWMutex
	closed    bool
	closeOnce sync.Once
	wg        sync.WaitGroup
}

func NewAccessCollector(client pb.AccessLogServiceClient, cfg AccessCollectorConfig) *AccessCollector {
	if client == nil {
		return nil
	}
	if cfg.BufferSize <= 0 {
		cfg.BufferSize = defaultAccessBuffer
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = defaultAccessBatchSize
	}
	if cfg.FlushInterval <= 0 {
		cfg.FlushInterval = defaultAccessFlushEvery
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

// NewAccessCollectorWithToken is a convenience wrapper that uses default
// buffering/flush behavior while attaching bearer auth for ingest RPCs.
func NewAccessCollectorWithToken(client pb.AccessLogServiceClient, token string) *AccessCollector {
	return NewAccessCollector(client, AccessCollectorConfig{AuthToken: token})
}

func (c *AccessCollector) Record(event *pb.AccessLogEvent) {
	if c == nil || event == nil {
		return
	}
	c.mu.RLock()
	if c.closed {
		c.mu.RUnlock()
		return
	}
	select {
	case c.ch <- event:
	default:
		log.Warn().Str("path", event.Path).Msg("access collector buffer full, dropping event")
	}
	c.mu.RUnlock()
}

func (c *AccessCollector) Close() {
	if c == nil {
		return
	}
	c.closeOnce.Do(func() {
		c.mu.Lock()
		c.closed = true
		close(c.done)
		c.mu.Unlock()
	})
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

		ctx, cancel := context.WithTimeout(context.Background(), defaultAccessFlushTimeout)
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
