package filesystem

import (
	"context"
	"sync"
	"testing"
	"time"

	pb "github.com/beam-cloud/airstore/proto"
	"google.golang.org/grpc"
)

type fakeAccessLogClient struct {
	mu     sync.Mutex
	calls  int
	events int
}

func (f *fakeAccessLogClient) IngestAccessEvents(_ context.Context, in *pb.IngestAccessEventsRequest, _ ...grpc.CallOption) (*pb.IngestAccessEventsResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	f.events += len(in.Events)
	return &pb.IngestAccessEventsResponse{Ok: true, Accepted: int32(len(in.Events))}, nil
}

func (f *fakeAccessLogClient) snapshot() (calls int, events int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls, f.events
}

func waitFor(t *testing.T, timeout time.Duration, pred func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if pred() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("condition not met before timeout")
}

func TestAccessCollectorFlushesOnBatchSize(t *testing.T) {
	client := &fakeAccessLogClient{}
	collector := NewAccessCollector(client, AccessCollectorConfig{
		BufferSize:    16,
		BatchSize:     2,
		FlushInterval: time.Hour, // force batch-size flush
	})
	t.Cleanup(collector.Close)

	collector.Record(&pb.AccessLogEvent{EventId: "e1", Path: "sources/a.txt"})
	collector.Record(&pb.AccessLogEvent{EventId: "e2", Path: "sources/b.txt"})

	waitFor(t, 2*time.Second, func() bool {
		calls, events := client.snapshot()
		return calls == 1 && events == 2
	})
}

func TestAccessCollectorDrainsOnClose(t *testing.T) {
	client := &fakeAccessLogClient{}
	collector := NewAccessCollector(client, AccessCollectorConfig{
		BufferSize:    16,
		BatchSize:     100, // prevent size-triggered flush
		FlushInterval: time.Hour,
	})

	collector.Record(&pb.AccessLogEvent{EventId: "e1", Path: "skills/AGENTS.md"})
	collector.Close()

	calls, events := client.snapshot()
	if calls != 1 {
		t.Fatalf("expected 1 flush call on close, got %d", calls)
	}
	if events != 1 {
		t.Fatalf("expected 1 drained event on close, got %d", events)
	}
}
