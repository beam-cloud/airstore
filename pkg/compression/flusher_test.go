package compression

import (
	"bytes"
	"context"
	"sync"
	"testing"
	"time"

	"github.com/beam-cloud/airstore/pkg/instrumentation"

	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// ---------------------------------------------------------------------------
// Mock S3 uploader
// ---------------------------------------------------------------------------

type mockS3 struct {
	mu      sync.Mutex
	uploads []s3Upload
}

type s3Upload struct {
	Bucket string
	Key    string
	Body   []byte
}

func (m *mockS3) PutObject(_ context.Context, input *s3.PutObjectInput, _ ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	var body []byte
	if input.Body != nil {
		buf := new(bytes.Buffer)
		buf.ReadFrom(input.Body)
		body = buf.Bytes()
	}
	m.mu.Lock()
	m.uploads = append(m.uploads, s3Upload{
		Bucket: *input.Bucket,
		Key:    *input.Key,
		Body:   body,
	})
	m.mu.Unlock()
	return &s3.PutObjectOutput{}, nil
}

func (m *mockS3) getUploads() []s3Upload {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := make([]s3Upload, len(m.uploads))
	copy(cp, m.uploads)
	return cp
}

// ---------------------------------------------------------------------------
// Mock access recorder
// ---------------------------------------------------------------------------

type mockRecorder struct {
	mu     sync.Mutex
	events []instrumentation.AccessEvent
}

func (r *mockRecorder) Record(_ context.Context, ev instrumentation.AccessEvent) error {
	r.mu.Lock()
	r.events = append(r.events, ev)
	r.mu.Unlock()
	return nil
}

func (r *mockRecorder) Flush() error { return nil }

func (r *mockRecorder) getEvents() []instrumentation.AccessEvent {
	r.mu.Lock()
	defer r.mu.Unlock()
	cp := make([]instrumentation.AccessEvent, len(r.events))
	copy(cp, r.events)
	return cp
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

func TestFlusher_ProcessWritesS3AndRedis(t *testing.T) {
	rdb, _ := newTestRedis(t)
	store := NewCompressedStore(rdb, Config{
		ContentCacheMaxBytes: 10 * 1024 * 1024,
		ContentCacheTTL:      5 * time.Minute,
	})
	s3mock := &mockS3{}
	recorder := &mockRecorder{}

	flusher := NewAsyncFlusher(s3mock, "airstore", store, recorder)
	defer flusher.Shutdown()

	compressed := []byte("stripped email content")
	ptr := &CompressedPointer{
		S3Key:            "compressed/ws-123/query/result/strip.abc",
		OriginalTokens:   1000,
		CompressedTokens: 200,
		Strategy:         "strip",
		CreatedAt:        time.Now().Unix(),
		Size:             5000,
	}

	flusher.Enqueue(FlushItem{
		CompressedData: compressed,
		S3Key:          ptr.S3Key,
		WorkspaceID:    1,
		WorkspaceExtID: "ws-123",
		QueryPath:      "/sources/gmail/inbox",
		ResultID:       "result-1",
		Strategy:       "strip",
		Pointer:        ptr,
		AccessEvent: instrumentation.AccessEvent{
			Path:     "/sources/gmail/inbox/email.txt",
			Strategy: "strip",
			Outcome:  "compressed",
		},
	})

	// Wait for async processing
	time.Sleep(200 * time.Millisecond)

	// S3 upload should have happened
	uploads := s3mock.getUploads()
	if len(uploads) != 1 {
		t.Fatalf("expected 1 S3 upload, got %d", len(uploads))
	}
	if uploads[0].Bucket != "airstore-ws-123" {
		t.Errorf("bucket: got %q, want %q", uploads[0].Bucket, "airstore-ws-123")
	}
	if uploads[0].Key != ptr.S3Key {
		t.Errorf("key: got %q, want %q", uploads[0].Key, ptr.S3Key)
	}
	if !bytes.Equal(uploads[0].Body, compressed) {
		t.Error("S3 body doesn't match compressed data")
	}

	// Redis pointer should exist
	ctx := context.Background()
	gotPtr := store.GetPointer(ctx, 1, "/sources/gmail/inbox", "result-1", "strip")
	if gotPtr == nil {
		t.Fatal("pointer not found in Redis after flush")
	}
	if gotPtr.OriginalTokens != 1000 || gotPtr.CompressedTokens != 200 {
		t.Errorf("pointer tokens: got %d/%d, want 1000/200", gotPtr.OriginalTokens, gotPtr.CompressedTokens)
	}

	// Redis content cache should exist
	gotContent := store.GetContent(ctx, 1, "/sources/gmail/inbox", "result-1", "strip")
	if gotContent == nil {
		t.Fatal("content not found in Redis after flush")
	}
	if !bytes.Equal(gotContent, compressed) {
		t.Error("cached content doesn't match")
	}

	// Access event should have been recorded
	events := recorder.getEvents()
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	if events[0].Outcome != "compressed" {
		t.Errorf("event outcome: got %q", events[0].Outcome)
	}
}

func TestFlusher_SkipsS3WhenNoData(t *testing.T) {
	rdb, _ := newTestRedis(t)
	store := NewCompressedStore(rdb, Config{
		ContentCacheMaxBytes: 10 * 1024 * 1024,
		ContentCacheTTL:      5 * time.Minute,
	})
	s3mock := &mockS3{}
	recorder := &mockRecorder{}

	flusher := NewAsyncFlusher(s3mock, "airstore", store, recorder)
	defer flusher.Shutdown()

	// Enqueue item with no compressed data (e.g. passthrough or error)
	flusher.Enqueue(FlushItem{
		AccessEvent: instrumentation.AccessEvent{
			Path:    "/sources/gmail/inbox/email.txt",
			Outcome: "passthrough",
		},
	})

	time.Sleep(200 * time.Millisecond)

	// No S3 upload
	if len(s3mock.getUploads()) != 0 {
		t.Error("should not upload to S3 when no compressed data")
	}

	// Access event still recorded
	if len(recorder.getEvents()) != 1 {
		t.Error("access event should still be recorded")
	}
}

func TestFlusher_ShutdownDrains(t *testing.T) {
	rdb, _ := newTestRedis(t)
	store := NewCompressedStore(rdb, Config{
		ContentCacheMaxBytes: 10 * 1024 * 1024,
		ContentCacheTTL:      5 * time.Minute,
	})
	recorder := &mockRecorder{}
	flusher := NewAsyncFlusher(nil, "", store, recorder)

	// Enqueue several items rapidly
	for i := 0; i < 10; i++ {
		flusher.Enqueue(FlushItem{
			AccessEvent: instrumentation.AccessEvent{Path: "/test"},
		})
	}

	// Shutdown should drain all items
	flusher.Shutdown()

	events := recorder.getEvents()
	if len(events) != 10 {
		t.Errorf("expected 10 events after shutdown drain, got %d", len(events))
	}
}

func TestFlusher_CacheHitPathAfterFlush(t *testing.T) {
	// This tests the complete cache flow:
	// 1. Flusher writes pointer + content to Redis
	// 2. Subsequent GetPointer + GetContent returns the cached data
	// This is the exact flow the gateway uses on cache hit.
	rdb, _ := newTestRedis(t)
	store := NewCompressedStore(rdb, Config{
		ContentCacheMaxBytes: 10 * 1024 * 1024,
		ContentCacheTTL:      5 * time.Minute,
	})
	flusher := NewAsyncFlusher(nil, "", store, &mockRecorder{})

	compressed := []byte("compressed content")
	flusher.Enqueue(FlushItem{
		WorkspaceID: 1,
		QueryPath:   "/q",
		ResultID:    "r1",
		Strategy:    "strip",
		CompressedData: compressed,
		Pointer: &CompressedPointer{
			S3Key:            "s3key",
			OriginalTokens:   500,
			CompressedTokens: 100,
			Strategy:         "strip",
			Size:             2000,
		},
		AccessEvent: instrumentation.AccessEvent{Path: "/q/file.txt"},
	})

	time.Sleep(200 * time.Millisecond)
	flusher.Shutdown()

	ctx := context.Background()

	// Simulate the gateway cache-hit path
	ptr := store.GetPointer(ctx, 1, "/q", "r1", "strip")
	if ptr == nil {
		t.Fatal("cache hit path: pointer not found")
	}
	if ptr.OriginalTokens != 500 || ptr.CompressedTokens != 100 {
		t.Errorf("pointer tokens: %d/%d", ptr.OriginalTokens, ptr.CompressedTokens)
	}

	cached := store.GetContent(ctx, 1, "/q", "r1", "strip")
	if cached == nil {
		t.Fatal("cache hit path: content not found")
	}
	if !bytes.Equal(cached, compressed) {
		t.Error("cache hit path: content mismatch")
	}

	// Different strategy should miss
	if store.GetPointer(ctx, 1, "/q", "r1", "chain") != nil {
		t.Error("different strategy should not hit strip cache")
	}
}
