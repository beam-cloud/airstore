package compression

import (
	"bytes"
	"context"
	"sync"

	"github.com/beam-cloud/airstore/pkg/instrumentation"
	"github.com/beam-cloud/airstore/pkg/types"
	"github.com/rs/zerolog/log"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

const flushBufferSize = 4096

// FlushItem is the unit of work dispatched to the async flusher.
type FlushItem struct {
	// Compressed content to persist (nil if compression failed / was skipped)
	CompressedData []byte
	S3Key          string
	WorkspaceID    uint
	WorkspaceExtID string
	QueryPath      string
	ResultID       string
	Strategy       string             // compression strategy (used as part of cache key)
	Pointer        *CompressedPointer // nil if nothing to store

	// Always present
	AccessEvent instrumentation.AccessEvent
}

// S3Uploader is the minimal S3 interface needed by the flusher.
type S3Uploader interface {
	PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error)
}

// AsyncFlusher runs a background goroutine that writes compressed content
// to S3, updates Redis pointers, and emits S2 access events — all
// without blocking the filesystem read path.
type AsyncFlusher struct {
	ch           chan FlushItem
	done         chan struct{}
	wg           sync.WaitGroup
	s3           S3Uploader
	bucketPrefix string // combined with WorkspaceExtID per item
	store        *CompressedStore
	recorder     instrumentation.AccessRecorder
}

// NewAsyncFlusher creates and starts the background flusher goroutine.
// bucketPrefix is the S3 bucket prefix (e.g. "airstore"); the per-workspace
// bucket is computed as "{prefix}-{workspaceExtId}".
func NewAsyncFlusher(
	s3Client S3Uploader,
	bucketPrefix string,
	store *CompressedStore,
	recorder instrumentation.AccessRecorder,
) *AsyncFlusher {
	f := &AsyncFlusher{
		ch:           make(chan FlushItem, flushBufferSize),
		done:         make(chan struct{}),
		s3:           s3Client,
		bucketPrefix: bucketPrefix,
		store:        store,
		recorder:     recorder,
	}
	f.wg.Add(1)
	go f.loop()
	return f
}

// Enqueue dispatches an item to the background flusher. Non-blocking.
// If the buffer is full the item is dropped (access event is lost).
func (f *AsyncFlusher) Enqueue(item FlushItem) {
	select {
	case f.ch <- item:
	default:
		log.Warn().Str("path", item.AccessEvent.Path).Msg("async flusher buffer full, dropping item")
	}
}

// Shutdown signals the flusher to stop and waits for it to drain.
func (f *AsyncFlusher) Shutdown() {
	close(f.done)
	f.wg.Wait()
}

func (f *AsyncFlusher) loop() {
	defer f.wg.Done()
	for {
		select {
		case item := <-f.ch:
			f.process(item)
		case <-f.done:
			// Drain remaining items
			for {
				select {
				case item := <-f.ch:
					f.process(item)
				default:
					return
				}
			}
		}
	}
}

func (f *AsyncFlusher) process(item FlushItem) {
	ctx := context.Background()

	// 1. Write compressed content to S3 (if we have data to store)
	if item.CompressedData != nil && item.S3Key != "" && f.s3 != nil && f.bucketPrefix != "" && item.WorkspaceExtID != "" {
		bucket := types.WorkspaceBucketName(f.bucketPrefix, item.WorkspaceExtID)
		_, err := f.s3.PutObject(ctx, &s3.PutObjectInput{
			Bucket:      aws.String(bucket),
			Key:         aws.String(item.S3Key),
			Body:        bytes.NewReader(item.CompressedData),
			ContentType: aws.String("application/octet-stream"),
		})
		if err != nil {
			log.Warn().Err(err).Str("bucket", bucket).Str("key", item.S3Key).Msg("failed to write compressed content to S3")
		}
	}

	// 2. Write Redis pointer (if compression succeeded)
	if item.Pointer != nil && f.store != nil {
		if err := f.store.SetPointer(ctx, item.WorkspaceID, item.QueryPath, item.ResultID, item.Strategy, item.Pointer); err != nil {
			log.Warn().Err(err).Str("queryPath", item.QueryPath).Msg("failed to set compressed pointer")
		}
		// Also cache the content in Redis (budget-gated)
		if item.CompressedData != nil {
			if err := f.store.SetContent(ctx, item.WorkspaceID, item.QueryPath, item.ResultID, item.Strategy, item.CompressedData); err != nil {
				log.Warn().Err(err).Str("queryPath", item.QueryPath).Msg("failed to cache compressed content")
			}
		}
	}

	// 3. Always emit the access event
	if f.recorder != nil {
		if err := f.recorder.Record(ctx, item.AccessEvent); err != nil {
			log.Warn().Err(err).Str("path", item.AccessEvent.Path).Msg("failed to record access event")
		}
	}
}
