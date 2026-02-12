package instrumentation

import (
	"context"
	"fmt"
	"sync"

	"github.com/beam-cloud/airstore/pkg/common"
	"github.com/rs/zerolog/log"
)

const (
	// defaultBufferSize is the channel capacity for async event delivery.
	defaultBufferSize = 4096
)

// S2Recorder writes access events to an S2 stream asynchronously.
// Record() is non-blocking; events are buffered and flushed by a background goroutine.
type S2Recorder struct {
	s2   *common.S2Client
	ch   chan AccessEvent
	done chan struct{}
	wg   sync.WaitGroup
}

// NewS2Recorder creates and starts an S2-backed access recorder.
// Call Flush() on shutdown to drain buffered events.
func NewS2Recorder(s2 *common.S2Client) *S2Recorder {
	r := &S2Recorder{
		s2:   s2,
		ch:   make(chan AccessEvent, defaultBufferSize),
		done: make(chan struct{}),
	}
	r.wg.Add(1)
	go r.loop()
	return r
}

// Record enqueues an event for async delivery. Non-blocking: if the buffer
// is full the event is dropped (logged as a warning).
func (r *S2Recorder) Record(_ context.Context, event AccessEvent) error {
	select {
	case r.ch <- event:
		return nil
	default:
		log.Warn().Str("path", event.Path).Msg("access recorder buffer full, dropping event")
		return nil
	}
}

// Flush signals the background goroutine to stop and waits for it to
// drain remaining events.
func (r *S2Recorder) Flush() error {
	close(r.done)
	r.wg.Wait()
	return nil
}

// loop is the background goroutine that drains the channel and writes to S2.
func (r *S2Recorder) loop() {
	defer r.wg.Done()
	for {
		select {
		case event := <-r.ch:
			r.send(event)
		case <-r.done:
			// Drain remaining events
			for {
				select {
				case event := <-r.ch:
					r.send(event)
				default:
					return
				}
			}
		}
	}
}

// AccessStreamName returns the S2 stream name for an access session.
func AccessStreamName(sessionID string) string {
	return fmt.Sprintf("access.%s.events", sessionID)
}

func (r *S2Recorder) send(event AccessEvent) {
	if r.s2 == nil || !r.s2.Enabled() {
		return
	}
	stream := AccessStreamName(event.SessionID)
	if err := r.s2.Append(context.Background(), stream, event); err != nil {
		log.Warn().Err(err).Str("stream", stream).Msg("failed to append access event to S2")
	}
}
