package instrumentation

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/beam-cloud/airstore/pkg/common"
	"github.com/rs/zerolog/log"
)

const (
	// defaultBufferSize is the channel capacity for async event delivery.
	defaultBufferSize = 4096
)

// EventFlusher writes access events to an S2 stream asynchronously.
// Record() is non-blocking; events are buffered and flushed by a background goroutine.
type EventFlusher struct {
	s2       *common.S2Client
	ch       chan AccessEvent
	done     chan struct{}
	wg       sync.WaitGroup
	closeOnce sync.Once
}

// NewEventFlusher creates and starts an S2-backed access recorder.
// Call Flush() on shutdown to drain buffered events.
func NewEventFlusher(s2 *common.S2Client) *EventFlusher {
	f := &EventFlusher{
		s2:   s2,
		ch:   make(chan AccessEvent, defaultBufferSize),
		done: make(chan struct{}),
	}
	f.wg.Add(1)
	go f.loop()
	return f
}

// Record enqueues an event for async delivery. Non-blocking: if the buffer
// is full the event is dropped (logged as a warning).
func (f *EventFlusher) Record(_ context.Context, event AccessEvent) error {
	select {
	case f.ch <- event:
		return nil
	default:
		log.Warn().Str("path", event.Path).Msg("event flusher buffer full, dropping event")
		return nil
	}
}

// Flush signals the background goroutine to stop and waits for it to
// drain remaining events. Safe to call multiple times.
func (f *EventFlusher) Flush() error {
	f.closeOnce.Do(func() { close(f.done) })
	f.wg.Wait()
	return nil
}

// loop is the background goroutine that drains the channel and writes to S2.
func (f *EventFlusher) loop() {
	defer f.wg.Done()
	for {
		select {
		case event := <-f.ch:
			f.send(event)
		case <-f.done:
			// Drain remaining events
			for {
				select {
				case event := <-f.ch:
					f.send(event)
				default:
					return
				}
			}
		}
	}
}

const accessStreamPrefix = "access."
const accessStreamSuffix = ".events"

// AccessStreamName returns the S2 stream name for an access session.
func AccessStreamName(sessionID string) string {
	return fmt.Sprintf("%s%s%s", accessStreamPrefix, sessionID, accessStreamSuffix)
}

// AccessStreamPrefix returns the prefix used for all access log streams.
func AccessStreamPrefix() string {
	return accessStreamPrefix
}

// SessionIDFromStreamName extracts the session ID from a stream name
// of the form "access.{session_id}.events".
func SessionIDFromStreamName(name string) string {
	if !strings.HasPrefix(name, accessStreamPrefix) || !strings.HasSuffix(name, accessStreamSuffix) {
		return ""
	}
	return name[len(accessStreamPrefix) : len(name)-len(accessStreamSuffix)]
}

func (f *EventFlusher) send(event AccessEvent) {
	if f.s2 == nil || !f.s2.Enabled() {
		return
	}
	stream := AccessStreamName(event.SessionID)
	if err := f.s2.Append(context.Background(), stream, event); err != nil {
		log.Warn().Err(err).Str("stream", stream).Msg("failed to append access event to S2")
	}
}
