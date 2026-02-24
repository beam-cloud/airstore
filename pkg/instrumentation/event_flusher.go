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
	s2        *common.S2Client
	ch        chan AccessEvent
	done      chan struct{}
	wg        sync.WaitGroup
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
const accessWorkspaceSeparator = "."

// AccessStreamName returns the S2 stream name for an access session.
// Legacy format: access.{session_id}.events
func AccessStreamName(sessionID string) string {
	return fmt.Sprintf("%s%s%s", accessStreamPrefix, sessionID, accessStreamSuffix)
}

// AccessWorkspaceStreamName returns a workspace-scoped access stream name.
// Format: access.{workspace_id}.{session_id}.events
//
// If workspaceID is empty, it falls back to the legacy AccessStreamName format.
func AccessWorkspaceStreamName(workspaceID, sessionID string) string {
	if workspaceID == "" {
		return AccessStreamName(sessionID)
	}
	if sessionID == "" {
		sessionID = workspaceID
	}
	return fmt.Sprintf(
		"%s%s%s%s%s",
		accessStreamPrefix,
		workspaceID,
		accessWorkspaceSeparator,
		sessionID,
		accessStreamSuffix,
	)
}

// AccessStreamPrefix returns the prefix used for all access log streams.
// This is the global/legacy prefix ("access.").
func AccessStreamPrefix() string {
	return accessStreamPrefix
}

// AccessWorkspaceStreamPrefix returns the stream-name prefix for a workspace.
// Format: access.{workspace_id}.
//
// If workspaceID is empty, it falls back to AccessStreamPrefix().
func AccessWorkspaceStreamPrefix(workspaceID string) string {
	if workspaceID == "" {
		return AccessStreamPrefix()
	}
	return fmt.Sprintf("%s%s%s", accessStreamPrefix, workspaceID, accessWorkspaceSeparator)
}

// SessionIDFromStreamName extracts the session ID from a stream name
// of the form "access.{session_id}.events".
func SessionIDFromStreamName(name string) string {
	return extractSessionFromAccessStream(name, accessStreamPrefix)
}

// SessionIDFromWorkspaceStreamName extracts a session ID for a specific workspace
// from stream names of the form "access.{workspace_id}.{session_id}.events".
// It returns an empty string when the stream does not belong to that workspace.
func SessionIDFromWorkspaceStreamName(name, workspaceID string) string {
	if workspaceID == "" {
		return SessionIDFromStreamName(name)
	}
	return extractSessionFromAccessStream(name, AccessWorkspaceStreamPrefix(workspaceID))
}

func (f *EventFlusher) send(event AccessEvent) {
	if f.s2 == nil || !f.s2.Enabled() {
		return
	}
	stream := AccessWorkspaceStreamName(event.WorkspaceID, event.SessionID)
	if err := f.s2.Append(context.Background(), stream, event); err != nil {
		log.Warn().Err(err).Str("stream", stream).Msg("failed to append access event to S2")
	}
}

func extractSessionFromAccessStream(name, prefix string) string {
	if !strings.HasPrefix(name, prefix) || !strings.HasSuffix(name, accessStreamSuffix) {
		return ""
	}
	start := len(prefix)
	end := len(name) - len(accessStreamSuffix)

	// Guard malformed or legacy names that don't contain a session segment
	// between prefix and suffix, e.g. "access.{workspace}.events".
	if start >= end {
		return ""
	}

	return name[start:end]
}
