package instrumentation

import (
	"context"
	"time"
)

// Event is a generic product analytics event (tool usage, workspace growth, etc.).
type Event struct {
	Type       string
	Timestamp  time.Time
	Properties map[string]any
}

// EventRecorder records product analytics events. Implementations must be safe
// for concurrent use.
type EventRecorder interface {
	Record(ctx context.Context, event Event)
}

// NewEvent creates an Event with the given type, properties, and current timestamp.
func NewEvent(eventType string, props map[string]any) Event {
	return Event{
		Type:       eventType,
		Timestamp:  time.Now(),
		Properties: props,
	}
}
