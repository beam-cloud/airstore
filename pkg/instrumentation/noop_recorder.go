package instrumentation

import "context"

// NoopRecorder discards all events. Used when instrumentation is disabled or in tests.
type NoopRecorder struct{}

func NewNoopRecorder() *NoopRecorder { return &NoopRecorder{} }

func (n *NoopRecorder) Record(_ context.Context, _ AccessEvent) error { return nil }
func (n *NoopRecorder) Flush() error                                  { return nil }
