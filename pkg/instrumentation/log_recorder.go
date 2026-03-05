package instrumentation

import (
	"context"

	"github.com/rs/zerolog/log"
)

// LogRecorder writes product analytics events as structured zerolog JSON lines.
// Fluentbit (or similar) scrapes these from gateway stdout using the "event_type" field.
type LogRecorder struct{}

func NewLogRecorder() *LogRecorder { return &LogRecorder{} }

func (r *LogRecorder) Record(_ context.Context, event Event) {
	e := log.Info().
		Str("event_type", event.Type).
		Time("event_ts", event.Timestamp)

	for k, v := range event.Properties {
		switch val := v.(type) {
		case string:
			e = e.Str(k, val)
		case int:
			e = e.Int(k, val)
		case int64:
			e = e.Int64(k, val)
		case uint:
			e = e.Uint(k, val)
		case float64:
			e = e.Float64(k, val)
		case bool:
			e = e.Bool(k, val)
		default:
			e = e.Interface(k, val)
		}
	}

	e.Msg("event")
}
