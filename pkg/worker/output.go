package worker

import (
	"bytes"
	"context"
	"strings"
	"sync"

	"github.com/beam-cloud/airstore/pkg/streams"
	"github.com/rs/zerolog/log"
)

// LogPublisher is an interface for publishing task logs
type LogPublisher interface {
	PublishLog(ctx context.Context, taskID string, stream string, data string) error
}

// S2LogPublisher publishes logs to S2 streams
type S2LogPublisher struct {
	client *streams.S2Client
}

// NewS2LogPublisher creates a log publisher that writes to S2
func NewS2LogPublisher(client *streams.S2Client) *S2LogPublisher {
	return &S2LogPublisher{client: client}
}

// PublishLog implements LogPublisher
func (p *S2LogPublisher) PublishLog(ctx context.Context, taskID, stream, data string) error {
	return p.client.AppendLog(ctx, taskID, stream, data)
}

// PublishStatus publishes a status event to S2
func (p *S2LogPublisher) PublishStatus(ctx context.Context, taskID, status string, exitCode *int, errorMsg string) error {
	return p.client.AppendStatus(ctx, taskID, status, exitCode, errorMsg)
}

// SandboxOutput captures and manages sandbox stdout/stderr output.
// It implements the runtime.OutputWriter interface.
type SandboxOutput struct {
	sandboxID string
	buffer    bytes.Buffer
	mu        sync.Mutex
	maxSize   int // Maximum buffer size (0 = unlimited)
}

// NewSandboxOutput creates a new SandboxOutput for the given sandbox ID.
// maxSize limits the buffer size (0 = unlimited, recommended: 1MB = 1<<20)
func NewSandboxOutput(sandboxID string, maxSize int) *SandboxOutput {
	return &SandboxOutput{
		sandboxID: sandboxID,
		maxSize:   maxSize,
	}
}

// Write implements io.Writer and runtime.OutputWriter
func (o *SandboxOutput) Write(p []byte) (n int, err error) {
	o.mu.Lock()
	defer o.mu.Unlock()

	// Enforce max size by discarding oldest data if needed
	if o.maxSize > 0 && o.buffer.Len()+len(p) > o.maxSize {
		// Keep the most recent data
		overflow := o.buffer.Len() + len(p) - o.maxSize
		if overflow > o.buffer.Len() {
			o.buffer.Reset()
			// Truncate p to fit maxSize
			if len(p) > o.maxSize {
				p = p[len(p)-o.maxSize:]
			}
		} else {
			// Discard oldest bytes
			o.buffer.Next(overflow)
		}
	}

	return o.buffer.Write(p)
}

// String returns all captured output as a string
func (o *SandboxOutput) String() string {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.buffer.String()
}

// Bytes returns all captured output as bytes
func (o *SandboxOutput) Bytes() []byte {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.buffer.Bytes()
}

// Lines returns the output split into lines
func (o *SandboxOutput) Lines() []string {
	o.mu.Lock()
	defer o.mu.Unlock()

	s := o.buffer.String()
	if s == "" {
		return nil
	}

	lines := strings.Split(strings.TrimSuffix(s, "\n"), "\n")
	return lines
}

// Len returns the current buffer length
func (o *SandboxOutput) Len() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.buffer.Len()
}

// Reset clears the buffer
func (o *SandboxOutput) Reset() {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.buffer.Reset()
}

// Log writes the captured output to the logger with the given message.
// Each line is logged separately for readability.
func (o *SandboxOutput) Log(message string) {
	lines := o.Lines()
	if len(lines) == 0 {
		return
	}

	// Log each line of output
	for _, line := range lines {
		if line == "" {
			continue
		}
		log.Info().
			Str("sandbox_id", o.sandboxID).
			Str("output", line).
			Msg(message)
	}
}

// LogSummary logs a summary of the output, truncating if it exceeds maxLines
func (o *SandboxOutput) LogSummary(maxLines int) {
	lines := o.Lines()
	if len(lines) == 0 {
		return
	}

	// Determine which lines to show
	total := len(lines)
	showAll := total <= maxLines
	headCount, tailCount := maxLines/2, maxLines/2

	for i, line := range lines {
		if line == "" {
			continue
		}

		if showAll || i < headCount || i >= total-tailCount {
			log.Info().
				Str("sandbox_id", o.sandboxID).
				Str("output", line).
				Msg("sandbox output")
		} else if i == headCount {
			log.Info().
				Str("sandbox_id", o.sandboxID).
				Int("hidden", total-maxLines).
				Msg("... truncated ...")
		}
	}
}

// StreamingOutput wraps SandboxOutput and also publishes logs to Redis for streaming
type StreamingOutput struct {
	*SandboxOutput
	taskID       string
	logPublisher LogPublisher
	ctx          context.Context
	lineBuf      bytes.Buffer
}

// NewStreamingOutput creates a new StreamingOutput that captures output and streams to Redis
func NewStreamingOutput(ctx context.Context, taskID string, logPublisher LogPublisher, maxSize int) *StreamingOutput {
	return &StreamingOutput{
		SandboxOutput: NewSandboxOutput(taskID, maxSize),
		taskID:        taskID,
		logPublisher:  logPublisher,
		ctx:           ctx,
	}
}

// Write implements io.Writer - captures output and publishes complete lines to Redis
func (o *StreamingOutput) Write(p []byte) (n int, err error) {
	// Write to underlying buffer
	n, err = o.SandboxOutput.Write(p)
	if err != nil {
		return n, err
	}

	// Buffer and publish complete lines
	if o.logPublisher != nil {
		o.lineBuf.Write(p)
		for {
			line, readErr := o.lineBuf.ReadString('\n')
			if readErr != nil {
				// Put incomplete line back in buffer
				o.lineBuf.WriteString(line)
				break
			}
			// Publish complete line (strip trailing newline)
			line = strings.TrimSuffix(line, "\n")
			if line != "" {
				if pubErr := o.logPublisher.PublishLog(o.ctx, o.taskID, "stdout", line); pubErr != nil {
					log.Warn().Err(pubErr).Str("task_id", o.taskID).Msg("failed to publish log")
				}
			}
		}
	}

	return n, nil
}

// Flush publishes any remaining buffered output
func (o *StreamingOutput) Flush() {
	if o.logPublisher != nil && o.lineBuf.Len() > 0 {
		remaining := o.lineBuf.String()
		if remaining != "" {
			_ = o.logPublisher.PublishLog(o.ctx, o.taskID, "stdout", remaining)
		}
		o.lineBuf.Reset()
	}
}
