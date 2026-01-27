package worker

import (
	"bytes"
	"strings"
	"sync"

	"github.com/rs/zerolog/log"
)

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
