package worker

import (
	"context"
	"io"
	"os"
	"strings"
	"sync"

	"github.com/beam-cloud/airstore/pkg/streams"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

// TaskOutput handles task stdout/stderr with multiple destinations.
// Zero allocation for the common case of just writing.
type TaskOutput struct {
	taskID  string
	stream  string // "stdout" or "stderr"
	writers []io.Writer
	lineBuf []byte
	mu      sync.Mutex
}

// NewTaskOutput creates an output handler for a task stream.
func NewTaskOutput(taskID, stream string, writers ...io.Writer) *TaskOutput {
	return &TaskOutput{
		taskID:  taskID,
		stream:  stream,
		writers: writers,
	}
}

// Write implements io.Writer. Buffers partial lines, flushes complete ones.
func (o *TaskOutput) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}

	o.mu.Lock()
	defer o.mu.Unlock()

	o.lineBuf = append(o.lineBuf, p...)

	// Process complete lines
	for {
		idx := indexByte(o.lineBuf, '\n')
		if idx < 0 {
			break
		}

		line := string(o.lineBuf[:idx])
		o.lineBuf = o.lineBuf[idx+1:]

		if line != "" {
			o.emit(line)
		}
	}

	return len(p), nil
}

// Flush writes any remaining buffered content.
func (o *TaskOutput) Flush() {
	o.mu.Lock()
	defer o.mu.Unlock()

	if len(o.lineBuf) > 0 {
		o.emit(string(o.lineBuf))
		o.lineBuf = nil
	}
}

func (o *TaskOutput) emit(line string) {
	for _, w := range o.writers {
		w.Write([]byte(line))
	}
}

func indexByte(b []byte, c byte) int {
	for i, v := range b {
		if v == c {
			return i
		}
	}
	return -1
}

// --- Writers ---

// ConsoleWriter writes task output to the worker's console with context.
type ConsoleWriter struct {
	taskID string
	stream string
	logger zerolog.Logger
}

// NewConsoleWriter creates a writer that logs to the worker console.
func NewConsoleWriter(taskID, stream string) *ConsoleWriter {
	return &ConsoleWriter{
		taskID: taskID,
		stream: stream,
		logger: log.With().Str("task", taskID).Str("stream", stream).Logger(),
	}
}

func (w *ConsoleWriter) Write(p []byte) (int, error) {
	line := strings.TrimSpace(string(p))
	if line != "" {
		w.logger.Info().Msg(line)
	}
	return len(p), nil
}

// S2Writer writes task output to S2 streams.
type S2Writer struct {
	client *streams.S2Client
	taskID string
	stream string
	ctx    context.Context
}

// NewS2Writer creates a writer that appends to S2.
func NewS2Writer(ctx context.Context, client *streams.S2Client, taskID, stream string) *S2Writer {
	return &S2Writer{
		client: client,
		taskID: taskID,
		stream: stream,
		ctx:    ctx,
	}
}

func (w *S2Writer) Write(p []byte) (int, error) {
	line := string(p)
	if line != "" && w.client != nil && w.client.Enabled() {
		if err := w.client.AppendLog(w.ctx, w.taskID, w.stream, line); err != nil {
			log.Warn().Err(err).Str("task", w.taskID).Msg("s2 write failed")
		}
	}
	return len(p), nil
}

// FileWriter writes to a file (useful for debugging).
type FileWriter struct {
	file *os.File
}

// NewFileWriter creates a writer that appends to a file.
func NewFileWriter(path string) (*FileWriter, error) {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}
	return &FileWriter{file: f}, nil
}

func (w *FileWriter) Write(p []byte) (int, error) {
	return w.file.Write(append(p, '\n'))
}

func (w *FileWriter) Close() error {
	return w.file.Close()
}

// --- Factory ---

// OutputConfig configures task output destinations.
type OutputConfig struct {
	TaskID   string
	S2Client *streams.S2Client
	Console  bool // Write to worker stdout
}

// NewOutputPair creates stdout and stderr writers for a task.
func NewOutputPair(ctx context.Context, cfg OutputConfig) (stdout, stderr *TaskOutput) {
	var stdoutWriters, stderrWriters []io.Writer

	// S2 streams
	if cfg.S2Client != nil && cfg.S2Client.Enabled() {
		stdoutWriters = append(stdoutWriters, NewS2Writer(ctx, cfg.S2Client, cfg.TaskID, "stdout"))
		stderrWriters = append(stderrWriters, NewS2Writer(ctx, cfg.S2Client, cfg.TaskID, "stderr"))
	}

	// Console output
	if cfg.Console {
		stdoutWriters = append(stdoutWriters, NewConsoleWriter(cfg.TaskID, "stdout"))
		stderrWriters = append(stderrWriters, NewConsoleWriter(cfg.TaskID, "stderr"))
	}

	return NewTaskOutput(cfg.TaskID, "stdout", stdoutWriters...),
		NewTaskOutput(cfg.TaskID, "stderr", stderrWriters...)
}
