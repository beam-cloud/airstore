package worker

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"strings"
	"sync"

	"github.com/beam-cloud/airstore/pkg/common"
	gatewayclient "github.com/beam-cloud/airstore/pkg/gateway/client"
	"github.com/beam-cloud/airstore/pkg/types"
	pb "github.com/beam-cloud/airstore/proto"
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
	client *common.S2Client
	taskID string
	stream string
	ctx    context.Context
}

// NewS2Writer creates a writer that appends to S2.
func NewS2Writer(ctx context.Context, client *common.S2Client, taskID, stream string) *S2Writer {
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

// OutputWriter intercepts structured output messages from agent stdout
// (type=output, output_append, output_done) and sends them to the gateway via gRPC.
type OutputWriter struct {
	ctx         context.Context
	client      *gatewayclient.GatewayClient
	workspaceID uint32
	taskID      string
	runID       string
	outputIDs   map[string]string // local output_id -> server-generated UUID
	mu          sync.Mutex
}

func NewOutputWriter(ctx context.Context, client *gatewayclient.GatewayClient, task types.RunExecution) *OutputWriter {
	var taskID, runID string
	if task.ExecutionPolicy != nil {
		taskID = anyToTrimmedString(task.ExecutionPolicy[types.AgentExecutionMetaKeyOriginTaskID])
		runID = anyToTrimmedString(task.ExecutionPolicy[types.AgentExecutionMetaKeyRunID])
	}
	return &OutputWriter{
		ctx:         ctx,
		client:      client,
		workspaceID: uint32(task.WorkspaceId),
		taskID:      taskID,
		runID:       runID,
		outputIDs:   make(map[string]string),
	}
}

func (w *OutputWriter) Write(p []byte) (int, error) {
	if w.taskID == "" || w.client == nil {
		return len(p), nil
	}
	line := strings.TrimSpace(string(p))
	if line == "" || line[0] != '{' {
		return len(p), nil
	}
	var payload map[string]any
	if json.Unmarshal([]byte(line), &payload) != nil {
		return len(p), nil
	}
	switch anyToTrimmedString(payload["type"]) {
	case "output":
		go w.createOutput(payload)
	case "output_append":
		go w.appendRows(payload)
	case "output_done":
		go w.finalizeOutput(payload)
	}
	return len(p), nil
}

func (w *OutputWriter) createOutput(payload map[string]any) {
	localID := anyToTrimmedString(payload["output_id"])
	req := &pb.CreateTaskOutputRequest{
		WorkspaceId: w.workspaceID,
		TaskId:      w.taskID,
		RunId:       w.runID,
		OutputType:  anyToTrimmedString(payload["output_type"]),
		Title:       anyToTrimmedString(payload["title"]),
	}
	if v := payload["schema"]; v != nil {
		if b, err := json.Marshal(v); err == nil {
			req.SchemaJson = string(b)
		}
	}
	if v := payload["data"]; v != nil {
		if b, err := json.Marshal(v); err == nil {
			req.DataJson = string(b)
		}
	}
	if v := payload["metadata"]; v != nil {
		if b, err := json.Marshal(v); err == nil {
			req.MetadataJson = string(b)
		}
	}

	serverID, err := w.client.CreateTaskOutput(w.ctx, req)
	if err != nil {
		log.Warn().Err(err).Str("task", w.taskID).Msg("output create failed")
		return
	}
	if localID != "" && serverID != "" {
		w.mu.Lock()
		w.outputIDs[localID] = serverID
		w.mu.Unlock()
	}
}

func (w *OutputWriter) appendRows(payload map[string]any) {
	localID := anyToTrimmedString(payload["output_id"])
	w.mu.Lock()
	serverID := w.outputIDs[localID]
	w.mu.Unlock()
	if serverID == "" {
		log.Warn().Str("output_id", localID).Msg("output_append for unknown output_id")
		return
	}
	rowsJSON, _ := json.Marshal(payload["rows"])
	if err := w.client.AppendTaskOutputRows(w.ctx, &pb.AppendTaskOutputRowsRequest{
		WorkspaceId: w.workspaceID,
		OutputId:    serverID,
		RowsJson:    string(rowsJSON),
	}); err != nil {
		log.Warn().Err(err).Str("output", serverID).Msg("output append failed")
	}
}

func (w *OutputWriter) finalizeOutput(payload map[string]any) {
	localID := anyToTrimmedString(payload["output_id"])
	w.mu.Lock()
	serverID := w.outputIDs[localID]
	w.mu.Unlock()
	if serverID == "" {
		return
	}
	summary := anyToTrimmedString(payload["summary"])
	if summary == "" {
		return
	}
	if err := w.client.FinalizeTaskOutput(w.ctx, &pb.FinalizeTaskOutputRequest{
		WorkspaceId: w.workspaceID,
		OutputId:    serverID,
		Summary:     summary,
	}); err != nil {
		log.Warn().Err(err).Str("output", serverID).Msg("output finalize failed")
	}
}

// --- Factory ---

// OutputConfig configures task output destinations.
type OutputConfig struct {
	TaskID   string
	S2Client *common.S2Client
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
