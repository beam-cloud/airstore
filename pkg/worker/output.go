package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/beam-cloud/airstore/pkg/common"
	"github.com/beam-cloud/airstore/pkg/types"
	pb "github.com/beam-cloud/airstore/proto"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

// TaskStreamOutput handles task stdout/stderr with multiple destinations.
// Zero allocation for the common case of just writing.
type TaskStreamOutput struct {
	taskID  string
	stream  string // "stdout" or "stderr"
	writers []io.Writer
	lineBuf []byte
	mu      sync.Mutex
}

// NewTaskStreamOutput creates an output handler for a task stream.
func NewTaskStreamOutput(taskID, stream string, writers ...io.Writer) *TaskStreamOutput {
	return &TaskStreamOutput{
		taskID:  taskID,
		stream:  stream,
		writers: writers,
	}
}

// Write implements io.Writer. Buffers partial lines, flushes complete ones.
func (o *TaskStreamOutput) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}

	o.mu.Lock()
	defer o.mu.Unlock()

	o.lineBuf = append(o.lineBuf, p...)

	// Process complete lines
	for {
		idx := bytes.IndexByte(o.lineBuf, '\n')
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
func (o *TaskStreamOutput) Flush() {
	o.mu.Lock()
	defer o.mu.Unlock()

	if len(o.lineBuf) > 0 {
		o.emit(string(o.lineBuf))
		o.lineBuf = nil
	}
}

func (o *TaskStreamOutput) emit(line string) {
	for _, w := range o.writers {
		w.Write([]byte(line))
	}
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

// OutputWriter intercepts structured output messages from agent stdout
// (type=output, output_append, output_done) and sends them to the gateway via gRPC.
type OutputWriter struct {
	ctx         context.Context
	client      taskOutputClient
	workspaceID uint32
	taskID      string
	runID       string
	agentID     string
	outputIDs   map[string]string // local output_id -> server-generated UUID
	events      chan outputEvent
	tracker     *taskOutputTracker
	done        chan struct{}
	closed      atomic.Bool
	closeOnce   sync.Once
	sendMu      sync.Mutex
}

type outputEvent struct {
	kind    string
	payload map[string]any
}

func newOutputWriter(
	ctx context.Context,
	client taskOutputClient,
	task types.RunExecution,
	tracker *taskOutputTracker,
) *OutputWriter {
	ids := outputIDsFromTask(task)
	writer := &OutputWriter{
		ctx:         ctx,
		client:      client,
		workspaceID: ids.workspaceID,
		taskID:      ids.taskID,
		runID:       ids.runID,
		agentID:     ids.agentID,
		outputIDs:   make(map[string]string),
		events:      make(chan outputEvent, 64),
		tracker:     tracker,
		done:        make(chan struct{}),
	}
	go writer.run()
	return writer
}

func (w *OutputWriter) Write(p []byte) (int, error) {
	if w.taskID == "" || w.client == nil {
		return len(p), nil
	}
	if w.closed.Load() {
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

	var evt outputEvent
	switch anyToTrimmedString(payload["type"]) {
	case "output":
		evt = outputEvent{kind: "output", payload: payload}
	case "output_append":
		evt = outputEvent{kind: "output_append", payload: payload}
	case "output_done":
		evt = outputEvent{kind: "output_done", payload: payload}
	default:
		return len(p), nil
	}

	w.sendMu.Lock()
	defer w.sendMu.Unlock()
	if w.closed.Load() {
		return len(p), nil
	}
	select {
	case <-w.ctx.Done():
	case w.events <- evt:
	}
	return len(p), nil
}

func (w *OutputWriter) run() {
	defer close(w.done)
	for evt := range w.events {
		switch evt.kind {
		case "output":
			w.createOutput(evt.payload)
		case "output_append":
			w.appendRows(evt.payload)
		case "output_done":
			w.finalizeOutput(evt.payload)
		}
	}
}

func (w *OutputWriter) Wait() {
	if w == nil {
		return
	}
	w.sendMu.Lock()
	w.closed.Store(true)
	w.closeOnce.Do(func() {
		close(w.events)
	})
	w.sendMu.Unlock()
	<-w.done
}

func (w *OutputWriter) createOutput(payload map[string]any) {
	localID := anyToTrimmedString(payload["output_id"])
	outputType := anyToTrimmedString(payload["output_type"])
	title := anyToTrimmedString(payload["title"])
	data := mapFromAny(payload["data"])
	metadata := mapFromAny(payload["metadata"])
	uri := anyToTrimmedString(payload["uri"])
	path := anyToTrimmedString(payload["path"])
	if path == "" {
		path = anyToTrimmedString(data["path"])
	}
	for _, key := range []string{
		types.TaskOutputMetadataArtifactKey,
		types.TaskOutputMetadataArtifactLabel,
		types.TaskOutputMetadataArtifactKind,
		types.TaskOutputMetadataArtifactRole,
	} {
		if payload[key] == nil {
			continue
		}
		if metadata == nil {
			metadata = map[string]any{}
		}
		if _, exists := metadata[key]; !exists {
			metadata[key] = payload[key]
		}
	}

	serverID, err := publishOutputCandidate(w.ctx, w.client, taskOutputIDs{
		workspaceID: w.workspaceID,
		taskID:      w.taskID,
		runID:       w.runID,
		agentID:     w.agentID,
	}, w.tracker, outputCandidate{
		LocalID:    localID,
		OutputType: outputType,
		Title:      title,
		URI:        uri,
		Path:       path,
		Data:       data,
		Metadata:   metadata,
		Role:       types.TaskOutputArtifactRolePrimary,
	})
	if err != nil {
		log.Warn().Err(err).Str("task", w.taskID).Msg("output create failed")
		return
	}
	if localID != "" && serverID != "" {
		w.outputIDs[localID] = serverID
	}
}

func (w *OutputWriter) appendRows(payload map[string]any) {
	localID := anyToTrimmedString(payload["output_id"])
	serverID := w.outputIDs[localID]
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
	serverID := w.outputIDs[localID]
	if serverID == "" {
		log.Warn().Str("output_id", localID).Msg("output_done for unknown output_id")
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
