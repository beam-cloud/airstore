package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os"
	"strings"
	"sync"
	"sync/atomic"

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

type taskOutputClient interface {
	CreateTaskOutput(ctx context.Context, req *pb.CreateTaskOutputRequest) (string, error)
	AppendTaskOutputRows(ctx context.Context, req *pb.AppendTaskOutputRowsRequest) error
	FinalizeTaskOutput(ctx context.Context, req *pb.FinalizeTaskOutputRequest) error
	UpdateTaskOutputStatus(ctx context.Context, req *pb.UpdateTaskOutputStatusRequest) error
}

type outputEvent struct {
	kind    string
	payload map[string]any
}

type taskOutputTracker struct {
	created atomic.Bool
	mu      sync.Mutex
	seen    map[string]struct{}
	primary map[string]struct{}
	outputByIdentity map[string]string // identity key -> server output ID
}

func (t *taskOutputTracker) MarkCreated() {
	if t != nil {
		t.created.Store(true)
	}
}

func (t *taskOutputTracker) HasCreated() bool {
	return t != nil && t.created.Load()
}

func (t *taskOutputTracker) HasEquivalent(candidate outputCandidate) bool {
	if t == nil {
		return false
	}
	identity := candidate.identityKey()
	key := candidate.artifactKey()

	t.mu.Lock()
	defer t.mu.Unlock()
	if identity != "" {
		if _, ok := t.seen[identity]; ok {
			if _, hasServerID := t.outputByIdentity[identity]; hasServerID {
				return false
			}
			return true
		}
	}
	if key != "" && candidate.isPrimaryDeliverable() {
		_, ok := t.primary[key]
		return ok
	}
	return false
}

func (t *taskOutputTracker) Remember(candidate outputCandidate) {
	t.RememberWithID(candidate, "")
}

func (t *taskOutputTracker) RememberWithID(candidate outputCandidate, serverID string) {
	if t == nil {
		return
	}
	t.created.Store(true)
	identity := candidate.identityKey()
	key := candidate.artifactKey()

	t.mu.Lock()
	defer t.mu.Unlock()
	if t.seen == nil {
		t.seen = make(map[string]struct{})
	}
	if t.primary == nil {
		t.primary = make(map[string]struct{})
	}
	if t.outputByIdentity == nil {
		t.outputByIdentity = make(map[string]string)
	}
	if identity != "" {
		t.seen[identity] = struct{}{}
		if serverID != "" {
			t.outputByIdentity[identity] = serverID
		}
	}
	if key != "" && candidate.isPrimaryDeliverable() {
		t.primary[key] = struct{}{}
	}
}

// TrackedOutputSummaries returns a list of {serverID, identityKey} pairs
// for all outputs that were published and have a server-generated ID.
func (t *taskOutputTracker) TrackedOutputSummaries() []trackedOutputSummary {
	if t == nil {
		return nil
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	var out []trackedOutputSummary
	for identity, serverID := range t.outputByIdentity {
		if serverID != "" {
			out = append(out, trackedOutputSummary{OutputID: serverID, Identity: identity})
		}
	}
	return out
}

type trackedOutputSummary struct {
	OutputID string
	Identity string
}

// PredecessorID returns the server output ID of a previously tracked output
// matching the same identity key as candidate, if any.
func (t *taskOutputTracker) PredecessorID(candidate outputCandidate) string {
	if t == nil {
		return ""
	}
	identity := candidate.identityKey()
	if identity == "" {
		return ""
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.outputByIdentity[identity]
}

func NewOutputWriter(ctx context.Context, client *gatewayclient.GatewayClient, task types.RunExecution) *OutputWriter {
	return newOutputWriter(ctx, client, task, nil)
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

// extractAssistantText scans raw stream-json output (from claude --print
// --output-format stream-json), pulls out assistant text blocks, and returns
// the last `limit` characters of concatenated text.  This is used to build
// context for approval summary extraction without sending tool call / tool
// result noise to the summariser.
func extractAssistantText(raw []byte, limit int) string {
	var texts []string
	totalLen := 0

	for _, line := range bytes.Split(raw, []byte("\n")) {
		line = bytes.TrimSpace(line)
		if len(line) == 0 || line[0] != '{' {
			continue
		}

		var envelope struct {
			Type    string `json:"type"`
			Message *struct {
				Content []struct {
					Type string `json:"type"`
					Text string `json:"text"`
				} `json:"content"`
			} `json:"message"`
			// "result" type messages carry a top-level result string.
			Result  string `json:"result"`
			IsError bool   `json:"is_error"`
		}
		if err := json.Unmarshal(line, &envelope); err != nil {
			continue
		}

		switch envelope.Type {
		case "assistant":
			if envelope.Message == nil {
				continue
			}
			for _, block := range envelope.Message.Content {
				if block.Type == "text" && block.Text != "" {
					texts = append(texts, block.Text)
					totalLen += len(block.Text)
				}
			}
		case "result":
			if !envelope.IsError && envelope.Result != "" {
				texts = append(texts, envelope.Result)
				totalLen += len(envelope.Result)
			}
		}
	}

	if totalLen == 0 {
		return ""
	}

	// Concatenate all texts with double-newline separators, then take the
	// last `limit` characters so the most recent content is preserved.
	var buf bytes.Buffer
	for i, t := range texts {
		if i > 0 {
			buf.WriteString("\n\n")
		}
		buf.WriteString(t)
	}
	s := buf.String()
	if limit > 0 && len(s) > limit {
		s = s[len(s)-limit:]
		// The byte slice may start mid-rune; advance past any
		// continuation bytes (10xxxxxx) to the next valid rune boundary.
		for len(s) > 0 && s[0]&0xC0 == 0x80 {
			s = s[1:]
		}
	}
	return s
}
