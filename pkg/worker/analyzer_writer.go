package worker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"unicode/utf8"

	"github.com/google/uuid"

	gatewayclient "github.com/beam-cloud/airstore/pkg/gateway/client"
	"github.com/beam-cloud/airstore/pkg/types"
	agentsignal "github.com/beam-cloud/airstore/pkg/worker/agentsignal/baml_client"
	signaltypes "github.com/beam-cloud/airstore/pkg/worker/agentsignal/baml_client/types"
	"github.com/rs/zerolog/log"
)

const (
	keyContent     = "content"
	keySummary     = "summary"
	keyPath        = "path"
	keyURI         = "uri"
	keyTags        = "tags"
	keyDeeplink    = "deeplink"
	keySourceTitle = "source_title"
	keySourceURL   = "source_url"

	// Internal bookkeeping keys — prefixed with _ so the mapper skips them.
	keyTool            = "_tool"
	keySource          = "_source"
	keySourcePrompt    = "_source_prompt"
	keySourceInput     = "_source_input"
	keySourceInputText = "_source_input_text"
	keySourceResult    = "_source_result"
	keySourceExcerpt   = "_source_excerpt"
	keyBatchID         = "_batch_id"

	sourceAssistantResponse = "assistant_response"
)

// AnalyzerWriter is an io.Writer that inspects agent stdout lines,
// runs qualifying tool completions through the BAML ExtractOutputs
// classifier, and creates TaskOutput records via the gateway gRPC.
//
// It rate-limits to one in-flight BAML call per task and queues
// additional candidates so we don't overwhelm the classifier.
type AnalyzerWriter struct {
	ctx         context.Context
	analyzer    OutputAnalyzer
	client      *gatewayclient.GatewayClient
	workspaceID uint32
	taskID      string
	runID       string
	agentID     string
	bamlEnv     map[string]string
	tracker     *taskOutputTracker

	mu       sync.Mutex
	inflight bool
	queue    []analyzerJob
	closed   bool
	wg       sync.WaitGroup
}

type analyzerJob struct {
	toolName   string
	toolInput  string
	toolResult string
}

func NewAnalyzerWriter(
	ctx context.Context,
	analyzer OutputAnalyzer,
	client *gatewayclient.GatewayClient,
	task types.RunExecution,
	bamlEnv map[string]string,
) *AnalyzerWriter {
	return newAnalyzerWriter(ctx, analyzer, client, task, bamlEnv, nil)
}

func newAnalyzerWriter(
	ctx context.Context,
	analyzer OutputAnalyzer,
	client *gatewayclient.GatewayClient,
	task types.RunExecution,
	bamlEnv map[string]string,
	tracker *taskOutputTracker,
) *AnalyzerWriter {
	ids := outputIDsFromTask(task)
	return &AnalyzerWriter{
		ctx:         ctx,
		analyzer:    analyzer,
		client:      client,
		workspaceID: ids.workspaceID,
		taskID:      ids.taskID,
		runID:       ids.runID,
		agentID:     ids.agentID,
		bamlEnv:     bamlEnv,
		tracker:     tracker,
	}
}

func (w *AnalyzerWriter) outputIDs() taskOutputIDs {
	return taskOutputIDs{
		workspaceID: w.workspaceID,
		taskID:      w.taskID,
		runID:       w.runID,
		agentID:     w.agentID,
	}
}

func (w *AnalyzerWriter) Write(p []byte) (int, error) {
	if w.taskID == "" || w.client == nil || w.analyzer == nil {
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

	if !w.analyzer.ShouldAnalyze(payload) {
		return len(p), nil
	}
	toolName, toolInput, toolResult, ok := w.analyzer.PrepareInput(payload)
	if !ok {
		return len(p), nil
	}

	w.enqueue(analyzerJob{toolName: toolName, toolInput: toolInput, toolResult: toolResult})
	return len(p), nil
}

func (w *AnalyzerWriter) enqueue(job analyzerJob) {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return
	}
	if w.inflight {
		if len(w.queue) < 32 {
			w.queue = append(w.queue, job)
		}
		w.mu.Unlock()
		return
	}
	w.inflight = true
	w.wg.Add(1)
	w.mu.Unlock()
	go w.process(job)
}

func (w *AnalyzerWriter) process(job analyzerJob) {
	defer w.finishProcess()

	job.toolName = sanitizeUTF8(job.toolName)
	job.toolInput = sanitizeUTF8(job.toolInput)
	job.toolResult = sanitizeUTF8(job.toolResult)

	outputs, err := w.callExtractOutputs(job)
	if err != nil {
		log.Warn().Err(err).Str("task", w.taskID).Str("tool", job.toolName).
			Msg("BAML ExtractOutputs failed")
		return
	}

	fallbackURI := extractDeepLink(job.toolResult)

	publishable := 0
	for _, out := range outputs {
		r := extractedResult{out}
		if !r.isNone() && !r.isIntermediate() {
			publishable++
		}
	}
	var batchID string
	if publishable > 1 {
		batchID = uuid.NewString()
	}

	for _, out := range outputs {
		if out.Kind == signaltypes.OutputKindNONE {
			continue
		}
		if derefStr(out.Uri) == "" && fallbackURI != "" {
			out.Uri = &fallbackURI
		}
		w.createOutputWithBatch(out, job.toolName, job.toolInput, job.toolResult, batchID)
	}
}

func (w *AnalyzerWriter) finishProcess() {
	var next analyzerJob
	hasNext := false

	w.mu.Lock()
	if len(w.queue) > 0 {
		next = w.queue[0]
		w.queue = w.queue[1:]
		hasNext = true
		w.wg.Add(1)
	} else {
		w.inflight = false
	}
	w.mu.Unlock()

	if hasNext {
		go w.process(next)
	}
	w.wg.Done()
}

func (w *AnalyzerWriter) Wait() {
	if w == nil {
		return
	}
	w.mu.Lock()
	w.closed = true
	w.mu.Unlock()
	w.wg.Wait()
}

// extractDeepLink scans a tool result JSON string for well-known URL fields.
// This provides a deterministic fallback when the BAML classifier misses a
// URL that the tool client included in its structured response.
func extractDeepLink(toolResult string) string {
	var parsed map[string]any
	if json.Unmarshal([]byte(toolResult), &parsed) != nil {
		return ""
	}
	for _, key := range []string{"url", "html_url", "permalink", "web_url", "video_url"} {
		if v, ok := parsed[key].(string); ok && strings.HasPrefix(v, "http") {
			return v
		}
	}
	return ""
}

// ---------------------------------------------------------------------------
// extractedResult wraps a BAML ExtractedOutput and provides methods for
// deriving output candidates. Same pattern as Artifact wrapping TaskOutput.
// ---------------------------------------------------------------------------

type extractedResult struct {
	out signaltypes.ExtractedOutput
}

func (r extractedResult) title() string      { return strings.TrimSpace(r.out.Title) }
func (r extractedResult) summary() string    { return strings.TrimSpace(derefStr(r.out.Summary)) }
func (r extractedResult) content() string    { return strings.TrimSpace(derefStr(r.out.Content)) }
func (r extractedResult) path() string       { return derefStr(r.out.Path) }
func (r extractedResult) uri() string        { return derefStr(r.out.Uri) }
func (r extractedResult) outputType() string { return kindToOutputType(r.out.Kind) }
func (r extractedResult) isNone() bool       { return r.out.Kind == signaltypes.OutputKindNONE }

func (r extractedResult) isIntermediate() bool {
	return isIntermediatePath(r.path())
}

// candidate builds a base outputCandidate from the BAML output. Callers
// enrich the returned candidate with context-specific fields (tool data,
// assistant content, etc.) before publishing.
func (r extractedResult) candidate(role string) outputCandidate {
	data := map[string]any{}
	metadata := map[string]any{}

	if s := r.summary(); s != "" {
		data[keySummary] = s
	}
	if p := r.path(); p != "" {
		data[keyPath] = p
		metadata[keyPath] = p
	}
	if u := r.uri(); u != "" {
		data[keyURI] = u
		metadata[keyDeeplink] = u
	}

	if len(r.out.Data_fields) > 0 {
		var fieldsMeta []map[string]string
		for _, f := range r.out.Data_fields {
			key := strings.TrimSpace(f.Key)
			val := strings.TrimSpace(f.Value)
			if key == "" || val == "" {
				continue
			}
			if _, exists := data[key]; !exists {
				data[key] = val
			}
			fieldsMeta = append(fieldsMeta, map[string]string{
				"key":   key,
				"type":  strings.TrimSpace(f.Type),
				"label": strings.TrimSpace(f.Label),
			})
		}
		if len(fieldsMeta) > 0 {
			metadata["data_fields"] = fieldsMeta
		}
	}
	if len(r.out.Tags) > 0 {
		data[keyTags] = r.out.Tags
		metadata[keyTags] = r.out.Tags
	}

	if key := derefStr(r.out.Artifact_key); key != "" {
		metadata[types.TaskOutputMetadataArtifactKey] = key
	}
	if label := derefStr(r.out.Artifact_label); label != "" {
		metadata[types.TaskOutputMetadataArtifactLabel] = label
	}
	if kind := derefStr(r.out.Artifact_kind); kind != "" {
		metadata[types.TaskOutputMetadataArtifactKind] = kind
	}
	return outputCandidate{
		OutputType: r.outputType(),
		Title:      r.title(),
		Summary:    r.summary(),
		URI:        r.uri(),
		Path:       r.path(),
		Data:       data,
		Metadata:   metadata,
		Role:       role,
	}
}

// ---------------------------------------------------------------------------
// Output creation
// ---------------------------------------------------------------------------

func (w *AnalyzerWriter) createOutputWithBatch(out signaltypes.ExtractedOutput, toolName, toolInput, toolResult, batchID string) {
	r := extractedResult{out}
	if r.isIntermediate() {
		return
	}

	c := r.candidate(types.TaskOutputArtifactRoleSupporting)
	c.Metadata[keyTool] = toolName
	if batchID != "" {
		c.Metadata[keyBatchID] = batchID
	}

	if content := r.content(); content != "" {
		c.Data[keyContent] = content
	}

	if parsedInput, ok := decodeStructuredPayload(toolInput); ok {
		c.Metadata[keySourceInput] = parsedInput
	} else if strings.TrimSpace(toolInput) != "" {
		c.Metadata[keySourceInputText] = strings.TrimSpace(toolInput)
	}

	if parsedResult, ok := decodeStructuredPayload(toolResult); ok {
		c.Data[keySourceResult] = parsedResult

		if sourceTitle := firstMatchingString(parsedResult, "video_title", "title", "name", "subject"); sourceTitle != "" {
			c.Data[keySourceTitle] = sourceTitle
			if shouldPreferSourceTitle(c.Title, sourceTitle) {
				c.Title = sourceTitle
			}
		}
		if sourceURL := firstMatchingString(parsedResult, "video_url", "url", "uri", "permalink", "html_url", "web_url"); sourceURL != "" {
			if c.URI == "" {
				c.URI = sourceURL
				c.Data[keyURI] = sourceURL
				c.Metadata[keyDeeplink] = sourceURL
			}
			c.Data[keySourceURL] = sourceURL
		}

		if len(out.Data_fields) == 0 {
			promoteToolResultFields(c.Data, parsedResult)
		}
	} else if strings.TrimSpace(toolResult) != "" {
		c.Data[keySourceExcerpt] = strings.TrimSpace(toolResult)
	}

	if _, err := publishOutputCandidate(w.ctx, w.client, w.outputIDs(), w.tracker, c); err != nil {
		log.Warn().Err(err).Str("task", w.taskID).Str("title", out.Title).
			Msg("analyzer: output create failed")
	}
}

type finalResponseExtractor func(
	ctx context.Context,
	userMessage *string,
	assistantMessage string,
	bamlEnv map[string]string,
) ([]signaltypes.ExtractedOutput, error)

type blockingOutputMetadata struct {
	Kind            string
	InputKind       types.InputKind
	WaitGroupID     string
	ApprovalSurface bool
}

type assistantResponsePersistOptions struct {
	Extract       finalResponseExtractor
	MinLen        int
	Status        string
	Blocking      *blockingOutputMetadata
	FallbackTitle string
}

func defaultFinalResponseExtractor(
	ctx context.Context,
	userMessage *string,
	assistantMessage string,
	bamlEnv map[string]string,
) (out []signaltypes.ExtractedOutput, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("ExtractFinalResponseOutput panicked: %v", r)
		}
	}()
	return agentsignal.ExtractFinalResponseOutput(
		ctx, userMessage, assistantMessage,
		agentsignal.WithEnv(bamlEnv),
	)
}

func defaultApprovalResponseExtractor(
	ctx context.Context,
	_ *string,
	assistantMessage string,
	bamlEnv map[string]string,
) (out []signaltypes.ExtractedOutput, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("ExtractApprovalOutput panicked: %v", r)
		}
	}()
	return agentsignal.ExtractApprovalOutput(
		ctx, assistantMessage,
		agentsignal.WithEnv(bamlEnv),
	)
}

const minResponseOutputLen = 200
const minApprovalOutputLen = 40

func persistAssistantResponseOutputs(
	ctx context.Context,
	client taskOutputClient,
	task types.RunExecution,
	tracker *taskOutputTracker,
	userMessage *string,
	assistantMessage string,
	bamlEnv map[string]string,
	opts assistantResponsePersistOptions,
) error {
	if client == nil {
		return nil
	}
	assistantMessage = strings.TrimSpace(sanitizeUTF8(assistantMessage))
	if len(assistantMessage) < opts.MinLen {
		return nil
	}
	if userMessage != nil {
		sanitized := sanitizeUTF8(*userMessage)
		userMessage = &sanitized
	}

	extract := opts.Extract
	if extract == nil {
		extract = defaultFinalResponseExtractor
	}
	outputs, err := extract(ctx, userMessage, assistantMessage, bamlEnv)
	if err != nil {
		return err
	}

	ids := outputIDsFromTask(task)
	if ids.taskID == "" {
		return nil
	}

	count := 0
	for _, out := range outputs {
		r := extractedResult{out}
		if !r.isNone() && r.title() != "" && r.content() != "" {
			count++
		}
	}

	var batchID string
	if count > 1 {
		batchID = uuid.NewString()
	}

	promptMeta := ""
	if userMessage != nil {
		promptMeta = strings.TrimSpace(*userMessage)
	}

	if count == 0 {
		fallback := fallbackAssistantResponseCandidate(assistantMessage, promptMeta, opts)
		if fallback == nil {
			return nil
		}
		if _, err := publishOutputCandidate(ctx, client, ids, tracker, *fallback); err != nil {
			log.Warn().Err(err).Str("task", ids.taskID).Str("title", fallback.Title).
				Msg("assistant response fallback output create failed")
		}
		return nil
	}

	published := 0
	for _, out := range outputs {
		r := extractedResult{out}
		if r.isNone() || r.title() == "" || r.content() == "" {
			continue
		}

		role := types.TaskOutputArtifactRoleSupporting
		if published == 0 {
			role = types.TaskOutputArtifactRolePrimary
		}
		published++

		c := r.candidate(role)
		if c.OutputType == "" {
			c.OutputType = "text"
		}
		c.Data[keyContent] = r.content()
		c.Metadata[keySource] = sourceAssistantResponse
		if promptMeta != "" {
			c.Metadata[keySourcePrompt] = promptMeta
		}
		if batchID != "" {
			c.Metadata[keyBatchID] = batchID
		}
		if opts.Status != "" {
			c.Status = opts.Status
		}
		if opts.Blocking != nil {
			applyBlockingMetadata(c.Metadata, opts.Blocking)
		}

		if _, err := publishOutputCandidate(ctx, client, ids, tracker, c); err != nil {
			log.Warn().Err(err).Str("task", ids.taskID).Str("title", r.title()).
				Msg("assistant response output create failed")
		}
	}
	return nil
}

func fallbackAssistantResponseCandidate(
	assistantMessage, promptMeta string,
	opts assistantResponsePersistOptions,
) *outputCandidate {
	if opts.Blocking == nil {
		return nil
	}
	title := strings.TrimSpace(opts.FallbackTitle)
	if title == "" {
		return nil
	}
	candidate := &outputCandidate{
		OutputType: "text",
		Title:      title,
		Data: map[string]any{
			keyContent: assistantMessage,
		},
		Metadata: map[string]any{
			keySource: sourceAssistantResponse,
		},
		Role:   types.TaskOutputArtifactRolePrimary,
		Status: opts.Status,
	}
	if promptMeta != "" {
		candidate.Metadata[keySourcePrompt] = promptMeta
	}
	applyBlockingMetadata(candidate.Metadata, opts.Blocking)
	return candidate
}

func persistFinalResponseOutput(
	ctx context.Context,
	client taskOutputClient,
	task types.RunExecution,
	tracker *taskOutputTracker,
	userMessage *string,
	assistantMessage string,
	bamlEnv map[string]string,
	extract finalResponseExtractor,
) error {
	if extract == nil {
		extract = defaultFinalResponseExtractor
	}
	return persistAssistantResponseOutputs(
		ctx,
		client,
		task,
		tracker,
		userMessage,
		assistantMessage,
		bamlEnv,
		assistantResponsePersistOptions{
			Extract: extract,
			MinLen:  minResponseOutputLen,
		},
	)
}

func persistApprovalResponseOutput(
	ctx context.Context,
	client taskOutputClient,
	task types.RunExecution,
	tracker *taskOutputTracker,
	userMessage *string,
	assistantMessage string,
	bamlEnv map[string]string,
) error {
	return persistAssistantResponseOutputs(
		ctx,
		client,
		task,
		tracker,
		userMessage,
		assistantMessage,
		bamlEnv,
		assistantResponsePersistOptions{
			Extract: defaultApprovalResponseExtractor,
			MinLen:  minApprovalOutputLen,
			Status:  types.TaskOutputStatusPending,
			Blocking: &blockingOutputMetadata{
				Kind:            types.TaskOutputBlockingKindApproval,
				InputKind:       types.InputKindApproveReject,
				WaitGroupID:     approvalWaitGroupID(task, assistantMessage),
				ApprovalSurface: true,
			},
			FallbackTitle: "Approval Required",
		},
	)
}

func approvalWaitGroupID(task types.RunExecution, assistantMessage string) string {
	ids := outputIDsFromTask(task)
	seed := firstNonEmptyTrimmed(ids.taskID, ids.runID, task.ExternalId)
	if seed == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(seed + "\n" + strings.TrimSpace(sanitizeUTF8(assistantMessage))))
	return hex.EncodeToString(sum[:8])
}

func applyBlockingMetadata(metadata map[string]any, block *blockingOutputMetadata) {
	if block == nil || metadata == nil {
		return
	}
	if kind := strings.TrimSpace(block.Kind); kind != "" {
		metadata[types.TaskOutputMetadataBlockingKind] = kind
	}
	if inputKind := strings.TrimSpace(string(block.InputKind)); inputKind != "" {
		metadata[types.TaskOutputMetadataInputKind] = inputKind
	}
	if waitGroupID := strings.TrimSpace(block.WaitGroupID); waitGroupID != "" {
		metadata[types.TaskOutputMetadataWaitGroupID] = waitGroupID
	}
	if block.ApprovalSurface {
		metadata[types.TaskOutputMetadataApprovalUI] = true
	}
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// callExtractOutputs wraps the BAML ExtractOutputs call with panic recovery.
// The generated BAML client panics on encoding errors (e.g. invalid UTF-8 in
// protobuf string fields) instead of returning an error.
func (w *AnalyzerWriter) callExtractOutputs(job analyzerJob) (outputs []signaltypes.ExtractedOutput, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("ExtractOutputs panicked: %v", r)
		}
	}()
	return agentsignal.ExtractOutputs(
		w.ctx, job.toolName, job.toolInput, job.toolResult,
		agentsignal.WithEnv(w.bamlEnv),
	)
}

func sanitizeUTF8(s string) string {
	if utf8.ValidString(s) {
		return s
	}
	return strings.ToValidUTF8(s, "\uFFFD")
}

func derefStr(p *string) string {
	if p != nil {
		return *p
	}
	return ""
}

// isIntermediatePath returns true for local file paths that won't be
// accessible via the UI. Only files under /workspace/ have working
// presigned URLs; everything else is undownloadable.
func isIntermediatePath(path string) bool {
	if path == "" {
		return false
	}
	if !strings.HasPrefix(path, "/") {
		return false
	}
	return !strings.HasPrefix(strings.ToLower(path), "/workspace/")
}

func kindToOutputType(kind signaltypes.OutputKind) string {
	switch kind {
	case signaltypes.OutputKindFILE_CREATED, signaltypes.OutputKindFILE_MODIFIED:
		return "file"
	case signaltypes.OutputKindEMAIL_SENT, signaltypes.OutputKindEMAIL_DRAFT:
		return "email"
	case signaltypes.OutputKindLINK_CREATED:
		return "link"
	case signaltypes.OutputKindAPI_CALL:
		return "json"
	case signaltypes.OutputKindREPORT:
		return "text"
	default:
		return "json"
	}
}

func decodeStructuredPayload(raw string) (any, bool) {
	if strings.TrimSpace(raw) == "" {
		return nil, false
	}
	var parsed any
	if err := json.Unmarshal([]byte(raw), &parsed); err != nil {
		return nil, false
	}
	return parsed, true
}

func firstMatchingString(value any, keys ...string) string {
	switch typed := value.(type) {
	case map[string]any:
		for _, key := range keys {
			for k, child := range typed {
				if strings.EqualFold(k, key) {
					if text, ok := child.(string); ok && strings.TrimSpace(text) != "" {
						return strings.TrimSpace(text)
					}
				}
			}
		}
		for _, key := range keys {
			for _, child := range typed {
				if match := firstMatchingString(child, key); match != "" {
					return match
				}
			}
		}
	case []any:
		for _, key := range keys {
			for _, child := range typed {
				if match := firstMatchingString(child, key); match != "" {
					return match
				}
			}
		}
	}
	return ""
}

// promoteToolResultFields copies scalar fields (string, number, bool) and
// simple string arrays from the parsed tool result into the top-level Data
// map so that the view system can bind to them directly. Only used as a
// fallback when BAML doesn't provide data_fields.
func promoteToolResultFields(data map[string]any, parsedResult any) {
	resultMap, ok := parsedResult.(map[string]any)
	if !ok {
		return
	}
	for key, val := range resultMap {
		if _, exists := data[key]; exists {
			continue
		}
		switch v := val.(type) {
		case string:
			if v != "" {
				data[key] = v
			}
		case float64, bool:
			data[key] = v
		case []any:
			if len(v) > 0 {
				if _, isStr := v[0].(string); isStr {
					data[key] = v
				}
			}
		}
	}
}

func shouldPreferSourceTitle(currentTitle, sourceTitle string) bool {
	current := strings.TrimSpace(strings.ToLower(currentTitle))
	source := strings.TrimSpace(sourceTitle)
	if current == "" || source == "" || strings.EqualFold(currentTitle, sourceTitle) {
		return current == "" && source != ""
	}
	for _, prefix := range []string{"created ", "updated ", "saved ", "generated ", "wrote "} {
		if strings.HasPrefix(current, prefix) {
			return true
		}
	}
	for _, marker := range []string{" pdf", " report", " document", " transcript", " file"} {
		if strings.Contains(current, marker) {
			return true
		}
	}
	return false
}
