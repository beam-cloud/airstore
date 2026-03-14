package worker

import (
	"context"
	"encoding/json"
	"strings"
	"sync"

	gatewayclient "github.com/beam-cloud/airstore/pkg/gateway/client"
	"github.com/beam-cloud/airstore/pkg/types"
	agentsignal "github.com/beam-cloud/airstore/pkg/worker/agentsignal/baml_client"
	signaltypes "github.com/beam-cloud/airstore/pkg/worker/agentsignal/baml_client/types"
	"github.com/rs/zerolog/log"
)

const (
	keyContent         = "content"
	keySummary         = "summary"
	keyPath            = "path"
	keyURI             = "uri"
	keyTags            = "tags"
	keyTool            = "tool"
	keyDeeplink        = "deeplink"
	keyClassifierKind  = "classifier_kind"
	keySource          = "source"
	keySourcePrompt    = "source_prompt"
	keySourceInput     = "source_input"
	keySourceInputText = "source_input_text"
	keySourceResult    = "source_result"
	keySourceTitle     = "source_title"
	keySourceURL       = "source_url"
	keySourceExcerpt   = "source_excerpt"

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

	outputs, err := agentsignal.ExtractOutputs(
		w.ctx, job.toolName, job.toolInput, job.toolResult,
		agentsignal.WithEnv(w.bamlEnv),
	)
	if err != nil {
		log.Warn().Err(err).Str("task", w.taskID).Str("tool", job.toolName).
			Msg("BAML ExtractOutputs failed")
		return
	}

	fallbackURI := extractDeepLink(job.toolResult)
	for _, out := range outputs {
		if out.Kind == signaltypes.OutputKindNONE {
			continue
		}
		if derefStr(out.Uri) == "" && fallbackURI != "" {
			out.Uri = &fallbackURI
		}
		w.createOutput(out, job.toolName, job.toolInput, job.toolResult)
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
	metadata := map[string]any{
		keyClassifierKind: string(r.out.Kind),
	}

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

func (w *AnalyzerWriter) createOutput(out signaltypes.ExtractedOutput, toolName, toolInput, toolResult string) {
	r := extractedResult{out}
	if r.isIntermediate() {
		return
	}

	c := r.candidate(types.TaskOutputArtifactRoleSupporting)
	c.Metadata[keyTool] = toolName

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
			c.Metadata[keySourceTitle] = sourceTitle
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
			c.Metadata[keySourceURL] = sourceURL
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
) (signaltypes.ExtractedOutput, error)

func defaultFinalResponseExtractor(
	ctx context.Context,
	userMessage *string,
	assistantMessage string,
	bamlEnv map[string]string,
) (signaltypes.ExtractedOutput, error) {
	return agentsignal.ExtractFinalResponseOutput(
		ctx, userMessage, assistantMessage,
		agentsignal.WithEnv(bamlEnv),
	)
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
	if client == nil {
		return nil
	}
	assistantMessage = strings.TrimSpace(assistantMessage)
	if assistantMessage == "" {
		return nil
	}
	if extract == nil {
		extract = defaultFinalResponseExtractor
	}

	out, err := extract(ctx, userMessage, assistantMessage, bamlEnv)
	if err != nil {
		return err
	}

	r := extractedResult{out}
	if r.isNone() || r.title() == "" || r.content() == "" {
		return nil
	}

	ids := outputIDsFromTask(task)
	if ids.taskID == "" {
		return nil
	}

	c := r.candidate(types.TaskOutputArtifactRolePrimary)
	c.OutputType = "text"
	c.Data[keyContent] = r.content()
	c.Metadata[keySource] = sourceAssistantResponse
	if userMessage != nil && strings.TrimSpace(*userMessage) != "" {
		c.Metadata[keySourcePrompt] = strings.TrimSpace(*userMessage)
	}

	_, err = publishOutputCandidate(ctx, client, ids, tracker, c)
	return err
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

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
	return firstMatchingStringRecursive(value, keys)
}

func firstMatchingStringRecursive(value any, keys []string) string {
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
		for _, child := range typed {
			if match := firstMatchingStringRecursive(child, keys); match != "" {
				return match
			}
		}
	case []any:
		for _, child := range typed {
			if match := firstMatchingStringRecursive(child, keys); match != "" {
				return match
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
