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
	pb "github.com/beam-cloud/airstore/proto"
	"github.com/rs/zerolog/log"
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

	mu       sync.Mutex
	inflight bool
	queue    []analyzerJob
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
	if w.inflight {
		if len(w.queue) < 32 {
			w.queue = append(w.queue, job)
		}
		w.mu.Unlock()
		return
	}
	w.inflight = true
	w.mu.Unlock()
	go w.process(job)
}

func (w *AnalyzerWriter) process(job analyzerJob) {
	defer func() {
		w.mu.Lock()
		if len(w.queue) > 0 {
			next := w.queue[0]
			w.queue = w.queue[1:]
			w.mu.Unlock()
			go w.process(next)
		} else {
			w.inflight = false
			w.mu.Unlock()
		}
	}()

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
		w.createOutput(out, job.toolName)
	}
}

// extractDeepLink scans a tool result JSON string for well-known URL fields.
// This provides a deterministic fallback when the BAML classifier misses a
// URL that the tool client included in its structured response.
func extractDeepLink(toolResult string) string {
	var parsed map[string]any
	if json.Unmarshal([]byte(toolResult), &parsed) != nil {
		return ""
	}
	for _, key := range []string{"url", "html_url", "permalink", "web_url"} {
		if v, ok := parsed[key].(string); ok && strings.HasPrefix(v, "http") {
			return v
		}
	}
	return ""
}

func (w *AnalyzerWriter) createOutput(out signaltypes.ExtractedOutput, toolName string) {
	path := derefStr(out.Path)
	uri := derefStr(out.Uri)
	summary := derefStr(out.Summary)

	if isIntermediatePath(path) {
		return
	}

	data := map[string]any{}
	metadata := map[string]any{"tool": toolName}
	setIfNonEmpty := func(m map[string]any, key, val string) {
		if val != "" {
			m[key] = val
		}
	}
	setIfNonEmpty(data, "summary", summary)
	setIfNonEmpty(data, "path", path)
	setIfNonEmpty(data, "uri", uri)
	setIfNonEmpty(metadata, "deeplink", uri)
	setIfNonEmpty(metadata, "path", path)

	dataJSON, _ := json.Marshal(data)
	metadataJSON, _ := json.Marshal(metadata)

	serverID, err := w.client.CreateTaskOutput(w.ctx, &pb.CreateTaskOutputRequest{
		WorkspaceId:  w.workspaceID,
		TaskId:       w.taskID,
		RunId:        w.runID,
		AgentId:      w.agentID,
		OutputType:   kindToOutputType(out.Kind),
		Title:        out.Title,
		DataJson:     string(dataJSON),
		MetadataJson: string(metadataJSON),
		Uri:          uri,
	})
	if err != nil {
		log.Warn().Err(err).Str("task", w.taskID).Str("title", out.Title).
			Msg("analyzer: output create failed")
		return
	}

	if summary != "" {
		_ = w.client.FinalizeTaskOutput(w.ctx, &pb.FinalizeTaskOutputRequest{
			WorkspaceId: w.workspaceID,
			OutputId:    serverID,
			Summary:     summary,
		})
	}
}

func derefStr(p *string) string {
	if p != nil {
		return *p
	}
	return ""
}

// isIntermediatePath returns true for file paths that are clearly
// intermediate scratch work and should never be surfaced as outputs.
func isIntermediatePath(path string) bool {
	if path == "" {
		return false
	}
	lp := strings.ToLower(path)

	if strings.HasPrefix(lp, "/tmp/") || lp == "/tmp" {
		return true
	}

	// Images outside of /workspace/ are always intermediate (screenshots, etc.)
	for _, ext := range []string{".png", ".jpg", ".jpeg", ".gif", ".webp", ".bmp", ".svg"} {
		if strings.HasSuffix(lp, ext) && !strings.Contains(lp, "/workspace/") {
			return true
		}
	}

	// JSON files in the workspace are almost always internal state/config, not deliverables.
	// Real deliverables go to external systems (Google Drive, GitHub, etc.)
	if strings.HasSuffix(lp, ".json") {
		return true
	}

	return false
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
