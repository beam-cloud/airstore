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

	"github.com/beam-cloud/airstore/pkg/types"
	agentsignal "github.com/beam-cloud/airstore/pkg/worker/agentsignal/baml_client"
	signaltypes "github.com/beam-cloud/airstore/pkg/worker/agentsignal/baml_client/types"
	pb "github.com/beam-cloud/airstore/proto"
	"github.com/google/uuid"
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

	// Internal bookkeeping keys are prefixed with `_` so the view mapper
	// skips them when projecting user-facing fields.
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

type taskOutputClient interface {
	CreateTaskOutput(ctx context.Context, req *pb.CreateTaskOutputRequest) (string, error)
	AppendTaskOutputRows(ctx context.Context, req *pb.AppendTaskOutputRowsRequest) error
	FinalizeTaskOutput(ctx context.Context, req *pb.FinalizeTaskOutputRequest) error
	UpdateTaskOutputStatus(ctx context.Context, req *pb.UpdateTaskOutputStatusRequest) error
}

// taskOutputIDs extracts the IDs that artifact publishers need from a task's
// execution policy.
type taskOutputIDs struct {
	workspaceID uint32
	taskID      string
	runID       string
	agentID     string
}

func outputIDsFromTask(task types.RunExecution) taskOutputIDs {
	ids := taskOutputIDs{workspaceID: uint32(task.WorkspaceId)}
	if task.ExecutionPolicy != nil {
		ids.taskID = anyToTrimmedString(task.ExecutionPolicy[types.AgentExecutionMetaKeyOriginTaskID])
		ids.runID = anyToTrimmedString(task.ExecutionPolicy[types.AgentExecutionMetaKeyRunID])
		ids.agentID = anyToTrimmedString(task.ExecutionPolicy[types.AgentExecutionMetaKeyAgentID])
	}
	return ids
}

type trackedOutputSummary struct {
	OutputID  string
	Identity  string
	EntityKey string
}

type taskOutputTracker struct {
	mu               sync.Mutex
	seen             map[string]struct{}
	primary          map[string]struct{}
	outputByIdentity map[string]trackedOutputSummary
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
			if summary, hasServerID := t.outputByIdentity[identity]; hasServerID && summary.OutputID != "" {
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

func (t *taskOutputTracker) RememberWithID(candidate outputCandidate, serverID string) {
	if t == nil {
		return
	}
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
		t.outputByIdentity = make(map[string]trackedOutputSummary)
	}
	if identity != "" {
		t.seen[identity] = struct{}{}
		t.outputByIdentity[identity] = trackedOutputSummary{
			OutputID:  serverID,
			Identity:  identity,
			EntityKey: candidate.fanOutEntityKey(),
		}
	}
	if key != "" && candidate.isPrimaryDeliverable() {
		t.primary[key] = struct{}{}
	}
}

// TrackedOutputSummaries returns a list of published outputs with their
// identity keys so fan-out and waiting flows can reason about them.
func (t *taskOutputTracker) TrackedOutputSummaries() []trackedOutputSummary {
	if t == nil {
		return nil
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	var out []trackedOutputSummary
	for _, summary := range t.outputByIdentity {
		if summary.OutputID != "" {
			out = append(out, summary)
		}
	}
	return out
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
	return t.outputByIdentity[identity].OutputID
}

// outputCandidate is the canonical worker-side shape for a task artifact before
// it is persisted. All artifact publishers normalize into this form so that
// metadata defaults, dedup, and persistence live in one module.
type outputCandidate struct {
	LocalID    string
	OutputType string
	Title      string
	Summary    string
	URI        string
	Path       string
	Data       map[string]any
	Metadata   map[string]any
	Role       string
	Status     string
}

func (c outputCandidate) identityKey() string {
	key := normalizeArtifactToken(anyToTrimmedString(c.Metadata[types.TaskOutputMetadataArtifactKey]))
	title := normalizeArtifactToken(c.Title)
	path := firstNonEmptyTrimmed(c.Path, anyToTrimmedString(c.Data[keyPath]))
	uri := strings.ToLower(firstNonEmptyTrimmed(c.URI, anyToTrimmedString(c.Data[keyURI]), anyToTrimmedString(c.Metadata[keyDeeplink])))

	switch {
	case key != "" && uri != "":
		return "key:" + key + "|uri:" + uri
	case key != "" && path != "":
		return "key:" + key + "|path:" + path
	case key != "" && title != "":
		return "key:" + key + "|title:" + title
	case uri != "":
		return "uri:" + uri
	case path != "":
		return "path:" + path
	case key != "":
		return "key:" + key
	case title != "":
		return "type:" + normalizeArtifactToken(c.OutputType) + "|title:" + title
	default:
		return ""
	}
}

func (c outputCandidate) fanOutEntityKey() string {
	key := normalizeArtifactToken(anyToTrimmedString(c.Metadata[types.TaskOutputMetadataArtifactKey]))
	kind := normalizeArtifactToken(anyToTrimmedString(c.Metadata[types.TaskOutputMetadataArtifactKind]))
	outputType := normalizeArtifactToken(c.OutputType)

	if kind == "email" || strings.Contains(key, "email") || strings.Contains(outputType, "email") {
		recipient := normalizeFanOutEntityValue(firstNonEmptyTrimmed(
			anyToTrimmedString(c.Data["to"]),
			anyToTrimmedString(c.Data["recipient"]),
			anyToTrimmedString(c.Data["email"]),
		))
		if recipient != "" {
			return "email:" + recipient
		}
		company := normalizeFanOutEntityValue(anyToTrimmedString(c.Data["company"]))
		if company != "" {
			return "company:" + company
		}
	}

	path := firstNonEmptyTrimmed(c.Path, anyToTrimmedString(c.Data[keyPath]))
	if path != "" {
		return "path:" + path
	}

	uri := strings.ToLower(firstNonEmptyTrimmed(c.URI, anyToTrimmedString(c.Data[keyURI]), anyToTrimmedString(c.Metadata[keyDeeplink])))
	if uri != "" {
		return "uri:" + uri
	}

	title := normalizeArtifactToken(c.Title)
	if title != "" {
		return "title:" + title
	}

	return ""
}

func (c outputCandidate) artifactKey() string {
	return normalizeArtifactToken(anyToTrimmedString(c.Metadata[types.TaskOutputMetadataArtifactKey]))
}

func (c outputCandidate) artifactRole() string {
	role := normalizeArtifactRole(anyToTrimmedString(c.Metadata[types.TaskOutputMetadataArtifactRole]))
	if role != "" {
		return role
	}
	return normalizeArtifactRole(c.Role)
}

func (c outputCandidate) isPrimaryDeliverable() bool {
	return c.artifactRole() == types.TaskOutputArtifactRolePrimary
}

func (c outputCandidate) shouldPersist() bool {
	return c.OutputType != "" && c.Title != ""
}

func (c outputCandidate) normalize() outputCandidate {
	n := c
	n.OutputType = strings.TrimSpace(n.OutputType)
	n.Title = strings.TrimSpace(n.Title)
	n.Summary = strings.TrimSpace(n.Summary)
	n.URI = strings.TrimSpace(n.URI)
	n.Path = strings.TrimSpace(n.Path)
	n.Role = normalizeArtifactRole(n.Role)
	n.Data = cloneAnyMap(c.Data)
	n.Metadata = cloneAnyMap(c.Metadata)

	if n.Path == "" {
		n.Path = anyToTrimmedString(n.Data[keyPath])
	}
	if n.Path != "" && anyToTrimmedString(n.Data[keyPath]) == "" {
		n.Data[keyPath] = n.Path
	}

	if n.URI == "" {
		n.URI = firstNonEmptyTrimmed(
			anyToTrimmedString(n.Data[keyURI]),
			anyToTrimmedString(n.Metadata[keyDeeplink]),
		)
	}
	if n.URI != "" {
		if anyToTrimmedString(n.Data[keyURI]) == "" {
			n.Data[keyURI] = n.URI
		}
		if anyToTrimmedString(n.Metadata[keyDeeplink]) == "" {
			n.Metadata[keyDeeplink] = n.URI
		}
	}

	if n.Summary != "" && anyToTrimmedString(n.Data[keySummary]) == "" {
		n.Data[keySummary] = n.Summary
	}

	n.Metadata = defaultArtifactMetadata(n.Metadata, n.Role)
	return n
}

func (c outputCandidate) buildRequest(ids taskOutputIDs) (*pb.CreateTaskOutputRequest, error) {
	req := &pb.CreateTaskOutputRequest{
		WorkspaceId: ids.workspaceID,
		TaskId:      ids.taskID,
		RunId:       ids.runID,
		AgentId:     ids.agentID,
		OutputType:  c.OutputType,
		Title:       c.Title,
	}

	if len(c.Data) > 0 {
		b, err := json.Marshal(c.Data)
		if err != nil {
			return nil, err
		}
		req.DataJson = string(b)
	}
	if len(c.Metadata) > 0 {
		b, err := json.Marshal(c.Metadata)
		if err != nil {
			return nil, err
		}
		req.MetadataJson = string(b)
	}
	if c.URI != "" {
		req.Uri = c.URI
	}
	if c.Status != "" {
		req.Status = c.Status
	}

	return req, nil
}

func publishOutputCandidate(
	ctx context.Context,
	client taskOutputClient,
	ids taskOutputIDs,
	tracker *taskOutputTracker,
	candidate outputCandidate,
) (string, error) {
	if client == nil || ids.taskID == "" {
		return "", nil
	}

	normalized := candidate.normalize()
	if !normalized.shouldPersist() {
		return "", nil
	}
	if tracker != nil && tracker.HasEquivalent(normalized) {
		return "", nil
	}

	req, err := normalized.buildRequest(ids)
	if err != nil {
		return "", err
	}
	var predecessorID string
	if tracker != nil {
		predecessorID = tracker.PredecessorID(normalized)
	}

	serverID, err := client.CreateTaskOutput(ctx, req)
	if err != nil {
		return "", err
	}
	if tracker != nil {
		tracker.RememberWithID(normalized, serverID)
	}

	if predecessorID != "" {
		if err := client.UpdateTaskOutputStatus(ctx, &pb.UpdateTaskOutputStatusRequest{
			WorkspaceId: ids.workspaceID,
			OutputId:    predecessorID,
			Status:      types.TaskOutputStatusCancelled,
		}); err != nil {
			log.Warn().Err(err).Str("predecessor", predecessorID).Msg("failed to supersede predecessor output")
		}
	}

	if normalized.Summary != "" {
		if err := client.FinalizeTaskOutput(ctx, &pb.FinalizeTaskOutputRequest{
			WorkspaceId: ids.workspaceID,
			OutputId:    serverID,
			Summary:     normalized.Summary,
		}); err != nil {
			log.Warn().Err(err).Str("task", ids.taskID).Str("output", serverID).Msg("output finalize failed after create")
		}
	}

	return serverID, nil
}

type finalResponseExtractor func(
	ctx context.Context,
	userMessage *string,
	assistantMessage string,
	bamlEnv map[string]string,
) ([]signaltypes.ExtractedOutput, error)

const (
	minResponseOutputLen = 200
	minApprovalOutputLen = 40
)

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

type responseArtifactPlan struct {
	Extract       finalResponseExtractor
	MinLen        int
	Status        string
	Blocking      *types.TaskOutputBlockingMetadata
	Filter        func(signaltypes.ExtractedOutput) bool
	FallbackTitle string
}

func persistAssistantResponseOutputs(
	ctx context.Context,
	client taskOutputClient,
	task types.RunExecution,
	tracker *taskOutputTracker,
	userMessage *string,
	assistantMessage string,
	bamlEnv map[string]string,
	plan responseArtifactPlan,
) (bool, error) {
	pipeline := newResponseArtifactPipeline(ctx, client, task, tracker)
	return pipeline.Persist(userMessage, assistantMessage, bamlEnv, plan)
}

func isPublishableFinalResponseOutput(out signaltypes.ExtractedOutput) bool {
	switch out.Kind {
	case signaltypes.OutputKindEMAIL_DRAFT, signaltypes.OutputKindEMAIL_SENT:
		return false
	default:
		return true
	}
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
) (bool, error) {
	return persistAssistantResponseOutputs(
		ctx,
		client,
		task,
		tracker,
		userMessage,
		assistantMessage,
		bamlEnv,
		newFinalResponseArtifactPlan(extract),
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
) (bool, error) {
	return persistAssistantResponseOutputs(
		ctx,
		client,
		task,
		tracker,
		userMessage,
		assistantMessage,
		bamlEnv,
		newApprovalResponseArtifactPlan(task, assistantMessage),
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

type responseArtifactPipeline struct {
	ctx     context.Context
	client  taskOutputClient
	ids     taskOutputIDs
	tracker *taskOutputTracker
}

func newResponseArtifactPipeline(ctx context.Context, client taskOutputClient, task types.RunExecution, tracker *taskOutputTracker) responseArtifactPipeline {
	return responseArtifactPipeline{
		ctx:     ctx,
		client:  client,
		ids:     outputIDsFromTask(task),
		tracker: tracker,
	}
}

func (p responseArtifactPipeline) Persist(userMessage *string, assistantMessage string, bamlEnv map[string]string, plan responseArtifactPlan) (bool, error) {
	if p.client == nil {
		return false, nil
	}
	assistantMessage = strings.TrimSpace(sanitizeUTF8(assistantMessage))
	if len(assistantMessage) < plan.MinLen {
		return false, nil
	}
	if userMessage != nil {
		sanitized := sanitizeUTF8(*userMessage)
		userMessage = &sanitized
	}

	extract := plan.Extract
	if extract == nil {
		extract = defaultFinalResponseExtractor
	}
	outputs, err := extract(p.ctx, userMessage, assistantMessage, bamlEnv)
	if err != nil {
		return false, err
	}
	if p.ids.taskID == "" {
		return false, nil
	}

	count := 0
	for _, out := range outputs {
		if plan.Filter != nil && !plan.Filter(out) {
			continue
		}
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
		fallback := fallbackResponseArtifactCandidate(assistantMessage, promptMeta, plan)
		if fallback == nil {
			return false, nil
		}
		if _, err := publishOutputCandidate(p.ctx, p.client, p.ids, p.tracker, *fallback); err != nil {
			log.Warn().Err(err).Str("task", p.ids.taskID).Str("title", fallback.Title).
				Msg("assistant response fallback output create failed")
			return false, nil
		}
		return true, nil
	}

	published := 0
	publishedAny := false
	for _, out := range outputs {
		if plan.Filter != nil && !plan.Filter(out) {
			continue
		}
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
		if plan.Status != "" {
			c.Status = plan.Status
		}
		if plan.Blocking != nil {
			plan.Blocking.Apply(c.Metadata)
		}

		if _, err := publishOutputCandidate(p.ctx, p.client, p.ids, p.tracker, c); err != nil {
			log.Warn().Err(err).Str("task", p.ids.taskID).Str("title", r.title()).
				Msg("assistant response output create failed")
			continue
		}
		publishedAny = true
	}
	return publishedAny, nil
}

func fallbackResponseArtifactCandidate(
	assistantMessage, promptMeta string,
	plan responseArtifactPlan,
) *outputCandidate {
	if plan.Blocking == nil {
		return nil
	}
	title := strings.TrimSpace(plan.FallbackTitle)
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
		Status: plan.Status,
	}
	if promptMeta != "" {
		candidate.Metadata[keySourcePrompt] = promptMeta
	}
	plan.Blocking.Apply(candidate.Metadata)
	return candidate
}

func newFinalResponseArtifactPlan(extract finalResponseExtractor) responseArtifactPlan {
	if extract == nil {
		extract = defaultFinalResponseExtractor
	}
	return responseArtifactPlan{
		Extract: extract,
		MinLen:  minResponseOutputLen,
		Filter:  isPublishableFinalResponseOutput,
	}
}

func newApprovalResponseArtifactPlan(task types.RunExecution, assistantMessage string) responseArtifactPlan {
	return responseArtifactPlan{
		Extract: defaultApprovalResponseExtractor,
		MinLen:  minApprovalOutputLen,
		Status:  types.TaskOutputStatusPending,
		Blocking: &types.TaskOutputBlockingMetadata{
			Kind:            types.TaskOutputBlockingKindApproval,
			InputKind:       types.InputKindApproveReject,
			WaitGroupID:     approvalWaitGroupID(task, assistantMessage),
			ApprovalSurface: true,
		},
		FallbackTitle: "Approval Required",
	}
}

// defaultArtifactMetadata normalizes BAML-provided artifact values and sets
// defaults for role. BAML is the source of truth for artifact_key,
// artifact_label, and artifact_kind - this function only normalizes tokens
// and fills in the role default when the model omits it.
func defaultArtifactMetadata(metadata map[string]any, role string) map[string]any {
	m := cloneAnyMap(metadata)

	if key := anyToTrimmedString(m[types.TaskOutputMetadataArtifactKey]); key != "" {
		m[types.TaskOutputMetadataArtifactKey] = normalizeArtifactToken(key)
	}
	if kind := anyToTrimmedString(m[types.TaskOutputMetadataArtifactKind]); kind != "" {
		m[types.TaskOutputMetadataArtifactKind] = normalizeArtifactToken(kind)
	}

	if normalized := normalizeArtifactRole(firstNonEmptyTrimmed(anyToTrimmedString(m[types.TaskOutputMetadataArtifactRole]), role)); normalized != "" {
		m[types.TaskOutputMetadataArtifactRole] = normalized
	} else {
		m[types.TaskOutputMetadataArtifactRole] = types.TaskOutputArtifactRoleSupporting
	}

	return m
}

func normalizeArtifactToken(value string) string {
	value = strings.TrimSpace(strings.ToLower(value))
	if value == "" {
		return ""
	}
	var b strings.Builder
	lastDash := false
	for _, r := range value {
		switch {
		case (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9'):
			b.WriteRune(r)
			lastDash = false
		default:
			if !lastDash {
				b.WriteByte('-')
				lastDash = true
			}
		}
	}
	return strings.Trim(b.String(), "-")
}

func normalizeArtifactRole(value string) string {
	switch strings.TrimSpace(strings.ToLower(value)) {
	case types.TaskOutputArtifactRolePrimary:
		return types.TaskOutputArtifactRolePrimary
	case types.TaskOutputArtifactRoleSupporting:
		return types.TaskOutputArtifactRoleSupporting
	case types.TaskOutputArtifactRoleIncidental:
		return types.TaskOutputArtifactRoleIncidental
	default:
		return ""
	}
}

func cloneAnyMap(metadata map[string]any) map[string]any {
	if len(metadata) == 0 {
		return map[string]any{}
	}
	cloned := make(map[string]any, len(metadata))
	for key, value := range metadata {
		cloned[key] = value
	}
	return cloned
}

func firstNonEmptyTrimmed(values ...string) string {
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			return trimmed
		}
	}
	return ""
}

func mapFromAny(value any) map[string]any {
	switch typed := value.(type) {
	case nil:
		return nil
	case map[string]any:
		return cloneAnyMap(typed)
	default:
		raw, err := json.Marshal(value)
		if err != nil {
			return nil
		}
		var decoded map[string]any
		if err := json.Unmarshal(raw, &decoded); err != nil {
			return nil
		}
		return decoded
	}
}

func normalizeFanOutEntityValue(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	if start := strings.Index(raw, "<"); start >= 0 && strings.HasSuffix(raw, ">") {
		raw = raw[start+1 : len(raw)-1]
	}
	return strings.ToLower(strings.TrimSpace(raw))
}

func sanitizeUTF8(s string) string {
	if utf8.ValidString(s) {
		return s
	}
	return strings.ToValidUTF8(s, "\uFFFD")
}
