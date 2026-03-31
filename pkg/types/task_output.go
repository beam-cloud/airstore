package types

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

const (
	TaskOutputMetadataArtifactKey           = "artifact_key"
	TaskOutputMetadataArtifactLabel         = "artifact_label"
	TaskOutputMetadataArtifactKind          = "artifact_kind"
	TaskOutputMetadataArtifactRole          = "artifact_role"
	TaskOutputMetadataViewSchemaMatch       = "view_schema_match"
	TaskOutputMetadataViewSchemaViewID      = "view_schema_view_id"
	TaskOutputMetadataViewSchemaSheetID     = "view_schema_sheet_id"
	TaskOutputMetadataViewSchemaComponentID = "view_schema_component_id"
	TaskOutputMetadataViewSchemaKeys        = "view_schema_keys"
	TaskOutputMetadataViewSchemaColumns     = "view_schema_columns"
	TaskOutputMetadataBlockerID             = "_blocker_id"
	TaskOutputMetadataBlockingKind          = "_blocking_kind"
	TaskOutputMetadataInputKind             = "_input_kind"
	TaskOutputMetadataWaitGroupID           = "_wait_group_id"
	TaskOutputMetadataApprovalUI            = "_approval_surface"

	TaskOutputArtifactRolePrimary    = "primary"
	TaskOutputArtifactRoleSupporting = "supporting"
	TaskOutputArtifactRoleIncidental = "incidental"

	TaskOutputBlockingKindApproval = "approval"
	TaskOutputBlockingKindInput    = "input"

	TaskOutputTypeEmail = "email"

	TaskOutputStatusActive    = "active"
	TaskOutputStatusPending   = "pending"
	TaskOutputStatusApproved  = "approved"
	TaskOutputStatusRejected  = "rejected"
	TaskOutputStatusCancelled = "cancelled"
)

// TaskOutput is a structured output produced by an agent during a task.
type TaskOutput struct {
	ID          string         `json:"id"`
	WorkspaceID uint           `json:"workspace_id"`
	TaskID      string         `json:"task_id"`
	RunID       *string        `json:"run_id,omitempty"`
	AgentID     *string        `json:"agent_id,omitempty"`
	AgentName   string         `json:"agent_name,omitempty"`
	OutputType  string         `json:"output_type"`
	Title       string         `json:"title"`
	Summary     *string        `json:"summary,omitempty"`
	URI         *string        `json:"uri,omitempty"`
	Data        map[string]any `json:"data"`
	Metadata    map[string]any `json:"metadata,omitempty"`
	Status      string         `json:"status"`
	ArchivedAt  *time.Time     `json:"archived_at,omitempty"`
	CreatedAt   time.Time      `json:"created_at"`
}

type TaskOutputListFilter struct {
	TaskID          *string  `json:"task_id,omitempty"`
	TaskIDs         []string `json:"task_ids,omitempty"`
	AgentID         *string  `json:"agent_id,omitempty"`
	AgentIDIsNull   bool     `json:"agent_id_is_null,omitempty"`
	OutputType      *string  `json:"output_type,omitempty"`
	ExcludeArchived bool     `json:"exclude_archived,omitempty"`
	SourceViewID    *string  `json:"source_view_id,omitempty"`
	Limit           int      `json:"limit,omitempty"`
}

type ErrTaskOutputNotFound struct {
	ID string
}

func (e *ErrTaskOutputNotFound) Error() string {
	return "task output not found: " + e.ID
}

type ErrTaskOutputConflict struct {
	ID                  string
	WorkspaceID         uint
	TaskID              string
	ExistingWorkspaceID uint
	ExistingTaskID      string
}

func (e *ErrTaskOutputConflict) Error() string {
	return fmt.Sprintf(
		"task output id %s for workspace %d task %s conflicts with existing output in workspace %d task %s",
		e.ID,
		e.WorkspaceID,
		e.TaskID,
		e.ExistingWorkspaceID,
		e.ExistingTaskID,
	)
}

type TaskOutputBlockingMetadata struct {
	BlockerID       string    `json:"blocker_id,omitempty"`
	Kind            string    `json:"kind,omitempty"`
	InputKind       InputKind `json:"input_kind,omitempty"`
	WaitGroupID     string    `json:"wait_group_id,omitempty"`
	ApprovalSurface bool      `json:"approval_surface,omitempty"`
}

func (m TaskOutputBlockingMetadata) Apply(metadata map[string]any) map[string]any {
	if metadata == nil {
		metadata = map[string]any{}
	}
	if blockerID := strings.TrimSpace(m.BlockerID); blockerID != "" {
		metadata[TaskOutputMetadataBlockerID] = blockerID
	}
	if kind := strings.TrimSpace(m.Kind); kind != "" {
		metadata[TaskOutputMetadataBlockingKind] = kind
	}
	if inputKind := strings.TrimSpace(string(m.InputKind)); inputKind != "" {
		metadata[TaskOutputMetadataInputKind] = inputKind
	}
	if waitGroupID := strings.TrimSpace(m.WaitGroupID); waitGroupID != "" {
		metadata[TaskOutputMetadataWaitGroupID] = waitGroupID
	}
	if m.ApprovalSurface {
		metadata[TaskOutputMetadataApprovalUI] = true
	}
	return metadata
}

func (m TaskOutputBlockingMetadata) IsApproval() bool {
	return strings.EqualFold(strings.TrimSpace(m.Kind), TaskOutputBlockingKindApproval)
}

func (o *TaskOutput) Blocking() TaskOutputBlockingMetadata {
	if o == nil {
		return TaskOutputBlockingMetadata{}
	}
	return TaskOutputBlockingMetadata{
		BlockerID:       o.MetadataString(TaskOutputMetadataBlockerID),
		Kind:            o.MetadataString(TaskOutputMetadataBlockingKind),
		InputKind:       InputKind(o.MetadataString(TaskOutputMetadataInputKind)),
		WaitGroupID:     o.MetadataString(TaskOutputMetadataWaitGroupID),
		ApprovalSurface: taskOutputMapBool(o.Metadata, TaskOutputMetadataApprovalUI),
	}
}

func (o *TaskOutput) SetBlocking(blocking TaskOutputBlockingMetadata) {
	if o == nil {
		return
	}
	o.Metadata = blocking.Apply(o.Metadata)
}

func (o *TaskOutput) MetadataString(keys ...string) string {
	if o == nil {
		return ""
	}
	return taskOutputMapString(o.Metadata, keys...)
}

func (o *TaskOutput) DataString(keys ...string) string {
	if o == nil {
		return ""
	}
	return taskOutputMapString(o.Data, keys...)
}

func (o *TaskOutput) ArtifactKey() string {
	return o.MetadataString(TaskOutputMetadataArtifactKey)
}

func (o *TaskOutput) ArtifactLabel() string {
	return o.MetadataString(TaskOutputMetadataArtifactLabel)
}

func (o *TaskOutput) ArtifactKind() string {
	return o.MetadataString(TaskOutputMetadataArtifactKind)
}

func (o *TaskOutput) ArtifactRole() string {
	switch strings.TrimSpace(strings.ToLower(o.MetadataString(TaskOutputMetadataArtifactRole))) {
	case TaskOutputArtifactRolePrimary:
		return TaskOutputArtifactRolePrimary
	case TaskOutputArtifactRoleSupporting:
		return TaskOutputArtifactRoleSupporting
	case TaskOutputArtifactRoleIncidental:
		return TaskOutputArtifactRoleIncidental
	default:
		return ""
	}
}

func CanonicalArtifactLifecycleKey(artifactKey string) string {
	artifactKey = normalizeTaskOutputArtifactToken(artifactKey)
	if artifactKey == "" {
		return ""
	}
	tokens := strings.Split(artifactKey, "-")
	filtered := make([]string, 0, len(tokens))
	for _, token := range tokens {
		token = normalizeTaskOutputArtifactToken(token)
		if token == "" {
			continue
		}
		if _, lifecycle := taskOutputArtifactLifecycleTokens[token]; lifecycle {
			continue
		}
		filtered = append(filtered, token)
	}
	if len(filtered) == 0 {
		return artifactKey
	}
	return strings.Join(filtered, "-")
}

func CanonicalArtifactFamilyKey(artifactKey, artifactKind, outputType string) string {
	key := CanonicalArtifactLifecycleKey(artifactKey)
	kind := normalizeTaskOutputArtifactToken(artifactKind)
	outputType = normalizeTaskOutputArtifactToken(outputType)

	if base := firstNonEmptyTaskOutputArtifactFamily(kind, outputType); base != "" {
		if key == "" {
			return base
		}
		if taskOutputArtifactTokenSubset(base, key) || taskOutputArtifactTokenSubset(key, base) {
			return base
		}
	}
	if key != "" {
		return key
	}
	if kind != "" {
		return kind
	}
	return outputType
}

func (o *TaskOutput) WaitGroupID() string {
	return o.Blocking().WaitGroupID
}

func (o *TaskOutput) HasApprovalSurface() bool {
	return o.Blocking().ApprovalSurface
}

func (o *TaskOutput) IsPending() bool {
	return o != nil && strings.EqualFold(strings.TrimSpace(o.Status), TaskOutputStatusPending)
}

func (o *TaskOutput) IsApprovalArtifact() bool {
	if o == nil || !o.IsPending() {
		return false
	}
	blocking := o.Blocking()
	return blocking.IsApproval() ||
		blocking.InputKind == InputKindApproveReject ||
		blocking.ApprovalSurface
}

func (o *TaskOutput) IsDraftEmail() bool {
	if o == nil || strings.TrimSpace(o.OutputType) != TaskOutputTypeEmail {
		return false
	}
	hasDraftID := o.DataString("draft_id", "draftId") != "" ||
		o.MetadataString("draft_id", "draftId") != ""
	if !hasDraftID {
		return false
	}
	hasMessageID := o.DataString("message_id", "messageId") != "" ||
		o.MetadataString("message_id", "messageId") != ""
	return !hasMessageID
}

func (o *TaskOutput) ShouldHideInWorkspace() bool {
	if o == nil {
		return false
	}
	role := o.ArtifactRole()
	if role == TaskOutputArtifactRoleIncidental || role == TaskOutputArtifactRoleSupporting {
		return true
	}
	if strings.TrimSpace(o.OutputType) != TaskOutputTypeEmail {
		return false
	}
	if o.HasApprovalSurface() || o.IsPending() {
		return true
	}
	return o.IsDraftEmail()
}

type TaskBlockerPayload struct {
	InputKind InputKind      `json:"-"`
	Summary   string         `json:"summary,omitempty"`
	Details   string         `json:"details,omitempty"`
	Fields    map[string]any `json:"-"`
}

func NewTaskBlockerPayload(inputKind InputKind, waitingSummary, assistantMessage string) TaskBlockerPayload {
	payload := TaskBlockerPayload{
		InputKind: inputKind,
		Summary:   strings.TrimSpace(waitingSummary),
		Details:   strings.TrimSpace(assistantMessage),
	}
	if payload.Summary == "" {
		return payload
	}

	var parsed map[string]any
	if json.Unmarshal([]byte(payload.Summary), &parsed) != nil || len(parsed) == 0 {
		return payload
	}
	if payload.Details != "" {
		if _, ok := parsed["details"]; !ok {
			parsed["details"] = payload.Details
		}
	}

	decoded := ParseTaskBlockerPayload(parsed)
	decoded.InputKind = inputKind
	return decoded
}

func ParseTaskBlockerPayload(values map[string]any) TaskBlockerPayload {
	if len(values) == 0 {
		return TaskBlockerPayload{}
	}
	cloned := cloneTaskOutputMap(values)
	return TaskBlockerPayload{
		Summary: taskOutputMapString(cloned, "summary"),
		Details: taskOutputMapString(cloned, "details"),
		Fields:  cloned,
	}
}

func (p TaskBlockerPayload) ToMap() map[string]any {
	if len(p.Fields) > 0 {
		return cloneTaskOutputMap(p.Fields)
	}

	summary := strings.TrimSpace(p.Summary)
	details := strings.TrimSpace(p.Details)
	payload := map[string]any{}

	switch p.InputKind {
	case InputKindApproveReject:
		if summary != "" {
			payload["summary"] = summary
		}
		if details != "" {
			payload["details"] = details
		}
	default:
		if details != "" {
			payload["details"] = details
		} else if summary != "" {
			payload["details"] = summary
		}
		if summary != "" && summary != details {
			payload["summary"] = summary
		}
	}

	return payload
}

var taskOutputArtifactLifecycleTokens = map[string]struct{}{
	"approval":  {},
	"approved":  {},
	"blocked":   {},
	"cancelled": {},
	"canceled":  {},
	"complete":  {},
	"completed": {},
	"delivered": {},
	"draft":     {},
	"executed":  {},
	"final":     {},
	"finalized": {},
	"pending":   {},
	"queued":    {},
	"rejected":  {},
	"revised":   {},
	"revision":  {},
	"sent":      {},
}

func normalizeTaskOutputArtifactToken(value string) string {
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

func taskOutputArtifactTokenSubset(sub, super string) bool {
	sub = normalizeTaskOutputArtifactToken(sub)
	super = normalizeTaskOutputArtifactToken(super)
	if sub == "" || super == "" {
		return false
	}
	subTokens := strings.Split(sub, "-")
	superSet := make(map[string]struct{}, len(strings.Split(super, "-")))
	for _, token := range strings.Split(super, "-") {
		superSet[token] = struct{}{}
	}
	for _, token := range subTokens {
		if _, ok := superSet[token]; !ok {
			return false
		}
	}
	return true
}

func firstNonEmptyTaskOutputArtifactFamily(values ...string) string {
	for _, value := range values {
		value = normalizeTaskOutputArtifactToken(value)
		if value != "" {
			return value
		}
	}
	return ""
}

type ResolvedBlockerItem struct {
	OutputID string `json:"output_id"`
	Title    string `json:"title,omitempty"`
	ItemKey  string `json:"item_key,omitempty"`
}

type ResolvedBlocker struct {
	ID              string                `json:"id,omitempty"`
	Kind            string                `json:"kind,omitempty"`
	InputKind       string                `json:"input_kind,omitempty"`
	Status          string                `json:"status,omitempty"`
	WaitGroupID     string                `json:"wait_group_id,omitempty"`
	OutputID        string                `json:"output_id,omitempty"`
	OutputStatus    string                `json:"output_status,omitempty"`
	OutputIDs       []string              `json:"output_ids,omitempty"`
	ApprovalSurface bool                  `json:"approval_surface,omitempty"`
	Summary         string                `json:"summary,omitempty"`
	Details         string                `json:"details,omitempty"`
	Items           []ResolvedBlockerItem `json:"items,omitempty"`
	PayloadJSON     map[string]any        `json:"payload_json,omitempty"`
}

func taskOutputMapString(values map[string]any, keys ...string) string {
	for _, key := range keys {
		if len(values) == 0 || strings.TrimSpace(key) == "" {
			continue
		}
		value, ok := values[key]
		if !ok {
			continue
		}
		if text, ok := value.(string); ok && strings.TrimSpace(text) != "" {
			return strings.TrimSpace(text)
		}
	}
	return ""
}

func taskOutputMapBool(values map[string]any, key string) bool {
	if len(values) == 0 || strings.TrimSpace(key) == "" {
		return false
	}
	value, ok := values[key]
	if !ok {
		return false
	}
	switch typed := value.(type) {
	case bool:
		return typed
	case string:
		switch strings.ToLower(strings.TrimSpace(typed)) {
		case "1", "true", "yes", "on":
			return true
		default:
			return false
		}
	default:
		return false
	}
}

func cloneTaskOutputMap(values map[string]any) map[string]any {
	if len(values) == 0 {
		return map[string]any{}
	}
	cloned := make(map[string]any, len(values))
	for key, value := range values {
		cloned[key] = value
	}
	return cloned
}
