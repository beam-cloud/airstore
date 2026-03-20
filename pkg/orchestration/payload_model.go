package orchestration

import (
	"encoding/json"
	"strings"

	"github.com/beam-cloud/airstore/pkg/types"
)

// ResumeDirective captures the session-resume metadata that used to be spread
// across several raw payload keys.
type ResumeDirective struct {
	Enabled         bool
	ExcludeRunID    string
	CheckpointRunID string
}

func parseResumeDirective(values map[string]any) ResumeDirective {
	if len(values) == 0 {
		return ResumeDirective{}
	}
	return ResumeDirective{
		Enabled:         boolFromAny(values[types.OrchestrationOutboxPayloadResumeSession]),
		ExcludeRunID:    strings.TrimSpace(streamValueAsString(values, types.OrchestrationOutboxPayloadResumeExcludeRunID)),
		CheckpointRunID: strings.TrimSpace(streamValueAsString(values, types.OrchestrationOutboxPayloadResumeCheckpointRunID)),
	}
}

func (r ResumeDirective) apply(values map[string]any) {
	if values == nil {
		return
	}
	if r.Enabled {
		values[types.OrchestrationOutboxPayloadResumeSession] = true
	}
	if excludeRunID := strings.TrimSpace(r.ExcludeRunID); excludeRunID != "" {
		values[types.OrchestrationOutboxPayloadResumeExcludeRunID] = excludeRunID
	}
	if checkpointRunID := strings.TrimSpace(r.CheckpointRunID); checkpointRunID != "" {
		values[types.OrchestrationOutboxPayloadResumeCheckpointRunID] = checkpointRunID
	}
}

func (r ResumeDirective) excludeRunIDs() []string {
	if strings.TrimSpace(r.ExcludeRunID) == "" {
		return nil
	}
	return []string{strings.TrimSpace(r.ExcludeRunID)}
}

type WakeDirective struct {
	DelayMinutes   int
	Reason         string
	FollowUpPrompt string
	Agenda         []*types.TaskWakeAgendaItem
}

func parseWakeDirective(values map[string]any) WakeDirective {
	if len(values) == 0 {
		return WakeDirective{}
	}
	return WakeDirective{
		DelayMinutes:   intFromAny(values[types.OrchestrationOutboxPayloadWakeDelayMinutes]),
		Reason:         streamValueAsString(values, types.OrchestrationOutboxPayloadWakeReason),
		FollowUpPrompt: streamValueAsString(values, types.OrchestrationOutboxPayloadWakeFollowUpPrompt),
		Agenda:         parseWakeAgendaPayload(streamValueAsString(values, types.OrchestrationOutboxPayloadWakeAgenda)),
	}
}

func parseWakeAgendaPayload(raw string) []*types.TaskWakeAgendaItem {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}

	type wakeAgendaPayloadItem struct {
		Type   string `json:"type"`
		Title  string `json:"title"`
		Reason string `json:"reason"`
	}

	var payload []wakeAgendaPayloadItem
	if err := json.Unmarshal([]byte(raw), &payload); err != nil {
		return nil
	}

	items := make([]*types.TaskWakeAgendaItem, 0, len(payload))
	for idx, item := range payload {
		title := strings.TrimSpace(item.Title)
		reason := strings.TrimSpace(item.Reason)
		if title == "" && reason == "" {
			continue
		}
		items = append(items, &types.TaskWakeAgendaItem{
			Seq:    idx + 1,
			Type:   strings.TrimSpace(item.Type),
			Title:  title,
			Reason: reason,
		})
	}
	return items
}

func parseSubtaskRequestsPayload(raw string) []*types.SubtaskRequest {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	var reqs []*types.SubtaskRequest
	if err := json.Unmarshal([]byte(raw), &reqs); err != nil {
		return nil
	}
	filtered := reqs[:0]
	for _, r := range reqs {
		if strings.TrimSpace(r.Prompt) != "" {
			filtered = append(filtered, r)
		}
	}
	return filtered
}

func (w WakeDirective) signal() *types.RunExecutionWakeSignal {
	if w.DelayMinutes <= 0 && strings.TrimSpace(w.Reason) == "" && strings.TrimSpace(w.FollowUpPrompt) == "" && len(w.Agenda) == 0 {
		return nil
	}
	return &types.RunExecutionWakeSignal{
		DelayMinutes:   w.DelayMinutes,
		Reason:         strings.TrimSpace(w.Reason),
		FollowUpPrompt: strings.TrimSpace(w.FollowUpPrompt),
		WakeAgenda:     w.Agenda,
	}
}

func (w WakeDirective) apply(values map[string]any) {
	if values == nil {
		return
	}
	if w.DelayMinutes > 0 {
		values[types.OrchestrationOutboxPayloadWakeDelayMinutes] = w.DelayMinutes
	}
	if reason := strings.TrimSpace(w.Reason); reason != "" {
		values[types.OrchestrationOutboxPayloadWakeReason] = reason
	}
	if prompt := strings.TrimSpace(w.FollowUpPrompt); prompt != "" {
		values[types.OrchestrationOutboxPayloadWakeFollowUpPrompt] = prompt
	}
	if len(w.Agenda) > 0 {
		if body, err := json.Marshal(w.Agenda); err == nil {
			values[types.OrchestrationOutboxPayloadWakeAgenda] = string(body)
		}
	}
}

type TaskCommandPayload struct {
	Message           string
	Prompt            string
	OriginalMessage   string
	SessionID         string
	SessionKey        *string
	AgentID           *string
	HookID            *uint
	TimeoutMs         int
	Policy            RunExecutionPolicy
	Lane              *string
	ExtraSystemPrompt *string
	InputProvenance   *InputProvenance
	Deliver           *bool
	Attachments       []map[string]any
	InstanceKey       string
	Label             *string
	SpawnedBy         *string
	Priority          string
	Provider          *string
	Model             *string
	AgentConfig       map[string]any
	Resume            ResumeDirective
}

func newTaskCommandPayload(
	params AgentCommandParams,
	runPolicy RunExecutionPolicy,
	instanceKey string,
	agentConfig map[string]any,
	agentProvider string,
	agentModel string,
) TaskCommandPayload {
	return TaskCommandPayload{
		Message:           strings.TrimSpace(params.Message),
		SessionID:         strings.TrimSpace(params.SessionID),
		SessionKey:        trimOptionalString(params.SessionKey),
		AgentID:           trimOptionalString(params.AgentID),
		HookID:            params.HookID,
		TimeoutMs:         timeoutOrDefault(params.TimeoutMs, 600000),
		Policy:            runPolicy,
		Lane:              trimOptionalString(params.Lane),
		ExtraSystemPrompt: trimOptionalString(params.ExtraSystemPrompt),
		InputProvenance:   params.InputProvenance,
		Deliver:           params.Deliver,
		Attachments:       cloneAttachmentMaps(params.Attachments),
		InstanceKey:       strings.TrimSpace(instanceKey),
		Label:             trimOptionalString(params.Label),
		SpawnedBy:         trimOptionalString(params.SpawnedBy),
		Priority:          strings.TrimSpace(params.Priority),
		Provider:          strPtrMaybe(agentProvider),
		Model:             strPtrMaybe(agentModel),
		AgentConfig:       cloneAnyMap(agentConfig),
	}
}

func parseTaskCommandPayload(payload map[string]any) TaskCommandPayload {
	provider, model := providerModelFromPayload(payload)
	return TaskCommandPayload{
		Message:           strings.TrimSpace(stringFromPayload(payload, "message")),
		Prompt:            strings.TrimSpace(stringFromPayload(payload, "prompt")),
		OriginalMessage:   strings.TrimSpace(stringFromPayload(payload, "original_message")),
		SessionID:         strings.TrimSpace(stringFromPayload(payload, "session_id")),
		SessionKey:        strPtrMaybe(stringFromPayload(payload, "session_key")),
		AgentID:           strPtrMaybe(stringFromPayload(payload, types.AgentExecutionMetaKeyAgentID)),
		HookID:            uintPtrFromPayload(payload, "hook_id"),
		TimeoutMs:         intFromPayload(payload, "timeout_ms", 600000),
		Policy:            runPolicyFromPayload(payload),
		Lane:              strPtrMaybe(stringFromPayload(payload, "lane")),
		ExtraSystemPrompt: strPtrMaybe(stringFromPayload(payload, "extra_system_prompt")),
		InputProvenance:   decodeInputProvenance(payload["input_provenance"]),
		Deliver:           decodeOptionalBool(payload["deliver"]),
		Attachments:       decodeAttachmentMaps(payload["attachments"]),
		InstanceKey:       strings.TrimSpace(stringFromPayload(payload, types.AgentExecutionMetaKeyInstanceKey)),
		Label:             strPtrMaybe(stringFromPayload(payload, "label")),
		SpawnedBy:         strPtrMaybe(stringFromPayload(payload, "spawned_by")),
		Priority:          strings.TrimSpace(stringFromPayload(payload, "priority")),
		Provider:          provider,
		Model:             model,
		AgentConfig:       mapFromPayload(payload, agentPayloadKeyAgentConfig),
		Resume:            parseResumeDirective(payload),
	}
}

func (p TaskCommandPayload) PromptText() string {
	if prompt := strings.TrimSpace(p.Prompt); prompt != "" {
		return prompt
	}
	return strings.TrimSpace(p.Message)
}

func (p TaskCommandPayload) ToMap() map[string]any {
	payload := map[string]any{
		"message":    strings.TrimSpace(p.Message),
		"session_id": strings.TrimSpace(p.SessionID),
		"timeout_ms": p.TimeoutMs,
		"policy":     NormalizeRunExecutionPolicy(p.Policy),
	}
	if prompt := strings.TrimSpace(p.Prompt); prompt != "" {
		payload["prompt"] = prompt
	}
	if original := strings.TrimSpace(p.OriginalMessage); original != "" {
		payload["original_message"] = original
	}
	if p.SessionKey != nil {
		payload["session_key"] = strings.TrimSpace(*p.SessionKey)
	}
	if p.AgentID != nil {
		payload[types.AgentExecutionMetaKeyAgentID] = strings.TrimSpace(*p.AgentID)
	}
	if p.HookID != nil {
		payload["hook_id"] = *p.HookID
	}
	if p.Lane != nil {
		payload["lane"] = strings.TrimSpace(*p.Lane)
	}
	if p.ExtraSystemPrompt != nil {
		payload["extra_system_prompt"] = strings.TrimSpace(*p.ExtraSystemPrompt)
	}
	if p.InputProvenance != nil {
		payload["input_provenance"] = p.InputProvenance
	}
	if p.Deliver != nil {
		payload["deliver"] = *p.Deliver
	}
	if len(p.Attachments) > 0 {
		payload["attachments"] = cloneAttachmentMaps(p.Attachments)
	}
	if instanceKey := strings.TrimSpace(p.InstanceKey); instanceKey != "" {
		payload[types.AgentExecutionMetaKeyInstanceKey] = instanceKey
	}
	if p.Label != nil {
		payload["label"] = strings.TrimSpace(*p.Label)
	}
	if p.SpawnedBy != nil {
		payload["spawned_by"] = strings.TrimSpace(*p.SpawnedBy)
	}
	if priority := strings.TrimSpace(p.Priority); priority != "" {
		payload["priority"] = priority
	}
	if p.Provider != nil {
		payload[agentConfigKeyProvider] = strings.TrimSpace(*p.Provider)
	}
	if p.Model != nil {
		payload[agentConfigKeyModel] = strings.TrimSpace(*p.Model)
	}
	if len(p.AgentConfig) > 0 {
		payload[agentPayloadKeyAgentConfig] = cloneAnyMap(p.AgentConfig)
	}
	p.Resume.apply(payload)
	return payload
}

type DispatchEnvelope struct {
	TaskID       string
	Prompt       string
	Reason       string
	RetryDelayMs int
	RetryAttempt int
	Resume       ResumeDirective
	Wake         WakeDirective
}

func parseDispatchEnvelope(values map[string]any) DispatchEnvelope {
	return DispatchEnvelope{
		TaskID:       strings.TrimSpace(streamValueAsString(values, types.OrchestrationOutboxPayloadTaskID)),
		Prompt:       dispatchPromptFromValues(values),
		Reason:       strings.TrimSpace(streamValueAsString(values, types.OrchestrationOutboxPayloadReason)),
		RetryDelayMs: intFromAny(values[types.OrchestrationOutboxPayloadRetryDelay]),
		RetryAttempt: intFromAny(values[types.OrchestrationOutboxPayloadDispatchAttempt]),
		Resume:       parseResumeDirective(values),
		Wake:         parseWakeDirective(values),
	}
}

func (e DispatchEnvelope) ToMap() map[string]any {
	values := map[string]any{}
	if taskID := strings.TrimSpace(e.TaskID); taskID != "" {
		values[types.OrchestrationOutboxPayloadTaskID] = taskID
	}
	if prompt := strings.TrimSpace(e.Prompt); prompt != "" {
		values[types.OrchestrationOutboxPayloadDispatchPrompt] = prompt
	}
	if reason := strings.TrimSpace(e.Reason); reason != "" {
		values[types.OrchestrationOutboxPayloadReason] = reason
	}
	if e.RetryDelayMs > 0 {
		values[types.OrchestrationOutboxPayloadRetryDelay] = e.RetryDelayMs
	}
	if e.RetryAttempt > 0 {
		values[types.OrchestrationOutboxPayloadDispatchAttempt] = e.RetryAttempt
	}
	e.Resume.apply(values)
	e.Wake.apply(values)
	return values
}

type RunResultEnvelope struct {
	TaskID          string
	AttemptID       string
	ExitCode        int
	ErrorText       string
	ResultKey       string
	RetryAttempt    int
	WaitingForInput bool
	Wake            WakeDirective
	SubtaskRequests []*types.SubtaskRequest
}

func parseRunResultEnvelope(values map[string]any) RunResultEnvelope {
	return RunResultEnvelope{
		TaskID:          strings.TrimSpace(streamValueAsString(values, types.OrchestrationOutboxPayloadTaskID)),
		AttemptID:       strings.TrimSpace(streamValueAsString(values, types.OrchestrationOutboxPayloadAttemptID)),
		ExitCode:        intFromAny(values[types.OrchestrationOutboxPayloadExitCode]),
		ErrorText:       streamValueAsString(values, types.OrchestrationOutboxPayloadError),
		ResultKey:       strings.TrimSpace(streamValueAsString(values, types.OrchestrationOutboxPayloadIdempotency)),
		RetryAttempt:    intFromAny(values[types.OrchestrationOutboxPayloadDispatchAttempt]),
		WaitingForInput: boolFromAny(values[types.OrchestrationOutboxPayloadWaitingForInput]),
		Wake:            parseWakeDirective(values),
		SubtaskRequests: parseSubtaskRequestsPayload(streamValueAsString(values, types.OrchestrationOutboxPayloadSubtaskRequests)),
	}
}

func (e RunResultEnvelope) ToMap() map[string]any {
	values := map[string]any{
		types.OrchestrationOutboxPayloadTaskID:          strings.TrimSpace(e.TaskID),
		types.OrchestrationOutboxPayloadAttemptID:       strings.TrimSpace(e.AttemptID),
		types.OrchestrationOutboxPayloadExitCode:        e.ExitCode,
		types.OrchestrationOutboxPayloadWaitingForInput: e.WaitingForInput,
	}
	if errText := strings.TrimSpace(e.ErrorText); errText != "" {
		values[types.OrchestrationOutboxPayloadError] = errText
	}
	if resultKey := strings.TrimSpace(e.ResultKey); resultKey != "" {
		values[types.OrchestrationOutboxPayloadIdempotency] = resultKey
	}
	if e.RetryAttempt > 0 {
		values[types.OrchestrationOutboxPayloadDispatchAttempt] = e.RetryAttempt
	}
	e.Wake.apply(values)
	if len(e.SubtaskRequests) > 0 {
		if body, err := json.Marshal(e.SubtaskRequests); err == nil {
			values[types.OrchestrationOutboxPayloadSubtaskRequests] = string(body)
		}
	}
	return values
}

func decodeInputProvenance(value any) *InputProvenance {
	if value == nil {
		return nil
	}
	raw, err := json.Marshal(value)
	if err != nil {
		return nil
	}
	var decoded InputProvenance
	if err := json.Unmarshal(raw, &decoded); err != nil {
		return nil
	}
	decoded.Source = trimOptionalString(decoded.Source)
	decoded.MessageID = trimOptionalString(decoded.MessageID)
	decoded.Channel = trimOptionalString(decoded.Channel)
	decoded.ToolCallID = trimOptionalString(decoded.ToolCallID)
	decoded.CorrelationID = trimOptionalString(decoded.CorrelationID)
	if decoded.Source == nil && decoded.MessageID == nil && decoded.Channel == nil && decoded.ToolCallID == nil && decoded.CorrelationID == nil {
		return nil
	}
	return &decoded
}

func decodeOptionalBool(value any) *bool {
	switch typed := value.(type) {
	case bool:
		return &typed
	default:
		return nil
	}
}

func decodeAttachmentMaps(value any) []map[string]any {
	switch typed := value.(type) {
	case nil:
		return nil
	case []map[string]any:
		return cloneAttachmentMaps(typed)
	case []any:
		out := make([]map[string]any, 0, len(typed))
		for _, item := range typed {
			raw, err := json.Marshal(item)
			if err != nil {
				continue
			}
			var decoded map[string]any
			if err := json.Unmarshal(raw, &decoded); err != nil {
				continue
			}
			out = append(out, decoded)
		}
		return out
	default:
		return nil
	}
}

func cloneAttachmentMaps(values []map[string]any) []map[string]any {
	if len(values) == 0 {
		return nil
	}
	out := make([]map[string]any, 0, len(values))
	for _, value := range values {
		out = append(out, cloneAnyMap(value))
	}
	return out
}
