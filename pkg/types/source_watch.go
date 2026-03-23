package types

import (
	"fmt"
	"strings"
)

const DefaultSourceWatchEventType = "fs.create"

type SourceWatchBlockerEntry struct {
	Integration    string
	Path           string
	EntityLabel    string
	EntityKey      string
	SourceOutputID string
	ThreadID       string
	MessageID      string
}

func CanonicalizeSourceWatchRequest(req *SourceWatchRequest) *SourceWatchRequest {
	if req == nil {
		return nil
	}

	normalized := *req
	normalized.Integration = strings.TrimSpace(normalized.Integration)
	normalized.Reason = strings.TrimSpace(normalized.Reason)
	normalized.Query = strings.TrimSpace(normalized.Query)
	normalized.FilenameFormat = strings.TrimSpace(normalized.FilenameFormat)
	normalized.EntityKey = strings.TrimSpace(normalized.EntityKey)
	normalized.EntityLabel = strings.TrimSpace(normalized.EntityLabel)
	normalized.SourceOutputID = strings.TrimSpace(normalized.SourceOutputID)
	normalized.ThreadID = strings.TrimSpace(normalized.ThreadID)
	normalized.MessageID = strings.TrimSpace(normalized.MessageID)
	normalized.EventTypes = NormalizeSourceWatchEventTypes(normalized.EventTypes)
	return &normalized
}

func NormalizeSourceWatchRequest(req *SourceWatchRequest) *SourceWatchRequest {
	normalized := CanonicalizeSourceWatchRequest(req)
	if normalized == nil {
		return nil
	}
	if normalized.Integration == "" {
		return nil
	}
	if normalized.Query == "" && normalized.ThreadID == "" && normalized.MessageID == "" {
		return nil
	}
	return normalized
}

func NormalizeSourceWatchEventTypes(eventTypes []string) []string {
	if len(eventTypes) == 0 {
		return []string{DefaultSourceWatchEventType}
	}

	normalized := make([]string, 0, len(eventTypes))
	seen := make(map[string]struct{}, len(eventTypes))
	for _, raw := range eventTypes {
		eventType := strings.TrimSpace(raw)
		if eventType == "" {
			continue
		}
		if _, exists := seen[eventType]; exists {
			continue
		}
		seen[eventType] = struct{}{}
		normalized = append(normalized, eventType)
	}
	if len(normalized) == 0 {
		return []string{DefaultSourceWatchEventType}
	}
	return normalized
}

func MergeSourceWatchRequests(base, incoming *SourceWatchRequest) *SourceWatchRequest {
	base = CanonicalizeSourceWatchRequest(base)
	incoming = CanonicalizeSourceWatchRequest(incoming)
	if base == nil {
		return NormalizeSourceWatchRequest(incoming)
	}
	if incoming == nil {
		return NormalizeSourceWatchRequest(base)
	}

	merged := *base
	merged.Reason = firstNonEmptySourceWatchValue(incoming.Reason, base.Reason)
	merged.Query = firstNonEmptySourceWatchValue(base.Query, incoming.Query)
	merged.FilenameFormat = firstNonEmptySourceWatchValue(base.FilenameFormat, incoming.FilenameFormat)
	merged.EntityKey = firstNonEmptySourceWatchValue(base.EntityKey, incoming.EntityKey)
	merged.EntityLabel = firstNonEmptySourceWatchValue(base.EntityLabel, incoming.EntityLabel)
	merged.SourceOutputID = firstNonEmptySourceWatchValue(base.SourceOutputID, incoming.SourceOutputID)
	merged.ThreadID = firstNonEmptySourceWatchValue(base.ThreadID, incoming.ThreadID)
	merged.MessageID = firstNonEmptySourceWatchValue(base.MessageID, incoming.MessageID)
	merged.IncludeAttachments = base.IncludeAttachments || incoming.IncludeAttachments
	merged.IncludeInline = base.IncludeInline || incoming.IncludeInline
	merged.IncludeMessageBody = base.IncludeMessageBody || incoming.IncludeMessageBody
	merged.EventTypes = NormalizeSourceWatchEventTypes(append(append([]string{}, base.EventTypes...), incoming.EventTypes...))
	return NormalizeSourceWatchRequest(&merged)
}

func SourceWatchRequestMergeKey(req *SourceWatchRequest) string {
	normalized := NormalizeSourceWatchRequest(req)
	if normalized == nil {
		return ""
	}
	return strings.Join([]string{
		strings.ToLower(normalized.Integration),
		normalized.EntityKey,
		normalized.ThreadID,
		normalized.MessageID,
		normalized.Query,
	}, "\x00")
}

func SourceWatchRequestSignature(req *SourceWatchRequest) string {
	normalized := NormalizeSourceWatchRequest(req)
	if normalized == nil {
		return ""
	}
	return strings.Join([]string{
		strings.ToLower(normalized.Integration),
		normalized.Query,
		normalized.FilenameFormat,
		strings.Join(normalized.EventTypes, ","),
		normalized.EntityKey,
		normalized.EntityLabel,
		normalized.SourceOutputID,
		normalized.ThreadID,
		normalized.MessageID,
		fmt.Sprintf("%t", normalized.IncludeAttachments),
		fmt.Sprintf("%t", normalized.IncludeInline),
		fmt.Sprintf("%t", normalized.IncludeMessageBody),
	}, "\x00")
}

func NewSourceWatchBlockerSpec(summary, details string, entries []SourceWatchBlockerEntry) *TaskBlockerSpec {
	summary = strings.TrimSpace(summary)
	details = strings.TrimSpace(details)
	if summary == "" && details == "" && len(entries) == 0 {
		return nil
	}

	payload := NewTaskBlockerPayload(InputKindFreeText, summary, details).ToMap()
	payload["source_watch_count"] = len(entries)
	if len(entries) > 0 {
		watches := make([]map[string]any, 0, len(entries))
		for _, entry := range entries {
			watches = append(watches, map[string]any{
				"integration":      strings.TrimSpace(entry.Integration),
				"path":             strings.TrimSpace(entry.Path),
				"entity_label":     strings.TrimSpace(entry.EntityLabel),
				"entity_key":       strings.TrimSpace(entry.EntityKey),
				"source_output_id": strings.TrimSpace(entry.SourceOutputID),
				"thread_id":        strings.TrimSpace(entry.ThreadID),
				"message_id":       strings.TrimSpace(entry.MessageID),
			})
		}
		payload["source_watches"] = watches
	}

	return &TaskBlockerSpec{
		Kind:        TaskBlockerKindInput,
		InputKind:   InputKindFreeText,
		PayloadJSON: payload,
	}
}

func firstNonEmptySourceWatchValue(values ...string) string {
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			return trimmed
		}
	}
	return ""
}
