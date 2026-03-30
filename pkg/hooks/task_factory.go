package hooks

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/beam-cloud/airstore/pkg/orchestration"
	"github.com/beam-cloud/airstore/pkg/repository"
	"github.com/beam-cloud/airstore/pkg/types"
	"github.com/rs/zerolog/log"
)

const hookInputSource = "filesystem_hook"

// SourceWatchFinder looks up sleeping tasks that have registered interest
// in a particular integration entity via correlation keys (cross-workspace).
type SourceWatchFinder interface {
	FindTasksByCorrelationKeys(ctx context.Context, integration string, keys []string) ([]repository.TaskSourceWatchMatch, error)
}

// TaskFactory bridges hook events into the agent orchestration pipeline.
type TaskFactory struct {
	agents            *orchestration.AgentAPI
	sourceWatchFinder SourceWatchFinder
	contextEnricher   ContextEnricher
}

func NewTaskFactory(_ repository.BackendRepository, _ repository.TaskQueue, _ string, agents *orchestration.AgentAPI) *TaskFactory {
	return &TaskFactory{agents: agents}
}

func (f *TaskFactory) SetSourceWatchFinder(finder SourceWatchFinder) {
	if f != nil {
		f.sourceWatchFinder = finder
	}
}

func (f *TaskFactory) SetContextEnricher(enricher ContextEnricher) {
	if f != nil {
		f.contextEnricher = enricher
	}
}

// CreateTask implements hooks.TaskCreator.
func (f *TaskFactory) CreateTask(
	ctx context.Context,
	hook *types.Hook,
	eventID, event, prompt string,
	data map[string]any,
) error {
	if f.agents == nil {
		return fmt.Errorf("agent API is unavailable")
	}
	if hook == nil {
		return fmt.Errorf("hook is required")
	}
	if strings.TrimSpace(prompt) == "" {
		return fmt.Errorf("prompt is required")
	}

	normalizedEventID := strings.TrimSpace(eventID)
	if normalizedEventID == "" {
		normalizedEventID = fmt.Sprintf("%d", time.Now().UnixNano())
	}
	idempotencyKey := hookIdempotencyKey(hook.ExternalId, normalizedEventID, data)

	switch hook.DeliveryMode {
	case types.HookDeliveryModeTaskInput:
		return f.deliverTaskInput(ctx, hook, prompt, idempotencyKey)
	case "", types.HookDeliveryModeSpawnTask:
		return f.spawnTask(ctx, hook, normalizedEventID, event, prompt, data, idempotencyKey)
	default:
		return fmt.Errorf("unsupported hook delivery mode %q", hook.DeliveryMode)
	}
}

func (f *TaskFactory) spawnTask(
	ctx context.Context,
	hook *types.Hook,
	normalizedEventID, event, prompt string,
	data map[string]any,
	idempotencyKey string,
) error {
	if hook.AgentId == nil || strings.TrimSpace(*hook.AgentId) == "" {
		return fmt.Errorf("hook agent_id is required")
	}

	// Check if any sleeping tasks are watching for entities in this event.
	// If ALL correlation keys match sleeping tasks, wake them and skip spawn.
	if routed := f.routeToSleepingTasks(ctx, hook, data); routed {
		return nil
	}

	sessionID := hookSessionID(hook.ExternalId, normalizedEventID)
	lane := hookLane(hook.ExternalId, normalizedEventID)
	source := hookInputSource
	correlationID := normalizedEventID
	var correlationPtr *string
	if correlationID != "" {
		correlationPtr = &correlationID
	}
	label := fmt.Sprintf("Hook %s", hook.Path)
	spawnedBy := fmt.Sprintf("hook:%s", hook.ExternalId)
	hookID := hook.Id

	_, deduped, err := f.agents.AcceptAgentCommand(ctx, hook.WorkspaceId, orchestration.AgentCommandParams{
		Message:        prompt,
		AgentID:        hook.AgentId,
		SessionID:      sessionID,
		Lane:           &lane,
		IdempotencyKey: idempotencyKey,
		HookID:         &hookID,
		InputProvenance: &orchestration.InputProvenance{
			Source:        &source,
			CorrelationID: correlationPtr,
		},
		Label:     &label,
		SpawnedBy: &spawnedBy,
		Attachments: []map[string]any{
			buildHookAttachment(hook, event, data),
		},
	})
	if err != nil {
		return fmt.Errorf("accept hook task: %w", err)
	}
	if deduped {
		log.Debug().Str("hook", hook.ExternalId).Str("event_id", normalizedEventID).Msg("hook task deduped")
	}
	return nil
}

// routeToSleepingTasks checks if any sleeping tasks have registered
// source watches matching this event's correlation keys. If matches
// are found, delivers input to wake those tasks and returns true if
// ALL keys were matched (meaning no new task spawn is needed).
func (f *TaskFactory) routeToSleepingTasks(ctx context.Context, hook *types.Hook, data map[string]any) bool {
	if f.sourceWatchFinder == nil || f.agents == nil {
		return false
	}

	integration := strings.ToLower(strings.TrimSpace(anyToString(data["integration"])))
	keysCSV := strings.TrimSpace(anyToString(data["correlation_keys"]))
	if integration == "" || keysCSV == "" {
		return false
	}

	keys := splitCorrelationKeys(keysCSV)
	if len(keys) == 0 {
		return false
	}

	matches, err := f.sourceWatchFinder.FindTasksByCorrelationKeys(ctx, integration, keys)
	if err != nil {
		log.Warn().Err(err).Str("integration", integration).
			Msg("source watch lookup failed, falling through to spawn")
		return false
	}
	if len(matches) == 0 {
		return false
	}

	newItems := strings.TrimSpace(anyToString(data["new_items"]))
	matchedKeys := make(map[string]struct{}, len(matches))
	wokeCount := 0
	for _, match := range matches {
		if _, done := matchedKeys[match.CorrelationKey]; done {
			continue
		}
		matchedKeys[match.CorrelationKey] = struct{}{}

		wakePrompt := buildSourceWakePrompt(match, integration, newItems)
		if f.contextEnricher != nil {
			if content := f.contextEnricher.FetchSourceContent(ctx, match.WorkspaceID, integration, data); content != "" {
				wakePrompt = wakePrompt + "\n\n" + content
			}
			if viewRows := f.contextEnricher.FetchViewRows(ctx, match.WorkspaceID, match.TaskID); viewRows != "" {
				wakePrompt = wakePrompt + "\n\n" + viewRows
			}
		}
		wakeIdempotency := fmt.Sprintf("source_wake:%s:%s:%s",
			match.TaskID, match.CorrelationKey, anyToString(data["new_items_hash"]))

		_, err := f.agents.SubmitTaskInput(
			ctx,
			match.WorkspaceID,
			match.TaskID,
			types.InputKindFreeText,
			nil,
			wakePrompt,
			wakeIdempotency,
			nil,
		)
		if err != nil {
			log.Warn().Err(err).
				Str("task_id", match.TaskID).Str("correlation_key", match.CorrelationKey).
				Msg("failed to wake sleeping task via source watch")
			continue
		}
		wokeCount++
		log.Info().
			Str("task_id", match.TaskID).Str("integration", integration).
			Str("correlation_key", match.CorrelationKey).Str("reason", match.Reason).
			Msg("woke sleeping task via source watch correlation")
	}

	allMatched := len(matchedKeys) >= len(keys)
	if wokeCount > 0 && allMatched {
		log.Info().
			Int("woke", wokeCount).Int("keys", len(keys)).
			Str("integration", integration).
			Msg("all event correlation keys matched sleeping tasks, skipping spawn")
		return true
	}
	return false
}

func splitCorrelationKeys(csv string) []string {
	raw := strings.Split(csv, ",")
	out := make([]string, 0, len(raw))
	for _, s := range raw {
		s = strings.TrimSpace(s)
		if s != "" {
			out = append(out, s)
		}
	}
	return out
}

func buildSourceWakePrompt(match repository.TaskSourceWatchMatch, integration, newItems string) string {
	var b strings.Builder
	b.WriteString("New activity detected on ")
	b.WriteString(integration)
	b.WriteString(" entity you were watching.\n")
	if newItems != "" {
		b.WriteString("New items: ")
		b.WriteString(newItems)
		b.WriteString("\n")
	}
	if match.Reason != "" {
		b.WriteString("Original watch reason: ")
		b.WriteString(match.Reason)
		b.WriteString("\n")
	}
	if integration == "gmail" && match.CorrelationKey != "" {
		b.WriteString("Gmail Thread ID: ")
		b.WriteString(match.CorrelationKey)
		b.WriteString("\nIMPORTANT: Use --thread-id ")
		b.WriteString(match.CorrelationKey)
		b.WriteString(" when replying to keep the conversation in the same thread.\n")
	}
	b.WriteString("Resume your task and process the new information.")
	return b.String()
}

func (f *TaskFactory) deliverTaskInput(
	ctx context.Context,
	hook *types.Hook,
	prompt string,
	idempotencyKey string,
) error {
	if hook.TargetTaskID == nil || strings.TrimSpace(*hook.TargetTaskID) == "" {
		return fmt.Errorf("hook target_task_id is required for task input delivery")
	}
	_, err := f.agents.SubmitTaskInput(
		ctx,
		hook.WorkspaceId,
		strings.TrimSpace(*hook.TargetTaskID),
		types.InputKindFreeText,
		nil,
		prompt,
		idempotencyKey,
		nil,
	)
	if err != nil {
		return fmt.Errorf("deliver hook task input: %w", err)
	}
	return nil
}

func hookIdempotencyKey(hookExternalID, eventID string, data map[string]any) string {
	hookExternalID = strings.TrimSpace(hookExternalID)

	// Source-originated events carry new_items_hash or new_items; use
	// content-based keys so the same batch of items dedupes regardless of
	// the stream event ID.
	if key := sourceChangeIdempotencyKey(hookExternalID, data); key != "" {
		return compressHookIdempotencyKey(key)
	}

	eventID = strings.TrimSpace(eventID)
	key := fmt.Sprintf("hook:%s:%s", hookExternalID, eventID)
	return compressHookIdempotencyKey(key)
}

func compressHookIdempotencyKey(key string) string {
	if len(key) <= 180 {
		return key
	}
	sum := sha1.Sum([]byte(key))
	return "hook:" + hex.EncodeToString(sum[:])
}

func sourceChangeIdempotencyKey(hookExternalID string, data map[string]any) string {
	path := NormalizePath(strings.TrimSpace(anyToString(data["path"])))
	if path == "" {
		return ""
	}
	integration := strings.ToLower(strings.TrimSpace(anyToString(data["integration"])))

	hash := strings.TrimSpace(anyToString(data["new_items_hash"]))
	if hash == "" {
		hash = hashSourceItemsCSV(anyToString(data["new_items"]))
	}
	if hash == "" {
		return ""
	}
	return fmt.Sprintf("hook:%s:source:%s:%s:%s", hookExternalID, path, integration, hash)
}

func hashSourceItemsCSV(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}

	parts := strings.Split(raw, ",")
	seen := make(map[string]struct{}, len(parts))
	ids := make([]string, 0, len(parts))
	for _, part := range parts {
		id := strings.TrimSpace(part)
		if id == "" {
			continue
		}
		if _, exists := seen[id]; exists {
			continue
		}
		seen[id] = struct{}{}
		ids = append(ids, id)
	}
	if len(ids) == 0 {
		return ""
	}

	sort.Strings(ids)
	sum := sha1.Sum([]byte(strings.Join(ids, "\n")))
	return hex.EncodeToString(sum[:])
}

func hookSessionID(hookExternalID, eventID string) string {
	return compressHookScopedID("hook-session:", hookIsolationSeed(hookExternalID, eventID))
}

func hookLane(hookExternalID, eventID string) string {
	return compressHookScopedID("hook-lane:", hookIsolationSeed(hookExternalID, eventID))
}

func hookIsolationSeed(hookExternalID, eventID string) string {
	return fmt.Sprintf("%s:%s", strings.TrimSpace(hookExternalID), strings.TrimSpace(eventID))
}

func compressHookScopedID(prefix, seed string) string {
	raw := prefix + seed
	if len(raw) <= 180 {
		return raw
	}
	sum := sha1.Sum([]byte(raw))
	return prefix + hex.EncodeToString(sum[:])
}

func buildHookAttachment(hook *types.Hook, event string, data map[string]any) map[string]any {
	attachment := map[string]any{
		"type":             "hook_event",
		"hook_id":          hook.Id,
		"hook_external_id": hook.ExternalId,
		"path":             hook.Path,
		"event":            event,
	}
	for _, key := range []string{
		"integration",
		"new_count",
		"new_items",
		"new_items_hash",
		"removed_count",
		"removed_items",
		"path",
		"workspace_id",
		"old_path",
		"new_path",
		"move_op_id",
	} {
		if v, ok := data[key]; ok && v != nil {
			attachment[key] = v
		}
	}
	return attachment
}
