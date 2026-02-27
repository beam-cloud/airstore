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

// TaskFactory bridges hook events into the agent orchestration pipeline.
type TaskFactory struct {
	agents *orchestration.AgentAPI
}

func NewTaskFactory(_ repository.BackendRepository, _ repository.TaskQueue, _ string, agents *orchestration.AgentAPI) *TaskFactory {
	return &TaskFactory{agents: agents}
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
	if hook.AgentId == nil || strings.TrimSpace(*hook.AgentId) == "" {
		return fmt.Errorf("hook agent_id is required")
	}
	if strings.TrimSpace(prompt) == "" {
		return fmt.Errorf("prompt is required")
	}

	idempotencyKey := hookIdempotencyKey(hook.ExternalId, eventID, event, data)
	sessionID := hookSessionID(hook.ExternalId, eventID)
	source := hookInputSource
	correlationID := strings.TrimSpace(eventID)
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
		log.Debug().Str("hook", hook.ExternalId).Str("event_id", eventID).Msg("hook task deduped")
	}
	return nil
}

func hookIdempotencyKey(hookExternalID, eventID, event string, data map[string]any) string {
	hookExternalID = strings.TrimSpace(hookExternalID)

	if event == EventSourceChange {
		if key := sourceChangeIdempotencyKey(hookExternalID, data); key != "" {
			return compressHookIdempotencyKey(key)
		}
	}

	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		eventID = fmt.Sprintf("%d", time.Now().UnixNano())
	}
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
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		eventID = fmt.Sprintf("%d", time.Now().UnixNano())
	}
	raw := fmt.Sprintf("hook-%s-%s", strings.TrimSpace(hookExternalID), eventID)
	if len(raw) <= 180 {
		return raw
	}
	sum := sha1.Sum([]byte(raw))
	return "hook-" + hex.EncodeToString(sum[:])
}

func buildHookAttachment(hook *types.Hook, event string, data map[string]any) map[string]any {
	attachment := map[string]any{
		"type":             "hook_event",
		"hook_id":          hook.Id,
		"hook_external_id": hook.ExternalId,
		"path":             hook.Path,
		"event":            event,
	}
	for _, key := range []string{"integration", "new_count", "new_items", "new_items_hash", "path", "workspace_id"} {
		if v, ok := data[key]; ok && v != nil {
			attachment[key] = v
		}
	}
	return attachment
}
