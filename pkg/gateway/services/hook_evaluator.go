package services

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/beam-cloud/airstore/pkg/common"
	"github.com/beam-cloud/airstore/pkg/repository"
	"github.com/beam-cloud/airstore/pkg/types"
	"github.com/rs/zerolog/log"
)

// HookEvaluator processes filesystem events and creates tasks for matching hooks.
// It is the consumer side of the hook system: EventStream -> HookEvaluator -> TaskFactory.
type HookEvaluator struct {
	cache    *hookCache
	factory  *TaskFactory
	debounce *common.Debouncer
	rdb      *common.RedisClient
}

// NewHookEvaluator creates an evaluator that matches events against cached hooks.
func NewHookEvaluator(store repository.FilesystemStore, factory *TaskFactory, rdb *common.RedisClient) *HookEvaluator {
	return &HookEvaluator{
		cache: &hookCache{
			hooks: make(map[uint][]*types.Hook),
			store: store,
		},
		factory:  factory,
		debounce: common.NewDebouncer(2 * time.Second),
		rdb:      rdb,
	}
}

// Handle is the single entry point called by the EventStream consumer.
func (e *HookEvaluator) Handle(id string, data map[string]any) {
	eventType, _ := data["event"].(string)
	wsId := parseUintFromAny(data["workspace_id"])
	path, _ := data["path"].(string)
	if wsId == 0 || path == "" {
		return
	}

	switch eventType {
	case "fs.create":
		e.fireHooks(wsId, path, types.HookTriggerOnCreate, data)
	case "fs.write":
		key := fmt.Sprintf("%d:%s", wsId, path)
		e.debounce.Call(key, func() {
			e.fireHooks(wsId, path, types.HookTriggerOnWrite, data)
		})
	case "source.change":
		e.fireHooks(wsId, path, types.HookTriggerOnChange, data)
	}
}

// InvalidateCache clears the hook cache for a workspace. Called on hook CRUD.
func (e *HookEvaluator) InvalidateCache(wsId uint) {
	e.cache.Invalidate(wsId)
}

// fireHooks matches hooks for the given event and creates tasks.
func (e *HookEvaluator) fireHooks(wsId uint, path string, trigger types.HookTrigger, data map[string]any) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	hooks := e.cache.Match(ctx, wsId, path, trigger)
	if len(hooks) == 0 {
		return
	}

	for _, h := range hooks {
		// Per-hook cooldown: SETNX returns true only if key didn't exist
		cooldownKey := common.Keys.HookCooldown(h.ExternalId)
		set, err := e.rdb.SetNX(ctx, cooldownKey, "1", 5*time.Minute).Result()
		if err != nil || !set {
			log.Debug().
				Str("hook", h.ExternalId).
				Str("path", path).
				Msg("hook on cooldown, skipping")
			continue
		}

		token, err := decryptToken(h.EncryptedToken)
		if err != nil {
			log.Warn().Err(err).Str("hook", h.ExternalId).Msg("failed to decrypt hook token")
			e.rdb.Del(ctx, cooldownKey)
			continue
		}

		prompt := buildPrompt(h.Prompt, trigger, data)
		task, err := e.factory.Create(ctx, TaskParams{
			WorkspaceId:       h.WorkspaceId,
			CreatedByMemberId: h.CreatedByMemberId,
			MemberToken:       token,
			Prompt:            prompt,
		})
		if err != nil {
			log.Warn().Err(err).Str("hook", h.ExternalId).Msg("failed to create task from hook")
			e.rdb.Del(ctx, cooldownKey)
			continue
		}

		log.Info().
			Str("hook", h.ExternalId).
			Str("task", task.ExternalId).
			Str("trigger", string(trigger)).
			Str("path", path).
			Msg("hook fired")
	}
}

// buildPrompt enriches the hook's prompt with trigger context.
func buildPrompt(base string, trigger types.HookTrigger, data map[string]any) string {
	var ctx string
	switch trigger {
	case types.HookTriggerOnCreate:
		if path, ok := data["path"].(string); ok {
			ctx = fmt.Sprintf("Triggered by: new file at %s", path)
		}
	case types.HookTriggerOnWrite:
		if path, ok := data["path"].(string); ok {
			ctx = fmt.Sprintf("Triggered by: file modified at %s", path)
		}
	case types.HookTriggerOnChange:
		path, _ := data["path"].(string)
		newCount, _ := data["new_count"].(string)
		ctx = fmt.Sprintf("Triggered by: %s new results in %s", newCount, path)
	}

	if ctx == "" {
		return base
	}
	return base + "\n\n" + ctx
}

// decryptToken decodes the encrypted token. Currently JSON-encoded (matching
// IntegrationConnection.Credentials pattern). TODO: real encryption.
func decryptToken(encrypted []byte) (string, error) {
	if len(encrypted) == 0 {
		return "", fmt.Errorf("empty token")
	}
	// Currently stored as JSON string (matching IntegrationConnection pattern)
	var token string
	if err := json.Unmarshal(encrypted, &token); err != nil {
		// Fall back to raw bytes if not JSON-encoded
		return string(encrypted), nil
	}
	return token, nil
}

// encryptToken encodes a raw token for storage. Currently JSON-encoded.
// TODO: real encryption.
func encryptToken(raw string) ([]byte, error) {
	return json.Marshal(raw)
}

// parseUintFromAny extracts a uint from an any value (handles string/float64/int from Redis).
func parseUintFromAny(v any) uint {
	switch val := v.(type) {
	case float64:
		return uint(val)
	case int:
		return uint(val)
	case int64:
		return uint(val)
	case uint:
		return val
	case string:
		n, _ := strconv.ParseUint(val, 10, 64)
		return uint(n)
	default:
		return 0
	}
}

// ============================================================
// hookCache: in-memory cache of hooks per workspace.
// Hooks change infrequently; events are frequent.
// Same pattern as Prometheus rule caching.
// ============================================================

type hookCache struct {
	mu    sync.RWMutex
	hooks map[uint][]*types.Hook // workspaceId -> active hooks
	store repository.FilesystemStore
}

// Match returns hooks for a workspace/path/trigger combination.
// Loads from DB on cache miss.
func (c *hookCache) Match(ctx context.Context, wsId uint, path string, trigger types.HookTrigger) []*types.Hook {
	c.mu.RLock()
	hooks, ok := c.hooks[wsId]
	c.mu.RUnlock()

	if !ok {
		hooks = c.load(ctx, wsId)
	}

	var matched []*types.Hook
	for _, h := range hooks {
		if !h.Active || h.Trigger != trigger {
			continue
		}
		// Hook path is a prefix of the event path, or exact match
		if path == h.Path || strings.HasPrefix(path, h.Path+"/") {
			matched = append(matched, h)
		}
	}
	return matched
}

// Invalidate clears the cache for a workspace. Called by hooks API after CRUD
// and by EventBus handler for cross-replica invalidation.
func (c *hookCache) Invalidate(wsId uint) {
	c.mu.Lock()
	delete(c.hooks, wsId)
	c.mu.Unlock()
}

func (c *hookCache) load(ctx context.Context, wsId uint) []*types.Hook {
	hooks, err := c.store.ListHooks(ctx, wsId)
	if err != nil {
		log.Warn().Err(err).Uint("workspace", wsId).Msg("hook cache: failed to load")
		return nil
	}

	c.mu.Lock()
	c.hooks[wsId] = hooks
	c.mu.Unlock()
	return hooks
}
