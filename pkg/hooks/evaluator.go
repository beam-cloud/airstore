package hooks

import (
	"context"
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

// Stream event types (emitted by StorageService / SourceService).
const (
	EventFsCreate     = "fs.create"
	EventFsWrite      = "fs.write"
	EventSourceChange = "source.change"
)

// TaskCreator abstracts task creation so the evaluator doesn't depend on
// the services package (avoids import cycle).
type TaskCreator interface {
	CreateTask(ctx context.Context, workspaceId uint, createdByMemberId *uint, memberToken, prompt string) error
}

// Evaluator processes filesystem events and creates tasks for matching hooks.
// EventStream -> Evaluator -> TaskCreator.
type Evaluator struct {
	cache    *hookCache
	creator  TaskCreator
	debounce *common.Debouncer
	rdb      *common.RedisClient
}

// NewEvaluator creates an evaluator that matches events against cached hooks.
func NewEvaluator(store repository.FilesystemStore, creator TaskCreator, rdb *common.RedisClient) *Evaluator {
	return &Evaluator{
		cache: &hookCache{
			hooks: make(map[uint][]*types.Hook),
			store: store,
		},
		creator:  creator,
		debounce: common.NewDebouncer(2 * time.Second),
		rdb:      rdb,
	}
}

// Handle is the single entry point called by the EventStream consumer.
func (e *Evaluator) Handle(id string, data map[string]any) {
	eventType, _ := data["event"].(string)
	wsId := ParseUint(data["workspace_id"])
	path, _ := data["path"].(string)
	if wsId == 0 || path == "" {
		return
	}

	switch eventType {
	case EventFsCreate:
		e.fireHooks(wsId, path, types.HookTriggerOnCreate, data)
	case EventFsWrite:
		e.debounce.Call(fmt.Sprintf("%d:%s", wsId, path), func() {
			e.fireHooks(wsId, path, types.HookTriggerOnWrite, data)
		})
	case EventSourceChange:
		e.fireHooks(wsId, path, types.HookTriggerOnChange, data)
	}
}

// InvalidateCache clears the hook cache for a workspace. Called on hook CRUD.
func (e *Evaluator) InvalidateCache(wsId uint) {
	e.cache.Invalidate(wsId)
}

// fireHooks matches hooks for the given event and creates tasks.
func (e *Evaluator) fireHooks(wsId uint, path string, trigger types.HookTrigger, data map[string]any) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	hooks := e.cache.Match(ctx, wsId, path, trigger)
	if len(hooks) == 0 {
		return
	}

	for _, h := range hooks {
		// Token revoked (FK set to NULL by ON DELETE SET NULL) -- hook can't fire
		if h.TokenId == nil {
			log.Warn().Str("hook", h.ExternalId).Msg("hook token revoked, skipping")
			continue
		}

		cooldownKey := common.Keys.HookCooldown(h.ExternalId)
		set, err := e.rdb.SetNX(ctx, cooldownKey, "1", 5*time.Minute).Result()
		if err != nil || !set {
			log.Debug().Str("hook", h.ExternalId).Str("path", path).Msg("hook on cooldown, skipping")
			continue
		}

		token, err := DecryptToken(h.EncryptedToken)
		if err != nil {
			log.Warn().Err(err).Str("hook", h.ExternalId).Msg("failed to decrypt hook token")
			e.rdb.Del(ctx, cooldownKey)
			continue
		}

		prompt := BuildPrompt(h.Prompt, trigger, data)
		if err := e.creator.CreateTask(ctx, h.WorkspaceId, h.CreatedByMemberId, token, prompt); err != nil {
			log.Warn().Err(err).Str("hook", h.ExternalId).Msg("failed to create task from hook")
			e.rdb.Del(ctx, cooldownKey)
			continue
		}

		log.Info().
			Str("hook", h.ExternalId).
			Str("trigger", string(trigger)).
			Str("path", path).
			Msg("hook fired")
	}
}

// BuildPrompt enriches the hook's prompt with trigger context.
func BuildPrompt(base string, trigger types.HookTrigger, data map[string]any) string {
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

// ParseUint extracts a uint from an any value (handles string/float64/int from Redis).
func ParseUint(v any) uint {
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

// hookCache: in-memory cache of hooks per workspace.
type hookCache struct {
	mu    sync.RWMutex
	hooks map[uint][]*types.Hook
	store repository.FilesystemStore
}

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
		if path == h.Path || strings.HasPrefix(path, h.Path+"/") {
			matched = append(matched, h)
		}
	}
	return matched
}

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
