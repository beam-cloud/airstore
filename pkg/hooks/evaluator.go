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

// Stream event types emitted by StorageService / SourceService.
const (
	EventFsCreate     = "fs.create"
	EventFsWrite      = "fs.write"
	EventSourceChange = "source.change"
)

// TaskCreator abstracts task creation (avoids import cycle with services/).
type TaskCreator interface {
	CreateTask(ctx context.Context, workspaceId uint, createdByMemberId *uint, memberToken, prompt string) error
}

// Evaluator matches filesystem events against hooks and spawns tasks.
type Evaluator struct {
	cache    *hookCache
	creator  TaskCreator
	debounce *debouncer
	cooldown *cooldown
}

func NewEvaluator(store repository.FilesystemStore, creator TaskCreator, rdb *common.RedisClient) *Evaluator {
	return &Evaluator{
		cache:    newHookCache(store),
		creator:  creator,
		debounce: newDebouncer(2 * time.Second),
		cooldown: newCooldown(rdb, 5*time.Minute),
	}
}

// Handle is called by the EventStream consumer for each event.
func (e *Evaluator) Handle(id string, data map[string]any) {
	eventType, _ := data["event"].(string)
	wsId := parseUint(data["workspace_id"])
	path, _ := data["path"].(string)
	if wsId == 0 || path == "" {
		return
	}

	log.Debug().Str("event", eventType).Uint("ws", wsId).Str("path", path).Msg("hook event")

	switch eventType {
	case EventFsWrite:
		// Debounce writes so burst of chunks → one fire.
		e.debounce.call(fmt.Sprintf("%d:%s", wsId, path), func() {
			e.fire(wsId, path, eventType, data)
		})
	case EventFsCreate, EventSourceChange:
		e.fire(wsId, path, eventType, data)
	}
}

func (e *Evaluator) InvalidateCache(wsId uint) { e.cache.invalidate(wsId) }

// fire matches hooks and creates tasks. Cooldown prevents repeated fires.
func (e *Evaluator) fire(wsId uint, path, event string, data map[string]any) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	for _, h := range e.cache.match(ctx, wsId, path) {
		if h.TokenId == nil {
			log.Warn().Str("hook", h.ExternalId).Msg("hook token revoked")
			continue
		}
		if !e.cooldown.acquire(ctx, h.ExternalId) {
			log.Debug().Str("hook", h.ExternalId).Msg("hook on cooldown")
			continue
		}

		token, err := DecodeToken(h.EncryptedToken)
		if err != nil {
			e.cooldown.release(ctx, h.ExternalId)
			log.Warn().Err(err).Str("hook", h.ExternalId).Msg("bad token")
			continue
		}

		prompt := enrichPrompt(h.Prompt, event, data)
		if err := e.creator.CreateTask(ctx, h.WorkspaceId, h.CreatedByMemberId, token, prompt); err != nil {
			e.cooldown.release(ctx, h.ExternalId)
			log.Warn().Err(err).Str("hook", h.ExternalId).Msg("task creation failed")
			continue
		}

		log.Info().Str("hook", h.ExternalId).Str("event", event).Str("path", path).Msg("hook fired")
	}
}

// enrichPrompt appends event context so the agent knows what happened.
func enrichPrompt(base, event string, data map[string]any) string {
	path, _ := data["path"].(string)
	var line string
	switch event {
	case EventFsCreate:
		line = fmt.Sprintf("Event: new file uploaded at %s", path)
	case EventFsWrite:
		line = fmt.Sprintf("Event: file created or modified at %s", path)
	case EventSourceChange:
		if n, _ := data["new_count"].(string); n != "" {
			line = fmt.Sprintf("Event: %s new results in %s", n, path)
		} else {
			line = fmt.Sprintf("Event: new results in %s", path)
		}
	}
	if line == "" {
		return base
	}
	return base + "\n\n" + line
}

// NormalizePath ensures a path starts with / and has no trailing slash.
func NormalizePath(p string) string {
	if !strings.HasPrefix(p, "/") {
		p = "/" + p
	}
	if len(p) > 1 {
		p = strings.TrimRight(p, "/")
	}
	return p
}

// ParseUint extracts a uint from any (string/float64/int from Redis/JSON).
func ParseUint(v any) uint { return parseUint(v) }

func parseUint(v any) uint {
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

// --- cooldown: prevents a hook from firing repeatedly ---
// Uses Redis SETNX in remote mode, in-memory map in local mode.

type cooldown struct {
	rdb *common.RedisClient
	ttl time.Duration
	mu  sync.Mutex
	mem map[string]time.Time // fallback when rdb is nil
}

func newCooldown(rdb *common.RedisClient, ttl time.Duration) *cooldown {
	return &cooldown{rdb: rdb, ttl: ttl, mem: make(map[string]time.Time)}
}

func (c *cooldown) acquire(ctx context.Context, hookId string) bool {
	if c.rdb != nil {
		key := common.Keys.HookCooldown(hookId)
		set, err := c.rdb.SetNX(ctx, key, "1", c.ttl).Result()
		return err == nil && set
	}
	// In-memory fallback
	c.mu.Lock()
	defer c.mu.Unlock()
	if exp, ok := c.mem[hookId]; ok && time.Now().Before(exp) {
		return false
	}
	c.mem[hookId] = time.Now().Add(c.ttl)
	return true
}

func (c *cooldown) release(ctx context.Context, hookId string) {
	if c.rdb != nil {
		c.rdb.Del(ctx, common.Keys.HookCooldown(hookId))
		return
	}
	c.mu.Lock()
	delete(c.mem, hookId)
	c.mu.Unlock()
}

// --- hookCache: in-memory cache of hooks per workspace ---

type hookCache struct {
	mu    sync.RWMutex
	hooks map[uint][]*types.Hook
	store repository.FilesystemStore
}

func newHookCache(store repository.FilesystemStore) *hookCache {
	return &hookCache{hooks: make(map[uint][]*types.Hook), store: store}
}

func (c *hookCache) match(ctx context.Context, wsId uint, path string) []*types.Hook {
	c.mu.RLock()
	hooks, ok := c.hooks[wsId]
	c.mu.RUnlock()

	if !ok {
		hooks = c.load(ctx, wsId)
	}

	var out []*types.Hook
	for _, h := range hooks {
		if h.Active && (path == h.Path || strings.HasPrefix(path, h.Path+"/")) {
			out = append(out, h)
		}
	}
	return out
}

func (c *hookCache) invalidate(wsId uint) {
	c.mu.Lock()
	delete(c.hooks, wsId)
	c.mu.Unlock()
}

func (c *hookCache) load(ctx context.Context, wsId uint) []*types.Hook {
	hooks, err := c.store.ListHooks(ctx, wsId)
	if err != nil {
		log.Warn().Err(err).Uint("ws", wsId).Msg("hook cache load failed")
		return nil
	}
	c.mu.Lock()
	c.hooks[wsId] = hooks
	c.mu.Unlock()
	return hooks
}
