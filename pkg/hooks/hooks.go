// Package hooks watches filesystem paths and spawns tasks when things change.
//
// Data flow: Event → Handle → debounce → match → submit → task
// Retry flow: Poll (30s) → reap stuck → retry failed
package hooks

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/beam-cloud/airstore/pkg/repository"
	"github.com/beam-cloud/airstore/pkg/types"
	"github.com/rs/zerolog/log"
)

// Event types emitted by StorageService / SourceService.
const (
	EventFsCreate     = "fs.create"
	EventFsWrite      = "fs.write"
	EventFsDelete     = "fs.delete"
	EventSourceChange = "source.change"
)

const (
	defaultMaxAttempts  = 3
	debounceDelay       = 2 * time.Second
	pollInterval        = 30 * time.Second
	baseRetryDelay      = 10 * time.Second
	retryMultiplier     = 3.0
	maxRetryDelay       = 5 * time.Minute
	stuckPendingTimeout = 2 * time.Minute  // pending/scheduled: never started
	stuckRunningTimeout = 10 * time.Minute // running: started but no result
)

// TaskCreator abstracts task creation (avoids import cycle with services/).
type TaskCreator interface {
	CreateTask(ctx context.Context, workspaceId uint, createdByMemberId *uint, memberToken, prompt string, hookId uint, attempt, maxAttempts int) error
}

// Engine is the single entry point for hooks. It matches events to hooks,
// debounces writes, submits tasks, and polls for retries.
type Engine struct {
	cache    hookCache
	creator  TaskCreator
	backend  repository.BackendRepository
	store    repository.FilesystemStore
	debounce debouncer
}

func NewEngine(store repository.FilesystemStore, creator TaskCreator, backend repository.BackendRepository) *Engine {
	return &Engine{
		cache:    hookCache{hooks: make(map[uint][]*types.Hook), store: store},
		creator:  creator,
		backend:  backend,
		store:    store,
		debounce: debouncer{delay: debounceDelay, state: make(map[string]*debounceEntry)},
	}
}

// --- Event handling ---

// Handle is called by the EventStream consumer for each event.
func (eng *Engine) Handle(id string, data map[string]any) {
	event, _ := data["event"].(string)
	wsId := ParseUint(data["workspace_id"])
	path, _ := data["path"].(string)
	if wsId == 0 || path == "" {
		return
	}

	log.Debug().Str("event", event).Uint("ws", wsId).Str("path", path).Msg("hook event")

	switch event {
	case EventFsWrite:
		eng.debounce.call(fmt.Sprintf("%d:%s", wsId, path), func() {
			eng.matchAndFire(wsId, path, event, data)
		})
	case EventFsCreate, EventFsDelete, EventSourceChange:
		eng.matchAndFire(wsId, path, event, data)
	}
}

func (eng *Engine) InvalidateCache(wsId uint) { eng.cache.invalidate(wsId) }

func (eng *Engine) matchAndFire(wsId uint, path, event string, data map[string]any) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for _, h := range eng.cache.match(ctx, wsId, path) {
		eng.submit(ctx, h, event, data)
	}
}

func (eng *Engine) submit(ctx context.Context, h *types.Hook, event string, data map[string]any) {
	if h.TokenId == nil {
		return
	}
	token, err := DecodeToken(h.EncryptedToken)
	if err != nil {
		log.Warn().Err(err).Str("hook", h.ExternalId).Msg("bad token")
		return
	}
	prompt := enrichPrompt(h.Prompt, event, data)
	// The DB unique constraint (idx_task_hook_one_active) is the sole guard.
	// If a task is already active for this hook, the insert fails. That's fine.
	if err := eng.creator.CreateTask(ctx, h.WorkspaceId, h.CreatedByMemberId, token, prompt,
		h.Id, 1, defaultMaxAttempts); err != nil {
		return // constraint rejection or real error -- either way, nothing to do
	}
	log.Info().Str("hook", h.ExternalId).Str("event", event).Msg("hook fired")
}

// --- Retry polling ---

// Start runs the retry poller. Call as a goroutine.
func (eng *Engine) Start(ctx context.Context) {
	log.Info().Dur("interval", pollInterval).Msg("hook retry poller started")
	t := time.NewTicker(pollInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			eng.Poll(ctx)
		}
	}
}

// Poll runs one poll cycle (exported for testing).
func (eng *Engine) Poll(ctx context.Context) {
	eng.reapStuck(ctx)
	eng.retryFailed(ctx)
}

// reapStuck marks hook tasks stuck in pending/running for too long as failed.
func (eng *Engine) reapStuck(ctx context.Context) {
	eng.reapByTimeout(ctx, stuckPendingTimeout) // Pending tasks that never started get a short leash.
	eng.reapByTimeout(ctx, stuckRunningTimeout) // Running tasks get more time (the agent might be working).

}

func (eng *Engine) reapByTimeout(ctx context.Context, timeout time.Duration) {
	tasks, err := eng.backend.GetStuckHookTasks(ctx, timeout)
	if err != nil || len(tasks) == 0 {
		return
	}
	for _, t := range tasks {
		log.Warn().Str("task", t.ExternalId).Str("status", string(t.Status)).Dur("age", timeout).Msg("reaping stuck task")
		eng.backend.SetTaskResult(ctx, t.ExternalId, -1, "stuck: no response from worker")
	}
}

// retryFailed creates new attempts for failed hook tasks.
func (eng *Engine) retryFailed(ctx context.Context) {
	tasks, err := eng.backend.GetRetryableTasks(ctx)
	if err != nil || len(tasks) == 0 {
		return
	}
	now := time.Now()
	for _, t := range tasks {
		eng.maybeRetry(ctx, t, now)
	}
}

func (eng *Engine) maybeRetry(ctx context.Context, t *types.Task, now time.Time) {
	if t.FinishedAt == nil || t.HookId == nil || t.Attempt >= t.MaxAttempts || now.Before(t.FinishedAt.Add(retryDelay(t.Attempt))) {
		return
	}
	if eng.hasRunningTask(ctx, *t.HookId) {
		return
	}
	hook, err := eng.store.GetHookById(ctx, *t.HookId)
	if err != nil || hook == nil {
		return
	}
	token, err := DecodeToken(hook.EncryptedToken)
	if err != nil {
		return
	}
	next := t.Attempt + 1
	if err := eng.creator.CreateTask(ctx, t.WorkspaceId, t.CreatedByMemberId, token, t.Prompt,
		*t.HookId, next, t.MaxAttempts); err != nil {
		return // unique constraint prevents duplicate attempts across replicas
	}
	log.Info().Str("hook", hook.ExternalId).Int("attempt", next).Msg("retrying")
}

func (eng *Engine) hasRunningTask(ctx context.Context, hookId uint) bool {
	task, err := eng.backend.GetActiveHookTask(ctx, hookId)
	if err != nil {
		return true // fail closed
	}
	return task != nil
}

func retryDelay(attempt int) time.Duration {
	d := time.Duration(float64(baseRetryDelay) * math.Pow(retryMultiplier, float64(attempt-1)))
	if d > maxRetryDelay {
		return maxRetryDelay
	}
	return d
}

// --- Prompt enrichment ---

func enrichPrompt(base, event string, data map[string]any) string {
	path, _ := data["path"].(string)
	var line string
	switch event {
	case EventFsCreate:
		line = "Event: new file at " + path
	case EventFsWrite:
		line = "Event: file changed at " + path
	case EventFsDelete:
		line = "Event: file deleted at " + path
	case EventSourceChange:
		if n, _ := data["new_count"].(string); n != "" {
			line = fmt.Sprintf("Event: %s new results in %s", n, path)
		} else {
			line = "Event: new results in " + path
		}
	}
	if line == "" {
		return base
	}
	return base + "\n\n" + line
}

// --- Token encode/decode ---

// EncodeToken serializes a token for storage. TODO: real encryption.
func EncodeToken(raw string) ([]byte, error) { return json.Marshal(raw) }

// DecodeToken deserializes a stored token.
func DecodeToken(stored []byte) (string, error) {
	if len(stored) == 0 {
		return "", fmt.Errorf("empty token")
	}
	var s string
	if err := json.Unmarshal(stored, &s); err != nil {
		return "", fmt.Errorf("decode token: %w", err)
	}
	return s, nil
}

// --- Path + type utilities ---

// NormalizePath ensures leading / and no trailing /.
func NormalizePath(p string) string {
	if !strings.HasPrefix(p, "/") {
		p = "/" + p
	}
	if len(p) > 1 { // preserve bare "/"
		p = strings.TrimRight(p, "/")
	}
	return p
}

// ParseUint extracts a uint from any (string/float64/int from Redis/JSON).
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

// --- Hook cache ---

type hookCache struct {
	mu    sync.RWMutex
	hooks map[uint][]*types.Hook
	store repository.FilesystemStore
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
	c.mu.Lock()
	defer c.mu.Unlock()

	// Double-check: another goroutine may have loaded while we waited for the lock.
	if hooks, ok := c.hooks[wsId]; ok {
		return hooks
	}

	hooks, err := c.store.ListHooks(ctx, wsId)
	if err != nil {
		log.Warn().Err(err).Uint("ws", wsId).Msg("hook cache load failed")
		return nil
	}

	c.hooks[wsId] = hooks
	return hooks
}

// --- Debouncer ---

type debouncer struct {
	delay time.Duration
	mu    sync.Mutex
	state map[string]*debounceEntry
}

type debounceEntry struct {
	timer *time.Timer
	gen   uint64
}

func (d *debouncer) call(key string, fn func()) {
	d.mu.Lock()
	defer d.mu.Unlock()
	e, ok := d.state[key]
	if ok {
		e.timer.Stop()
		e.gen++
	} else {
		e = &debounceEntry{}
		d.state[key] = e
	}
	gen := e.gen
	e.timer = time.AfterFunc(d.delay, func() {
		d.mu.Lock()
		if e.gen != gen {
			d.mu.Unlock()
			return
		}
		delete(d.state, key)
		d.mu.Unlock()
		fn()
	})
}
