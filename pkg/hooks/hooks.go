// Package hooks watches filesystem paths and spawns tasks when things change
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

const (
	EventFsCreate     = "fs.create"
	EventFsWrite      = "fs.write"
	EventFsDelete     = "fs.delete"
	EventSourceChange = "source.change"
)

const (
	maxAttempts   = 3
	debounceDelay = 2 * time.Second
	pollInterval  = 30 * time.Second
	retryBase     = 10 * time.Second
	retryMax      = 5 * time.Minute
	stuckPending  = 2 * time.Minute
	stuckRunning  = 10 * time.Minute
)

type TaskCreator interface {
	CreateTask(ctx context.Context, wsId uint, memberId *uint, token, prompt string, hookId uint, attempt, max int) error
}

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

func (eng *Engine) Handle(id string, data map[string]any) {
	event, _ := data["event"].(string)
	wsId := ParseUint(data["workspace_id"])
	path, _ := data["path"].(string)
	if wsId == 0 || path == "" {
		return
	}
	fire := func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		for _, h := range eng.cache.match(ctx, wsId, path) {
			eng.submit(ctx, h, event, data)
		}
	}
	if event == EventFsWrite {
		eng.debounce.call(fmt.Sprintf("%d:%s", wsId, path), fire)
	} else {
		fire()
	}
}

func (eng *Engine) InvalidateCache(wsId uint) { eng.cache.invalidate(wsId) }

func (eng *Engine) submit(ctx context.Context, h *types.Hook, event string, data map[string]any) {
	if h.TokenId == nil {
		return
	}
	token, err := DecodeToken(h.EncryptedToken)
	if err != nil {
		return
	}
	prompt := enrichPrompt(h.Prompt, event, data)
	if err := eng.creator.CreateTask(ctx, h.WorkspaceId, h.CreatedByMemberId, token, prompt,
		h.Id, 1, maxAttempts); err != nil {
		return // DB constraint rejects duplicates -- expected
	}
	log.Info().Str("hook", h.ExternalId).Str("event", event).Msg("hook fired")
}

// Start runs the retry poller. Call as a goroutine.
func (eng *Engine) Start(ctx context.Context) {
	log.Info().Msg("hook poller started")
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

// Poll reaps stuck tasks and retries failed ones. Exported for testing.
func (eng *Engine) Poll(ctx context.Context) {
	for _, timeout := range []time.Duration{stuckPending, stuckRunning} {
		if tasks, err := eng.backend.GetStuckHookTasks(ctx, timeout); err == nil {
			for _, t := range tasks {
				log.Warn().Str("task", t.ExternalId).Msg("reaping stuck task")
				eng.backend.SetTaskResult(ctx, t.ExternalId, -1, "stuck: no response from worker")
			}
		}
	}
	tasks, err := eng.backend.GetRetryableTasks(ctx)
	if err != nil {
		return
	}
	now := time.Now()
	for _, t := range tasks {
		if t.FinishedAt == nil || t.HookId == nil || t.Attempt >= t.MaxAttempts {
			continue
		}
		if now.Before(t.FinishedAt.Add(retryDelay(t.Attempt))) {
			continue
		}
		hook, err := eng.store.GetHookById(ctx, *t.HookId)
		if err != nil || hook == nil {
			continue
		}
		token, _ := DecodeToken(hook.EncryptedToken)
		if token == "" {
			continue
		}
		next := t.Attempt + 1
		if err := eng.creator.CreateTask(ctx, t.WorkspaceId, t.CreatedByMemberId, token, t.Prompt,
			*t.HookId, next, t.MaxAttempts); err != nil {
			continue
		}
		log.Info().Str("hook", hook.ExternalId).Int("attempt", next).Msg("retrying")
	}
}

func retryDelay(attempt int) time.Duration {
	d := time.Duration(float64(retryBase) * math.Pow(3, float64(attempt-1)))
	if d > retryMax {
		return retryMax
	}
	return d
}

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
		line = "Event: new results in " + path
	}
	if line == "" {
		return base
	}
	return base + "\n\n" + line
}

// Token encode/decode (JSON for now, TODO: real encryption)

func EncodeToken(raw string) ([]byte, error) { return json.Marshal(raw) }

func DecodeToken(stored []byte) (string, error) {
	if len(stored) == 0 {
		return "", fmt.Errorf("empty token")
	}
	var s string
	if err := json.Unmarshal(stored, &s); err != nil {
		return "", err
	}
	return s, nil
}

// Utilities

func NormalizePath(p string) string {
	if !strings.HasPrefix(p, "/") {
		p = "/" + p
	}
	if len(p) > 1 {
		p = strings.TrimRight(p, "/")
	}
	return p
}

func ParseUint(v any) uint {
	switch v := v.(type) {
	case float64:
		return uint(v)
	case int:
		return uint(v)
	case int64:
		return uint(v)
	case uint:
		return v
	case string:
		n, _ := strconv.ParseUint(v, 10, 64)
		return uint(n)
	default:
		return 0
	}
}

// Hook cache (in-memory, invalidated on CRUD)

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
	if hooks, ok := c.hooks[wsId]; ok {
		return hooks
	}
	hooks, _ := c.store.ListHooks(ctx, wsId)
	c.hooks[wsId] = hooks
	return hooks
}

// Debouncer

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
