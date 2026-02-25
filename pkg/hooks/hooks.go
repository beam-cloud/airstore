// Package hooks watches filesystem paths and spawns tasks when things change.
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
	"github.com/beam-cloud/airstore/pkg/skills"
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

// SkillReader reads skill content from workspace storage.
type SkillReader interface {
	// ReadSkillContent reads the SKILL.md file for a skill path (e.g., /skills/email-triage).
	// Returns the full content of the SKILL.md file.
	ReadSkillContent(ctx context.Context, workspaceId uint, skillPath string) (string, error)
}

type Engine struct {
	cache       hookCache
	creator     TaskCreator
	backend     repository.BackendRepository
	store       repository.FilesystemStore
	skillReader SkillReader
	debounce    debouncer
}

func NewEngine(store repository.FilesystemStore, creator TaskCreator, backend repository.BackendRepository, skillReader SkillReader) *Engine {
	return &Engine{
		cache:       hookCache{hooks: make(map[uint][]*types.Hook), store: store},
		creator:     creator,
		backend:     backend,
		store:       store,
		skillReader: skillReader,
		debounce:    debouncer{delay: debounceDelay, state: make(map[string]*debounceEntry)},
	}
}

func (eng *Engine) Handle(id string, data map[string]any) {
	event, _ := data["event"].(string)
	wsId := ParseUint(data["workspace_id"])
	path, _ := data["path"].(string)

	if wsId == 0 || path == "" || event == "" {
		log.Warn().Str("id", id).Str("event", event).Str("path", path).
			Interface("workspace_id", data["workspace_id"]).
			Msg("hook engine: dropping malformed event")
		return
	}

	fire := func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		hooks := eng.cache.match(ctx, wsId, path)
		if len(hooks) == 0 {
			log.Debug().Str("event", event).Str("path", path).Uint("workspace_id", wsId).
				Msg("hook engine: no matching hooks")
			return
		}

		for _, h := range hooks {
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
		log.Warn().Str("hook", h.ExternalId).Msg("hook has no token, skipping (re-create hook to fix)")
		return
	}

	token, err := DecodeToken(h.EncryptedToken)
	if err != nil || token == "" {
		log.Warn().Err(err).Str("hook", h.ExternalId).Msg("hook token invalid, skipping")
		return
	}

	prompt := eng.buildPrompt(ctx, h, event, data)
	if err := eng.creator.CreateTask(ctx, h.WorkspaceId, h.CreatedByMemberId, token, prompt,
		h.Id, 1, maxAttempts); err != nil {
		return // DB constraint rejects duplicates -- expected
	}

	log.Info().Str("hook", h.ExternalId).Str("event", event).Str("skill_path", h.SkillPath).Msg("hook fired")
}

// buildPrompt constructs a structured prompt for the Claude Code task.
//
// The prompt has three clearly separated sections:
//   1. Trigger context  – what happened (event type, path, integration, items)
//   2. Skill instructions – from SKILL.md (if a skill is attached)
//   3. Additional context – user-provided extra instructions on the hook
//
// This structure gives Claude a clear "situation → instructions → extra context"
// flow so it knows what triggered the task before reading what to do about it.
func (eng *Engine) buildPrompt(ctx context.Context, h *types.Hook, event string, data map[string]any) string {
	var sections []string

	// --- Section 1: Trigger context ---
	trigger := buildTriggerContext(event, data)
	if trigger != "" {
		sections = append(sections, trigger)
	}

	// --- Section 2: Skill instructions (if any) ---
	var skillMeta *skills.AirstoreSkillMeta
	if h.SkillPath != "" && eng.skillReader != nil {
		skillContent, err := eng.skillReader.ReadSkillContent(ctx, h.WorkspaceId, h.SkillPath)
		if err != nil {
			log.Warn().Err(err).Str("skill_path", h.SkillPath).Msg("failed to read skill content")
		} else {
			// Parse manifest for metadata (needs/writes)
			manifest, parseErr := skills.Parse([]byte(skillContent))
			if parseErr == nil {
				skillMeta = manifest.AirstoreMetadata()
			}

			// Extract the instruction body (everything after frontmatter)
			instructions := skills.ExtractInstructions([]byte(skillContent))
			if instructions != "" {
				sections = append(sections, instructions)
			}
		}
	}

	// --- Section 3: Integration/skill context ---
	if meta := buildSkillContext(skillMeta, data); meta != "" {
		sections = append(sections, meta)
	}

	// --- Section 4: Additional user-provided prompt ---
	if h.Prompt != "" {
		sections = append(sections, h.Prompt)
	}

	return strings.Join(sections, "\n\n")
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
	// Reap tasks stuck in pending/running
	for _, timeout := range []time.Duration{stuckPending, stuckRunning} {
		if tasks, err := eng.backend.GetStuckHookRunExecutions(ctx, timeout); err == nil {
			for _, t := range tasks {
				log.Warn().Str("task", t.ExternalId).Msg("reaping stuck task")
				eng.backend.SetRunExecutionResult(ctx, t.ExternalId, -1, "stuck: no response from worker")
			}
		}
	}

	// Retry failed tasks with backoff
	tasks, err := eng.backend.GetRetryableRunExecutions(ctx)
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

		// Mark the original task as exhausted BEFORE creating the retry.
		// Without this, the retry poller picks up the same failed task
		// on every tick and creates infinite duplicate retries.
		eng.backend.MarkRunExecutionRetried(ctx, t.ExternalId)

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

// buildTriggerContext constructs the "what happened" section of the prompt.
// This is always the first thing Claude sees so it understands the trigger.
func buildTriggerContext(event string, data map[string]any) string {
	path, _ := data["path"].(string)
	integration, _ := data["integration"].(string)
	newCount, _ := data["new_count"].(string)
	newItems, _ := data["new_items"].(string)

	// Use relative paths — the airstore filesystem is mounted at the working
	// directory (/workspace). Absolute paths like /sources/... don't exist
	// inside the container; only workspace-relative paths work.
	relPath := strings.TrimPrefix(path, "/")

	var b strings.Builder

	switch event {
	case EventFsCreate:
		b.WriteString("## Trigger\n\n")
		b.WriteString("A new file was created at `" + relPath + "`.\n")
		b.WriteString("Read it from your working directory: `" + relPath + "`")

	case EventFsWrite:
		b.WriteString("## Trigger\n\n")
		b.WriteString("A file was modified at `" + relPath + "`.\n")
		b.WriteString("Read the updated content from: `" + relPath + "`")

	case EventFsDelete:
		b.WriteString("## Trigger\n\n")
		b.WriteString("A file was deleted at `" + relPath + "`.")

	case EventSourceChange:
		b.WriteString("## Trigger\n\n")
		if integration != "" {
			b.WriteString("Source: **" + integration + "**\n")
		}
		b.WriteString(newCount + " new item(s) appeared in `" + relPath + "/`.\n")
		b.WriteString("The new content is in your working directory at: `" + relPath + "/`\n")
		b.WriteString("List and read the files there to see what changed.")

		// Include item IDs if the source poller provided them
		if newItems != "" {
			b.WriteString("\n\nNew items: " + newItems)
		}

	default:
		return ""
	}

	return b.String()
}

// buildSkillContext adds integration/output path hints from the skill's metadata.
// This helps Claude understand which integrations are relevant and where to write output.
func buildSkillContext(meta *skills.AirstoreSkillMeta, data map[string]any) string {
	if meta == nil {
		return ""
	}

	var parts []string

	if len(meta.Needs) > 0 {
		integration, _ := data["integration"].(string)
		matched := false
		for _, need := range meta.Needs {
			if need == integration {
				matched = true
				break
			}
		}
		if !matched && integration != "" {
			parts = append(parts, "Note: this skill is designed for "+strings.Join(meta.Needs, ", ")+
				" but was triggered by "+integration+". Adapt accordingly.")
		}
	}

	if len(meta.Writes) > 0 {
		relPaths := make([]string, len(meta.Writes))
		for i, w := range meta.Writes {
			relPaths[i] = "`" + strings.TrimPrefix(w, "/") + "`"
		}
		parts = append(parts, "Write output to: "+strings.Join(relPaths, ", "))
	}

	if len(parts) == 0 {
		return ""
	}
	return strings.Join(parts, "\n")
}

// enrichPrompt is kept for backward compatibility with existing tests.
// New code should use buildPrompt instead.
func enrichPrompt(base, event string, data map[string]any) string {
	trigger := buildTriggerContext(event, data)
	if trigger == "" {
		return base
	}
	return trigger + "\n\n" + base
}

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
