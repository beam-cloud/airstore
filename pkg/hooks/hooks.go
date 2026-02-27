// Package hooks watches filesystem paths and spawns tasks when things change.
package hooks

import (
	"context"
	"encoding/json"
	"fmt"
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
	debounceDelay = 2 * time.Second
)

type TaskCreator interface {
	CreateTask(ctx context.Context, hook *types.Hook, eventID, event, prompt string, data map[string]any) error
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

	fire := func(eventID, resolvedEvent string, payload map[string]any) {
		effectivePayload := cloneEventData(payload)
		resolvedPath, _ := effectivePayload["path"].(string)
		if resolvedPath == "" {
			resolvedPath = path
		}
		effectivePayload["path"] = resolvedPath
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		effectiveEvent := resolvedEvent
		if isFilesystemEvent(effectiveEvent) {
			effectiveEvent = eng.reconcileFilesystemEvent(ctx, effectiveEvent, resolvedPath)
			effectivePayload["event"] = effectiveEvent
		}

		hooks := eng.cache.match(ctx, wsId, resolvedPath)
		if len(hooks) == 0 {
			log.Debug().Str("event", effectiveEvent).Str("path", resolvedPath).Uint("workspace_id", wsId).
				Msg("hook engine: no matching hooks")
			return
		}

		for _, h := range hooks {
			eng.submit(ctx, h, eventID, effectiveEvent, effectivePayload)
		}
	}

	if isFilesystemEvent(event) {
		eng.debounce.call(fmt.Sprintf("%d:%s", wsId, path), id, event, data, fire)
	} else {
		fire(id, event, data)
	}
}

func (eng *Engine) InvalidateCache(wsId uint) { eng.cache.invalidate(wsId) }

func (eng *Engine) submit(ctx context.Context, h *types.Hook, eventID, event string, data map[string]any) {
	if !eng.ensureHookAgent(ctx, h) {
		return
	}

	prompt := eng.buildPrompt(ctx, h, event, data)
	if err := eng.creator.CreateTask(ctx, h, eventID, event, prompt, data); err != nil {
		log.Warn().Err(err).Str("hook", h.ExternalId).Msg("hook engine: failed to enqueue agent task")
		return
	}

	log.Info().
		Str("hook", h.ExternalId).
		Str("event", event).
		Strs("skill_paths", types.NormalizeSkillPaths(h.SkillPaths, h.SkillPath)).
		Msg("hook fired")
}

func (eng *Engine) reconcileFilesystemEvent(ctx context.Context, event, path string) string {
	if event != EventFsDelete || eng.store == nil {
		return event
	}
	normalizedPath := NormalizePath(path)
	dirMeta, fileMeta, symlink, err := eng.store.StatPath(ctx, normalizedPath)
	if err != nil {
		log.Debug().
			Err(err).
			Str("event", event).
			Str("path", normalizedPath).
			Msg("hook engine: failed to reconcile fs event")
		return event
	}
	if dirMeta != nil || fileMeta != nil || strings.TrimSpace(symlink) != "" {
		// If the path still exists at fire-time, treat delete as transient noise.
		return EventFsWrite
	}
	return event
}

// buildPrompt constructs a structured prompt for the Claude Code task.
//
// The prompt has three clearly separated sections:
//  1. Trigger context    – what happened (event type, path, integration, items)
//  2. Skill references   – names + paths to SKILL.md files (not full contents)
//  3. Additional context – user-provided extra instructions on the hook
func (eng *Engine) buildPrompt(ctx context.Context, h *types.Hook, event string, data map[string]any) string {
	var sections []string

	// --- Section 1: Trigger context ---
	trigger := buildTriggerContext(event, data)
	if trigger != "" {
		sections = append(sections, trigger)
	}

	// --- Section 2: Skill references (if any) ---
	skillPaths := types.NormalizeSkillPaths(h.SkillPaths, h.SkillPath)
	if len(skillPaths) > 0 {
		sections = append(sections, buildSkillReferences(ctx, eng.skillReader, h.WorkspaceId, skillPaths))
	}

	// --- Section 3: Additional user-provided prompt ---
	if h.Prompt != "" {
		sections = append(sections, h.Prompt)
	}

	return strings.Join(sections, "\n\n")
}

// Start blocks until the engine context is cancelled.
func (eng *Engine) Start(ctx context.Context) {
	<-ctx.Done()
}

// Poll is retained as a no-op for compatibility with existing tests.
func (eng *Engine) Poll(ctx context.Context) {
	_ = ctx
}

// buildTriggerContext constructs the "what happened" section of the prompt.
// This is always the first thing Claude sees so it understands the trigger.
func buildTriggerContext(event string, data map[string]any) string {
	path, _ := data["path"].(string)
	absPath := workspaceAbsolutePath(path)
	integration, _ := data["integration"].(string)
	newCount, _ := data["new_count"].(string)
	newItems, _ := data["new_items"].(string)

	var b strings.Builder

	switch event {
	case EventFsCreate:
		b.WriteString("## Trigger\n\n")
		b.WriteString("A new file was created at `" + absPath + "`.\n")
		b.WriteString("Read it from: `" + absPath + "`")

	case EventFsWrite:
		b.WriteString("## Trigger\n\n")
		b.WriteString("A file was modified at `" + absPath + "`.\n")
		b.WriteString("Read the updated content from: `" + absPath + "`")

	case EventFsDelete:
		b.WriteString("## Trigger\n\n")
		b.WriteString("A file was deleted at `" + absPath + "`.")

	case EventSourceChange:
		b.WriteString("## Trigger\n\n")
		if integration != "" {
			b.WriteString("Source: **" + integration + "**\n")
		}
		b.WriteString(newCount + " new item(s) appeared in `" + absPath + "/`.\n")
		b.WriteString("List and read the files under `" + absPath + "/` to see what changed.")

		// Include item IDs if the source poller provided them
		if newItems != "" {
			b.WriteString("\n\nNew items: " + newItems)
		}

	default:
		return ""
	}

	return b.String()
}

// buildSkillReferences emits a compact "## Skills" section that tells the agent
// which skills are relevant and where to find them, without inlining the full
// SKILL.md contents (which would make the task prompt too verbose).
func buildSkillReferences(ctx context.Context, reader SkillReader, workspaceId uint, skillPaths []string) string {
	var b strings.Builder
	b.WriteString("## Skills\n\n")
	b.WriteString("The following skills are available in your workspace. Read the SKILL.md file at each path for detailed instructions.\n")

	for _, sp := range skillPaths {
		skillPath := workspaceAbsolutePath(sp)
		name := ""
		if reader != nil {
			content, err := reader.ReadSkillContent(ctx, workspaceId, sp)
			if err == nil {
				manifest, parseErr := skills.Parse([]byte(content))
				if parseErr == nil {
					name = manifest.Name
				}
			}
		}
		if name != "" {
			b.WriteString(fmt.Sprintf("\n- **%s** — read `%s/SKILL.md`", name, skillPath))
		} else {
			b.WriteString(fmt.Sprintf("\n- `%s/SKILL.md`", skillPath))
		}
	}

	return b.String()
}

// buildSkillContext adds integration/output path hints from the skill's metadata.
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

func (eng *Engine) ensureHookAgent(ctx context.Context, hook *types.Hook) bool {
	if hook == nil {
		return false
	}
	if hook.AgentId != nil && strings.TrimSpace(*hook.AgentId) != "" {
		return true
	}
	if eng.backend == nil {
		log.Warn().Str("hook", hook.ExternalId).Msg("hook has no agent and backend is unavailable")
		return false
	}

	profile, err := ResolveHookAgent(ctx, eng.backend, hook.WorkspaceId, hook.Path, nil, nil)
	if err != nil || profile == nil {
		log.Warn().Err(err).Str("hook", hook.ExternalId).Msg("failed to resolve hook agent")
		return false
	}

	agentID := profile.ID
	hook.AgentId = &agentID
	if eng.store != nil {
		if err := eng.store.UpdateHook(ctx, hook); err != nil {
			log.Warn().Err(err).Str("hook", hook.ExternalId).Msg("failed to persist hook agent linkage")
		}
	}
	return true
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
	timer         *time.Timer
	gen           uint64
	latestEventID string
	latestEvent   string
	latestData    map[string]any
	sawCreate     bool
	sawWrite      bool
	sawDelete     bool
}

func (d *debouncer) call(
	key, eventID, event string,
	data map[string]any,
	fn func(eventID, event string, data map[string]any),
) {
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
	e.absorb(eventID, event, data)

	gen := e.gen
	e.timer = time.AfterFunc(d.delay, func() {
		d.mu.Lock()
		if e.gen != gen {
			d.mu.Unlock()
			return
		}
		finalEventID := e.latestEventID
		finalEvent := e.resolveEvent()
		finalData := cloneEventData(e.latestData)
		finalData["event"] = finalEvent
		delete(d.state, key)
		d.mu.Unlock()
		fn(finalEventID, finalEvent, finalData)
	})
}

func (e *debounceEntry) absorb(eventID, event string, data map[string]any) {
	e.latestEventID = eventID
	e.latestEvent = event
	e.latestData = cloneEventData(data)

	switch event {
	case EventFsCreate:
		e.sawCreate = true
	case EventFsWrite:
		e.sawWrite = true
	case EventFsDelete:
		e.sawDelete = true
	}
}

// resolveEvent collapses a burst of fs.* events to a single "end result" event.
func (e *debounceEntry) resolveEvent() string {
	// Treat mixed delete+write/create bursts as "file exists" outcomes.
	// Copy/replace flows can emit transient deletes for the same path.
	if e.sawDelete {
		if e.sawCreate {
			return EventFsCreate
		}
		if e.sawWrite {
			return EventFsWrite
		}
		return EventFsDelete
	}
	if e.sawCreate {
		return EventFsCreate
	}
	if e.sawWrite {
		return EventFsWrite
	}
	return e.latestEvent
}

func cloneEventData(data map[string]any) map[string]any {
	if len(data) == 0 {
		return map[string]any{}
	}
	cloned := make(map[string]any, len(data))
	for k, v := range data {
		cloned[k] = v
	}
	return cloned
}

func isFilesystemEvent(event string) bool {
	switch event {
	case EventFsCreate, EventFsWrite, EventFsDelete:
		return true
	default:
		return false
	}
}

func workspaceAbsolutePath(path string) string {
	normalized := NormalizePath(path)
	if normalized == "/" {
		return "/workspace"
	}
	return "/workspace" + normalized
}
