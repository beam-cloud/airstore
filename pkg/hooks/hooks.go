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
	EventFsCreate = "fs.create"
	EventFsWrite  = "fs.write"
	EventFsDelete = "fs.delete"
)

const (
	debounceDelay = 2 * time.Second
	submitTimeout = 10 * time.Second
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
	event := strings.TrimSpace(mapString(data, "event"))
	wsId := ParseUint(data["workspace_id"])
	rawPath := strings.TrimSpace(mapString(data, "path"))

	if wsId == 0 || rawPath == "" || event == "" {
		log.Warn().Str("id", id).Str("event", event).Str("path", rawPath).
			Interface("workspace_id", data["workspace_id"]).
			Msg("hook engine: dropping malformed event")
		return
	}
	path := NormalizePath(rawPath)

	fire := func(eventID, resolvedEvent string, payload map[string]any) {
		eng.dispatchEvent(eventID, wsId, path, resolvedEvent, payload)
	}

	if isFilesystemEvent(event) {
		eng.debounce.call(fmt.Sprintf("%d:%s", wsId, path), id, event, data, fire)
	} else {
		fire(id, event, data)
	}
}

func (eng *Engine) InvalidateCache(wsId uint) { eng.cache.invalidate(wsId) }

func (eng *Engine) dispatchEvent(
	eventID string,
	workspaceID uint,
	fallbackPath string,
	event string,
	data map[string]any,
) {
	effectivePayload := cloneEventData(data)
	resolvedPath := NormalizePath(strings.TrimSpace(mapString(effectivePayload, "path")))
	if resolvedPath == "/" && strings.TrimSpace(mapString(effectivePayload, "path")) == "" {
		resolvedPath = fallbackPath
	}
	effectivePayload["path"] = resolvedPath

	ctx, cancel := context.WithTimeout(context.Background(), submitTimeout)
	defer cancel()

	effectiveEvent := event
	if isFilesystemEvent(effectiveEvent) {
		effectiveEvent = eng.reconcileFilesystemEvent(ctx, effectiveEvent, resolvedPath)
		effectivePayload["event"] = effectiveEvent
	}

	hooks := eng.cache.match(ctx, workspaceID, resolvedPath, effectiveEvent)
	if len(hooks) == 0 {
		log.Debug().Str("event", effectiveEvent).Str("path", resolvedPath).Uint("workspace_id", workspaceID).
			Msg("hook engine: no matching hooks")
		return
	}

	for _, h := range hooks {
		eng.submit(ctx, h, eventID, effectiveEvent, effectivePayload)
	}
}

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
//  1. Trigger context  – what happened (event type, path, integration, items)
//  2. Skill references – names + paths to SKILL.md files (not full contents)
//  3. Task line        – user-provided extra instructions from the hook prompt
func (eng *Engine) buildPrompt(ctx context.Context, h *types.Hook, event string, data map[string]any) string {
	trigger := buildTriggerContext(event, data)

	// --- Section 2: Skill references (if any) ---
	skillPaths := types.NormalizeSkillPaths(h.SkillPaths, h.SkillPath)
	skillReferences := ""
	if len(skillPaths) > 0 {
		skillReferences = buildSkillReferences(ctx, eng.skillReader, h.WorkspaceId, skillPaths)
	}

	// --- Section 3: User task prompt ---
	taskLine := ""
	taskPrompt := strings.TrimSpace(h.Prompt)
	if taskPrompt != "" {
		taskLine = "Task: " + taskPrompt
	}

	return joinPromptSections(trigger, skillReferences, taskLine)
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
	absPath := workspaceAbsolutePath(mapString(data, "path"))
	switch event {
	case EventFsCreate:
		if integration := strings.TrimSpace(mapString(data, "integration")); integration != "" {
			return buildSourceTrigger("created", absPath, integration, data)
		}
		return "A new file was created at `" + absPath + "`.\nRead it from: `" + absPath + "`"

	case EventFsWrite:
		return "A file was modified at `" + absPath + "`."

	case EventFsDelete:
		if integration := strings.TrimSpace(mapString(data, "integration")); integration != "" {
			return buildSourceTrigger("removed", absPath, integration, data)
		}
		return "A file was deleted at `" + absPath + "`."

	default:
		return ""
	}
}

func buildSourceTrigger(verb, absPath, integration string, data map[string]any) string {
	lines := make([]string, 0, 3)
	lines = append(lines, "Source: **"+integration+"**")

	if verb == "created" {
		count := strings.TrimSpace(mapString(data, "new_count"))
		lines = append(lines, count+" new item(s) appeared in `"+absPath+"/`.")
		trigger := strings.Join(lines, "\n")
		if items := strings.TrimSpace(mapString(data, "new_items")); items != "" {
			return trigger + "\n\nNew items: " + items
		}
		return trigger
	}

	count := strings.TrimSpace(mapString(data, "removed_count"))
	lines = append(lines, count+" item(s) were removed from `"+absPath+"/`.")
	trigger := strings.Join(lines, "\n")
	if items := strings.TrimSpace(mapString(data, "removed_items")); items != "" {
		return trigger + "\n\nRemoved items: " + items
	}
	return trigger
}

// buildSkillReferences emits a compact "## Skills" section that tells the agent
// which skills are relevant and where to find them, without inlining the full
// SKILL.md contents (which would make the task prompt too verbose).
func buildSkillReferences(ctx context.Context, reader SkillReader, workspaceId uint, skillPaths []string) string {
	lines := []string{
		"## Skills",
		"",
		"The following skills are available in your workspace. Read the SKILL.md file at each path for detailed instructions.",
	}
	for _, sp := range skillPaths {
		skillPath := workspaceAbsolutePath(sp)
		name := resolveSkillName(ctx, reader, workspaceId, sp)
		if name != "" {
			lines = append(lines, fmt.Sprintf("- **%s** — read `%s/SKILL.md`", name, skillPath))
		} else {
			lines = append(lines, fmt.Sprintf("- `%s/SKILL.md`", skillPath))
		}
	}
	return strings.Join(lines, "\n")
}

func resolveSkillName(ctx context.Context, reader SkillReader, workspaceId uint, skillPath string) string {
	if reader == nil {
		return ""
	}
	content, err := reader.ReadSkillContent(ctx, workspaceId, skillPath)
	if err != nil {
		return ""
	}
	manifest, err := skills.Parse([]byte(content))
	if err != nil {
		return ""
	}
	return strings.TrimSpace(manifest.Name)
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

func (c *hookCache) match(ctx context.Context, wsId uint, path string, event string) []*types.Hook {
	path = NormalizePath(path)
	c.mu.RLock()
	hooks, ok := c.hooks[wsId]
	c.mu.RUnlock()

	if !ok {
		hooks = c.load(ctx, wsId)
	}

	var out []*types.Hook
	for _, h := range hooks {
		hookPath := NormalizePath(h.Path)
		if !h.Active {
			continue
		}
		if !hookPathMatchesPath(hookPath, path) {
			continue
		}
		if !hookMatchesEvent(h, event) {
			continue
		}
		out = append(out, h)
	}
	return out
}

func hookMatchesEvent(h *types.Hook, event string) bool {
	if len(h.EventTypes) == 0 {
		return true
	}
	for _, et := range h.EventTypes {
		if et == event {
			return true
		}
	}
	return false
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

	hooks, err := c.store.ListHooks(ctx, wsId)
	if err != nil {
		log.Warn().
			Err(err).
			Uint("workspace_id", wsId).
			Msg("hook cache: failed to load hooks")
		// Do not cache failures; retry on the next event.
		return nil
	}
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

func joinPromptSections(sections ...string) string {
	filtered := make([]string, 0, len(sections))
	for _, section := range sections {
		if strings.TrimSpace(section) == "" {
			continue
		}
		filtered = append(filtered, section)
	}
	return strings.Join(filtered, "\n\n")
}

func hookPathMatchesPath(hookPath, path string) bool {
	hookPath = NormalizePath(hookPath)
	path = NormalizePath(path)
	if hookPath == "/" {
		return true
	}
	return path == hookPath || strings.HasPrefix(path, hookPath+"/")
}

func mapString(data map[string]any, key string) string {
	if len(data) == 0 {
		return ""
	}
	value, ok := data[key]
	if !ok || value == nil {
		return ""
	}
	if asString, ok := value.(string); ok {
		return asString
	}
	return fmt.Sprintf("%v", value)
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
