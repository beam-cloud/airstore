package views

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	baml "github.com/beam-cloud/airstore/pkg/views/baml_client"
	bamltypes "github.com/beam-cloud/airstore/pkg/views/baml_client/types"

	"github.com/beam-cloud/airstore/pkg/clients"
	"github.com/beam-cloud/airstore/pkg/common"
	"github.com/beam-cloud/airstore/pkg/orchestration"
	"github.com/beam-cloud/airstore/pkg/repository"
	"github.com/beam-cloud/airstore/pkg/skills"
	"github.com/beam-cloud/airstore/pkg/types"
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
)

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

type ChatMessage struct {
	Role       string            `json:"role"`
	Content    string            `json:"content"`
	Timestamp  int64             `json:"ts"`
	Operations []OperationResult `json:"operations,omitempty"`
}

type AttachedFile struct {
	Path        string `json:"path"`
	Name        string `json:"name"`
	ContentType string `json:"content_type,omitempty"`
}

type ChatState struct {
	ID              string        `json:"id"`
	WorkspaceID     string        `json:"workspace_id"`
	Status          string        `json:"status"`
	ViewContent     string        `json:"view_content"`
	PublishedViewID string        `json:"published_view_id,omitempty"`
	Messages        []ChatMessage `json:"messages"`
	CreatedAt       int64         `json:"created_at"`
	UpdatedAt       int64         `json:"updated_at"`
}

type DraftSummary struct {
	ID          string `json:"id"`
	Status      string `json:"status"`
	ViewName    string `json:"view_name,omitempty"`
	ViewID      string `json:"view_id,omitempty"`
	Description string `json:"description,omitempty"`
	CreatedAt   int64  `json:"created_at"`
	UpdatedAt   int64  `json:"updated_at"`
}

type PartialChatResponse struct {
	Message     string
	ViewContent string
	UpdateType  string
}

type OperationResult struct {
	Type       string `json:"type"`
	Name       string `json:"name"`
	Status     string `json:"status"`
	Error      string `json:"error,omitempty"`
	AgentID    string `json:"agent_id,omitempty"`
	TaskID     string `json:"task_id,omitempty"`
	AgentName  string `json:"agent_name,omitempty"`
	Message    string `json:"message,omitempty"`
	ScheduleID string `json:"schedule_id,omitempty"`
}

// S2 stream entry types — used for both draft log and draft index.
type draftStreamEntry struct {
	Type        string `json:"type"`
	Role        string `json:"role,omitempty"`
	Content     string `json:"content,omitempty"`
	WorkspaceID string `json:"workspace_id,omitempty"`
	DraftID     string `json:"draft_id,omitempty"`
	Description string `json:"description,omitempty"`
	ViewName    string `json:"view_name,omitempty"`
	ViewID      string `json:"view_id,omitempty"`
	Timestamp   int64  `json:"ts"`
}

const releaseViewPublishLockScript = `
if redis.call('get', KEYS[1]) == ARGV[1] then
	return redis.call('del', KEYS[1])
end
return 0
`

// ---------------------------------------------------------------------------
// Copilot
// ---------------------------------------------------------------------------

type Copilot struct {
	s2       *common.S2Client
	redis    *common.RedisClient
	backend  repository.BackendRepository
	storage  *clients.StorageClient
	agentAPI *orchestration.AgentAPI
	store    *ViewStore
}

func NewCopilot(s2 *common.S2Client, redis *common.RedisClient, backend repository.BackendRepository, storage *clients.StorageClient, agentAPI *orchestration.AgentAPI, store *ViewStore) *Copilot {
	return &Copilot{s2: s2, redis: redis, backend: backend, storage: storage, agentAPI: agentAPI, store: store}
}

func (c *Copilot) ChatAvailable() bool {
	return c != nil && c.s2 != nil && c.s2.Enabled()
}

// UpdateTypeConversation is the BAML enum value for conversation-only updates
// (no view content changes). Exported so the API layer can reference it
// without importing the generated baml_client/types package directly.
const UpdateTypeConversation = string(bamltypes.ViewUpdateTypeCONVERSATION)

// s2Append is the single write path for all S2 operations.
func (c *Copilot) s2Append(ctx context.Context, stream string, entry any) error {
	if c.s2 == nil || !c.s2.Enabled() {
		return nil
	}
	return c.s2.Append(ctx, stream, entry)
}

func nowMS() int64 { return time.Now().UnixMilli() }

// ---------------------------------------------------------------------------
// Draft lifecycle
// ---------------------------------------------------------------------------

func (c *Copilot) CreateChatState(workspaceID string) *ChatState {
	now := nowMS()
	return &ChatState{
		ID:          uuid.New().String(),
		WorkspaceID: workspaceID,
		Status:      "active",
		Messages:    []ChatMessage{},
		CreatedAt:   now,
		UpdatedAt:   now,
	}
}

func (c *Copilot) LoadChatState(ctx context.Context, workspaceID, viewID string) (*ChatState, error) {
	if c.s2 == nil || !c.s2.Enabled() {
		return nil, fmt.Errorf("S2 not configured")
	}
	records, err := c.s2.Read(ctx, common.Streams.ViewDraft(viewID), 0, 1000)
	if err != nil {
		return nil, fmt.Errorf("read chat stream: %w", err)
	}
	if len(records) == 0 {
		return nil, fmt.Errorf("chat state not found")
	}

	state := &ChatState{ID: viewID, Status: "active", Messages: []ChatMessage{}}
	for _, rec := range records {
		var e draftStreamEntry
		if err := json.Unmarshal([]byte(rec.Body), &e); err != nil {
			continue
		}
		switch e.Type {
		case "meta":
			state.WorkspaceID = e.WorkspaceID
			state.CreatedAt = e.Timestamp
		case "message":
			state.Messages = append(state.Messages, ChatMessage{Role: e.Role, Content: e.Content, Timestamp: e.Timestamp})
		case "operations":
			var ops []OperationResult
			if json.Unmarshal([]byte(e.Content), &ops) == nil && len(ops) > 0 {
				for i := len(state.Messages) - 1; i >= 0; i-- {
					if state.Messages[i].Role == "assistant" {
						state.Messages[i].Operations = ops
						break
					}
				}
			}
		case "view":
			state.ViewContent = e.Content
		case "published_view_id":
			state.PublishedViewID = e.Content
		case "status":
			state.Status = e.Content
		}
		if e.Timestamp > state.UpdatedAt {
			state.UpdatedAt = e.Timestamp
		}
	}
	if state.WorkspaceID == "" || (workspaceID != "" && state.WorkspaceID != workspaceID) {
		return nil, fmt.Errorf("chat state not found")
	}
	if state.UpdatedAt == 0 {
		state.UpdatedAt = state.CreatedAt
	}
	return state, nil
}

func (c *Copilot) DeleteChatState(ctx context.Context, workspaceID, id string) error {
	if err := c.persistChat(ctx, id, "status", "discarded", "", ""); err != nil {
		return fmt.Errorf("persist status: %w", err)
	}
	return c.indexDraft(ctx, workspaceID, "discarded", id, "", "", "")
}

// ---------------------------------------------------------------------------
// Chat persistence — all S2 writes go through persistChat
// ---------------------------------------------------------------------------

func (c *Copilot) persistChat(ctx context.Context, viewID, entryType, content, role, workspaceID string) error {
	return c.s2Append(ctx, common.Streams.ViewDraft(viewID), draftStreamEntry{
		Type:        entryType,
		Content:     content,
		Role:        role,
		WorkspaceID: workspaceID,
		Timestamp:   nowMS(),
	})
}

func (c *Copilot) indexDraft(ctx context.Context, workspaceID, eventType, draftID, description, viewName, viewID string) error {
	return c.s2Append(ctx, common.Streams.ViewDraftIndex(workspaceID), draftStreamEntry{
		Type:        eventType,
		DraftID:     draftID,
		Description: description,
		ViewName:    viewName,
		ViewID:      viewID,
		Timestamp:   nowMS(),
	})
}

// Public persistence API — thin wrappers for callers.
func (c *Copilot) PersistChatMeta(ctx context.Context, cs *ChatState) error {
	return c.s2Append(ctx, common.Streams.ViewDraft(cs.ID), draftStreamEntry{
		Type: "meta", WorkspaceID: cs.WorkspaceID, Timestamp: cs.CreatedAt,
	})
}
func (c *Copilot) PersistViewContent(ctx context.Context, viewID, viewContent string) error {
	return c.persistChat(ctx, viewID, "view", viewContent, "", "")
}

func (c *Copilot) PersistPublishedViewID(ctx context.Context, chatID, viewID string) error {
	return c.persistChat(ctx, chatID, "published_view_id", viewID, "", "")
}

func (c *Copilot) PersistOperations(ctx context.Context, viewID string, results []OperationResult) {
	opsJSON, err := json.Marshal(results)
	if err != nil {
		return
	}
	_ = c.persistChat(ctx, viewID, "operations", string(opsJSON), "", "")
}

func (c *Copilot) IndexDraftCreated(ctx context.Context, workspaceID, draftID, desc, viewName, viewID string) error {
	return c.indexDraft(ctx, workspaceID, "created", draftID, desc, viewName, viewID)
}
func (c *Copilot) IndexDraftPublished(ctx context.Context, workspaceID, draftID, viewName, viewID string) error {
	return c.indexDraft(ctx, workspaceID, "published", draftID, "", viewName, viewID)
}

func (c *Copilot) IndexDraftPublishedAsync(workspaceID, draftID, viewName, viewID string) {
	if c == nil || strings.TrimSpace(workspaceID) == "" || strings.TrimSpace(draftID) == "" || strings.TrimSpace(viewID) == "" {
		return
	}

	viewName = strings.TrimSpace(viewName)
	go func() {
		indexCtx, indexCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer indexCancel()
		if err := c.IndexDraftPublished(indexCtx, workspaceID, draftID, viewName, viewID); err != nil {
			log.Warn().Err(err).
				Str("workspace_id", workspaceID).
				Str("draft_id", draftID).
				Str("view_id", viewID).
				Msg("failed to index published draft")
		}
	}()
}

// ---------------------------------------------------------------------------
// Draft listing
// ---------------------------------------------------------------------------

func (c *Copilot) ListDrafts(ctx context.Context, workspaceID string) ([]DraftSummary, error) {
	if c.s2 == nil || !c.s2.Enabled() {
		return nil, nil
	}
	records, err := c.s2.Read(ctx, common.Streams.ViewDraftIndex(workspaceID), 0, 1000)
	if err != nil {
		return nil, err
	}

	drafts := make(map[string]*DraftSummary)
	for _, rec := range records {
		var e draftStreamEntry
		if err := json.Unmarshal([]byte(rec.Body), &e); err != nil {
			continue
		}
		switch e.Type {
		case "created":
			drafts[e.DraftID] = &DraftSummary{
				ID: e.DraftID, Status: "active", Description: e.Description,
				ViewName: e.ViewName, ViewID: e.ViewID,
				CreatedAt: e.Timestamp, UpdatedAt: e.Timestamp,
			}
		case "published":
			if d, ok := drafts[e.DraftID]; ok {
				d.Status, d.ViewName, d.ViewID, d.UpdatedAt = "published", e.ViewName, e.ViewID, e.Timestamp
			}
		case "discarded":
			if d, ok := drafts[e.DraftID]; ok {
				d.Status, d.UpdatedAt = "discarded", e.Timestamp
			}
		}
	}

	// The workspace index is a secondary projection. If the async publish-index
	// append lags or is dropped, fall back to the authoritative draft stream so
	// published drafts still surface correctly across replicas, then repair the
	// index opportunistically in the background.
	reconcileCtx, reconcileCancel := context.WithTimeout(ctx, 2*time.Second)
	defer reconcileCancel()

	var numericWorkspaceID uint
	workspaceResolved := false
	resolveWorkspaceID := func() (uint, bool) {
		if workspaceResolved {
			return numericWorkspaceID, numericWorkspaceID != 0
		}
		workspaceResolved = true
		if c.backend == nil {
			return 0, false
		}
		ws, err := c.backend.GetWorkspaceByExternalId(reconcileCtx, workspaceID)
		if err != nil || ws == nil {
			return 0, false
		}
		numericWorkspaceID = ws.Id
		return numericWorkspaceID, true
	}

	viewBySourceDraftID := map[string]*types.View{}
	loadViewsBySourceDraftID := func() map[string]*types.View {
		if len(viewBySourceDraftID) > 0 || c.backend == nil {
			return viewBySourceDraftID
		}
		wsID, ok := resolveWorkspaceID()
		if !ok {
			return viewBySourceDraftID
		}
		views, err := c.backend.ListViews(reconcileCtx, wsID)
		if err != nil {
			return viewBySourceDraftID
		}
		for _, view := range views {
			if view == nil || strings.TrimSpace(view.SourceDraftID) == "" {
				continue
			}
			viewBySourceDraftID[strings.TrimSpace(view.SourceDraftID)] = view
		}
		return viewBySourceDraftID
	}

	for _, d := range drafts {
		if d == nil || d.Status == "published" || d.Status == "discarded" {
			continue
		}
		draft, err := c.LoadChatState(reconcileCtx, workspaceID, d.ID)
		if err != nil || draft == nil {
			continue
		}

		publishedViewID := strings.TrimSpace(draft.PublishedViewID)
		if publishedViewID == "" {
			if view := loadViewsBySourceDraftID()[d.ID]; view != nil {
				publishedViewID = strings.TrimSpace(view.ID)
				d.ViewName = strings.TrimSpace(view.Name)
			}
		}
		if publishedViewID == "" {
			continue
		}

		d.Status = "published"
		d.ViewID = publishedViewID
		if draft.UpdatedAt > d.UpdatedAt {
			d.UpdatedAt = draft.UpdatedAt
		}
		if d.ViewName == "" {
			if wsID, ok := resolveWorkspaceID(); ok {
				if view, err := c.backend.GetView(reconcileCtx, wsID, publishedViewID); err == nil && view != nil {
					d.ViewName = strings.TrimSpace(view.Name)
				}
			}
		}
		c.IndexDraftPublishedAsync(workspaceID, d.ID, d.ViewName, publishedViewID)
	}

	result := make([]DraftSummary, 0, len(drafts))
	for _, d := range drafts {
		result = append(result, *d)
	}
	sort.SliceStable(result, func(i, j int) bool {
		if result[i].UpdatedAt != result[j].UpdatedAt {
			return result[i].UpdatedAt > result[j].UpdatedAt
		}
		return result[i].CreatedAt > result[j].CreatedAt
	})
	return result, nil
}

// ---------------------------------------------------------------------------
// Publishing
// ---------------------------------------------------------------------------

func (c *Copilot) PublishView(ctx context.Context, cs *ChatState, workspaceID uint) (*types.View, error) {
	if cs.ViewContent == "" {
		return nil, fmt.Errorf("no view content to publish")
	}

	var def types.ViewDefinition
	if err := json.Unmarshal([]byte(cs.ViewContent), &def); err != nil {
		return nil, fmt.Errorf("invalid view definition: %w", err)
	}
	agents := c.loadWorkspaceAgents(ctx, workspaceID)
	preCanonAgents := append([]string{}, def.Agents...)
	normalizeViewDefinition(&def)
	canonicalizeViewAgentRefs(&def, agents, nil)
	normalizeViewDefinition(&def)
	classifyDetailTemplates(ctx, &def)

	log.Info().
		Strs("pre_canon_agents", preCanonAgents).
		Strs("post_canon_agents", def.Agents).
		Int("workspace_agents", len(agents)).
		Str("chat_state_id", cs.ID).
		Str("view_name", def.Name).
		Msg("view: publishing definition")

	const publishLockTTL = 30 * time.Second
	if c.redis != nil {
		lockKey := common.Keys.ViewPublishLock(cs.ID)
		lockToken := uuid.NewString()
		acquired, err := c.redis.SetNX(ctx, lockKey, lockToken, publishLockTTL).Result()
		if err != nil {
			return nil, fmt.Errorf("publish lock: %w", err)
		}
		if !acquired {
			time.Sleep(500 * time.Millisecond)
			if fresh, err := c.LoadChatState(ctx, cs.WorkspaceID, cs.ID); err == nil && fresh != nil && fresh.PublishedViewID != "" {
				cs.PublishedViewID = fresh.PublishedViewID
				cs.Status = "published"
				if v, err := c.backend.GetView(ctx, workspaceID, fresh.PublishedViewID); err == nil && v != nil {
					return v, nil
				}
			}
			if existingViews, err := c.backend.ListViews(ctx, workspaceID); err == nil {
				for _, existing := range existingViews {
					if existing != nil && strings.TrimSpace(existing.SourceDraftID) == cs.ID {
						return existing, nil
					}
				}
			}
			return nil, fmt.Errorf("view is being published by another replica")
		}
		defer func() {
			delCtx, delCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer delCancel()
			if _, err := c.redis.Eval(
				delCtx,
				releaseViewPublishLockScript,
				[]string{lockKey},
				lockToken,
			).Int64(); err != nil {
				log.Warn().Err(err).Str("view_id", cs.ID).Msg("failed to release publish lock")
			}
		}()
	}

	if cs.PublishedViewID == "" {
		if fresh, err := c.LoadChatState(ctx, cs.WorkspaceID, cs.ID); err == nil && fresh != nil && fresh.PublishedViewID != "" {
			cs.PublishedViewID = fresh.PublishedViewID
		}
	}
	if cs.PublishedViewID == "" {
		if existingViews, err := c.backend.ListViews(ctx, workspaceID); err == nil {
			for _, existing := range existingViews {
				if existing != nil && strings.TrimSpace(existing.SourceDraftID) == cs.ID {
					cs.PublishedViewID = existing.ID
					break
				}
			}
		}
	}

	var published *types.View
	if cs.PublishedViewID != "" {
		if existing, err := c.backend.GetView(ctx, workspaceID, cs.PublishedViewID); err == nil && existing != nil {
			existing.Name, existing.Description, existing.SourceDraftID, existing.Definition = def.Name, def.Description, cs.ID, def
			if err := c.backend.UpdateView(ctx, existing); err != nil {
				return nil, fmt.Errorf("update view: %w", err)
			}
			published = existing
		}
	}

	if published == nil {
		published = &types.View{
			WorkspaceID:   workspaceID,
			Name:          def.Name,
			Description:   def.Description,
			SourceDraftID: cs.ID,
			Definition:    def,
		}
		if err := c.backend.CreateView(ctx, published); err != nil {
			return nil, fmt.Errorf("create view: %w", err)
		}
	}

	cs.PublishedViewID = published.ID
	cs.Status = "published"

	persistCtx, persistCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer persistCancel()
	if err := c.persistChat(persistCtx, cs.ID, "published_view_id", published.ID, "", ""); err != nil {
		log.Warn().Err(err).Str("view_id", published.ID).Msg("failed to persist published view id")
	}
	if err := c.persistChat(persistCtx, cs.ID, "status", "published", "", ""); err != nil {
		log.Warn().Err(err).Str("view_id", published.ID).Msg("failed to persist published status")
	}

	return published, nil
}

// ---------------------------------------------------------------------------
// BAML generation
// ---------------------------------------------------------------------------

func (c *Copilot) FormatHistory(messages []ChatMessage) string {
	if len(messages) == 0 {
		return ""
	}
	var sb strings.Builder
	for _, m := range messages {
		role := "User"
		if m.Role == "assistant" {
			role = "Assistant"
		}
		fmt.Fprintf(&sb, "[%s] %s: %s\n", time.UnixMilli(m.Timestamp).Format("Jan 2 15:04"), role, m.Content)
	}
	return sb.String()
}

func (c *Copilot) GenerateStream(
	ctx context.Context,
	cs *ChatState,
	workspaceID uint,
	userMessage string,
	viewID string,
	attachedFiles []AttachedFile,
	onChunk func(partial *PartialChatResponse),
) (*bamltypes.ViewDraftResponse, error) {
	_ = c.persistChat(ctx, cs.ID, "message", userMessage, "user", "")
	cs.Messages = append(cs.Messages, ChatMessage{Role: "user", Content: userMessage, Timestamp: nowMS()})

	promptMessage := userMessage
	if len(attachedFiles) > 0 {
		fileContext := c.readAttachedFiles(ctx, workspaceID, attachedFiles)
		if fileContext != "" {
			promptMessage = fileContext + "\n" + userMessage
		}
	}

	history := c.FormatHistory(cs.Messages[:len(cs.Messages)-1])
	workspaceAgents := c.loadWorkspaceAgents(ctx, workspaceID)

	// Canonicalize agent refs in the current view against fresh DB state so the
	// model always sees real UUIDs — never stale names or kebab-keys from a prior
	// turn or from the frontend.
	if cs.ViewContent != "" {
		if canonical, err := normalizeViewContent(cs.ViewContent, workspaceAgents); err == nil {
			cs.ViewContent = canonical
		}
	}

	workspaceCtx := c.BuildWorkspaceContext(ctx, workspaceID)
	viewData := c.BuildViewDataContext(ctx, viewID, cs.ViewContent)
	activeTasks := c.BuildActiveTasksContext(ctx, workspaceID, cs.ViewContent, cs.PublishedViewID)

	ch, err := baml.Stream.WriteView(ctx, promptMessage, history, cs.ViewContent, workspaceCtx, ComponentRegistryDoc, viewData, activeTasks)
	if err != nil {
		return nil, fmt.Errorf("BAML WriteView stream: %w", err)
	}

	var final *bamltypes.ViewDraftResponse
	for val := range ch {
		if val.IsError {
			return nil, val.Error
		}
		if val.IsFinal {
			final = val.Final()
		} else if s := val.Stream(); s != nil && onChunk != nil {
			onChunk(&PartialChatResponse{
				Message:     deref(s.Message),
				ViewContent: deref(s.View_content),
				UpdateType:  derefEnum(s.Update_type),
			})
		}
	}
	if final == nil {
		return nil, fmt.Errorf("no final response from BAML stream")
	}
	// Drop truncated view_content — don't serve a half-built definition.
	// max_tokens on the BAML client (16384) should prevent this; if it still
	// happens the model generated an unusually large view.
	if final.View_content != "" && !json.Valid([]byte(final.View_content)) {
		log.Error().Int("len", len(final.View_content)).Msg("copilot view_content is invalid JSON (likely truncated) — discarding")
		final.View_content = ""
	}
	// When the view is empty (new project) the model must not return CONVERSATION —
	// that would discard the view_content. Override to VIEW_CREATE so the definition
	// is persisted. This guards against the LLM seeing existing workspace agents and
	// mistakenly treating a new project as an already-configured workspace.
	if final.Update_type == bamltypes.ViewUpdateTypeCONVERSATION && final.View_content != "" && !viewHasSheets(cs.ViewContent) {
		log.Warn().Msg("copilot returned CONVERSATION with view_content for empty view — overriding to VIEW_CREATE")
		final.Update_type = bamltypes.ViewUpdateTypeVIEW_CREATE
	}
	if final.Update_type != bamltypes.ViewUpdateTypeCONVERSATION && final.View_content != "" {
		if normalized, err := normalizeViewContent(final.View_content, workspaceAgents); err == nil {
			final.View_content = normalized
		}
		if cs.ViewContent != "" {
			if merged, err := mergePreserveSheets(cs.ViewContent, final.View_content, final.Removed_sheet_ids); err == nil {
				final.View_content = merged
			}
		}
	}

	_ = c.persistChat(ctx, cs.ID, "message", final.Message, "assistant", "")
	if final.Update_type != bamltypes.ViewUpdateTypeCONVERSATION && final.View_content != "" {
		_ = c.persistChat(ctx, cs.ID, "view", final.View_content, "", "")
		cs.ViewContent = final.View_content
	}

	cs.Messages = append(cs.Messages, ChatMessage{Role: "assistant", Content: final.Message, Timestamp: nowMS()})
	cs.UpdatedAt = nowMS()
	return final, nil
}

// ---------------------------------------------------------------------------
// File attachment reading
// ---------------------------------------------------------------------------

const (
	maxFileTextBytes  = 50 * 1024  // 50KB per text file
	maxTotalTextBytes = 100 * 1024 // 100KB total across all text attachments
	maxCSVPreviewRows = 200
)

var textExtensions = map[string]bool{
	".txt": true, ".md": true, ".json": true, ".csv": true, ".tsv": true,
	".xml": true, ".html": true, ".yml": true, ".yaml": true, ".log": true,
}

var imageExtensions = map[string]bool{
	".png": true, ".jpg": true, ".jpeg": true, ".gif": true, ".webp": true,
}

func isTextFile(name, contentType string) bool {
	ext := strings.ToLower(filepath.Ext(name))
	if textExtensions[ext] {
		return true
	}
	return strings.HasPrefix(contentType, "text/") ||
		contentType == "application/json" ||
		contentType == "application/xml"
}

func isCSVFile(name, contentType string) bool {
	ext := strings.ToLower(filepath.Ext(name))
	return ext == ".csv" || ext == ".tsv" || contentType == "text/csv"
}

func isImageFile(name, contentType string) bool {
	ext := strings.ToLower(filepath.Ext(name))
	return imageExtensions[ext] || strings.HasPrefix(contentType, "image/")
}

func (c *Copilot) readAttachedFiles(ctx context.Context, workspaceID uint, files []AttachedFile) string {
	if c.storage == nil || len(files) == 0 {
		return ""
	}
	ws, err := c.backend.GetWorkspace(ctx, workspaceID)
	if err != nil {
		log.Warn().Err(err).Uint("workspace_id", workspaceID).Msg("cannot read attached files: workspace lookup failed")
		return formatFileListFallback(files)
	}
	bucket := c.storage.WorkspaceBucketName(ws.ExternalId)

	var sb strings.Builder
	sb.WriteString("=== ATTACHED FILES ===\n\n")
	totalTextBytes := 0
	hasContent := false

	for _, f := range files {
		ct := f.ContentType
		if ct == "" {
			ct = inferContentType(f.Name)
		}

		if isImageFile(f.Name, ct) {
			fmt.Fprintf(&sb, "── File: %s (image, path: %s) ──\n", f.Name, f.Path)
			sb.WriteString("[Image file attached — content visible to the model as a reference.]\n\n")
			hasContent = true
			continue
		}

		if !isTextFile(f.Name, ct) {
			fmt.Fprintf(&sb, "── File: %s (path: %s) ──\n", f.Name, f.Path)
			sb.WriteString("[Binary or unsupported file format — content not readable.]\n\n")
			hasContent = true
			continue
		}

		key := strings.TrimPrefix(f.Path, "/")
		data, err := c.storage.Download(ctx, bucket, key)
		if err != nil {
			log.Warn().Err(err).Str("path", f.Path).Msg("failed to download attached file")
			fmt.Fprintf(&sb, "── File: %s (path: %s) ──\n", f.Name, f.Path)
			sb.WriteString("[Could not read file content.]\n\n")
			hasContent = true
			continue
		}

		remaining := maxTotalTextBytes - totalTextBytes
		if remaining <= 0 {
			fmt.Fprintf(&sb, "── File: %s (path: %s) ──\n", f.Name, f.Path)
			sb.WriteString("[Skipped — total attachment size limit reached.]\n\n")
			hasContent = true
			continue
		}

		if isCSVFile(f.Name, ct) {
			content := formatCSVPreview(f.Name, f.Path, data)
			if len(content) > remaining {
				content = content[:remaining] + "\n... (truncated)\n"
			}
			sb.WriteString(content)
			totalTextBytes += len(content)
		} else {
			text := string(data)
			if len(text) > maxFileTextBytes {
				text = text[:maxFileTextBytes] + "\n... (truncated)"
			}
			if len(text) > remaining {
				text = text[:remaining] + "\n... (truncated)"
			}
			fmt.Fprintf(&sb, "── File: %s (path: %s) ──\n", f.Name, f.Path)
			sb.WriteString(text)
			sb.WriteString("\n\n")
			totalTextBytes += len(text)
		}
		hasContent = true
	}

	if !hasContent {
		return ""
	}
	sb.WriteString("=== END ATTACHED FILES ===\n\n")
	return sb.String()
}

func formatCSVPreview(name, path string, data []byte) string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "── File: %s (CSV, path: %s) ──\n", name, path)

	r := csv.NewReader(bytes.NewReader(data))
	r.LazyQuotes = true
	r.FieldsPerRecord = -1

	headers, err := r.Read()
	if err != nil {
		sb.WriteString("[Could not parse CSV headers.]\n\n")
		return sb.String()
	}

	totalRows := 0
	var rows [][]string
	for {
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			break
		}
		totalRows++
		if len(rows) < maxCSVPreviewRows {
			rows = append(rows, record)
		}
	}

	fmt.Fprintf(&sb, "Headers: %s\n", strings.Join(headers, " | "))
	fmt.Fprintf(&sb, "Total rows: %d (showing first %d)\n\n", totalRows, len(rows))

	sb.WriteString(strings.Join(headers, " | "))
	sb.WriteString("\n")
	for range headers {
		sb.WriteString("--- | ")
	}
	sb.WriteString("\n")

	for _, row := range rows {
		for i, col := range row {
			if i > 0 {
				sb.WriteString(" | ")
			}
			if len(col) > 100 {
				col = col[:97] + "..."
			}
			sb.WriteString(col)
		}
		sb.WriteString("\n")
	}
	sb.WriteString("\n")
	return sb.String()
}

func formatFileListFallback(files []AttachedFile) string {
	var sb strings.Builder
	sb.WriteString("Attached files:\n")
	for _, f := range files {
		fmt.Fprintf(&sb, "- %s (path: %s)\n", f.Name, f.Path)
	}
	sb.WriteString("\n")
	return sb.String()
}

func inferContentType(name string) string {
	ext := strings.ToLower(filepath.Ext(name))
	switch ext {
	case ".csv":
		return "text/csv"
	case ".tsv":
		return "text/tab-separated-values"
	case ".json":
		return "application/json"
	case ".txt", ".md", ".log":
		return "text/plain"
	case ".xml":
		return "application/xml"
	case ".html":
		return "text/html"
	case ".png":
		return "image/png"
	case ".jpg", ".jpeg":
		return "image/jpeg"
	case ".gif":
		return "image/gif"
	case ".webp":
		return "image/webp"
	case ".pdf":
		return "application/pdf"
	default:
		return "application/octet-stream"
	}
}

// ---------------------------------------------------------------------------
// Workspace context for BAML prompt
// ---------------------------------------------------------------------------

func (c *Copilot) BuildWorkspaceContext(ctx context.Context, workspaceID uint) string {
	agents, err := c.backend.ListAgentProfiles(ctx, workspaceID)
	if err != nil || len(agents) == 0 {
		return "No agents configured in this workspace."
	}

	skillManifests := c.loadSkillManifests(ctx, workspaceID)
	var sb strings.Builder
	sb.WriteString("AGENTS AND SKILLS\n")
	sb.WriteString(strings.Repeat("─", 60) + "\n")
	sb.WriteString("If multiple agents share the same skills or output schema, treat them as alternatives.\n")
	sb.WriteString("Only include multiple agents in a view when the user explicitly wants multiple distinct agents or different components truly depend on different agents.\n")

	emittedSkills := make(map[string]bool)
	for _, a := range agents {
		fmt.Fprintf(&sb, "\n▸ Agent: %s (ID: %s)\n", a.Name, a.ID)
		if strings.TrimSpace(a.AgentKey) != "" {
			fmt.Fprintf(&sb, "  Key: %s\n", a.AgentKey)
		}
		if a.Role != "" {
			fmt.Fprintf(&sb, "  Role: %s\n", a.Role)
		}
		agentSkills := extractStringSlice(a.ConfigJSON, "skills")
		if len(agentSkills) == 0 {
			sb.WriteString("  Skills: (none assigned)\n")
		} else {
			sb.WriteString("  Skills:\n")
			for _, sn := range agentSkills {
				if ls, ok := skillManifests[sn]; ok {
					m := ls.Manifest
					fmt.Fprintf(&sb, "    • %s — %s\n", sn, m.Description)
					meta := m.AirstoreMetadata()
					if len(meta.Needs) > 0 {
						fmt.Fprintf(&sb, "      integrations: %s\n", strings.Join(meta.Needs, ", "))
					}
					if len(meta.Writes) > 0 {
						fmt.Fprintf(&sb, "      output paths: %s\n", strings.Join(meta.Writes, ", "))
					}
					emittedSkills[sn] = true
				} else {
					fmt.Fprintf(&sb, "    • %s\n", sn)
				}
			}
		}
	}

	if len(emittedSkills) > 0 {
		sb.WriteString("\n" + strings.Repeat("─", 60) + "\n")
		sb.WriteString("SKILL DEFINITIONS\n")
		sb.WriteString(strings.Repeat("─", 60) + "\n")
		sb.WriteString("Full skill instructions for the agents assigned to this project.\n")
		sb.WriteString("Use these to understand what the agents can do and how they work.\n\n")
		for name := range emittedSkills {
			ls := skillManifests[name]
			fmt.Fprintf(&sb, "── %s ──\n%s\n\n", name, strings.TrimSpace(ls.Content))
		}
	}

	if summaries := c.loadWorkspaceSchemaSummaries(ctx, workspaceID); len(summaries) > 0 {
		writeWorkspaceSchemaSummaries(&sb, summaries)
	} else {
		writeColdStartGuidance(&sb)
	}
	return sb.String()
}

func (c *Copilot) loadWorkspaceSchemaSummaries(ctx context.Context, workspaceID uint) []outputSchemaSummary {
	outputs, err := c.backend.ListWorkspaceTaskOutputs(ctx, workspaceID, types.TaskOutputListFilter{
		ExcludeArchived: false,
		Limit:           200,
	})
	if err != nil || len(outputs) == 0 {
		return nil
	}
	return summarizeWorkspaceSchemas(outputs)
}

func (c *Copilot) loadWorkspaceAgents(ctx context.Context, workspaceID uint) []*types.AgentProfile {
	agents, err := c.backend.ListAgentProfiles(ctx, workspaceID)
	if err != nil {
		return nil
	}
	return agents
}

func writeColdStartGuidance(sb *strings.Builder) {
	sb.WriteString("\n" + strings.Repeat("─", 60) + "\n")
	sb.WriteString("NO ARTIFACT OUTPUTS YET\n")
	sb.WriteString(strings.Repeat("─", 60) + "\n")
	sb.WriteString("No task outputs have been produced yet. Use these resilient defaults:\n")
	sb.WriteString("- Define columns with descriptive names and types\n")
	sb.WriteString("- Use source hints like \"title\", \"summary\", \"uri\", \"created_at\"\n")
	sb.WriteString("- The BAML mapper will dynamically resolve output data to columns at render time\n")
	sb.WriteString("- Keep column definitions semantic and minimal (3-5 columns)\n")
}

// BuildViewDataContext loads row data from MongoDB for a published view and
// formats it as a readable text table that can be injected into the BAML prompt.
func (c *Copilot) BuildViewDataContext(ctx context.Context, viewID string, viewContent string) string {
	if c.store == nil || !c.store.Available() || viewID == "" || viewContent == "" {
		return ""
	}

	var def types.ViewDefinition
	if err := json.Unmarshal([]byte(viewContent), &def); err != nil {
		return ""
	}

	const maxRowsPerSheet = 100
	const maxTotalChars = 50000

	var sb strings.Builder
	totalRows := 0

	for _, sheet := range def.Sheets {
		var tableComp *types.ComponentSpec
		for i := range sheet.Components {
			if sheet.Components[i].Type == "table" {
				tableComp = &sheet.Components[i]
				break
			}
		}
		if tableComp == nil {
			continue
		}

		rows, err := c.store.GetRows(ctx, viewID, sheet.ID, tableComp.ID)
		if err != nil || len(rows) == 0 {
			continue
		}

		columns := extractColumnKeys(tableComp)
		if len(columns) == 0 {
			continue
		}

		fmt.Fprintf(&sb, "\n── Sheet: %s (%d rows) ──\n", sheet.Name, len(rows))

		sb.WriteString("# | ")
		for _, col := range columns {
			sb.WriteString(col.label)
			sb.WriteString(" | ")
		}
		sb.WriteString("\n")
		sb.WriteString("--- | ")
		for range columns {
			sb.WriteString("--- | ")
		}
		sb.WriteString("\n")

		limit := len(rows)
		if limit > maxRowsPerSheet {
			limit = maxRowsPerSheet
		}
		for _, row := range rows[:limit] {
			fmt.Fprintf(&sb, "row:%s:%s | ", row.SheetID, row.ID)
			cells := row.MergedCells()
			for _, col := range columns {
				val := cells[col.key]
				if len(val) > 120 {
					val = val[:117] + "..."
				}
				sb.WriteString(val)
				sb.WriteString(" | ")
			}
			sb.WriteString("\n")
			totalRows++
		}
		if len(rows) > limit {
			fmt.Fprintf(&sb, "... and %d more rows\n", len(rows)-limit)
		}

		if sb.Len() > maxTotalChars {
			sb.WriteString("\n(data truncated for context limit)\n")
			break
		}
	}

	if totalRows == 0 {
		return ""
	}
	return sb.String()
}

// BuildActiveTasksContext loads active/waiting tasks for the view's agents
// and formats them for the BAML prompt so the model can reason about
// what to approve, reject, or dispatch.
func (c *Copilot) BuildActiveTasksContext(ctx context.Context, workspaceID uint, viewContent string, viewID string) string {
	if c.agentAPI == nil || viewContent == "" {
		return ""
	}

	var def types.ViewDefinition
	if err := json.Unmarshal([]byte(viewContent), &def); err != nil || len(def.Agents) == 0 {
		return ""
	}

	activeStates := []types.AgentTaskState{
		types.AgentTaskStateQueued,
		types.AgentTaskStateRunning,
		types.AgentTaskStateWaiting,
		types.AgentTaskStateSleeping,
	}

	var sb strings.Builder
	totalTasks := 0
	for _, agentRef := range def.Agents {
		ref := strings.TrimSpace(agentRef)
		if ref == "" {
			continue
		}
		tasks, _, _, err := c.agentAPI.ListTasksFiltered(ctx, workspaceID, types.AgentTaskListFilter{
			AgentID: &ref,
			States:  activeStates,
			Limit:   20,
		})
		if err != nil || len(tasks) == 0 {
			continue
		}

		for _, task := range tasks {
			totalTasks++
			title := ""
			if task.PayloadJSON != nil {
				if msg, ok := task.PayloadJSON["message"].(string); ok {
					title = msg
					if len(title) > 100 {
						title = title[:97] + "..."
					}
				}
			}
			agentName := task.AgentName
			if agentName == "" {
				agentName = ref
			}
			waitInfo := ""
			if task.State == types.AgentTaskStateWaiting {
				waitInfo = " [NEEDS ATTENTION]"
				if task.WaitingSummary != nil {
					waitInfo = fmt.Sprintf(" [NEEDS ATTENTION: %s]", *task.WaitingSummary)
				}
			}
			fmt.Fprintf(&sb, "- Task %s | agent: %s | state: %s | %s%s\n",
				task.ID, agentName, string(task.State), title, waitInfo)
		}
	}

	if viewID != "" {
		schedules, err := c.agentAPI.ListSchedulesByView(ctx, workspaceID, viewID)
		if err == nil && len(schedules) > 0 {
			sb.WriteString("\nSCHEDULES:\n")
			for _, s := range schedules {
				activeStr := "active"
				if !s.Active {
					activeStr = "paused"
				}
				prompt := s.Prompt
				if len(prompt) > 80 {
					prompt = prompt[:77] + "..."
				}
				fmt.Fprintf(&sb, "- Schedule %s | agent: %s | cron: %s (%s) | %s | %s\n",
					s.ExternalID, s.AgentID, s.CronExpr, s.Timezone, activeStr, prompt)
			}
		}
	}

	if totalTasks == 0 && sb.Len() == 0 {
		return ""
	}
	return sb.String()
}

type columnInfo struct {
	key   string
	label string
}

func extractColumnKeys(comp *types.ComponentSpec) []columnInfo {
	if comp == nil || comp.Config == nil {
		return nil
	}
	rawCols, ok := comp.Config["columns"]
	if !ok {
		return nil
	}

	var columns []columnInfo
	switch typed := rawCols.(type) {
	case []any:
		for _, item := range typed {
			if m, ok := item.(map[string]any); ok {
				key, _ := m["key"].(string)
				label, _ := m["label"].(string)
				if key == "" {
					continue
				}
				if label == "" {
					label = key
				}
				columns = append(columns, columnInfo{key: key, label: label})
			}
		}
	case []configColumn:
		for _, col := range typed {
			if col.Key == "" {
				continue
			}
			label := col.Label
			if label == "" {
				label = col.Key
			}
			columns = append(columns, columnInfo{key: col.Key, label: label})
		}
	}
	return columns
}

func viewHasSheets(viewContent string) bool {
	if viewContent == "" {
		return false
	}
	var def types.ViewDefinition
	if err := json.Unmarshal([]byte(viewContent), &def); err != nil {
		return false
	}
	return len(def.Sheets) > 0
}

func normalizeViewContent(viewContent string, agents []*types.AgentProfile) (string, error) {
	var def types.ViewDefinition
	if err := json.Unmarshal([]byte(viewContent), &def); err != nil {
		return "", err
	}
	normalizeViewDefinition(&def)
	canonicalizeViewAgentRefs(&def, agents, nil)
	normalizeViewDefinition(&def)
	normalized, err := json.Marshal(def)
	if err != nil {
		return "", err
	}
	return string(normalized), nil
}

func (c *Copilot) ReconcileViewContent(ctx context.Context, workspaceID uint, viewContent string, opResults []OperationResult) (string, error) {
	var def types.ViewDefinition
	if err := json.Unmarshal([]byte(viewContent), &def); err != nil {
		return "", err
	}
	normalizeViewDefinition(&def)
	canonicalizeViewAgentRefs(&def, c.loadWorkspaceAgents(ctx, workspaceID), opResults)
	normalizeViewDefinition(&def)
	normalized, err := json.Marshal(def)
	if err != nil {
		return "", err
	}
	return string(normalized), nil
}

// mergePreserveSheets ensures that sheets present in the previous view
// definition but absent from the new one are carried forward, unless the
// model explicitly marked them for removal.
func mergePreserveSheets(previousContent, newContent string, removedSheetIDs []string) (string, error) {
	var prev, next types.ViewDefinition
	if err := json.Unmarshal([]byte(previousContent), &prev); err != nil {
		return newContent, nil
	}
	if err := json.Unmarshal([]byte(newContent), &next); err != nil {
		return newContent, nil
	}

	newSheetIDs := make(map[string]bool, len(next.Sheets))
	for _, s := range next.Sheets {
		newSheetIDs[s.ID] = true
	}
	explicitRemovals := make(map[string]bool, len(removedSheetIDs))
	for _, id := range removedSheetIDs {
		id = strings.TrimSpace(id)
		if id != "" {
			explicitRemovals[id] = true
		}
	}

	changed := false
	for _, oldSheet := range prev.Sheets {
		if explicitRemovals[oldSheet.ID] {
			continue
		}
		if !newSheetIDs[oldSheet.ID] {
			next.Sheets = append(next.Sheets, oldSheet)
			changed = true
			log.Info().
				Str("sheet_id", oldSheet.ID).
				Str("sheet_name", oldSheet.Name).
				Msg("copilot merge: restored user-added sheet dropped by LLM")
		}
	}

	// Preserve agents from old definition that aren't in the new one.
	if len(prev.Agents) > 0 {
		agentSet := make(map[string]bool, len(next.Agents))
		for _, a := range next.Agents {
			agentSet[a] = true
		}
		for _, a := range prev.Agents {
			if !agentSet[a] {
				next.Agents = append(next.Agents, a)
				changed = true
			}
		}
	}

	if !changed {
		return newContent, nil
	}

	merged, err := json.Marshal(next)
	if err != nil {
		return newContent, nil
	}
	return string(merged), nil
}

func normalizeViewDefinition(def *types.ViewDefinition) {
	if def == nil {
		return
	}
	def.Name = strings.TrimSpace(def.Name)
	def.Description = strings.TrimSpace(def.Description)
	def.Agents = uniqueTrimmedStrings(def.Agents)
	referenced := collectSheetAgentRefs(def.Sheets)
	if len(referenced) > 0 {
		def.Agents = referenced
	}
	seenSheetIDs := make(map[string]struct{}, len(def.Sheets))
	for i := range def.Sheets {
		sheet := &def.Sheets[i]
		sheet.ID = ensureUniqueViewScopedID("sheet", sheet.ID, seenSheetIDs)
		sheet.Name = strings.TrimSpace(sheet.Name)
		sheet.Description = strings.TrimSpace(sheet.Description)
		if sheet.Layout.Columns <= 0 {
			sheet.Layout.Columns = 12
		}
		seenRelationIDs := make(map[string]struct{}, len(sheet.Relations))
		for j := range sheet.Relations {
			sheet.Relations[j].ID = ensureUniqueViewScopedID("relation", sheet.Relations[j].ID, seenRelationIDs)
			sheet.Relations[j].Name = strings.TrimSpace(sheet.Relations[j].Name)
			sheet.Relations[j].ToSheetID = strings.TrimSpace(sheet.Relations[j].ToSheetID)
			sheet.Relations[j].FromColumn = normalizeColumnKey(sheet.Relations[j].FromColumn)
			sheet.Relations[j].ToColumn = normalizeColumnKey(sheet.Relations[j].ToColumn)
		}
		seenComponentIDs := make(map[string]struct{}, len(sheet.Components))
		for j := range sheet.Components {
			sheet.Components[j].ID = ensureUniqueViewScopedID(componentIDPrefix(sheet.Components[j].Type), sheet.Components[j].ID, seenComponentIDs)
			if ds := sheet.Components[j].DataSource; ds != nil {
				normalizeDataSource(ds)
			}
			normalizeAgentConfig(sheet.Components[j].Config)
			normalizeComponentConfig(&sheet.Components[j])
		}
	}
}

// NormalizeDefinition canonicalizes a view definition for persistence.
// It enforces the same schema invariants used by the copilot publish path.
func NormalizeDefinition(def *types.ViewDefinition) {
	normalizeViewDefinition(def)
}

// classifyDetailTemplates runs BAML once per table component to determine the
// optimal detail view layout template. The deterministic fallback from
// normalizeComponentConfig is already in Config["detail_layout"]; this upgrades
// it with BAML intelligence. Skipped if columns haven't changed.
func classifyDetailTemplates(ctx context.Context, def *types.ViewDefinition) {
	for i := range def.Sheets {
		for j := range def.Sheets[i].Components {
			comp := &def.Sheets[i].Components[j]
			if !comp.IsTable() || comp.Config == nil {
				continue
			}
			cols := ConfigColumnsToMeta(comp.Config)
			if len(cols) == 0 {
				continue
			}
			schemaHash := columnSchemaHash(cols)
			if existing, ok := comp.Config["detail_layout_hash"].(string); ok && existing == schemaHash {
				continue
			}
			layout := ClassifyDetailTemplate(ctx, comp.Title, cols)
			comp.Config["detail_layout"] = layout
			comp.Config["detail_layout_hash"] = schemaHash
		}
	}
}

func ensureUniqueViewScopedID(prefix, raw string, seen map[string]struct{}) string {
	id := strings.TrimSpace(raw)
	if id != "" {
		if _, exists := seen[id]; !exists {
			seen[id] = struct{}{}
			return id
		}
	}
	for {
		candidate := prefix + "-" + uuid.NewString()
		if _, exists := seen[candidate]; exists {
			continue
		}
		seen[candidate] = struct{}{}
		return candidate
	}
}

func componentIDPrefix(componentType string) string {
	switch strings.TrimSpace(strings.ToLower(componentType)) {
	case types.ComponentTypeTable:
		return "table"
	case types.ComponentTypeAction:
		return "action"
	case "template":
		return "tmpl"
	case "config-panel":
		return "cfg"
	case "sequence":
		return "seq"
	default:
		return "component"
	}
}

func normalizeDataSource(ds *types.DataSource) {
	if ds == nil {
		return
	}
	ds.AgentID = strings.TrimSpace(ds.AgentID)
	ds.AgentIDs = uniqueTrimmedStrings(ds.AgentIDs)
	ds.OutputType = strings.TrimSpace(ds.OutputType)
	ds.ArtifactKey = normalizeToken(ds.ArtifactKey)
	ds.TimeRange = strings.TrimSpace(ds.TimeRange)
	ds.Statuses = uniqueTrimmedStrings(ds.Statuses)
}

func collectSheetAgentRefs(sheets []types.SheetSpec) []string {
	var refs []string
	for _, sheet := range sheets {
		for _, comp := range sheet.Components {
			if ds := comp.DataSource; ds != nil {
				refs = append(refs, ds.AgentID)
				refs = append(refs, ds.AgentIDs...)
			}
			if comp.Config == nil {
				continue
			}
			if ref, _ := comp.Config["agent_id"].(string); ref != "" {
				refs = append(refs, ref)
			}
			refs = append(refs, configAgentIDs(comp.Config["agent_ids"])...)
		}
	}
	return uniqueTrimmedStrings(refs)
}

func normalizeAgentConfig(config map[string]any) {
	if config == nil {
		return
	}
	if ref, ok := config["agent_id"].(string); ok {
		config["agent_id"] = strings.TrimSpace(ref)
	}
	if ids := uniqueTrimmedStrings(configAgentIDs(config["agent_ids"])); len(ids) > 0 {
		config["agent_ids"] = ids
	}
}

func normalizeComponentConfig(comp *types.ComponentSpec) {
	if comp == nil {
		return
	}
	normalizeLegacyComponentConfig(comp.Config)
	normalizeTransformColumns(comp)
}

func normalizeLegacyComponentConfig(config map[string]any) {
	if config == nil {
		return
	}
	if legacyChartType, ok := config["chartType"]; ok {
		if _, exists := config["chart_type"]; !exists {
			config["chart_type"] = legacyChartType
		}
		delete(config, "chartType")
	}
}

func normalizeTransformColumns(comp *types.ComponentSpec) {
	if comp == nil || comp.DataSource == nil || len(comp.DataSource.Transform) == 0 {
		return
	}

	used := make(map[string]int, len(comp.DataSource.Transform))
	keyAliases := map[string]string{}
	labels := map[string]string{}
	for i := range comp.DataSource.Transform {
		rule := &comp.DataSource.Transform[i]
		key, label := normalizeTransformRule(rule, used)
		original := strings.TrimSpace(rule.Column)
		if original != "" {
			if _, exists := keyAliases[original]; !exists {
				keyAliases[original] = key
			}
			normalizedOriginal := normalizeColumnKey(original)
			if normalizedOriginal != "" {
				if _, exists := keyAliases[normalizedOriginal]; !exists {
					keyAliases[normalizedOriginal] = key
				}
			}
		}
		if key != "" {
			if _, exists := keyAliases[key]; !exists {
				keyAliases[key] = key
			}
		}
		if label != "" {
			labels[key] = label
		}
		rule.Column = key
		rule.Source = strings.TrimSpace(rule.Source)
		rule.Type = normalizeColumnType(rule.Type)
		rule.Extract = strings.TrimSpace(rule.Extract)
		rule.Format = strings.TrimSpace(rule.Format)
	}

	if comp.IsTable() {
		if comp.Config == nil {
			comp.Config = map[string]any{}
		}
		repairTableColumnConfig(comp.Config, comp.DataSource.Transform, keyAliases, labels)
	}
}

func normalizeTransformRule(rule *types.TransformRule, used map[string]int) (string, string) {
	if rule == nil {
		return "", ""
	}
	original := strings.TrimSpace(rule.Column)
	key := normalizeColumnKey(original)
	if key == "" {
		key = normalizeColumnKey(sourceColumnHint(rule.Source))
	}
	if key == "" {
		key = "value"
	}
	if isReservedViewColumnKey(key) {
		key += "_value"
	}
	base := key
	if used[base] > 0 {
		key = fmt.Sprintf("%s_%d", base, used[base]+1)
	}
	used[base]++

	label := original
	if label == "" || strings.EqualFold(label, key) {
		label = ""
	}
	return key, label
}

func normalizeColumnKey(value string) string {
	value = strings.TrimSpace(strings.ToLower(value))
	if value == "" {
		return ""
	}
	var b strings.Builder
	lastUnderscore := false
	for _, r := range value {
		switch {
		case (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9'):
			b.WriteRune(r)
			lastUnderscore = false
		default:
			if !lastUnderscore {
				b.WriteByte('_')
				lastUnderscore = true
			}
		}
	}
	return strings.Trim(b.String(), "_")
}

func sourceColumnHint(source string) string {
	source = strings.TrimSpace(strings.Split(source, "|")[0])
	if source == "" {
		return ""
	}
	parts := splitPath(strings.TrimPrefix(strings.TrimPrefix(source, "data."), "metadata."))
	for i := len(parts) - 1; i >= 0; i-- {
		part := strings.TrimSpace(parts[i])
		if part == "" || part == "[]" {
			continue
		}
		return part
	}
	return ""
}

func isReservedViewColumnKey(key string) bool {
	switch strings.TrimSpace(key) {
	case "task_id", "output_id", "row_id", "sheet_id", "output_status", "source_output_ids":
		return true
	default:
		return false
	}
}

func repairTableColumnConfig(
	config map[string]any,
	rules []types.TransformRule,
	keyAliases map[string]string,
	labels map[string]string,
) {
	existing := parseConfigColumns(config)
	next := make([]configColumn, 0, len(existing)+len(rules))
	seen := make(map[string]struct{}, len(existing)+len(rules))
	for _, col := range existing {
		key := resolveNormalizedConfigColumnKey(col.Key, keyAliases)
		if key == "" {
			continue
		}
		col.Key = key
		col.Type = normalizeColumnType(col.Type)
		col.Format = strings.TrimSpace(col.Format)
		if col.Label == "" {
			col.Label = labels[key]
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		next = append(next, col)
	}

	for _, rule := range rules {
		if _, ok := seen[rule.Column]; ok {
			continue
		}
		next = append(next, configColumn{
			Key:    rule.Column,
			Label:  labels[rule.Column],
			Type:   normalizeColumnType(rule.Type),
			Format: strings.TrimSpace(rule.Format),
		})
		seen[rule.Column] = struct{}{}
	}
	if len(next) > 0 {
		config["columns"] = next
	}

	if rawSort, ok := config["defaultSort"].(map[string]any); ok {
		if column, _ := rawSort["column"].(string); column != "" {
			if normalized := resolveNormalizedConfigColumnKey(column, keyAliases); normalized != "" {
				rawSort["column"] = normalized
			}
		}
		if dir, _ := rawSort["direction"].(string); dir != "" {
			rawSort["direction"] = strings.ToLower(strings.TrimSpace(dir))
		}
	}
}

func resolveNormalizedConfigColumnKey(key string, aliases map[string]string) string {
	key = strings.TrimSpace(key)
	if key == "" {
		return ""
	}
	if normalized := aliases[key]; normalized != "" {
		return normalized
	}
	if normalized := aliases[normalizeColumnKey(key)]; normalized != "" {
		return normalized
	}
	if normalized := normalizeColumnKey(key); normalized != "" {
		if isReservedViewColumnKey(normalized) {
			return normalized + "_value"
		}
		return normalized
	}
	return ""
}

func configAgentIDs(value any) []string {
	switch ids := value.(type) {
	case []string:
		return append([]string(nil), ids...)
	case []any:
		out := make([]string, 0, len(ids))
		for _, raw := range ids {
			if ref, ok := raw.(string); ok {
				out = append(out, ref)
			}
		}
		return out
	default:
		return nil
	}
}

func uniqueTrimmedStrings(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, value := range values {
		trimmed := strings.TrimSpace(value)
		if trimmed == "" {
			continue
		}
		if _, ok := seen[trimmed]; ok {
			continue
		}
		seen[trimmed] = struct{}{}
		out = append(out, trimmed)
	}
	return out
}

func canonicalizeViewAgentRefs(def *types.ViewDefinition, agents []*types.AgentProfile, opResults []OperationResult) {
	if def == nil {
		return
	}
	resolver := buildAgentReferenceResolver(agents, opResults)
	def.Agents = canonicalizeAgentRefList(def.Agents, resolver)
	for i := range def.Sheets {
		for j := range def.Sheets[i].Components {
			if ds := def.Sheets[i].Components[j].DataSource; ds != nil {
				ds.AgentID = canonicalizeAgentRef(ds.AgentID, resolver)
				ds.AgentIDs = canonicalizeAgentRefList(ds.AgentIDs, resolver)
			}
			if def.Sheets[i].Components[j].Config == nil {
				continue
			}
			if ref, ok := def.Sheets[i].Components[j].Config["agent_id"].(string); ok {
				if canonical := canonicalizeAgentRef(ref, resolver); canonical != "" {
					def.Sheets[i].Components[j].Config["agent_id"] = canonical
				}
			}
			if ids := configAgentIDs(def.Sheets[i].Components[j].Config["agent_ids"]); len(ids) > 0 {
				def.Sheets[i].Components[j].Config["agent_ids"] = canonicalizeAgentRefList(ids, resolver)
			}
		}
	}
}

type agentReferenceResolver struct {
	byID   map[string]string
	byKey  map[string]string
	byName map[string]string
}

func buildAgentReferenceResolver(agents []*types.AgentProfile, opResults []OperationResult) agentReferenceResolver {
	resolver := agentReferenceResolver{
		byID:   map[string]string{},
		byKey:  map[string]string{},
		byName: map[string]string{},
	}

	nameCounts := map[string]int{}
	addName := func(name string) {
		key := strings.ToLower(strings.TrimSpace(name))
		if key != "" {
			nameCounts[key]++
		}
	}

	for _, agent := range agents {
		if agent == nil || strings.TrimSpace(agent.ID) == "" {
			continue
		}
		id := strings.TrimSpace(agent.ID)
		resolver.byID[id] = id
		if key := strings.TrimSpace(agent.AgentKey); key != "" {
			resolver.byKey[strings.ToLower(key)] = id
		}
		addName(agent.Name)
	}
	for _, result := range opResults {
		if strings.TrimSpace(result.AgentID) == "" {
			continue
		}
		id := strings.TrimSpace(result.AgentID)
		resolver.byID[id] = id
		addName(result.Name)
	}

	for _, agent := range agents {
		if agent == nil || strings.TrimSpace(agent.ID) == "" {
			continue
		}
		nameKey := strings.ToLower(strings.TrimSpace(agent.Name))
		if nameKey != "" && nameCounts[nameKey] == 1 {
			resolver.byName[nameKey] = strings.TrimSpace(agent.ID)
		}
	}
	for _, result := range opResults {
		if strings.TrimSpace(result.AgentID) == "" {
			continue
		}
		nameKey := strings.ToLower(strings.TrimSpace(result.Name))
		if nameKey != "" && nameCounts[nameKey] == 1 {
			resolver.byName[nameKey] = strings.TrimSpace(result.AgentID)
		}
	}

	return resolver
}

func canonicalizeAgentRefList(refs []string, resolver agentReferenceResolver) []string {
	out := make([]string, 0, len(refs))
	for _, ref := range refs {
		if canonical := canonicalizeAgentRef(ref, resolver); canonical != "" {
			out = append(out, canonical)
		}
	}
	return uniqueTrimmedStrings(out)
}

func canonicalizeAgentRef(ref string, resolver agentReferenceResolver) string {
	trimmed := strings.TrimSpace(ref)
	if trimmed == "" {
		return ""
	}
	if id := resolver.byID[trimmed]; id != "" {
		return id
	}
	lower := strings.ToLower(trimmed)
	if id := resolver.byKey[lower]; id != "" {
		return id
	}
	if id := resolver.byName[lower]; id != "" {
		return id
	}
	return trimmed
}

// findOrCreateAgent returns the existing agent for the given key, or creates
// one if it doesn't exist. This is the agent equivalent of installWorkspaceSkill
// — idempotent so the model can emit CREATE_AGENT freely without worrying about
// duplicates.
func (c *Copilot) findOrCreateAgent(ctx context.Context, workspaceID uint, key, name string, config map[string]any) (*types.AgentProfile, error) {
	if profile, err := c.backend.GetAgentProfileByKey(ctx, workspaceID, key); err == nil && profile != nil {
		if len(config) > 0 {
			if updated, err := c.agentAPI.UpdateAgent(ctx, workspaceID, profile.ID, nil, nil, nil, nil, nil, config, nil); err == nil {
				return updated, nil
			}
		}
		return profile, nil
	}
	return c.agentAPI.CreateAgent(ctx, workspaceID, key, name, config, nil)
}

// EnsureViewAgentsExist creates any agents referenced in the view that don't
// exist yet — handles the case where the model puts agent names in the view
// without emitting CREATE_AGENT operations.
func (c *Copilot) EnsureViewAgentsExist(ctx context.Context, workspaceID uint, viewContent string) []OperationResult {
	if c.agentAPI == nil || viewContent == "" {
		return nil
	}
	var def types.ViewDefinition
	if err := json.Unmarshal([]byte(viewContent), &def); err != nil {
		return nil
	}

	agents := c.loadWorkspaceAgents(ctx, workspaceID)
	resolver := buildAgentReferenceResolver(agents, nil)

	var results []OperationResult
	for _, ref := range collectAllViewAgentRefs(&def) {
		if canonicalizeAgentRef(ref, resolver) != ref {
			continue
		}
		if _, err := uuid.Parse(ref); err == nil {
			continue
		}
		name := agentNameFromKey(ref)
		profile, err := c.findOrCreateAgent(ctx, workspaceID, toAgentKey(name), name, nil)
		if err != nil {
			continue
		}
		results = append(results, OperationResult{
			Type: string(bamltypes.OperationTypeCREATE_AGENT), Name: name, Status: "done", AgentID: profile.ID,
		})
		resolver.byID[profile.ID] = profile.ID
		resolver.byKey[strings.ToLower(toAgentKey(name))] = profile.ID
	}
	return results
}

func collectAllViewAgentRefs(def *types.ViewDefinition) []string {
	refs := append([]string{}, def.Agents...)
	for _, sheet := range def.Sheets {
		for _, comp := range sheet.Components {
			if ds := comp.DataSource; ds != nil {
				refs = append(refs, ds.AgentID)
				refs = append(refs, ds.AgentIDs...)
			}
			if comp.Config != nil {
				if ref, _ := comp.Config["agent_id"].(string); ref != "" {
					refs = append(refs, ref)
				}
				refs = append(refs, configAgentIDs(comp.Config["agent_ids"])...)
			}
		}
	}
	for _, action := range def.Actions {
		if action.Config != nil {
			if ref, _ := action.Config["agent_id"].(string); ref != "" {
				refs = append(refs, ref)
			}
			refs = append(refs, configAgentIDs(action.Config["agent_ids"])...)
		}
	}
	return uniqueTrimmedStrings(refs)
}

func agentNameFromKey(key string) string {
	words := strings.Split(strings.TrimSpace(key), "-")
	for i, w := range words {
		if w != "" {
			words[i] = strings.ToUpper(w[:1]) + w[1:]
		}
	}
	return strings.Join(words, " ")
}

func findUniqueAgentProfileByName(agents []*types.AgentProfile, name string) *types.AgentProfile {
	normalized := strings.ToLower(strings.TrimSpace(name))
	if normalized == "" {
		return nil
	}
	var match *types.AgentProfile
	for _, agent := range agents {
		if agent == nil || strings.TrimSpace(agent.ID) == "" {
			continue
		}
		if strings.ToLower(strings.TrimSpace(agent.Name)) != normalized {
			continue
		}
		if match != nil {
			return nil
		}
		match = agent
	}
	return match
}

type loadedSkill struct {
	Manifest *skills.SkillManifest
	Content  string
}

func (c *Copilot) loadSkillManifests(ctx context.Context, workspaceID uint) map[string]*loadedSkill {
	result := make(map[string]*loadedSkill)
	if c.storage == nil {
		return result
	}
	ws, err := c.backend.GetWorkspace(ctx, workspaceID)
	if err != nil {
		return result
	}
	bucket := c.storage.WorkspaceBucketName(ws.ExternalId)
	objects, err := c.storage.ListObjects(ctx, bucket, skills.Dir+"/", 1000)
	if err != nil {
		return result
	}
	for _, obj := range objects.Contents {
		if obj.Key == nil {
			continue
		}
		name := skills.KeyToName(*obj.Key)
		if name == "" || result[name] != nil {
			continue
		}
		content, err := c.storage.Download(ctx, bucket, skills.ManifestKey(name))
		if err != nil {
			continue
		}
		manifest, err := skills.Parse(content)
		if err != nil {
			continue
		}
		result[name] = &loadedSkill{Manifest: manifest, Content: string(content)}
	}
	return result
}

// ---------------------------------------------------------------------------
// Operations — workspace mutations (agents, skills)
// ---------------------------------------------------------------------------

func (c *Copilot) ExecuteOperations(ctx context.Context, workspaceID uint, ops []bamltypes.Operation, viewID string) []OperationResult {
	sorted := make([]bamltypes.Operation, len(ops))
	copy(sorted, ops)
	sort.SliceStable(sorted, func(i, j int) bool {
		return operationPhase(sorted[i].Type) < operationPhase(sorted[j].Type)
	})

	state := newOperationExecutionState(sorted)
	state.viewID = viewID
	results := make([]OperationResult, 0, len(sorted))
	for _, op := range sorted {
		results = append(results, c.executeOne(ctx, workspaceID, op, state))
	}
	return results
}

type operationExecutionState struct {
	skillAliases map[string]string
	agentAliases map[string]string // name/key → UUID, populated as CREATE_AGENT ops execute
	viewID       string
}

func newOperationExecutionState(ops []bamltypes.Operation) *operationExecutionState {
	state := &operationExecutionState{
		skillAliases: map[string]string{},
	}
	for _, op := range ops {
		if op.Type != bamltypes.OperationTypeCREATE_SKILL && op.Type != bamltypes.OperationTypeINSTALL_SKILL {
			continue
		}

		var payload map[string]any
		if err := json.Unmarshal([]byte(op.Payload), &payload); err != nil {
			continue
		}

		requestedName := coalesceTrimmed(stringValue(payload, "name"), stringValue(payload, "skill_name"))
		content := stringValue(payload, "content")
		if content == "" {
			continue
		}

		_, resolvedName, err := skills.ResolveInstallName(requestedName, []byte(content))
		if err != nil {
			continue
		}
		state.rememberSkillAlias(requestedName, resolvedName)
	}
	return state
}

func (s *operationExecutionState) rememberSkillAlias(ref, resolved string) {
	if s == nil {
		return
	}
	resolved = strings.TrimSpace(resolved)
	if resolved == "" {
		return
	}
	if s.skillAliases == nil {
		s.skillAliases = map[string]string{}
	}
	for _, candidate := range []string{resolved, ref, skills.NameToPath(resolved)} {
		key := normalizeSkillReference(candidate)
		if key != "" {
			s.skillAliases[key] = resolved
		}
	}
}

func (s *operationExecutionState) resolveSkillAlias(ref string) string {
	trimmed := strings.TrimSpace(ref)
	if trimmed == "" {
		return ""
	}
	if s != nil {
		if resolved := strings.TrimSpace(s.skillAliases[normalizeSkillReference(trimmed)]); resolved != "" {
			return resolved
		}
	}
	return trimmed
}

func (s *operationExecutionState) rememberAgent(name, key, id string) {
	if s == nil || strings.TrimSpace(id) == "" {
		return
	}
	if s.agentAliases == nil {
		s.agentAliases = map[string]string{}
	}
	id = strings.TrimSpace(id)
	s.agentAliases[id] = id
	if name = strings.TrimSpace(name); name != "" {
		s.agentAliases[strings.ToLower(name)] = id
	}
	if key = strings.TrimSpace(key); key != "" {
		s.agentAliases[strings.ToLower(key)] = id
	}
}

func (s *operationExecutionState) resolveAgentAlias(ref string) string {
	ref = strings.TrimSpace(ref)
	if ref == "" {
		return ""
	}
	if s != nil && s.agentAliases != nil {
		if id := s.agentAliases[strings.ToLower(ref)]; id != "" {
			return id
		}
	}
	return ref
}

func operationPhase(opType bamltypes.OperationType) int {
	switch opType {
	case bamltypes.OperationTypeCREATE_AGENT:
		return 0
	case bamltypes.OperationTypeCREATE_SKILL, bamltypes.OperationTypeINSTALL_SKILL:
		return 1
	default:
		return 2
	}
}

func normalizeSkillReference(ref string) string {
	ref = strings.TrimSpace(ref)
	if ref == "" {
		return ""
	}
	ref = strings.TrimSuffix(ref, "/"+skills.ManifestFile)
	if pathName := skills.PathToName(ref); pathName != "" {
		ref = pathName
	}
	ref = strings.ToLower(ref)
	ref = strings.ReplaceAll(ref, "_", "-")
	ref = strings.Join(strings.Fields(ref), "-")
	for strings.Contains(ref, "--") {
		ref = strings.ReplaceAll(ref, "--", "-")
	}
	return strings.Trim(ref, "-")
}

func stringValue(payload map[string]any, key string) string {
	v, _ := payload[key].(string)
	return strings.TrimSpace(v)
}

// extractSkillNameFromRaw pulls a skill name out of a broken payload string.
// It handles cases like: bare name, "skill_name": "foo", name: foo, etc.
func extractSkillNameFromRaw(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	// Try to find skill_name or name value in broken JSON
	for _, key := range []string{"skill_name", "name"} {
		re := regexp.MustCompile(`"?` + key + `"?\s*[:=]\s*"?([a-zA-Z0-9_-]+)"?`)
		if m := re.FindStringSubmatch(raw); len(m) > 1 {
			return strings.TrimSpace(m[1])
		}
	}
	// If the raw string looks like a bare slug (skill name), use it directly.
	trimmed := strings.Trim(raw, `"' {}`)
	trimmed = strings.TrimSpace(trimmed)
	if len(trimmed) > 0 && len(trimmed) < 100 && !strings.ContainsAny(trimmed, "{}[]:\n") {
		slug := strings.ToLower(trimmed)
		slug = strings.ReplaceAll(slug, " ", "-")
		slug = strings.ReplaceAll(slug, "_", "-")
		if matched, _ := regexp.MatchString(`^[a-z0-9][a-z0-9-]*$`, slug); matched {
			return slug
		}
	}
	return ""
}

// repairPayloadJSON tries to fix common JSON issues from LLM output — mainly
// unescaped control characters (newlines/tabs) inside string values. Returns
// empty string if repair is not possible.
func repairPayloadJSON(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	var buf strings.Builder
	buf.Grow(len(raw))
	inString := false
	escaped := false
	for i := 0; i < len(raw); i++ {
		ch := raw[i]
		if escaped {
			buf.WriteByte(ch)
			escaped = false
			continue
		}
		if ch == '\\' && inString {
			buf.WriteByte(ch)
			escaped = true
			continue
		}
		if ch == '"' {
			inString = !inString
			buf.WriteByte(ch)
			continue
		}
		if inString && ch == '\n' {
			buf.WriteString(`\n`)
			continue
		}
		if inString && ch == '\r' {
			buf.WriteString(`\r`)
			continue
		}
		if inString && ch == '\t' {
			buf.WriteString(`\t`)
			continue
		}
		buf.WriteByte(ch)
	}
	return buf.String()
}

func uniqueStringSlice(value any) []string {
	var raw []string
	switch typed := value.(type) {
	case nil:
		return nil
	case string:
		raw = []string{typed}
	case []string:
		raw = typed
	case []any:
		raw = make([]string, 0, len(typed))
		for _, item := range typed {
			if text, ok := item.(string); ok {
				raw = append(raw, text)
			}
		}
	default:
		return nil
	}

	out := make([]string, 0, len(raw))
	seen := make(map[string]struct{}, len(raw))
	for _, item := range raw {
		trimmed := strings.TrimSpace(item)
		if trimmed == "" {
			continue
		}
		if _, ok := seen[trimmed]; ok {
			continue
		}
		seen[trimmed] = struct{}{}
		out = append(out, trimmed)
	}
	return out
}

func resolveSkillAliases(value any, state *operationExecutionState) []string {
	refs := uniqueStringSlice(value)
	out := make([]string, 0, len(refs))
	seen := make(map[string]struct{}, len(refs))
	for _, ref := range refs {
		resolved := ref
		if state != nil {
			resolved = state.resolveSkillAlias(ref)
		}
		if _, ok := seen[resolved]; ok {
			continue
		}
		seen[resolved] = struct{}{}
		out = append(out, resolved)
	}
	return out
}

func containsString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func (c *Copilot) executeOne(ctx context.Context, workspaceID uint, op bamltypes.Operation, state *operationExecutionState) OperationResult {
	opType := string(op.Type)
	fail := func(name, msg string) OperationResult {
		return OperationResult{Type: opType, Name: name, Status: "error", Error: msg}
	}
	done := func(name, agentID string) OperationResult {
		return OperationResult{Type: opType, Name: name, Status: "done", AgentID: agentID}
	}

	var payload map[string]any
	if err := json.Unmarshal([]byte(op.Payload), &payload); err != nil {
		repaired := repairPayloadJSON(op.Payload)
		if repaired != "" {
			if err2 := json.Unmarshal([]byte(repaired), &payload); err2 != nil {
				repaired = ""
			}
		}
		if repaired == "" {
			// For skill operations, salvage the skill name from the raw payload.
			if op.Type == bamltypes.OperationTypeINSTALL_SKILL || op.Type == bamltypes.OperationTypeCREATE_SKILL || op.Type == bamltypes.OperationTypeASSIGN_SKILL {
				if name := extractSkillNameFromRaw(op.Payload); name != "" {
					payload = map[string]any{"skill_name": name}
					log.Info().Str("op_type", opType).Str("extracted_name", name).Msg("salvaged skill name from invalid payload")
				}
			}
			if payload == nil {
				log.Warn().Str("op_type", opType).Str("raw_payload", truncate(op.Payload, 200)).Err(err).Msg("invalid payload JSON")
				return fail("", "invalid payload JSON")
			}
		}
	}
	str := func(key string) string {
		return stringValue(payload, key)
	}

	switch op.Type {
	case bamltypes.OperationTypeCREATE_AGENT:
		name := str("name")
		if name == "" {
			return fail("", "name is required")
		}
		key := toAgentKey(name)
		config := configFromPayload(payload, state)

		profile, err := c.findOrCreateAgent(ctx, workspaceID, key, name, config)
		if err != nil {
			return fail(name, err.Error())
		}
		state.rememberAgent(name, key, profile.ID)
		if role := coalesceTrimmed(str("role"), str("description")); role != "" && role != "generalist" {
			c.agentAPI.UpdateAgent(ctx, workspaceID, profile.ID, nil, &role, nil, nil, nil, nil, nil) //nolint:errcheck
		}
		log.Info().Str("agent_id", profile.ID).Str("name", name).Uint("workspace_id", workspaceID).Msg("copilot created agent")
		return done(name, profile.ID)

	case bamltypes.OperationTypeUPDATE_AGENT:
		agentID := state.resolveAgentAlias(str("agent_id"))
		if agentID == "" {
			return fail("", "agent_id is required")
		}
		var namePtr, rolePtr *string
		if n := str("name"); n != "" {
			namePtr = &n
		}
		if r := str("role"); r != "" {
			rolePtr = &r
		}
		profile, err := c.agentAPI.UpdateAgent(ctx, workspaceID, agentID, namePtr, rolePtr, nil, nil, nil, configFromPayload(payload, state), nil)
		if err != nil {
			return fail(agentID, err.Error())
		}
		return done(profile.Name, profile.ID)

	case bamltypes.OperationTypeCREATE_SKILL, bamltypes.OperationTypeINSTALL_SKILL:
		rawSkillName := coalesceTrimmed(str("name"), str("skill_name"))
		skillName := state.resolveSkillAlias(rawSkillName)
		content := str("content")
		if content == "" {
			if op.Type != bamltypes.OperationTypeINSTALL_SKILL {
				return fail(skillName, "content is required")
			}
			if skillName == "" {
				return fail("", "skill_name is required")
			}
			exists, err := c.skillExists(ctx, workspaceID, skillName)
			if err != nil {
				return fail(skillName, err.Error())
			}
			if !exists {
				return fail(skillName, "skill not found: "+skillName)
			}
			state.rememberSkillAlias(rawSkillName, skillName)
			return done(skillName, "")
		}

		exists, err := c.skillExists(ctx, workspaceID, skillName)
		if err != nil && skillName != "" {
			return fail(skillName, err.Error())
		}
		_, installedName, err := c.installWorkspaceSkill(ctx, workspaceID, skillName, []byte(content))
		if err != nil {
			return fail(skillName, err.Error())
		}
		if exists {
			log.Info().Str("skill", installedName).Uint("workspace_id", workspaceID).Msg("copilot updated skill")
		} else {
			log.Info().Str("skill", installedName).Uint("workspace_id", workspaceID).Msg("copilot created skill")
		}
		state.rememberSkillAlias(rawSkillName, installedName)
		return done(installedName, "")

	case bamltypes.OperationTypeASSIGN_SKILL:
		agentID := state.resolveAgentAlias(str("agent_id"))
		rawSkillName := coalesceTrimmed(str("skill_name"), str("name"))
		skillName := state.resolveSkillAlias(rawSkillName)
		if agentID == "" || skillName == "" {
			return fail("", "agent_id and skill_name are required")
		}
		profile, err := c.backend.GetAgentProfile(ctx, workspaceID, agentID)
		if err != nil {
			if profile, err = c.backend.GetAgentProfileByKey(ctx, workspaceID, agentID); err != nil {
				return fail(skillName, "agent not found: "+agentID)
			}
		}

		exists, err := c.skillExists(ctx, workspaceID, skillName)
		if err != nil {
			return fail(skillName, err.Error())
		}
		if !exists {
			return fail(skillName, "skill not found: "+skillName)
		}

		existing := extractStringSlice(profile.ConfigJSON, "skills")
		if containsString(existing, skillName) {
			state.rememberSkillAlias(rawSkillName, skillName)
			return done(skillName, profile.ID)
		}

		nextSkills := append(append([]string(nil), existing...), skillName)
		if _, err := c.agentAPI.UpdateAgent(
			ctx,
			workspaceID,
			profile.ID,
			nil,
			nil,
			nil,
			nil,
			nil,
			map[string]any{"skills": nextSkills},
			nil,
		); err != nil {
			return fail(skillName, err.Error())
		}
		state.rememberSkillAlias(rawSkillName, skillName)
		log.Info().Str("agent_id", profile.ID).Str("skill", skillName).Msg("copilot assigned skill")
		return done(skillName, profile.ID)

	case bamltypes.OperationTypeDISPATCH_TASK:
		agentID := state.resolveAgentAlias(str("agent_id"))
		message := str("message")
		if agentID == "" {
			return fail("", "agent_id is required")
		}
		if message == "" {
			return fail("", "message is required")
		}
		var label *string
		if l := str("label"); l != "" {
			label = &l
		}
		var sourceViewID *string
		if state.viewID != "" {
			sourceViewID = &state.viewID
		}
		spawnedBy := "copilot"
		task, _, err := c.agentAPI.AcceptAgentCommand(ctx, workspaceID, orchestration.AgentCommandParams{
			Message:      message,
			AgentID:      &agentID,
			Label:        label,
			SpawnedBy:    &spawnedBy,
			SourceViewID: sourceViewID,
		})
		if err != nil {
			return fail(message, err.Error())
		}
		agentName := task.AgentName
		if agentName == "" {
			agentName = agentID
		}
		log.Info().Str("task_id", task.ID).Str("agent_id", agentID).Msg("copilot dispatched task")
		return OperationResult{
			Type:      opType,
			Name:      coalesceTrimmed(derefStr(label), truncate(message, 60)),
			Status:    "done",
			TaskID:    task.ID,
			AgentName: agentName,
			Message:   message,
		}

	case bamltypes.OperationTypeAPPROVE_TASK:
		taskID := str("task_id")
		if taskID == "" {
			return fail("", "task_id is required")
		}
		action := types.TaskInputActionApprove
		task, err := c.agentAPI.SubmitTaskInput(ctx, workspaceID, taskID, types.InputKindApproveReject, &action, "", "", nil)
		if err != nil {
			return fail(taskID, err.Error())
		}
		log.Info().Str("task_id", taskID).Msg("copilot approved task")
		return OperationResult{
			Type:      opType,
			Name:      taskID,
			Status:    "done",
			TaskID:    taskID,
			AgentName: task.AgentName,
		}

	case bamltypes.OperationTypeREJECT_TASK:
		taskID := str("task_id")
		if taskID == "" {
			return fail("", "task_id is required")
		}
		reason := str("reason")
		action := types.TaskInputActionReject
		task, err := c.agentAPI.SubmitTaskInput(ctx, workspaceID, taskID, types.InputKindApproveReject, &action, reason, "", nil)
		if err != nil {
			return fail(taskID, err.Error())
		}
		log.Info().Str("task_id", taskID).Str("reason", reason).Msg("copilot rejected task")
		return OperationResult{
			Type:      opType,
			Name:      taskID,
			Status:    "done",
			TaskID:    taskID,
			AgentName: task.AgentName,
		}

	case bamltypes.OperationTypeCREATE_SCHEDULE:
		agentID := state.resolveAgentAlias(str("agent_id"))
		cronExpr := str("cron_expr")
		if cronExpr == "" {
			cronExpr = str("cron_expression")
		}
		if cronExpr == "" {
			cronExpr = str("schedule")
		}
		prompt := str("prompt")
		if prompt == "" {
			prompt = str("message")
		}
		if prompt == "" {
			prompt = str("task")
		}
		if agentID == "" || cronExpr == "" || prompt == "" {
			return fail("", "agent_id, cron_expr, and prompt are required")
		}
		// Resolve agent key/name to UUID — the scheduled_task table requires a UUID FK.
		profile, err := c.backend.GetAgentProfile(ctx, workspaceID, agentID)
		if err != nil {
			return fail(truncate(prompt, 60), fmt.Sprintf("agent not found: %s", agentID))
		}
		resolvedAgentID := profile.ID
		tz := str("timezone")
		var viewIDPtr *string
		if state.viewID != "" {
			viewIDPtr = &state.viewID
		}
		st, err := c.agentAPI.CreateSchedule(ctx, workspaceID, resolvedAgentID, cronExpr, tz, prompt, nil, nil, nil, nil, viewIDPtr)
		if err != nil {
			return fail(prompt, err.Error())
		}
		log.Info().Str("schedule_id", st.ExternalID).Str("agent_id", agentID).Str("cron", cronExpr).Msg("copilot created schedule")
		return OperationResult{
			Type:       opType,
			Name:       truncate(prompt, 60),
			Status:     "done",
			AgentID:    agentID,
			ScheduleID: st.ExternalID,
			Message:    cronExpr,
		}

	case bamltypes.OperationTypeDELETE_SCHEDULE:
		scheduleID := str("schedule_id")
		if scheduleID == "" {
			return fail("", "schedule_id is required")
		}
		if err := c.agentAPI.DeleteSchedule(ctx, workspaceID, scheduleID); err != nil {
			return fail(scheduleID, err.Error())
		}
		log.Info().Str("schedule_id", scheduleID).Msg("copilot deleted schedule")
		return OperationResult{
			Type:       opType,
			Name:       scheduleID,
			Status:     "done",
			ScheduleID: scheduleID,
		}

	case bamltypes.OperationTypeIMPORT_DATA:
		filePath := str("file_path")
		sheetID := str("sheet_id")
		if filePath == "" {
			return fail("", "file_path is required")
		}
		if sheetID == "" {
			return fail("", "sheet_id is required")
		}
		result, err := c.executeImportData(ctx, workspaceID, state.viewID, sheetID, filePath, payload)
		if err != nil {
			return fail(filePath, err.Error())
		}
		log.Info().
			Str("view_id", state.viewID).
			Str("sheet_id", sheetID).
			Str("import_id", result.ImportID).
			Int("rows", result.RowCount).
			Msg("copilot imported data")
		return OperationResult{
			Type:    opType,
			Name:    filePath,
			Status:  "done",
			Message: fmt.Sprintf("Imported %d rows into sheet", result.RowCount),
		}

	default:
		return fail("", "unknown operation type")
	}
}

func (c *Copilot) executeImportData(ctx context.Context, workspaceID uint, viewID, sheetID, filePath string, payload map[string]any) (*importDataResult, error) {
	if c.storage == nil {
		return nil, fmt.Errorf("storage not configured")
	}
	if c.store == nil || !c.store.Available() {
		return nil, fmt.Errorf("data store not configured")
	}

	v, err := c.backend.GetView(ctx, workspaceID, viewID)
	if err != nil {
		return nil, fmt.Errorf("view lookup: %w", err)
	}
	var componentID string
	for _, sheet := range v.Definition.Sheets {
		if sheet.ID == sheetID {
			for _, comp := range sheet.Components {
				if comp.IsTable() {
					componentID = comp.ID
					break
				}
			}
			break
		}
	}

	ws, err := c.backend.GetWorkspace(ctx, workspaceID)
	if err != nil {
		return nil, fmt.Errorf("workspace lookup: %w", err)
	}
	bucket := c.storage.WorkspaceBucketName(ws.ExternalId)
	key := strings.TrimPrefix(filePath, "/")
	data, err := c.storage.Download(ctx, bucket, key)
	if err != nil {
		return nil, fmt.Errorf("download file: %w", err)
	}

	csvReader := csv.NewReader(bytes.NewReader(data))
	csvReader.LazyQuotes = true
	csvReader.FieldsPerRecord = -1

	headers, err := csvReader.Read()
	if err != nil {
		return nil, fmt.Errorf("parse CSV headers: %w", err)
	}

	colMapping := make(map[string]string)
	if cm, ok := payload["column_mapping"]; ok {
		if mappingMap, ok := cm.(map[string]any); ok {
			for k, v := range mappingMap {
				if vs, ok := v.(string); ok {
					colMapping[k] = vs
				}
			}
		}
	}
	if len(colMapping) == 0 {
		for _, h := range headers {
			colMapping[h] = toImportColumnKey(h)
		}
	}

	importID := uuid.New().String()
	var rows []ViewRow
	rowIndex := 0

	for {
		record, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			break
		}

		pinned := make(map[string]string, len(colMapping))
		for i, header := range headers {
			if i >= len(record) {
				break
			}
			if colKey, ok := colMapping[header]; ok && strings.TrimSpace(record[i]) != "" {
				pinned[colKey] = strings.TrimSpace(record[i])
			}
		}

		if len(pinned) == 0 {
			rowIndex++
			continue
		}

		rowID := fmt.Sprintf("%s::%s:%d", sheetID, "import-"+importID, rowIndex)
		rows = append(rows, ViewRow{
			ID:          rowID,
			SheetID:     sheetID,
			ComponentID: componentID,
			GroupID:     "import:" + importID,
			RowKey:      fmt.Sprintf("import-%d", rowIndex),
			Cells:       map[string]string{},
			Pinned:      pinned,
			Source:      "import",
			ImportID:    importID,
			UpdatedAt:   time.Now(),
		})
		rowIndex++
	}

	if len(rows) == 0 {
		return nil, fmt.Errorf("no data rows found in CSV")
	}

	if err := c.store.UpsertRows(ctx, viewID, rows); err != nil {
		return nil, fmt.Errorf("upsert rows: %w", err)
	}

	return &importDataResult{
		ImportID: importID,
		RowCount: len(rows),
	}, nil
}

type importDataResult struct {
	ImportID string
	RowCount int
}

func toImportColumnKey(header string) string {
	key := strings.ToLower(strings.TrimSpace(header))
	key = strings.ReplaceAll(key, " ", "_")
	key = strings.ReplaceAll(key, "-", "_")
	var clean strings.Builder
	for _, r := range key {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '_' {
			clean.WriteRune(r)
		}
	}
	result := clean.String()
	if result == "" {
		return "col"
	}
	return result
}

func (c *Copilot) installWorkspaceSkill(ctx context.Context, workspaceID uint, requestedName string, content []byte) (*skills.SkillManifest, string, error) {
	if c.storage == nil {
		return nil, "", fmt.Errorf("storage not configured")
	}
	ws, err := c.backend.GetWorkspace(ctx, workspaceID)
	if err != nil {
		return nil, "", fmt.Errorf("workspace not found")
	}
	return skills.InstallContent(ctx, c.storage, ws.ExternalId, requestedName, content)
}

func (c *Copilot) skillExists(ctx context.Context, workspaceID uint, skillName string) (bool, error) {
	if strings.TrimSpace(skillName) == "" {
		return false, fmt.Errorf("skill name is required")
	}
	if c.storage == nil {
		return false, fmt.Errorf("storage not configured")
	}
	ws, err := c.backend.GetWorkspace(ctx, workspaceID)
	if err != nil {
		return false, fmt.Errorf("workspace not found")
	}
	return skills.ExistsInWorkspace(ctx, c.storage, ws.ExternalId, skillName)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

func derefStr(s *string) string {
	if s == nil {
		return ""
	}
	return strings.TrimSpace(*s)
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n-3] + "..."
}

func derefEnum(t *bamltypes.ViewUpdateType) string {
	if t == nil {
		return ""
	}
	return string(*t)
}

func toAgentKey(name string) string {
	return strings.ReplaceAll(strings.ReplaceAll(strings.ToLower(name), " ", "-"), "_", "-")
}

func coalesceTrimmed(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}

// configFromPayload extracts model and skills from an operation payload.
func configFromPayload(payload map[string]any, state *operationExecutionState) map[string]any {
	config := map[string]any{}
	if model, _ := payload["model"].(string); strings.TrimSpace(model) != "" {
		config["model"] = strings.TrimSpace(model)
	}
	if rawSkills, ok := payload["skills"]; ok {
		config["skills"] = resolveSkillAliases(rawSkills, state)
	}
	return config
}

// extractStringSlice pulls a []string from a map[string]any field.
func extractStringSlice(m map[string]any, key string) []string {
	return uniqueStringSlice(m[key])
}

// ---------------------------------------------------------------------------
// Component registry documentation — injected into BAML prompt
// ---------------------------------------------------------------------------

const ComponentRegistryDoc = `A view is a workspace for an ongoing objective.
Each sheet has a header bar that shows assigned agents and live task counts.
Sheets can contain multiple components arranged in a 12-column grid.
Actions are defined at the view level in the top-level "actions" array and
rendered on the project overview page.

COMPONENT TYPES:

- table: The sheet's primary data table.
  At render time a BAML mapper dynamically maps task output data into the
  column schema. Transform rules are semantic hints that guide the mapping:
  - column: machine-stable key (snake_case) describing what the column shows
  - source: dot-path hint (e.g. "data.recipe_name", "title", "uri")
  - type: display type (text, number, currency, date, link, email, status, tags, boolean)

  DataSource fields (on each component):
  - agent_id or agent_ids: which agent(s) produce data for this table (REQUIRED)
  - time_range: recency window, e.g. "30d", "7d" (default "30d")
  - artifact_key: filter to a specific artifact family (e.g. "company-research").
    Use when an agent produces multiple artifact families and you need a
    specific one. Omit to include all outputs from the agent.
  - statuses: optional status filter. Values: "active", "pending", "approved",
    "rejected", "cancelled". Omit to include all (default).

  Config: {
    columns: [{
      key: "column_name",
      label: "Display Name",
      type: "text|number|currency|date|link|email|status|tags|boolean",
      format?: "$" | "relative" | "short_date",
      frozen?: true,
      options?: [{"value": "Lead", "color": "blue"}]
    }],
    pageSize?: 25,
    defaultSort?: {"column": "created_at", "direction": "desc"}
  }

  Column types:
  - text: default, truncated with copy for long values
  - number: right-aligned, locale-formatted
  - currency: number with prefix symbol (format: "$", "EUR")
  - date: relative time with full date on hover
  - link: clickable external link showing domain
  - email: clickable mailto link
  - status: colored pill — MUST include options [{value, color}]
    Colors: blue, green, red, yellow, orange, purple, gray
  - tags: comma-separated pills
  - boolean: Yes/No badge

  LIFECYCLE & APPROVAL:
  For workflows with approvals or multi-stage tracking, add a status
  column (type: "status") to reflect lifecycle state. The mapper derives
  it from output_status. The UI auto-shows inline approve/reject buttons
  for pending rows and an entity detail modal for multi-output rows.

  MULTI-ENTITY (automatic):
  When a task produces multiple outputs sharing the same artifact_key
  (e.g. 10 emails sent, 5 listings scraped, 8 contacts researched),
  the mapper automatically creates one row per entity — no configuration
  needed. Design columns for per-entity fields (name, email, status),
  not aggregate summaries (total_count, top_picks).

  Hidden columns (auto-injected, do NOT define):
  task_id, row_id, sheet_id, output_id, output_status, source_output_ids

- action: Button on the project overview page. Opens a modal form that submits a task.
  Defined in the top-level "actions" array, NOT inside sheet components.
  Config: {
    agent_id, description, prompt_template (with {{field}} placeholders),
    button_label (verb-oriented), fields: [{name, label, required?, type?, placeholder?, options?}]
  }
  PROMPT TEMPLATE RULES:
  - ONLY use simple {{field_name}} placeholders matching a field's name.
  - NEVER use block syntax ({{#if}}, {{/if}}, {{#each}}, {{else}}).
  - Every placeholder must match a field in the fields array.
  - If a field is optional, still use {{field_name}} — empty values are handled.
  Mark required: true for mandatory inputs.

- template: Editable rich text with {{variable}} placeholders. For email
  drafts, message templates, or documents. Placed alongside the table.
  Config: {
    content: "Hi {{name}},\n\nI'm reaching out because...",
    variables: ["name", "company", "reason"],
    format: "markdown"
  }
  Variables can reference table column keys so agents can interpolate
  per-row values when sending. Position next to the table (e.g. colSpan: 4,
  col: 8) for side-by-side layout.

- config-panel: Settings form with typed fields. For campaign parameters,
  search criteria, or workflow configuration.
  Config: {
    fields: [
      {"key": "target_criteria", "label": "Target Criteria", "type": "textarea", "value": "Series A+ VCs"},
      {"key": "tone", "label": "Tone", "type": "select", "options": ["Professional", "Casual"], "value": "Professional"},
      {"key": "max_contacts", "label": "Max Contacts", "type": "number", "value": 50}
    ]
  }
  Field types: text, textarea, select, number.
  Agents receive config-panel values as context when executing tasks.

- sequence: Ordered steps showing a process or cadence. For follow-up
  timelines, workflow stages, or process documentation.
  Config: {
    steps: [
      {"label": "Initial outreach", "delay": "Day 0", "description": "Send personalized intro email"},
      {"label": "Follow up", "delay": "Day 3", "description": "Short check-in if no reply"},
      {"label": "Final touch", "delay": "Day 7", "description": "Last message, offer to reconnect later"}
    ]
  }

AGENT SELECTION:
Keep definition.agents minimal — only include agents actually used.
If several agents share the same skills, pick one unless the user wants multiple.

SHEET DESIGN:
- STRONG DEFAULT: use ONE sheet. Most workflows fit a single table.
  Only create multiple sheets when the user explicitly requests them or the data
  has genuinely distinct entity types (e.g. contacts vs emails vs pricing).
- Sheets can have multiple components. Place the table as the primary component
  (full-width or partial), with template/config-panel/sequence alongside.
  Example layout for email outreach:
    table at col:0 colSpan:8, template at col:8 colSpan:4,
    config-panel at col:8 colSpan:4 row:1, sequence at col:8 colSpan:4 row:2
- Generate concise sheet names tied to the workflow, not generic labels.
- Use sheet relations when rows should connect across sheets via stable keys
  like task_id, email, company_id, listing_id, or similar identifiers.`
