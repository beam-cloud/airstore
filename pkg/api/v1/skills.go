package apiv1

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/beam-cloud/airstore/pkg/clients"
	"github.com/beam-cloud/airstore/pkg/repository"
	"github.com/beam-cloud/airstore/pkg/skills"
	"github.com/labstack/echo/v4"
	"github.com/rs/zerolog/log"
)

type SkillsGroup struct {
	g       *echo.Group
	backend repository.BackendRepository
	storage *clients.StorageClient
	copilot *skills.Copilot
}

func NewSkillsGroup(g *echo.Group, backend repository.BackendRepository, storage *clients.StorageClient, copilot *skills.Copilot) *SkillsGroup {
	sg := &SkillsGroup{g: g, backend: backend, storage: storage, copilot: copilot}
	sg.g.GET("", sg.List)
	sg.g.GET("/:name", sg.Get)
	sg.g.POST("/install", sg.Install)
	sg.g.DELETE("/:name", sg.Uninstall)
	sg.g.POST("/generate", sg.Generate)
	sg.g.GET("/drafts", sg.ListDrafts)
	sg.g.POST("/drafts", sg.CreateDraft)
	sg.g.GET("/drafts/:draft_id", sg.GetDraft)
	sg.g.POST("/drafts/:draft_id/chat", sg.ChatDraft)
	sg.g.POST("/drafts/:draft_id/install", sg.InstallDraft)
	sg.g.DELETE("/drafts/:draft_id", sg.DeleteDraft)
	return sg
}

// SkillInfo is the API representation of an installed skill.
type SkillInfo struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	Path        string `json:"path"`
}

const draftSessionTTL = 30 * time.Minute

type draftSession struct {
	mu          sync.Mutex
	draft       *skills.Draft
	lastTouched time.Time
}

// draftsStore is a short-lived cache for active draft sessions.
// S2 remains the durable source of truth, and entries are evicted opportunistically.
var draftsStore = struct {
	sync.Mutex
	m map[string]*draftSession
}{m: make(map[string]*draftSession)}

type createDraftRequest struct {
	Description    string `json:"description"`
	InitialContent string `json:"initial_content,omitempty"`
	SkillName      string `json:"skill_name,omitempty"`
}

type createDraftResponse struct {
	DraftID string `json:"draft_id"`
}

type chatRequest struct {
	Message string `json:"message"`
}

type sseEvent struct {
	Event        string `json:"event"`
	Message      string `json:"message,omitempty"`
	SkillContent string `json:"skill_content,omitempty"`
	Error        string `json:"error,omitempty"`
}

// List returns all skills installed in the workspace.
func (sg *SkillsGroup) List(c echo.Context) error {
	ctx := c.Request().Context()

	ws, err := sg.backend.GetWorkspaceByExternalId(ctx, c.Param("workspace_id"))
	if err != nil || ws == nil {
		return ErrorResponse(c, http.StatusNotFound, "workspace not found")
	}
	if sg.storage == nil {
		return ErrorResponse(c, http.StatusServiceUnavailable, "storage not configured")
	}

	infos, err := listWorkspaceSkills(ctx, sg.storage, ws.ExternalId)
	if err != nil {
		return ErrorResponse(c, http.StatusInternalServerError, err.Error())
	}

	return SuccessResponse(c, infos)
}

// SkillDetail includes the full SKILL.md content alongside basic info.
type SkillDetail struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	Path        string `json:"path"`
	Content     string `json:"content"`
}

// Get returns a single installed skill including its full content.
func (sg *SkillsGroup) Get(c echo.Context) error {
	ctx := c.Request().Context()

	ws, err := sg.backend.GetWorkspaceByExternalId(ctx, c.Param("workspace_id"))
	if err != nil || ws == nil {
		return ErrorResponse(c, http.StatusNotFound, "workspace not found")
	}
	if sg.storage == nil {
		return ErrorResponse(c, http.StatusServiceUnavailable, "storage not configured")
	}

	name := c.Param("name")
	if name == "" {
		return ErrorResponse(c, http.StatusBadRequest, "name is required")
	}

	bucket := sg.storage.WorkspaceBucketName(ws.ExternalId)
	content, err := sg.storage.Download(ctx, bucket, skills.ManifestKey(name))
	if err != nil {
		if clients.IsNotFoundError(err) {
			return ErrorResponse(c, http.StatusNotFound, "skill not found")
		}
		return ErrorResponse(c, http.StatusInternalServerError, "failed to retrieve skill")
	}

	manifest, err := skills.Parse(content)
	if err != nil {
		return ErrorResponse(c, http.StatusInternalServerError, "failed to parse skill")
	}

	return SuccessResponse(c, SkillDetail{
		Name:        manifest.Name,
		Description: manifest.Description,
		Path:        skills.NameToPath(name),
		Content:     string(content),
	})
}

// InstallRequest is the payload for installing a skill from catalog content.
type InstallRequest struct {
	Name    string `json:"name"`
	Content string `json:"content"`
}

// Install writes a SKILL.md to the workspace skills folder.
func (sg *SkillsGroup) Install(c echo.Context) error {
	ctx := c.Request().Context()

	ws, err := sg.backend.GetWorkspaceByExternalId(ctx, c.Param("workspace_id"))
	if err != nil || ws == nil {
		return ErrorResponse(c, http.StatusNotFound, "workspace not found")
	}
	if sg.storage == nil {
		return ErrorResponse(c, http.StatusServiceUnavailable, "storage not configured")
	}

	var req InstallRequest
	if err := c.Bind(&req); err != nil {
		return ErrorResponse(c, http.StatusBadRequest, "invalid request body")
	}
	if req.Content == "" {
		return ErrorResponse(c, http.StatusBadRequest, "content is required")
	}

	manifest, skillName, err := skills.InstallContent(ctx, sg.storage, ws.ExternalId, req.Name, []byte(req.Content))
	if err != nil {
		status := http.StatusInternalServerError
		if strings.Contains(err.Error(), "invalid SKILL.md") || strings.Contains(err.Error(), "does not match") {
			status = http.StatusBadRequest
		}
		return ErrorResponse(c, status, fmt.Sprintf("failed to install skill: %s", err))
	}

	return SuccessResponse(c, SkillInfo{
		Name:        manifest.Name,
		Description: manifest.Description,
		Path:        skills.NameToPath(skillName),
	})
}

// Uninstall removes a skill and all its files from the workspace.
func (sg *SkillsGroup) Uninstall(c echo.Context) error {
	ctx := c.Request().Context()

	ws, err := sg.backend.GetWorkspaceByExternalId(ctx, c.Param("workspace_id"))
	if err != nil || ws == nil {
		return ErrorResponse(c, http.StatusNotFound, "workspace not found")
	}
	if sg.storage == nil {
		return ErrorResponse(c, http.StatusServiceUnavailable, "storage not configured")
	}

	name := c.Param("name")
	if name == "" {
		return ErrorResponse(c, http.StatusBadRequest, "skill name is required")
	}

	bucket := sg.storage.WorkspaceBucketName(ws.ExternalId)
	if err := deleteSkillByName(ctx, sg.storage, bucket, name); err != nil {
		return ErrorResponse(c, http.StatusInternalServerError, fmt.Sprintf("failed to delete skill: %s", err))
	}

	return SuccessResponse(c, map[string]string{"deleted": name})
}

// GenerateRequest is the payload for AI skill generation (backwards-compatible endpoint).
type GenerateRequest struct {
	Description string `json:"description"`
}

// Generate uses the BAML-backed copilot to produce a SKILL.md from a description.
// This is the backwards-compatible single-shot endpoint; prefer /drafts for iterative editing.
func (sg *SkillsGroup) Generate(c echo.Context) error {
	if sg.copilot == nil {
		return ErrorResponse(c, http.StatusServiceUnavailable, "skill copilot not configured")
	}

	var req GenerateRequest
	if err := decodeStrictBody(c, &req); err != nil {
		return ErrorResponse(c, http.StatusBadRequest, "invalid request body")
	}
	if strings.TrimSpace(req.Description) == "" {
		return ErrorResponse(c, http.StatusBadRequest, "description is required")
	}

	workspaceID := c.Param("workspace_id")
	draft := sg.copilot.CreateDraft(workspaceID)
	_ = sg.copilot.PersistMeta(c.Request().Context(), draft)

	resp, err := sg.copilot.Generate(c.Request().Context(), draft, req.Description)
	if err != nil {
		return ErrorResponse(c, http.StatusInternalServerError, fmt.Sprintf("generation failed: %s", err))
	}

	return SuccessResponse(c, map[string]string{"content": resp.Skill_content})
}

func getCachedDraftSession(draftID string) *draftSession {
	now := time.Now()
	draftsStore.Lock()
	defer draftsStore.Unlock()
	pruneDraftSessionsLocked(now)
	session := draftsStore.m[draftID]
	if session != nil {
		session.lastTouched = now
	}
	return session
}

func putDraftSession(draft *skills.Draft) *draftSession {
	if draft == nil {
		return nil
	}
	now := time.Now()
	draftsStore.Lock()
	defer draftsStore.Unlock()

	pruneDraftSessionsLocked(now)

	if existing := draftsStore.m[draft.ID]; existing != nil {
		existing.draft = draft
		existing.lastTouched = now
		return existing
	}

	session := &draftSession{draft: draft, lastTouched: now}
	draftsStore.m[draft.ID] = session
	return session
}

func deleteDraftSession(draftID string) {
	draftsStore.Lock()
	defer draftsStore.Unlock()
	delete(draftsStore.m, draftID)
}

func pruneDraftSessionsLocked(now time.Time) {
	for id, session := range draftsStore.m {
		if session == nil || now.Sub(session.lastTouched) > draftSessionTTL {
			delete(draftsStore.m, id)
		}
	}
}

func cloneDraft(draft *skills.Draft) *skills.Draft {
	if draft == nil {
		return nil
	}
	out := *draft
	out.Messages = append([]skills.DraftMessage(nil), draft.Messages...)
	return &out
}

func (sg *SkillsGroup) getDraftSession(c echo.Context, draftID string) (*draftSession, error) {
	workspaceID := c.Param("workspace_id")
	if session := getCachedDraftSession(draftID); session != nil {
		session.mu.Lock()
		cachedWorkspaceID := session.draft.WorkspaceID
		session.mu.Unlock()
		if cachedWorkspaceID == workspaceID {
			return session, nil
		}
		if cachedWorkspaceID != "" {
			return nil, fmt.Errorf("draft not found")
		}
	}

	draft, err := sg.copilot.LoadDraft(c.Request().Context(), workspaceID, draftID)
	if err != nil {
		return nil, err
	}
	return putDraftSession(draft), nil
}

// ListDrafts returns draft summaries for a workspace.
func (sg *SkillsGroup) ListDrafts(c echo.Context) error {
	if sg.copilot == nil {
		return ErrorResponse(c, http.StatusServiceUnavailable, "skill copilot not configured")
	}

	workspaceID := c.Param("workspace_id")
	drafts, err := sg.copilot.ListDrafts(c.Request().Context(), workspaceID)
	if err != nil {
		return ErrorResponse(c, http.StatusInternalServerError, err.Error())
	}
	if drafts == nil {
		drafts = []skills.DraftSummary{}
	}

	return SuccessResponse(c, drafts)
}

// CreateDraft creates a persisted draft session for iterative skill editing.
func (sg *SkillsGroup) CreateDraft(c echo.Context) error {
	if sg.copilot == nil {
		return ErrorResponse(c, http.StatusServiceUnavailable, "skill copilot not configured")
	}

	var req createDraftRequest
	if err := decodeStrictBody(c, &req); err != nil {
		return ErrorResponse(c, http.StatusBadRequest, "invalid request body")
	}
	if req.Description == "" {
		return ErrorResponse(c, http.StatusBadRequest, "description is required")
	}

	ctx := c.Request().Context()
	workspaceID := c.Param("workspace_id")
	draft := sg.copilot.CreateDraft(workspaceID)
	if req.InitialContent != "" {
		draft.SkillContent = req.InitialContent
		_ = sg.copilot.PersistSkill(ctx, draft.ID, req.InitialContent)
	}
	_ = sg.copilot.PersistMeta(ctx, draft)
	_ = sg.copilot.IndexDraftCreated(ctx, workspaceID, draft.ID, req.Description, req.SkillName)

	putDraftSession(draft)

	return SuccessResponse(c, createDraftResponse{DraftID: draft.ID})
}

// GetDraft returns the current draft state for an existing draft.
func (sg *SkillsGroup) GetDraft(c echo.Context) error {
	if sg.copilot == nil {
		return ErrorResponse(c, http.StatusServiceUnavailable, "skill copilot not configured")
	}

	session, err := sg.getDraftSession(c, c.Param("draft_id"))
	if err != nil {
		return ErrorResponse(c, http.StatusNotFound, "draft not found")
	}

	session.mu.Lock()
	draft := cloneDraft(session.draft)
	session.mu.Unlock()
	return SuccessResponse(c, draft)
}

// ChatDraft streams draft updates over SSE while the copilot edits the skill.
func (sg *SkillsGroup) ChatDraft(c echo.Context) error {
	if sg.copilot == nil {
		return ErrorResponse(c, http.StatusServiceUnavailable, "skill copilot not configured")
	}

	var req chatRequest
	if err := decodeStrictBody(c, &req); err != nil {
		return ErrorResponse(c, http.StatusBadRequest, "invalid request body")
	}
	if strings.TrimSpace(req.Message) == "" {
		return ErrorResponse(c, http.StatusBadRequest, "message is required")
	}

	session, err := sg.getDraftSession(c, c.Param("draft_id"))
	if err != nil {
		return ErrorResponse(c, http.StatusNotFound, "draft not found")
	}

	w := c.Response()
	flusher, ok := w.Writer.(http.Flusher)
	if !ok {
		return ErrorResponse(c, http.StatusInternalServerError, "streaming not supported")
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.WriteHeader(http.StatusOK)

	// Extend the write deadline so the server's global WriteTimeout (60s)
	// doesn't kill long-running BAML generations mid-stream.
	rc := http.NewResponseController(w)
	_ = rc.SetWriteDeadline(time.Now().Add(5 * time.Minute))

	writeSSE := func(evt sseEvent) {
		data, _ := json.Marshal(evt)
		fmt.Fprintf(w, "data: %s\n\n", data)
		flusher.Flush()
		_ = rc.SetWriteDeadline(time.Now().Add(5 * time.Minute))
	}

	writeSSE(sseEvent{Event: "generating"})

	// Use a detached context so the BAML call isn't killed by the
	// HTTP server's WriteTimeout (60s). The client can still disconnect,
	// which we detect via the original request context.
	genCtx, genCancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer genCancel()

	go func() {
		<-c.Request().Context().Done()
		genCancel()
	}()

	session.mu.Lock()
	defer session.mu.Unlock()
	resp, err := sg.copilot.GenerateStream(
		genCtx,
		session.draft,
		strings.TrimSpace(req.Message),
		func(partial *skills.PartialSkillDraftResponse) {
			writeSSE(sseEvent{
				Event:        "chunk",
				Message:      partial.Message,
				SkillContent: partial.SkillContent,
			})
		},
	)
	if err != nil {
		log.Error().Err(err).Str("draft_id", c.Param("draft_id")).Msg("skill generation failed")
		writeSSE(sseEvent{Event: "error", Error: err.Error()})
		writeSSE(sseEvent{Event: "done"})
		return nil
	}

	writeSSE(sseEvent{
		Event:        "done",
		Message:      resp.Message,
		SkillContent: resp.Skill_content,
	})

	return nil
}

// InstallDraft installs the current draft contents into the workspace.
func (sg *SkillsGroup) InstallDraft(c echo.Context) error {
	if sg.copilot == nil {
		return ErrorResponse(c, http.StatusServiceUnavailable, "skill copilot not configured")
	}

	session, err := sg.getDraftSession(c, c.Param("draft_id"))
	if err != nil {
		return ErrorResponse(c, http.StatusNotFound, "draft not found")
	}

	ctx := c.Request().Context()
	session.mu.Lock()
	manifest, err := sg.copilot.InstallDraft(ctx, session.draft)
	workspaceID := session.draft.WorkspaceID
	draftID := session.draft.ID
	session.mu.Unlock()
	if err != nil {
		return ErrorResponse(c, http.StatusBadRequest, err.Error())
	}

	deleteDraftSession(draftID)
	_ = sg.copilot.IndexDraftInstalled(ctx, workspaceID, draftID, manifest.Name)

	return SuccessResponse(c, SkillInfo{
		Name:        manifest.Name,
		Description: manifest.Description,
		Path:        skills.NameToPath(manifest.Name),
	})
}

// DeleteDraft removes a draft session and marks it as installed in the index.
func (sg *SkillsGroup) DeleteDraft(c echo.Context) error {
	if sg.copilot == nil {
		return ErrorResponse(c, http.StatusServiceUnavailable, "skill copilot not configured")
	}

	draftID := c.Param("draft_id")
	workspaceID := c.Param("workspace_id")

	deleteDraftSession(draftID)
	_ = sg.copilot.IndexDraftInstalled(c.Request().Context(), workspaceID, draftID, "")

	return SuccessResponse(c, map[string]bool{"deleted": true})
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// listWorkspaceSkills reads all SKILL.md manifests from the workspace bucket.
func listWorkspaceSkills(ctx context.Context, storage *clients.StorageClient, workspaceExternalID string) ([]SkillInfo, error) {
	bucket := storage.WorkspaceBucketName(workspaceExternalID)
	output, err := storage.ListObjects(ctx, bucket, skills.Dir+"/", 1000)
	if err != nil {
		return nil, err
	}

	seen := make(map[string]bool)
	result := make([]SkillInfo, 0)

	for _, obj := range output.Contents {
		if obj.Key == nil {
			continue
		}
		name := skills.KeyToName(*obj.Key)
		if name == "" || seen[name] {
			continue
		}
		seen[name] = true

		content, err := storage.Download(ctx, bucket, skills.ManifestKey(name))
		if err != nil {
			continue
		}
		manifest, err := skills.Parse(content)
		if err != nil {
			continue
		}

		result = append(result, SkillInfo{
			Name:        manifest.Name,
			Description: manifest.Description,
			Path:        skills.NameToPath(name),
		})
	}

	return result, nil
}

// deleteSkillByName removes all S3 objects under the skills/{name}/ prefix.
func deleteSkillByName(ctx context.Context, storage *clients.StorageClient, bucket, name string) error {
	prefix := skills.Dir + "/" + name + "/"
	for {
		out, err := storage.ListObjects(ctx, bucket, prefix, 1000)
		if err != nil {
			return fmt.Errorf("list skill objects: %w", err)
		}
		if len(out.Contents) == 0 {
			return nil
		}
		for _, obj := range out.Contents {
			if obj.Key == nil || *obj.Key == "" {
				continue
			}
			if err := storage.Delete(ctx, bucket, *obj.Key); err != nil {
				return fmt.Errorf("delete skill object %q: %w", *obj.Key, err)
			}
		}
		if len(out.Contents) < 1000 {
			return nil
		}
	}
}
