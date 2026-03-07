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
	sg.g.POST("/install", sg.Install)
	sg.g.DELETE("/:name", sg.Uninstall)
	sg.g.POST("/generate", sg.Generate)
	sg.g.GET("/drafts", sg.ListDrafts)
	sg.g.POST("/drafts", sg.CreateDraft)
	sg.g.GET("/drafts/:draft_id", sg.GetDraft)
	sg.g.POST("/drafts/:draft_id/chat", sg.ChatDraft)
	sg.g.POST("/drafts/:draft_id/install", sg.InstallDraft)
	return sg
}

// SkillInfo is the API representation of an installed skill.
type SkillInfo struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	Path        string `json:"path"`
}

// draftsStore is an in-memory store for active draft sessions.
// Drafts are rehydrated from S2 on load and kept live during chat.
var draftsStore = struct {
	sync.RWMutex
	m map[string]*skills.Draft
}{m: make(map[string]*skills.Draft)}

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
	if req.Name == "" || req.Content == "" {
		return ErrorResponse(c, http.StatusBadRequest, "name and content are required")
	}

	manifest, err := skills.Parse([]byte(req.Content))
	if err != nil {
		return ErrorResponse(c, http.StatusBadRequest, fmt.Sprintf("invalid SKILL.md: %s", err))
	}

	bucket := sg.storage.WorkspaceBucketName(ws.ExternalId)
	key := skills.ManifestKey(req.Name)

	if err := sg.storage.Upload(ctx, bucket, key, []byte(req.Content)); err != nil {
		return ErrorResponse(c, http.StatusInternalServerError, fmt.Sprintf("failed to install skill: %s", err))
	}

	return SuccessResponse(c, SkillInfo{
		Name:        manifest.Name,
		Description: manifest.Description,
		Path:        skills.NameToPath(req.Name),
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

func (sg *SkillsGroup) getDraft(c echo.Context, draftID string) (*skills.Draft, error) {
	draftsStore.RLock()
	draft, ok := draftsStore.m[draftID]
	draftsStore.RUnlock()
	if ok {
		return draft, nil
	}

	draft, err := sg.copilot.LoadDraft(c.Request().Context(), draftID)
	if err != nil {
		return nil, err
	}

	draftsStore.Lock()
	draftsStore.m[draftID] = draft
	draftsStore.Unlock()
	return draft, nil
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

	draftsStore.Lock()
	draftsStore.m[draft.ID] = draft
	draftsStore.Unlock()

	return SuccessResponse(c, createDraftResponse{DraftID: draft.ID})
}

// GetDraft returns the current draft state for an existing draft.
func (sg *SkillsGroup) GetDraft(c echo.Context) error {
	if sg.copilot == nil {
		return ErrorResponse(c, http.StatusServiceUnavailable, "skill copilot not configured")
	}

	draft, err := sg.getDraft(c, c.Param("draft_id"))
	if err != nil {
		return ErrorResponse(c, http.StatusNotFound, "draft not found")
	}

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

	draft, err := sg.getDraft(c, c.Param("draft_id"))
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

	resp, err := sg.copilot.GenerateStream(
		genCtx,
		draft,
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

	draft, err := sg.getDraft(c, c.Param("draft_id"))
	if err != nil {
		return ErrorResponse(c, http.StatusNotFound, "draft not found")
	}

	ctx := c.Request().Context()
	manifest, err := sg.copilot.InstallDraft(ctx, draft)
	if err != nil {
		return ErrorResponse(c, http.StatusBadRequest, err.Error())
	}

	_ = sg.copilot.IndexDraftInstalled(ctx, draft.WorkspaceID, draft.ID, manifest.Name)

	return SuccessResponse(c, SkillInfo{
		Name:        manifest.Name,
		Description: manifest.Description,
		Path:        skills.NameToPath(manifest.Name),
	})
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
