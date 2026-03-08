package skills

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	baml "github.com/beam-cloud/airstore/pkg/skills/baml_client"
	"github.com/beam-cloud/airstore/pkg/skills/baml_client/types"

	"github.com/beam-cloud/airstore/pkg/clients"
	"github.com/beam-cloud/airstore/pkg/common"
	"github.com/google/uuid"
)

type DraftMessage struct {
	Role      string `json:"role"` // "user" or "assistant"
	Content   string `json:"content"`
	Timestamp int64  `json:"ts"`
}

type Draft struct {
	ID           string         `json:"id"`
	WorkspaceID  string         `json:"workspace_id"`
	Status       string         `json:"status"` // "active", "installed", "discarded"
	SkillContent string         `json:"skill_content"`
	Messages     []DraftMessage `json:"messages"`
	CreatedAt    int64          `json:"created_at"`
	UpdatedAt    int64          `json:"updated_at"`
}

type Copilot struct {
	s2      *common.S2Client
	storage *clients.StorageClient
}

func NewCopilot(s2 *common.S2Client, storage *clients.StorageClient) *Copilot {
	return &Copilot{s2: s2, storage: storage}
}

func (c *Copilot) CreateDraft(workspaceID string) *Draft {
	now := time.Now().UnixMilli()
	return &Draft{
		ID:          uuid.New().String(),
		WorkspaceID: workspaceID,
		Status:      "active",
		Messages:    []DraftMessage{},
		CreatedAt:   now,
		UpdatedAt:   now,
	}
}

func (c *Copilot) LoadDraft(ctx context.Context, workspaceID, draftID string) (*Draft, error) {
	if c.s2 == nil || !c.s2.Enabled() {
		return nil, fmt.Errorf("S2 not configured")
	}

	records, err := c.s2.Read(ctx, common.Streams.SkillDraft(draftID), 0, 1000)
	if err != nil {
		return nil, fmt.Errorf("read draft stream: %w", err)
	}
	if len(records) == 0 {
		return nil, fmt.Errorf("draft not found")
	}

	return decodeDraftRecords(workspaceID, draftID, records)
}

func decodeDraftRecords(workspaceID, draftID string, records []common.ReadRecord) (*Draft, error) {
	draft := &Draft{ID: draftID, Status: "active", Messages: []DraftMessage{}}
	for _, rec := range records {
		var entry draftStreamEntry
		if err := json.Unmarshal([]byte(rec.Body), &entry); err != nil {
			continue
		}
		switch entry.Type {
		case "meta":
			draft.WorkspaceID = entry.WorkspaceID
			draft.CreatedAt = entry.Timestamp
			bumpDraftUpdatedAt(draft, entry.Timestamp)
		case "message":
			draft.Messages = append(draft.Messages, DraftMessage{
				Role:      entry.Role,
				Content:   entry.Content,
				Timestamp: entry.Timestamp,
			})
			bumpDraftUpdatedAt(draft, entry.Timestamp)
		case "skill":
			draft.SkillContent = entry.Content
			bumpDraftUpdatedAt(draft, entry.Timestamp)
		case "status":
			draft.Status = entry.Content
			bumpDraftUpdatedAt(draft, entry.Timestamp)
		}
	}
	if draft.WorkspaceID == "" || (workspaceID != "" && draft.WorkspaceID != workspaceID) {
		return nil, fmt.Errorf("draft not found")
	}
	if draft.UpdatedAt == 0 {
		draft.UpdatedAt = draft.CreatedAt
	}

	return draft, nil
}

func bumpDraftUpdatedAt(draft *Draft, timestamp int64) {
	if draft != nil && timestamp > draft.UpdatedAt {
		draft.UpdatedAt = timestamp
	}
}

type draftStreamEntry struct {
	Type        string `json:"type"`
	Role        string `json:"role,omitempty"`
	Content     string `json:"content,omitempty"`
	WorkspaceID string `json:"workspace_id,omitempty"`
	Timestamp   int64  `json:"ts"`
}

func (c *Copilot) appendEntry(ctx context.Context, draftID string, entry draftStreamEntry) error {
	if c.s2 == nil || !c.s2.Enabled() {
		return nil
	}
	return c.s2.Append(ctx, common.Streams.SkillDraft(draftID), entry)
}

func (c *Copilot) PersistMeta(ctx context.Context, draft *Draft) error {
	return c.appendEntry(ctx, draft.ID, draftStreamEntry{
		Type:        "meta",
		WorkspaceID: draft.WorkspaceID,
		Timestamp:   draft.CreatedAt,
	})
}

func (c *Copilot) PersistMessage(ctx context.Context, draftID, role, content string) error {
	return c.appendEntry(ctx, draftID, draftStreamEntry{
		Type:      "message",
		Role:      role,
		Content:   content,
		Timestamp: time.Now().UnixMilli(),
	})
}

func (c *Copilot) PersistSkill(ctx context.Context, draftID, content string) error {
	return c.appendEntry(ctx, draftID, draftStreamEntry{
		Type:      "skill",
		Content:   content,
		Timestamp: time.Now().UnixMilli(),
	})
}

func (c *Copilot) PersistStatus(ctx context.Context, draftID, status string) error {
	return c.appendEntry(ctx, draftID, draftStreamEntry{
		Type:      "status",
		Content:   status,
		Timestamp: time.Now().UnixMilli(),
	})
}

func (c *Copilot) formatHistory(messages []DraftMessage) string {
	if len(messages) == 0 {
		return ""
	}
	var sb strings.Builder
	for _, m := range messages {
		role := "User"
		if m.Role == "assistant" {
			role = "Assistant"
		}
		sb.WriteString(fmt.Sprintf("[%s] %s: %s\n", time.UnixMilli(m.Timestamp).Format("Jan 2 15:04"), role, m.Content))
	}
	return sb.String()
}

// DraftIndexEntry is appended to the workspace-level index stream.
type DraftIndexEntry struct {
	Type        string `json:"type"` // "created" or "installed"
	DraftID     string `json:"draft_id"`
	Description string `json:"description,omitempty"`
	SkillName   string `json:"skill_name,omitempty"`
	Timestamp   int64  `json:"ts"`
}

// DraftSummary is the API representation of a draft for listing.
type DraftSummary struct {
	ID          string `json:"id"`
	Status      string `json:"status"`
	SkillName   string `json:"skill_name,omitempty"`
	Description string `json:"description,omitempty"`
	CreatedAt   int64  `json:"created_at"`
	UpdatedAt   int64  `json:"updated_at"`
}

func (c *Copilot) IndexDraftCreated(ctx context.Context, workspaceID, draftID, description, skillName string) error {
	if c.s2 == nil || !c.s2.Enabled() {
		return nil
	}
	return c.s2.Append(ctx, common.Streams.SkillDraftIndex(workspaceID), DraftIndexEntry{
		Type:        "created",
		DraftID:     draftID,
		Description: description,
		SkillName:   skillName,
		Timestamp:   time.Now().UnixMilli(),
	})
}

func (c *Copilot) IndexDraftInstalled(ctx context.Context, workspaceID, draftID, skillName string) error {
	if c.s2 == nil || !c.s2.Enabled() {
		return nil
	}
	return c.s2.Append(ctx, common.Streams.SkillDraftIndex(workspaceID), DraftIndexEntry{
		Type:      "installed",
		DraftID:   draftID,
		SkillName: skillName,
		Timestamp: time.Now().UnixMilli(),
	})
}

// ListDrafts returns summaries of all drafts for a workspace.
func (c *Copilot) ListDrafts(ctx context.Context, workspaceID string) ([]DraftSummary, error) {
	if c.s2 == nil || !c.s2.Enabled() {
		return nil, nil
	}

	records, err := c.s2.Read(ctx, common.Streams.SkillDraftIndex(workspaceID), 0, 1000)
	if err != nil {
		return nil, err
	}

	drafts := make(map[string]*DraftSummary)
	for _, rec := range records {
		var entry DraftIndexEntry
		if err := json.Unmarshal([]byte(rec.Body), &entry); err != nil {
			continue
		}
		switch entry.Type {
		case "created":
			drafts[entry.DraftID] = &DraftSummary{
				ID:          entry.DraftID,
				Status:      "active",
				SkillName:   entry.SkillName,
				Description: entry.Description,
				CreatedAt:   entry.Timestamp,
				UpdatedAt:   entry.Timestamp,
			}
		case "installed":
			if d, ok := drafts[entry.DraftID]; ok {
				d.Status = "installed"
				if entry.SkillName != "" {
					d.SkillName = entry.SkillName
				}
				d.UpdatedAt = entry.Timestamp
			}
		}
	}

	result := make([]DraftSummary, 0, len(drafts))
	for _, d := range drafts {
		result = append(result, *d)
	}

	return result, nil
}

// ValidateContent checks if a skill content string is a valid SKILL.md.
func ValidateContent(content string) error {
	if content == "" {
		return fmt.Errorf("empty skill content")
	}
	_, err := Parse([]byte(content))
	return err
}

// Generate calls the BAML WriteSkill function and returns the response.
// It also persists messages and skill content to S2.
func (c *Copilot) Generate(ctx context.Context, draft *Draft, userMessage string) (*types.SkillDraftResponse, error) {
	_ = c.PersistMessage(ctx, draft.ID, "user", userMessage)

	draft.Messages = append(draft.Messages, DraftMessage{
		Role:      "user",
		Content:   userMessage,
		Timestamp: time.Now().UnixMilli(),
	})

	history := c.formatHistory(draft.Messages[:len(draft.Messages)-1])

	resp, err := baml.WriteSkill(ctx, userMessage, history, draft.SkillContent)
	if err != nil {
		return nil, fmt.Errorf("BAML WriteSkill: %w", err)
	}

	_ = c.PersistMessage(ctx, draft.ID, "assistant", resp.Message)

	if resp.Response_type == types.ResponseTypeSkillUpdate && resp.Skill_content != "" {
		_ = c.PersistSkill(ctx, draft.ID, resp.Skill_content)
		draft.SkillContent = resp.Skill_content
	} else {
		resp.Skill_content = draft.SkillContent
	}

	draft.Messages = append(draft.Messages, DraftMessage{
		Role:      "assistant",
		Content:   resp.Message,
		Timestamp: time.Now().UnixMilli(),
	})
	draft.UpdatedAt = time.Now().UnixMilli()

	return &resp, nil
}

// GenerateStream calls the BAML WriteSkill function with streaming and sends
// partial results to the provided callback. Returns the final response.
func (c *Copilot) GenerateStream(
	ctx context.Context,
	draft *Draft,
	userMessage string,
	onChunk func(partial *PartialSkillDraftResponse),
) (*types.SkillDraftResponse, error) {
	_ = c.PersistMessage(ctx, draft.ID, "user", userMessage)

	draft.Messages = append(draft.Messages, DraftMessage{
		Role:      "user",
		Content:   userMessage,
		Timestamp: time.Now().UnixMilli(),
	})

	history := c.formatHistory(draft.Messages[:len(draft.Messages)-1])

	ch, err := baml.Stream.WriteSkill(ctx, userMessage, history, draft.SkillContent)
	if err != nil {
		return nil, fmt.Errorf("BAML WriteSkill stream: %w", err)
	}

	var final *types.SkillDraftResponse
	for val := range ch {
		if val.IsError {
			return nil, val.Error
		}
		if val.IsFinal {
			f := val.Final()
			final = f
		} else {
			s := val.Stream()
			if s != nil && onChunk != nil {
				onChunk(&PartialSkillDraftResponse{
					Message:      derefStr(s.Message),
					SkillContent: derefStr(s.Skill_content),
				})
			}
		}
	}

	if final == nil {
		return nil, fmt.Errorf("no final response from BAML stream")
	}

	_ = c.PersistMessage(ctx, draft.ID, "assistant", final.Message)

	if final.Response_type == types.ResponseTypeSkillUpdate && final.Skill_content != "" {
		_ = c.PersistSkill(ctx, draft.ID, final.Skill_content)
		draft.SkillContent = final.Skill_content
	} else {
		final.Skill_content = draft.SkillContent
	}

	draft.Messages = append(draft.Messages, DraftMessage{
		Role:      "assistant",
		Content:   final.Message,
		Timestamp: time.Now().UnixMilli(),
	})
	draft.UpdatedAt = time.Now().UnixMilli()

	return final, nil
}

// InstallDraft validates and installs the current draft skill content.
func (c *Copilot) InstallDraft(ctx context.Context, draft *Draft) (*SkillManifest, error) {
	if draft.SkillContent == "" {
		return nil, fmt.Errorf("draft has no skill content")
	}

	manifest, err := Parse([]byte(draft.SkillContent))
	if err != nil {
		return nil, fmt.Errorf("invalid skill: %w", err)
	}

	if c.storage == nil {
		return nil, fmt.Errorf("storage not configured")
	}

	bucket := c.storage.WorkspaceBucketName(draft.WorkspaceID)
	key := ManifestKey(manifest.Name)

	if err := c.storage.Upload(ctx, bucket, key, []byte(draft.SkillContent)); err != nil {
		return nil, fmt.Errorf("upload skill: %w", err)
	}

	_ = c.PersistStatus(ctx, draft.ID, "installed")
	draft.Status = "installed"

	return manifest, nil
}

// PartialSkillDraftResponse is a simplified view of a streaming chunk for API consumers.
type PartialSkillDraftResponse struct {
	Message      string
	SkillContent string
}

func derefStr(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
