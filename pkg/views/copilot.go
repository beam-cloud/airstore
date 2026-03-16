package views

import (
	"context"
	"encoding/json"
	"fmt"
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

type DraftMessage struct {
	Role      string `json:"role"`
	Content   string `json:"content"`
	Timestamp int64  `json:"ts"`
}

type Draft struct {
	ID              string         `json:"id"`
	WorkspaceID     string         `json:"workspace_id"`
	Status          string         `json:"status"`
	ViewContent     string         `json:"view_content"`
	PublishedViewID string         `json:"published_view_id,omitempty"`
	Messages        []DraftMessage `json:"messages"`
	CreatedAt       int64          `json:"created_at"`
	UpdatedAt       int64          `json:"updated_at"`
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

type PartialViewDraftResponse struct {
	Message     string
	ViewContent string
	UpdateType  string
}

type OperationResult struct {
	Type    string `json:"type"`
	Name    string `json:"name"`
	Status  string `json:"status"`
	Error   string `json:"error,omitempty"`
	AgentID string `json:"agent_id,omitempty"`
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

// ---------------------------------------------------------------------------
// Copilot
// ---------------------------------------------------------------------------

type Copilot struct {
	s2       *common.S2Client
	backend  repository.BackendRepository
	storage  *clients.StorageClient
	agentAPI *orchestration.AgentAPI
}

func NewCopilot(s2 *common.S2Client, backend repository.BackendRepository, storage *clients.StorageClient, agentAPI *orchestration.AgentAPI) *Copilot {
	return &Copilot{s2: s2, backend: backend, storage: storage, agentAPI: agentAPI}
}

func (c *Copilot) DraftsAvailable() bool {
	return c != nil && c.s2 != nil && c.s2.Enabled()
}

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

func (c *Copilot) CreateDraft(workspaceID string) *Draft {
	now := nowMS()
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
	records, err := c.s2.Read(ctx, common.Streams.ViewDraft(draftID), 0, 1000)
	if err != nil {
		return nil, fmt.Errorf("read draft stream: %w", err)
	}
	if len(records) == 0 {
		return nil, fmt.Errorf("draft not found")
	}

	draft := &Draft{ID: draftID, Status: "active", Messages: []DraftMessage{}}
	for _, rec := range records {
		var e draftStreamEntry
		if err := json.Unmarshal([]byte(rec.Body), &e); err != nil {
			continue
		}
		switch e.Type {
		case "meta":
			draft.WorkspaceID = e.WorkspaceID
			draft.CreatedAt = e.Timestamp
		case "message":
			draft.Messages = append(draft.Messages, DraftMessage{Role: e.Role, Content: e.Content, Timestamp: e.Timestamp})
		case "view":
			draft.ViewContent = e.Content
		case "published_view_id":
			draft.PublishedViewID = e.Content
		case "status":
			draft.Status = e.Content
		}
		if e.Timestamp > draft.UpdatedAt {
			draft.UpdatedAt = e.Timestamp
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

func (c *Copilot) DeleteDraft(ctx context.Context, workspaceID, draftID string) error {
	if err := c.persistDraft(ctx, draftID, "status", "discarded", "", ""); err != nil {
		return fmt.Errorf("persist draft status: %w", err)
	}
	return c.indexDraft(ctx, workspaceID, "discarded", draftID, "", "", "")
}

// ---------------------------------------------------------------------------
// Draft persistence — all S2 writes go through two helpers
// ---------------------------------------------------------------------------

func (c *Copilot) persistDraft(ctx context.Context, draftID, entryType, content, role, workspaceID string) error {
	return c.s2Append(ctx, common.Streams.ViewDraft(draftID), draftStreamEntry{
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
func (c *Copilot) PersistMeta(ctx context.Context, draft *Draft) error {
	return c.s2Append(ctx, common.Streams.ViewDraft(draft.ID), draftStreamEntry{
		Type: "meta", WorkspaceID: draft.WorkspaceID, Timestamp: draft.CreatedAt,
	})
}
func (c *Copilot) PersistViewContent(ctx context.Context, draftID, viewContent string) error {
	return c.persistDraft(ctx, draftID, "view", viewContent, "", "")
}

func (c *Copilot) PersistPublishedViewID(ctx context.Context, draftID, viewID string) error {
	return c.persistDraft(ctx, draftID, "published_view_id", viewID, "", "")
}

func (c *Copilot) IndexDraftCreated(ctx context.Context, workspaceID, draftID, desc, viewName, viewID string) error {
	return c.indexDraft(ctx, workspaceID, "created", draftID, desc, viewName, viewID)
}
func (c *Copilot) IndexDraftPublished(ctx context.Context, workspaceID, draftID, viewName, viewID string) error {
	return c.indexDraft(ctx, workspaceID, "published", draftID, "", viewName, viewID)
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

	result := make([]DraftSummary, 0, len(drafts))
	for _, d := range drafts {
		result = append(result, *d)
	}
	return result, nil
}

// ---------------------------------------------------------------------------
// Publishing
// ---------------------------------------------------------------------------

func (c *Copilot) PublishView(ctx context.Context, draft *Draft, workspaceID uint) (*types.View, error) {
	if draft.ViewContent == "" {
		return nil, fmt.Errorf("draft has no view content")
	}

	var def types.ViewDefinition
	if err := json.Unmarshal([]byte(draft.ViewContent), &def); err != nil {
		return nil, fmt.Errorf("invalid view definition: %w", err)
	}
	normalizeViewDefinition(&def)
	canonicalizeViewAgentRefs(&def, c.loadWorkspaceAgents(ctx, workspaceID), nil)
	normalizeViewDefinition(&def)

	var published *types.View
	if draft.PublishedViewID != "" {
		if existing, err := c.backend.GetView(ctx, workspaceID, draft.PublishedViewID); err == nil && existing != nil {
			existing.Name, existing.Description, existing.Definition = def.Name, def.Description, def
			if err := c.backend.UpdateView(ctx, existing); err != nil {
				return nil, fmt.Errorf("update view: %w", err)
			}
			published = existing
		}
	}

	if published == nil {
		published = &types.View{WorkspaceID: workspaceID, Name: def.Name, Description: def.Description, Definition: def}
		if err := c.backend.CreateView(ctx, published); err != nil {
			return nil, fmt.Errorf("create view: %w", err)
		}
	}

	draft.PublishedViewID = published.ID
	draft.Status = "published"
	if err := c.persistDraft(ctx, draft.ID, "published_view_id", published.ID, "", ""); err != nil {
		return nil, fmt.Errorf("persist published view id: %w", err)
	}
	if err := c.persistDraft(ctx, draft.ID, "status", "published", "", ""); err != nil {
		return nil, fmt.Errorf("persist published draft status: %w", err)
	}
	return published, nil
}

// ---------------------------------------------------------------------------
// BAML generation
// ---------------------------------------------------------------------------

func (c *Copilot) FormatHistory(messages []DraftMessage) string {
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
	draft *Draft,
	workspaceID uint,
	userMessage string,
	onChunk func(partial *PartialViewDraftResponse),
) (*bamltypes.ViewDraftResponse, error) {
	_ = c.persistDraft(ctx, draft.ID, "message", userMessage, "user", "")
	draft.Messages = append(draft.Messages, DraftMessage{Role: "user", Content: userMessage, Timestamp: nowMS()})

	history := c.FormatHistory(draft.Messages[:len(draft.Messages)-1])
	workspaceAgents := c.loadWorkspaceAgents(ctx, workspaceID)
	workspaceCtx := c.BuildWorkspaceContext(ctx, workspaceID)

	ch, err := baml.Stream.WriteView(ctx, userMessage, history, draft.ViewContent, workspaceCtx, ComponentRegistryDoc)
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
			onChunk(&PartialViewDraftResponse{
				Message:     deref(s.Message),
				ViewContent: deref(s.View_content),
				UpdateType:  derefEnum(s.Update_type),
			})
		}
	}
	if final == nil {
		return nil, fmt.Errorf("no final response from BAML stream")
	}
	if final.Update_type != bamltypes.ViewUpdateTypeCONVERSATION && final.View_content != "" {
		if normalized, err := normalizeViewContent(final.View_content, workspaceAgents); err == nil {
			final.View_content = normalized
		}
		if draft.ViewContent != "" {
			if merged, err := mergePreserveSheets(draft.ViewContent, final.View_content, final.Removed_sheet_ids); err == nil {
				final.View_content = merged
			}
		}
	}

	_ = c.persistDraft(ctx, draft.ID, "message", final.Message, "assistant", "")
	if final.Update_type != bamltypes.ViewUpdateTypeCONVERSATION && final.View_content != "" {
		_ = c.persistDraft(ctx, draft.ID, "view", final.View_content, "", "")
		draft.ViewContent = final.View_content
	}

	draft.Messages = append(draft.Messages, DraftMessage{Role: "assistant", Content: final.Message, Timestamp: nowMS()})
	draft.UpdatedAt = nowMS()
	return final, nil
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
				if m, ok := skillManifests[sn]; ok {
					fmt.Fprintf(&sb, "    • %s — %s\n", sn, m.Description)
					meta := m.AirstoreMetadata()
					if len(meta.Needs) > 0 {
						fmt.Fprintf(&sb, "      integrations: %s\n", strings.Join(meta.Needs, ", "))
					}
					if len(meta.Writes) > 0 {
						fmt.Fprintf(&sb, "      output paths: %s\n", strings.Join(meta.Writes, ", "))
					}
				} else {
					fmt.Fprintf(&sb, "    • %s\n", sn)
				}
			}
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
	for i := range def.Sheets {
		sheet := &def.Sheets[i]
		sheet.ID = strings.TrimSpace(sheet.ID)
		sheet.Name = strings.TrimSpace(sheet.Name)
		sheet.Description = strings.TrimSpace(sheet.Description)
		if sheet.Layout.Columns <= 0 {
			sheet.Layout.Columns = 12
		}
		for j := range sheet.Relations {
			sheet.Relations[j].ID = strings.TrimSpace(sheet.Relations[j].ID)
			sheet.Relations[j].Name = strings.TrimSpace(sheet.Relations[j].Name)
			sheet.Relations[j].ToSheetID = strings.TrimSpace(sheet.Relations[j].ToSheetID)
			sheet.Relations[j].FromColumn = normalizeColumnKey(sheet.Relations[j].FromColumn)
			sheet.Relations[j].ToColumn = normalizeColumnKey(sheet.Relations[j].ToColumn)
		}
		for j := range sheet.Components {
			if ds := sheet.Components[j].DataSource; ds != nil {
				normalizeDataSource(ds)
			}
			normalizeAgentConfig(sheet.Components[j].Config)
			normalizeComponentConfig(&sheet.Components[j])
		}
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
	if ds.RowStrategy != nil {
		ds.RowStrategy.Description = strings.TrimSpace(ds.RowStrategy.Description)
		switch strings.ToLower(strings.TrimSpace(ds.RowStrategy.Mode)) {
		case "", types.RowStrategyModeTask:
			if ds.RowStrategy.Description == "" {
				ds.RowStrategy = nil
			} else {
				ds.RowStrategy.Mode = types.RowStrategyModeTask
			}
		case types.RowStrategyModeSplit:
			ds.RowStrategy.Mode = types.RowStrategyModeSplit
		default:
			ds.RowStrategy = nil
		}
	}
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
	case "task_id", "output_id":
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

func (c *Copilot) loadSkillManifests(ctx context.Context, workspaceID uint) map[string]*skills.SkillManifest {
	result := make(map[string]*skills.SkillManifest)
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
		result[name] = manifest
	}
	return result
}

// ---------------------------------------------------------------------------
// Operations — workspace mutations (agents, skills)
// ---------------------------------------------------------------------------

func (c *Copilot) ExecuteOperations(ctx context.Context, workspaceID uint, ops []bamltypes.Operation) []OperationResult {
	state := newOperationExecutionState(ops)
	results := make([]OperationResult, 0, len(ops))
	for _, op := range ops {
		results = append(results, c.executeOne(ctx, workspaceID, op, state))
	}
	return results
}

type operationExecutionState struct {
	skillAliases map[string]string
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
		return fail("", "invalid payload JSON")
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

		profile, err := c.agentAPI.CreateAgent(ctx, workspaceID, key, name, config, nil)
		if err != nil {
			return fail(name, err.Error())
		}
		if role := coalesceTrimmed(str("role"), str("description")); role != "" && role != "generalist" {
			c.agentAPI.UpdateAgent(ctx, workspaceID, profile.ID, nil, &role, nil, nil, nil, nil, nil) //nolint:errcheck
		}
		log.Info().Str("agent_id", profile.ID).Str("name", name).Uint("workspace_id", workspaceID).Msg("copilot created agent")
		return done(name, profile.ID)

	case bamltypes.OperationTypeUPDATE_AGENT:
		agentID := str("agent_id")
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
		agentID := str("agent_id")
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

	default:
		return fail("", "unknown operation type")
	}
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

func describeFields(m map[string]any, prefix string, out map[string]string, depth int) {
	if depth > 2 {
		return
	}
	for k, v := range m {
		path := prefix + "." + k
		switch val := v.(type) {
		case map[string]any:
			out[path] = "object"
			describeFields(val, path, out, depth+1)
		case []any:
			if len(val) > 0 {
				if nested, ok := val[0].(map[string]any); ok {
					out[path] = "array of objects"
					describeFields(nested, path+"[]", out, depth+1)
				} else {
					out[path] = fmt.Sprintf("array of %T", val[0])
				}
			} else {
				out[path] = "array"
			}
		case string:
			out[path] = "string"
		case float64:
			out[path] = "number"
		case bool:
			out[path] = "boolean"
		case nil:
			out[path] = "null"
		default:
			out[path] = fmt.Sprintf("%T", v)
		}
	}
}

// ---------------------------------------------------------------------------
// Component registry documentation — injected into BAML prompt
// ---------------------------------------------------------------------------

const ComponentRegistryDoc = `A view is a workbook of tabbed sheets.
Each sheet has a header bar that shows assigned agents, live task counts, and action buttons.
Each sheet has exactly one primary table.

COMPONENT TYPES (only two):

- table: The sheet's data table. Full-width, always present.
  At render time a BAML mapper dynamically maps task output data into the
  column schema. Transform rules and row strategy are semantic hints that guide
  the mapping:
  - column: machine-stable key (snake_case) describing what the column shows
  - source: dot-path hint (e.g. "data.recipe_name", "title", "uri")
  - type: display type (text, number, currency, date, link, email, status, tags, boolean)
  - row_strategy:
    - mode: "task" (default): synthesize one row per task — works great
      for most use cases. Omit row_strategy entirely to use this default.
    - mode: "split": expand a single task into many rows when the task
      produces multiple distinct entities (e.g. 10 emails sent, 5 listings
      scraped, 8 contacts researched). Each entity becomes its own row.
    - description: REQUIRED for split mode. Clearly describe what one row
      represents, e.g. "one row per email recipient" or "one row per property
      listing". This guides the mapper to separate entities correctly.

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

- action: Button in the active sheet header bar. Opens a modal form that submits a task.
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

AGENT SELECTION:
Keep definition.agents minimal — only include agents actually used.
If several agents share the same skills, pick one unless the user wants multiple.

SHEET DESIGN:
- STRONG DEFAULT: use ONE sheet. Most workflows fit a single table.
  Only create multiple sheets when the user explicitly requests them or the data
  has genuinely distinct entity types (e.g. contacts vs emails vs pricing).
- When using split row_strategy on a sheet, that handles item-level detail
  within the same sheet — you do NOT need a separate detail sheet.
- Generate concise sheet names tied to the workflow, not generic labels.
- Use sheet relations when rows should connect across sheets via stable keys
  like task_id, email, company_id, listing_id, or similar identifiers.`
