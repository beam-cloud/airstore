package views

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/beam-cloud/airstore/pkg/types"
	baml "github.com/beam-cloud/airstore/pkg/views/baml_client"
	bamltypes "github.com/beam-cloud/airstore/pkg/views/baml_client/types"
	viewprojection "github.com/beam-cloud/airstore/pkg/views/projection"
	"github.com/rs/zerolog/log"
)

// ---------------------------------------------------------------------------
// Detail section / action type constants — single source of truth.
// SectionType values mirror the BAML enum in baml_client/types.
// ---------------------------------------------------------------------------

const (
	SectionEmailThread   = string(bamltypes.SectionTypeEMAIL_THREAD)
	SectionApproval      = string(bamltypes.SectionTypeAPPROVAL)
	SectionInputForm     = string(bamltypes.SectionTypeINPUT_FORM)
	SectionTaskProgress  = string(bamltypes.SectionTypeTASK_PROGRESS)
	SectionOutputGallery = string(bamltypes.SectionTypeOUTPUT_GALLERY)
	SectionDataSummary   = string(bamltypes.SectionTypeDATA_SUMMARY)
	SectionSubtasks      = string(bamltypes.SectionTypeSUBTASKS)

	EmphasisPrimary   = string(bamltypes.SectionEmphasisPRIMARY)
	EmphasisSecondary = string(bamltypes.SectionEmphasisSECONDARY)
	EmphasisCollapsed = string(bamltypes.SectionEmphasisCOLLAPSED)

	ActionRetry  = "RETRY"
	ActionCancel = "CANCEL"
)

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

type DetailLayoutResponse struct {
	Sections []DetailSectionJSON `json:"sections" bson:"sections"`
	Actions  []ActionSpecJSON    `json:"actions" bson:"actions"`
}

type DetailSectionJSON struct {
	Type        string  `json:"type" bson:"type"`
	Title       string  `json:"title" bson:"title"`
	Description *string `json:"description,omitempty" bson:"description,omitempty"`
	Emphasis    string  `json:"emphasis" bson:"emphasis"`
}

type ActionSpecJSON struct {
	Type    string `json:"type" bson:"type"`
	Label   string `json:"label" bson:"label"`
	Primary bool   `json:"primary" bson:"primary"`
}

type DetailSurface string

const (
	DetailSurfaceDetails      DetailSurface = "details"
	DetailSurfaceConversation DetailSurface = "conversation"
	DetailSurfaceOutputs      DetailSurface = "outputs"
)

type DetailProjection struct {
	Surface        DetailSurface
	Blocker        *types.ResolvedBlocker
	Outputs        []*types.TaskOutput
	GalleryOutputs []*types.TaskOutput
	ThreadOutputs  []*types.TaskOutput
	HasTask        bool
	HasSubtasks    bool
	IsTaskWaiting  bool
	IsTaskError    bool
	IsTaskActive   bool
}

var (
	detailSectionConversation = newDetailSection(SectionEmailThread, "Conversation", EmphasisPrimary)
	detailSectionOutputs      = newDetailSection(SectionOutputGallery, "Outputs", EmphasisCollapsed)
	detailSectionSubtasks     = newDetailSection(SectionSubtasks, "Subtasks", EmphasisSecondary)
)

func newDetailSection(sectionType, title, emphasis string) DetailSectionJSON {
	return DetailSectionJSON{Type: sectionType, Title: title, Emphasis: emphasis}
}

func detailSectionDetails(emphasis string) DetailSectionJSON {
	return newDetailSection(SectionDataSummary, "Details", emphasis)
}

// ---------------------------------------------------------------------------
// DetailTemplateForComponent returns the best available detail layout template
// for a table component. Tries cached BAML result first, falls back to
// deterministic inference from columns.
// ---------------------------------------------------------------------------

func DetailTemplateForComponent(comp *types.ComponentSpec) DetailLayoutResponse {
	if comp != nil && comp.Config != nil {
		if tmpl, ok := parseDetailTemplate(comp.Config); ok {
			return tmpl
		}
	}
	var cols []types.ColumnMeta
	if comp != nil && comp.Config != nil {
		cols = ConfigColumnsToMeta(comp.Config)
	}
	return InferDetailTemplate(cols)
}

func ProjectDetail(task *types.AgentTask, outputs []*types.TaskOutput, subtasks []*types.AgentTask) DetailProjection {
	blocker := viewprojection.ProjectBlocker(task, outputs)
	blockerOutputIDs := blockerOutputIDSet(blocker)
	projection := DetailProjection{
		Blocker:     blocker,
		HasTask:     task != nil,
		HasSubtasks: len(subtasks) > 0,
	}
	if task != nil {
		projection.IsTaskWaiting = task.State == types.AgentTaskStateWaiting
		projection.IsTaskError = task.State == types.AgentTaskStateError
		projection.IsTaskActive = !task.State.IsTerminal()
	}
	for _, output := range outputs {
		if output == nil {
			continue
		}
		blockerOwned := isBlockerOwnedOutput(output, blockerOutputIDs)
		if includeDetailOutput(output, blockerOwned) {
			projection.Outputs = append(projection.Outputs, output)
		}
		if includeThreadOutput(output, blockerOwned) {
			projection.ThreadOutputs = append(projection.ThreadOutputs, output)
		}
		if includeGalleryOutput(output, blockerOwned) {
			projection.GalleryOutputs = append(projection.GalleryOutputs, output)
		}
	}
	projection.Surface = projection.resolveSurface()
	return projection
}

func blockerOutputIDSet(blocker *types.ResolvedBlocker) map[string]struct{} {
	if blocker == nil || len(blocker.OutputIDs) == 0 {
		return nil
	}
	ids := make(map[string]struct{}, len(blocker.OutputIDs))
	for _, outputID := range blocker.OutputIDs {
		outputID = strings.TrimSpace(outputID)
		if outputID == "" {
			continue
		}
		ids[outputID] = struct{}{}
	}
	if len(ids) == 0 {
		return nil
	}
	return ids
}

func isBlockerOwnedOutput(output *types.TaskOutput, blockerOutputIDs map[string]struct{}) bool {
	if output == nil || len(blockerOutputIDs) == 0 {
		return false
	}
	_, ok := blockerOutputIDs[strings.TrimSpace(output.ID)]
	return ok
}

func includeDetailOutput(output *types.TaskOutput, blockerOwned bool) bool {
	if output == nil || hidesDetailOutput(output.Status) {
		return false
	}
	if blockerOwned {
		return true
	}
	return output.OutputType != types.TaskOutputTypeEmail
}

func includeThreadOutput(output *types.TaskOutput, blockerOwned bool) bool {
	if output == nil || blockerOwned || hidesDetailOutput(output.Status) {
		return false
	}
	return output.OutputType == types.TaskOutputTypeEmail
}

func includeGalleryOutput(output *types.TaskOutput, blockerOwned bool) bool {
	if output == nil || blockerOwned || hidesDetailOutput(output.Status) {
		return false
	}
	return output.OutputType != types.TaskOutputTypeEmail
}

func hidesDetailOutput(status string) bool {
	switch strings.TrimSpace(status) {
	case types.TaskOutputStatusCancelled, types.TaskOutputStatusRejected:
		return true
	default:
		return false
	}
}

func (p DetailProjection) resolveSurface() DetailSurface {
	switch {
	case len(p.ThreadOutputs) > 0:
		return DetailSurfaceConversation
	case len(p.GalleryOutputs) > 0:
		return DetailSurfaceOutputs
	default:
		return DetailSurfaceDetails
	}
}

func ResolveProjectedLayout(template DetailLayoutResponse, projection DetailProjection) DetailLayoutResponse {
	var sections []DetailSectionJSON
	for _, s := range template.Sections {
		if projection.includesSection(s.Type) {
			sections = append(sections, s)
		}
	}
	sections = ensureDetailSections(sections, projection.requiredSections()...)
	return DetailLayoutResponse{Sections: sections, Actions: projection.actions()}
}

func (p DetailProjection) hasConversation() bool {
	return len(p.ThreadOutputs) > 0
}

func (p DetailProjection) hasOutputGallery() bool {
	return len(p.GalleryOutputs) > 0
}

func (p DetailProjection) includesSection(sectionType string) bool {
	switch sectionType {
	case SectionEmailThread:
		return p.hasConversation()
	case SectionTaskProgress:
		return false
	case SectionOutputGallery:
		return p.hasOutputGallery()
	case SectionSubtasks:
		return p.HasSubtasks
	case SectionApproval, SectionInputForm:
		return false
	default:
		return sectionType == SectionDataSummary
	}
}

func (p DetailProjection) requiredSections() []DetailSectionJSON {
	sections := []DetailSectionJSON{
		detailSectionDetails(EmphasisSecondary),
	}
	if p.hasConversation() {
		sections = append(sections, detailSectionConversation)
	}
	if p.hasOutputGallery() {
		sections = append(sections, detailSectionOutputs)
	}
	if p.HasSubtasks {
		sections = append(sections, detailSectionSubtasks)
	}
	return sections
}

func (p DetailProjection) actions() []ActionSpecJSON {
	actions := make([]ActionSpecJSON, 0, 2)
	if p.IsTaskError {
		actions = append(actions, ActionSpecJSON{Type: ActionRetry, Label: "Retry", Primary: true})
	}
	if p.IsTaskActive {
		actions = append(actions, ActionSpecJSON{Type: ActionCancel, Label: "Cancel", Primary: false})
	}
	return actions
}


func ensureDetailSections(sections []DetailSectionJSON, fallbacks ...DetailSectionJSON) []DetailSectionJSON {
	for _, fallback := range fallbacks {
		sections = ensureDetailSection(sections, fallback)
	}
	return sections
}

func ensureDetailSection(sections []DetailSectionJSON, fallback DetailSectionJSON) []DetailSectionJSON {
	for _, section := range sections {
		if section.Type == fallback.Type {
			return sections
		}
	}
	insertAt := len(sections)
	for i, section := range sections {
		if section.Type == SectionDataSummary {
			insertAt = i
			break
		}
	}
	sections = append(sections, DetailSectionJSON{})
	copy(sections[insertAt+1:], sections[insertAt:])
	sections[insertAt] = fallback
	return sections
}

// ---------------------------------------------------------------------------
// InferDetailTemplate builds a detail template deterministically from column
// metadata. Used as the fallback when no BAML classification is cached.
// ---------------------------------------------------------------------------

var emailColumnHints = []string{
	"recipient", "email", "to", "from", "subject", "thread",
	"sent", "delivered", "reply", "outreach", "message",
	"email_link", "thread_id", "message_id",
}

func InferDetailTemplate(columns []types.ColumnMeta) DetailLayoutResponse {
	hasEmailColumns := false
	for _, col := range columns {
		k := strings.ToLower(col.Key)
		for _, hint := range emailColumnHints {
			if strings.Contains(k, hint) {
				hasEmailColumns = true
				break
			}
		}
		if hasEmailColumns {
			break
		}
	}

	var sections []DetailSectionJSON

	if hasEmailColumns {
		sections = append(sections, detailSectionConversation)
	}

	dataEmphasis := EmphasisPrimary
	if hasEmailColumns {
		dataEmphasis = EmphasisSecondary
	}
	sections = append(sections,
		detailSectionDetails(dataEmphasis),
		detailSectionOutputs,
		detailSectionSubtasks,
	)

	return DetailLayoutResponse{Sections: sections}
}

// ---------------------------------------------------------------------------
// BAML classification — runs once per unique schema during publish.
// Falls back to InferDetailTemplate on failure.
// ---------------------------------------------------------------------------

type bamlEnvKey struct{}

func ClassifyDetailTemplate(ctx context.Context, tableTitle string, columns []types.ColumnMeta) DetailLayoutResponse {
	schema, _ := json.Marshal(columns)

	bamlCtx := context.WithValue(ctx, bamlEnvKey{}, map[string]string{
		"ANTHROPIC_API_KEY": os.Getenv("ANTHROPIC_API_KEY"),
	})

	layout, err := baml.ClassifyDetailTemplate(bamlCtx, tableTitle, string(schema))
	if err != nil {
		log.Warn().Err(err).Str("table", tableTitle).Msg("BAML ClassifyDetailTemplate failed, using deterministic fallback")
		return InferDetailTemplate(columns)
	}

	return toDetailLayoutResponse(layout)
}

func toDetailLayoutResponse(layout bamltypes.DetailLayout) DetailLayoutResponse {
	sections := make([]DetailSectionJSON, len(layout.Sections))
	for i, s := range layout.Sections {
		sections[i] = DetailSectionJSON{
			Type:        string(s.Type),
			Title:       s.Title,
			Description: s.Description,
			Emphasis:    string(s.Emphasis),
		}
	}
	return DetailLayoutResponse{Sections: sections}
}

// ---------------------------------------------------------------------------
// Config helpers
// ---------------------------------------------------------------------------

func parseDetailTemplate(config map[string]any) (DetailLayoutResponse, bool) {
	raw, ok := config["detail_layout"]
	if !ok {
		return DetailLayoutResponse{}, false
	}
	data, err := json.Marshal(raw)
	if err != nil {
		return DetailLayoutResponse{}, false
	}
	var layout DetailLayoutResponse
	if err := json.Unmarshal(data, &layout); err != nil {
		return DetailLayoutResponse{}, false
	}
	if len(layout.Sections) == 0 {
		return DetailLayoutResponse{}, false
	}
	return layout, true
}

func columnSchemaHash(cols []types.ColumnMeta) string {
	h := sha256.New()
	for _, c := range cols {
		fmt.Fprintf(h, "%s:%s\n", c.Key, c.Type)
	}
	return fmt.Sprintf("%x", h.Sum(nil))[:16]
}

func ConfigColumnsToMeta(config map[string]any) []types.ColumnMeta {
	raw, ok := config["columns"]
	if !ok {
		return nil
	}
	data, err := json.Marshal(raw)
	if err != nil {
		return nil
	}
	var cols []types.ColumnMeta
	if err := json.Unmarshal(data, &cols); err != nil {
		return nil
	}
	return cols
}
