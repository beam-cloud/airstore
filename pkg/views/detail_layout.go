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

var (
	detailSectionConversation = newDetailSection(SectionEmailThread, "Conversation", EmphasisPrimary)
	detailSectionApproval     = newDetailSection(SectionApproval, "Approval Required", EmphasisPrimary)
	detailSectionInput        = newDetailSection(SectionInputForm, "Input Required", EmphasisPrimary)
	detailSectionTaskStatus   = newDetailSection(SectionTaskProgress, "Task Status", EmphasisSecondary)
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

// ---------------------------------------------------------------------------
// ResolveLayout filters a schema-level template down to sections that have
// actual data, and generates the action bar from task state.
// Pure logic — no BAML, no IO.
// ---------------------------------------------------------------------------

func ResolveLayout(
	template DetailLayoutResponse,
	task *types.AgentTask,
	outputs []*types.TaskOutput,
	subtasks []*types.AgentTask,
) DetailLayoutResponse {
	runtime := deriveDetailRuntimeState(task, outputs, subtasks)
	var sections []DetailSectionJSON
	for _, s := range template.Sections {
		if runtime.includesSection(s.Type) {
			sections = append(sections, s)
		}
	}
	sections = ensureDetailSections(sections, runtime.requiredSections()...)
	return DetailLayoutResponse{Sections: sections, Actions: runtime.actions()}
}

type detailRuntimeState struct {
	hasTask            bool
	isTaskWaiting      bool
	isTaskError        bool
	isTaskActive       bool
	taskInputKind      types.InputKind
	hasEmail           bool
	hasOtherOutputs    bool
	hasApprovalBlocker bool
	hasInputBlocker    bool
	hasSubtasks        bool
}

type pendingOutputSignal struct {
	IsBlocker       bool
	IsApproval      bool
	IsInput         bool
	Kind            string
	InputKind       string
	WaitGroupID     string
	ApprovalSurface bool
}

func deriveDetailRuntimeState(
	task *types.AgentTask,
	outputs []*types.TaskOutput,
	subtasks []*types.AgentTask,
) detailRuntimeState {
	state := detailRuntimeState{
		hasTask:     task != nil,
		hasSubtasks: len(subtasks) > 0,
	}
	if task != nil {
		state.isTaskWaiting = task.State == types.AgentTaskStateWaiting
		state.isTaskError = task.State == types.AgentTaskStateError
		state.isTaskActive = !task.State.IsTerminal()
		state.taskInputKind = task.InputKind
	}
	for _, output := range outputs {
		if output == nil {
			continue
		}
		if output.OutputType == "email" {
			state.hasEmail = true
		} else {
			state.hasOtherOutputs = true
		}
		if output.Status != types.TaskOutputStatusPending {
			continue
		}
		signal := classifyPendingOutputSignal(output)
		switch {
		case signal.IsApproval:
			state.hasApprovalBlocker = true
		case signal.IsInput:
			state.hasInputBlocker = true
		}
	}
	return state
}

func (s detailRuntimeState) isWaiting() bool {
	return s.isTaskWaiting || s.hasApprovalBlocker || s.hasInputBlocker
}

func (s detailRuntimeState) needsApproval() bool {
	return s.hasApprovalBlocker || (s.isTaskWaiting && s.taskInputKind == types.InputKindApproveReject)
}

func (s detailRuntimeState) needsInput() bool {
	return s.isWaiting() && !s.needsApproval()
}

func (s detailRuntimeState) includesSection(sectionType string) bool {
	switch sectionType {
	case SectionEmailThread:
		return s.hasEmail
	case SectionApproval:
		return s.needsApproval()
	case SectionInputForm:
		return s.needsInput()
	case SectionTaskProgress:
		return s.hasTask
	case SectionOutputGallery:
		return s.hasOtherOutputs
	case SectionSubtasks:
		return s.hasSubtasks
	default:
		return sectionType == SectionDataSummary
	}
}

func (s detailRuntimeState) requiredSections() []DetailSectionJSON {
	sections := []DetailSectionJSON{
		detailSectionDetails(EmphasisSecondary),
	}
	if s.hasEmail {
		sections = append(sections, detailSectionConversation)
	}
	if s.needsApproval() {
		sections = append(sections, detailSectionApproval)
	}
	if s.needsInput() {
		sections = append(sections, detailSectionInput)
	}
	if s.hasOtherOutputs {
		sections = append(sections, detailSectionOutputs)
	}
	if s.hasSubtasks {
		sections = append(sections, detailSectionSubtasks)
	}
	return sections
}

func (s detailRuntimeState) actions() []ActionSpecJSON {
	actions := make([]ActionSpecJSON, 0, 2)
	if s.isTaskError {
		actions = append(actions, ActionSpecJSON{Type: ActionRetry, Label: "Retry", Primary: true})
	}
	if s.isTaskActive {
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

func classifyPendingOutputSignal(output *types.TaskOutput) pendingOutputSignal {
	if output == nil {
		return pendingOutputSignal{}
	}
	blockingKind := metadataStringValue(output.Metadata, types.TaskOutputMetadataBlockingKind)
	inputKind := metadataStringValue(output.Metadata, types.TaskOutputMetadataInputKind)
	approvalSurface := metadataBoolValue(output.Metadata, types.TaskOutputMetadataApprovalUI)

	signal := pendingOutputSignal{
		Kind:            blockingKind,
		InputKind:       inputKind,
		WaitGroupID:     metadataStringValue(output.Metadata, types.TaskOutputMetadataWaitGroupID),
		ApprovalSurface: approvalSurface,
	}
	switch {
	case blockingKind == types.TaskOutputBlockingKindApproval || approvalSurface || inputKind == string(types.InputKindApproveReject):
		signal.IsBlocker = true
		signal.IsApproval = true
	case blockingKind == types.TaskOutputBlockingKindInput || inputKind == string(types.InputKindFreeText):
		signal.IsBlocker = true
		signal.IsInput = true
	}
	return signal
}

func metadataBoolValue(values map[string]any, key string) bool {
	switch typed := values[key].(type) {
	case bool:
		return typed
	case int:
		return typed != 0
	case int32:
		return typed != 0
	case int64:
		return typed != 0
	case float32:
		return typed != 0
	case float64:
		return typed != 0
	default:
		switch strings.ToLower(metadataStringValue(values, key)) {
		case "1", "true", "yes", "on":
			return true
		default:
			return false
		}
	}
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

	sections = append(sections,
		detailSectionApproval,
		detailSectionInput,
	)

	dataEmphasis := EmphasisPrimary
	if hasEmailColumns {
		dataEmphasis = EmphasisSecondary
	}
	sections = append(sections,
		detailSectionDetails(dataEmphasis),
		detailSectionTaskStatus,
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
