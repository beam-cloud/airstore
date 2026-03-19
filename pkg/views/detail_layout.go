package views

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	baml "github.com/beam-cloud/airstore/pkg/views/baml_client"
	bamltypes "github.com/beam-cloud/airstore/pkg/views/baml_client/types"
	"github.com/beam-cloud/airstore/pkg/types"
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

	ActionApprove = "APPROVE"
	ActionReject  = "REJECT"
	ActionRetry   = "RETRY"
	ActionCancel  = "CANCEL"
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
	hasEmail := false
	hasOtherOutputs := false
	for _, o := range outputs {
		if o.OutputType == "email" {
			hasEmail = true
		} else {
			hasOtherOutputs = true
		}
	}

	isWaiting := task != nil && task.State == types.AgentTaskStateWaiting
	isApproval := isWaiting && task.InputKind == types.InputKindApproveReject

	var sections []DetailSectionJSON
	for _, s := range template.Sections {
		switch s.Type {
		case SectionEmailThread:
			if hasEmail {
				sections = append(sections, s)
			}
		case SectionApproval:
			if isApproval {
				sections = append(sections, s)
			}
		case SectionInputForm:
			if isWaiting && !isApproval {
				sections = append(sections, s)
			}
		case SectionTaskProgress:
			if task != nil {
				sections = append(sections, s)
			}
		case SectionOutputGallery:
			if hasOtherOutputs {
				sections = append(sections, s)
			}
		case SectionSubtasks:
			if len(subtasks) > 0 {
				sections = append(sections, s)
			}
		default:
			sections = append(sections, s)
		}
	}

	isTerminal := task != nil && task.State.IsTerminal()
	isActive := task != nil && !isTerminal

	var actions []ActionSpecJSON
	if isApproval {
		actions = append(actions,
			ActionSpecJSON{Type: ActionApprove, Label: "Approve", Primary: true},
			ActionSpecJSON{Type: ActionReject, Label: "Reject", Primary: false},
		)
	}
	if task != nil && task.State == types.AgentTaskStateError {
		actions = append(actions, ActionSpecJSON{Type: ActionRetry, Label: "Retry", Primary: true})
	}
	if isActive {
		actions = append(actions, ActionSpecJSON{Type: ActionCancel, Label: "Cancel", Primary: false})
	}

	return DetailLayoutResponse{Sections: sections, Actions: actions}
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
		sections = append(sections, DetailSectionJSON{
			Type: SectionEmailThread, Title: "Conversation", Emphasis: EmphasisPrimary,
		})
	}

	sections = append(sections,
		DetailSectionJSON{Type: SectionApproval, Title: "Approval Required", Emphasis: EmphasisPrimary},
		DetailSectionJSON{Type: SectionInputForm, Title: "Input Required", Emphasis: EmphasisPrimary},
	)

	dataEmphasis := EmphasisPrimary
	if hasEmailColumns {
		dataEmphasis = EmphasisSecondary
	}
	sections = append(sections,
		DetailSectionJSON{Type: SectionDataSummary, Title: "Details", Emphasis: dataEmphasis},
		DetailSectionJSON{Type: SectionTaskProgress, Title: "Task Status", Emphasis: EmphasisSecondary},
		DetailSectionJSON{Type: SectionOutputGallery, Title: "Outputs", Emphasis: EmphasisCollapsed},
		DetailSectionJSON{Type: SectionSubtasks, Title: "Subtasks", Emphasis: EmphasisSecondary},
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
