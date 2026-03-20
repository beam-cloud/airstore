package views

import (
	"testing"

	"github.com/beam-cloud/airstore/pkg/types"
)

func TestResolveLayoutSuppressesEmailSectionForCurrentApprovalDraft(t *testing.T) {
	template := DetailLayoutResponse{
		Sections: []DetailSectionJSON{
			{Type: SectionDataSummary, Title: "Details", Emphasis: EmphasisSecondary},
		},
	}
	task := &types.AgentTask{
		State: types.AgentTaskStateWaiting,
		CurrentBlocker: &types.TaskBlocker{
			ID:        "blocker-1",
			Kind:      types.TaskBlockerKindApproval,
			InputKind: types.InputKindApproveReject,
			Status:    types.TaskBlockerStatusOpen,
			OutputIDs: []string{"out-1"},
			PayloadJSON: map[string]any{
				"summary": "Approve this outreach draft",
			},
		},
	}
	outputs := []*types.TaskOutput{
		{
			ID:         "out-1",
			OutputType: types.TaskOutputTypeEmail,
			Status:     types.TaskOutputStatusPending,
		},
	}

	layout := ResolveLayout(template, task, outputs, nil)

	if !hasSection(layout.Sections, SectionApproval) {
		t.Fatalf("expected approval section in %#v", layout.Sections)
	}
	if hasSection(layout.Sections, SectionEmailThread) {
		t.Fatalf("did not expect email section in %#v", layout.Sections)
	}
	if hasAction(layout.Actions, "APPROVE") || hasAction(layout.Actions, "REJECT") {
		t.Fatalf("approval actions should be rendered in-section, got %#v", layout.Actions)
	}
}

func TestResolveLayoutKeepsHistoricalEmailSectionAlongsideCurrentApproval(t *testing.T) {
	template := DetailLayoutResponse{
		Sections: []DetailSectionJSON{
			{Type: SectionDataSummary, Title: "Details", Emphasis: EmphasisSecondary},
		},
	}
	task := &types.AgentTask{
		State: types.AgentTaskStateWaiting,
		CurrentBlocker: &types.TaskBlocker{
			ID:        "blocker-1",
			Kind:      types.TaskBlockerKindApproval,
			InputKind: types.InputKindApproveReject,
			Status:    types.TaskBlockerStatusOpen,
			OutputIDs: []string{"out-approval"},
		},
	}
	outputs := []*types.TaskOutput{
		{
			ID:         "out-history",
			OutputType: types.TaskOutputTypeEmail,
			Status:     types.TaskOutputStatusActive,
			Data: map[string]any{
				"thread_id": "thread-1",
			},
		},
		{
			ID:         "out-approval",
			OutputType: types.TaskOutputTypeEmail,
			Status:     types.TaskOutputStatusPending,
		},
	}

	layout := ResolveLayout(template, task, outputs, nil)

	if !hasSection(layout.Sections, SectionApproval) {
		t.Fatalf("expected approval section in %#v", layout.Sections)
	}
	if !hasSection(layout.Sections, SectionEmailThread) {
		t.Fatalf("expected historical email section in %#v", layout.Sections)
	}
}

func TestProjectDetailScopesOutputsToCurrentSurface(t *testing.T) {
	task := &types.AgentTask{
		State: types.AgentTaskStateWaiting,
		CurrentBlocker: &types.TaskBlocker{
			ID:        "blocker-1",
			Kind:      types.TaskBlockerKindApproval,
			InputKind: types.InputKindApproveReject,
			Status:    types.TaskBlockerStatusOpen,
			OutputIDs: []string{"out-approval"},
		},
	}
	outputs := []*types.TaskOutput{
		{
			ID:         "out-history",
			OutputType: types.TaskOutputTypeEmail,
			Status:     types.TaskOutputStatusActive,
			Data: map[string]any{
				"thread_id": "thread-1",
			},
		},
		{
			ID:         "out-approval",
			OutputType: types.TaskOutputTypeEmail,
			Status:     types.TaskOutputStatusPending,
		},
		{
			ID:         "out-report",
			OutputType: "text",
			Status:     types.TaskOutputStatusActive,
		},
		{
			ID:         "out-cancelled",
			OutputType: "text",
			Status:     types.TaskOutputStatusCancelled,
		},
	}

	projection := ProjectDetail(task, outputs, nil)

	if projection.Surface != DetailSurfaceApproval {
		t.Fatalf("projection surface = %q, want %q", projection.Surface, DetailSurfaceApproval)
	}
	if got, want := len(projection.Outputs), 2; got != want {
		t.Fatalf("projection output count = %d, want %d", got, want)
	}
	if projection.Outputs[0].ID != "out-approval" {
		t.Fatalf("first projection output = %q, want out-approval", projection.Outputs[0].ID)
	}
	if projection.Outputs[1].ID != "out-report" {
		t.Fatalf("second projection output = %q, want out-report", projection.Outputs[1].ID)
	}
	if got, want := len(projection.ThreadOutputs), 1; got != want {
		t.Fatalf("thread output count = %d, want %d", got, want)
	}
	if projection.ThreadOutputs[0].ID != "out-history" {
		t.Fatalf("thread output id = %q, want out-history", projection.ThreadOutputs[0].ID)
	}
	if got, want := len(projection.GalleryOutputs), 1; got != want {
		t.Fatalf("gallery output count = %d, want %d", got, want)
	}
	if projection.GalleryOutputs[0].ID != "out-report" {
		t.Fatalf("gallery output id = %q, want out-report", projection.GalleryOutputs[0].ID)
	}
}

func TestResolveLayoutInjectsInputSectionWithoutTemplateEntry(t *testing.T) {
	template := DetailLayoutResponse{
		Sections: []DetailSectionJSON{
			{Type: SectionDataSummary, Title: "Details", Emphasis: EmphasisSecondary},
		},
	}
	task := &types.AgentTask{
		State: types.AgentTaskStateWaiting,
		CurrentBlocker: &types.TaskBlocker{
			ID:        "blocker-1",
			Kind:      types.TaskBlockerKindInput,
			InputKind: types.InputKindFreeText,
			Status:    types.TaskBlockerStatusOpen,
			OutputIDs: []string{"out-1"},
		},
	}
	outputs := []*types.TaskOutput{
		{
			ID:         "out-1",
			OutputType: "text",
			Status:     types.TaskOutputStatusPending,
		},
	}

	layout := ResolveLayout(template, task, outputs, nil)

	if !hasSection(layout.Sections, SectionInputForm) {
		t.Fatalf("expected input section in %#v", layout.Sections)
	}
	if hasSection(layout.Sections, SectionApproval) {
		t.Fatalf("did not expect approval section in %#v", layout.Sections)
	}
}

func TestResolveLayoutDoesNotInferApprovalWithoutExplicitBlocker(t *testing.T) {
	template := DetailLayoutResponse{
		Sections: []DetailSectionJSON{
			{Type: SectionDataSummary, Title: "Details", Emphasis: EmphasisSecondary},
		},
	}
	task := &types.AgentTask{
		State:     types.AgentTaskStateWaiting,
		InputKind: types.InputKindApproveReject,
	}
	outputs := []*types.TaskOutput{
		{
			ID:         "out-1",
			OutputType: types.TaskOutputTypeEmail,
			Status:     types.TaskOutputStatusPending,
			Metadata: map[string]any{
				types.TaskOutputMetadataInputKind:   string(types.InputKindApproveReject),
				types.TaskOutputMetadataWaitGroupID: "wait-1",
				types.TaskOutputMetadataApprovalUI:  true,
			},
		},
	}

	layout := ResolveLayout(template, task, outputs, nil)

	if hasSection(layout.Sections, SectionApproval) {
		t.Fatalf("did not expect approval section in %#v", layout.Sections)
	}
	if hasSection(layout.Sections, SectionInputForm) {
		t.Fatalf("did not expect input section in %#v", layout.Sections)
	}
	if !hasSection(layout.Sections, SectionEmailThread) {
		t.Fatalf("expected email section in %#v", layout.Sections)
	}
}

func TestResolveLayoutUsesCurrentTaskBlockerWithoutOutputs(t *testing.T) {
	template := DetailLayoutResponse{
		Sections: []DetailSectionJSON{
			{Type: SectionDataSummary, Title: "Details", Emphasis: EmphasisSecondary},
		},
	}
	task := &types.AgentTask{
		State: types.AgentTaskStateWaiting,
		CurrentBlocker: &types.TaskBlocker{
			Kind:      types.TaskBlockerKindApproval,
			InputKind: types.InputKindApproveReject,
			Status:    types.TaskBlockerStatusOpen,
		},
	}

	layout := ResolveLayout(template, task, nil, nil)

	if !hasSection(layout.Sections, SectionApproval) {
		t.Fatalf("expected approval section in %#v", layout.Sections)
	}
	if hasSection(layout.Sections, SectionInputForm) {
		t.Fatalf("did not expect input section in %#v", layout.Sections)
	}
}

func TestResolveLayoutDropsUnknownSections(t *testing.T) {
	template := DetailLayoutResponse{
		Sections: []DetailSectionJSON{
			{Type: "UNKNOWN_SECTION", Title: "Unknown", Emphasis: EmphasisSecondary},
			{Type: SectionDataSummary, Title: "Details", Emphasis: EmphasisSecondary},
		},
	}

	layout := ResolveLayout(template, nil, nil, nil)

	if hasSection(layout.Sections, "UNKNOWN_SECTION") {
		t.Fatalf("unexpected unknown section in %#v", layout.Sections)
	}
	if !hasSection(layout.Sections, SectionDataSummary) {
		t.Fatalf("expected details section in %#v", layout.Sections)
	}
}

func hasSection(sections []DetailSectionJSON, sectionType string) bool {
	for _, section := range sections {
		if section.Type == sectionType {
			return true
		}
	}
	return false
}

func hasAction(actions []ActionSpecJSON, actionType string) bool {
	for _, action := range actions {
		if action.Type == actionType {
			return true
		}
	}
	return false
}
