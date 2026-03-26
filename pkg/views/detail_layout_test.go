package views

import (
	"testing"

	"github.com/beam-cloud/airstore/pkg/types"
)

func TestResolveLayoutNeverIncludesApprovalSection(t *testing.T) {
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

	layout := ResolveProjectedLayout(template, ProjectDetail(task, outputs, nil))

	if hasSection(layout.Sections, SectionApproval) {
		t.Fatalf("approval section should never appear in row detail, got %#v", layout.Sections)
	}
	if hasSection(layout.Sections, SectionInputForm) {
		t.Fatalf("input section should never appear in row detail, got %#v", layout.Sections)
	}
}

func TestResolveLayoutKeepsHistoricalEmailSectionWithoutApproval(t *testing.T) {
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

	layout := ResolveProjectedLayout(template, ProjectDetail(task, outputs, nil))

	if hasSection(layout.Sections, SectionApproval) {
		t.Fatalf("approval section should never appear in row detail, got %#v", layout.Sections)
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

	if projection.Surface != DetailSurfaceConversation {
		t.Fatalf("projection surface = %q, want %q", projection.Surface, DetailSurfaceConversation)
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

func TestResolveLayoutNeverIncludesInputSection(t *testing.T) {
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

	layout := ResolveProjectedLayout(template, ProjectDetail(task, outputs, nil))

	if hasSection(layout.Sections, SectionInputForm) {
		t.Fatalf("input section should never appear in row detail, got %#v", layout.Sections)
	}
	if hasSection(layout.Sections, SectionApproval) {
		t.Fatalf("approval section should never appear in row detail, got %#v", layout.Sections)
	}
}

func TestResolveLayoutDropsTemplateInputSectionWhenBlockerPresent(t *testing.T) {
	template := DetailLayoutResponse{
		Sections: []DetailSectionJSON{
			{Type: SectionDataSummary, Title: "Email Details", Emphasis: EmphasisSecondary},
			{Type: SectionInputForm, Title: "Additional Instructions", Emphasis: EmphasisSecondary},
			{Type: SectionOutputGallery, Title: "Outputs", Emphasis: EmphasisCollapsed},
		},
	}
	task := &types.AgentTask{
		State: types.AgentTaskStateWaiting,
		CurrentBlocker: &types.TaskBlocker{
			ID:        "blocker-1",
			Kind:      types.TaskBlockerKindInput,
			InputKind: types.InputKindFreeText,
			Status:    types.TaskBlockerStatusOpen,
		},
	}

	layout := ResolveProjectedLayout(template, ProjectDetail(task, nil, nil))

	if hasSection(layout.Sections, SectionInputForm) {
		t.Fatalf("input section should never appear in row detail, got %#v", layout.Sections)
	}
	if !hasSection(layout.Sections, SectionDataSummary) {
		t.Fatalf("expected data summary section in %#v", layout.Sections)
	}
}

func TestResolveLayoutDropsTemplateApprovalSectionWhenBlockerPresent(t *testing.T) {
	template := DetailLayoutResponse{
		Sections: []DetailSectionJSON{
			{Type: SectionDataSummary, Title: "Email Details", Emphasis: EmphasisSecondary},
			{Type: SectionApproval, Title: "Approval Gate", Emphasis: EmphasisCollapsed},
			{Type: SectionEmailThread, Title: "Conversation", Emphasis: EmphasisSecondary},
		},
	}
	task := &types.AgentTask{
		State: types.AgentTaskStateWaiting,
		CurrentBlocker: &types.TaskBlocker{
			ID:        "blocker-1",
			Kind:      types.TaskBlockerKindApproval,
			InputKind: types.InputKindApproveReject,
			Status:    types.TaskBlockerStatusOpen,
		},
	}
	outputs := []*types.TaskOutput{
		{
			ID:         "out-1",
			OutputType: types.TaskOutputTypeEmail,
			Status:     types.TaskOutputStatusActive,
			Data: map[string]any{
				"thread_id": "thread-1",
			},
		},
	}

	layout := ResolveProjectedLayout(template, ProjectDetail(task, outputs, nil))

	if hasSection(layout.Sections, SectionApproval) {
		t.Fatalf("approval section should never appear in row detail, got %#v", layout.Sections)
	}
	if !hasSection(layout.Sections, SectionDataSummary) {
		t.Fatalf("expected data summary section in %#v", layout.Sections)
	}
	if !hasSection(layout.Sections, SectionEmailThread) {
		t.Fatalf("expected email thread section in %#v", layout.Sections)
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

	layout := ResolveProjectedLayout(template, ProjectDetail(task, outputs, nil))

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

func TestResolveLayoutOmitsApprovalEvenWithCurrentTaskBlocker(t *testing.T) {
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

	layout := ResolveProjectedLayout(template, ProjectDetail(task, nil, nil))

	if hasSection(layout.Sections, SectionApproval) {
		t.Fatalf("approval section should never appear in row detail, got %#v", layout.Sections)
	}
	if hasSection(layout.Sections, SectionInputForm) {
		t.Fatalf("input section should never appear in row detail, got %#v", layout.Sections)
	}
}

func TestResolveLayoutDropsUnknownSections(t *testing.T) {
	template := DetailLayoutResponse{
		Sections: []DetailSectionJSON{
			{Type: "UNKNOWN_SECTION", Title: "Unknown", Emphasis: EmphasisSecondary},
			{Type: SectionDataSummary, Title: "Details", Emphasis: EmphasisSecondary},
		},
	}

	layout := ResolveProjectedLayout(template, ProjectDetail(nil, nil, nil))

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
