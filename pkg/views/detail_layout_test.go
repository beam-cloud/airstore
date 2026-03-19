package views

import (
	"testing"

	"github.com/beam-cloud/airstore/pkg/types"
)

func TestResolveLayoutInjectsApprovalAndEmailSections(t *testing.T) {
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
			OutputType: "email",
			Status:     types.TaskOutputStatusPending,
			Metadata: map[string]any{
				types.TaskOutputMetadataBlockingKind: types.TaskOutputBlockingKindApproval,
				types.TaskOutputMetadataInputKind:    string(types.InputKindApproveReject),
				types.TaskOutputMetadataApprovalUI:   true,
			},
		},
	}

	layout := ResolveLayout(template, task, outputs, nil)

	if !hasSection(layout.Sections, SectionEmailThread) {
		t.Fatalf("expected email section in %#v", layout.Sections)
	}
	if !hasSection(layout.Sections, SectionApproval) {
		t.Fatalf("expected approval section in %#v", layout.Sections)
	}
	if hasAction(layout.Actions, "APPROVE") || hasAction(layout.Actions, "REJECT") {
		t.Fatalf("approval actions should be rendered in-section, got %#v", layout.Actions)
	}
}

func TestResolveLayoutInjectsInputSectionWithoutTemplateEntry(t *testing.T) {
	template := DetailLayoutResponse{
		Sections: []DetailSectionJSON{
			{Type: SectionDataSummary, Title: "Details", Emphasis: EmphasisSecondary},
		},
	}
	task := &types.AgentTask{
		State:     types.AgentTaskStateWaiting,
		InputKind: types.InputKindFreeText,
	}
	outputs := []*types.TaskOutput{
		{
			ID:         "out-1",
			OutputType: "text",
			Status:     types.TaskOutputStatusPending,
			Metadata: map[string]any{
				types.TaskOutputMetadataBlockingKind: types.TaskOutputBlockingKindInput,
				types.TaskOutputMetadataInputKind:    string(types.InputKindFreeText),
			},
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

func TestResolveLayoutTreatsInputKindOnlyPendingApprovalAsApproval(t *testing.T) {
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
			ID:     "out-1",
			Status: types.TaskOutputStatusPending,
			Metadata: map[string]any{
				types.TaskOutputMetadataInputKind:   string(types.InputKindApproveReject),
				types.TaskOutputMetadataWaitGroupID: "wait-1",
				types.TaskOutputMetadataApprovalUI:  1,
			},
		},
	}

	layout := ResolveLayout(template, task, outputs, nil)

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
