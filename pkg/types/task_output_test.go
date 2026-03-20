package types

import "testing"

func TestTaskOutputBlockingRoundTrip(t *testing.T) {
	output := &TaskOutput{
		Metadata: map[string]any{},
	}
	output.SetBlocking(TaskOutputBlockingMetadata{
		BlockerID:       "blocker-1",
		Kind:            TaskOutputBlockingKindApproval,
		InputKind:       InputKindApproveReject,
		WaitGroupID:     "wait-1",
		ApprovalSurface: true,
	})

	decoded := output.Blocking()
	if decoded.BlockerID != "blocker-1" {
		t.Fatalf("blocker_id = %q, want blocker-1", decoded.BlockerID)
	}
	if decoded.Kind != TaskOutputBlockingKindApproval {
		t.Fatalf("kind = %q, want %q", decoded.Kind, TaskOutputBlockingKindApproval)
	}
	if decoded.InputKind != InputKindApproveReject {
		t.Fatalf("input_kind = %q, want %q", decoded.InputKind, InputKindApproveReject)
	}
	if decoded.WaitGroupID != "wait-1" {
		t.Fatalf("wait_group_id = %q, want wait-1", decoded.WaitGroupID)
	}
	if !decoded.ApprovalSurface {
		t.Fatal("expected approval_surface to round trip")
	}
}

func TestNewTaskBlockerPayloadPreservesStructuredApprovalSummary(t *testing.T) {
	payload := NewTaskBlockerPayload(
		InputKindApproveReject,
		`{"summary":"Send email","details":"Draft outreach to customer"}`,
		"Here is the draft email for approval.",
	).ToMap()

	if got := payload["summary"]; got != "Send email" {
		t.Fatalf("summary = %#v, want Send email", got)
	}
	if got := payload["details"]; got != "Draft outreach to customer" {
		t.Fatalf("details = %#v, want existing structured details", got)
	}
}

func TestNewTaskBlockerPayloadDefaultsFreeTextDetails(t *testing.T) {
	payload := NewTaskBlockerPayload(InputKindFreeText, "Need more context", "").ToMap()

	if got := payload["details"]; got != "Need more context" {
		t.Fatalf("details = %#v, want Need more context", got)
	}
	if got := payload["summary"]; got != "Need more context" {
		t.Fatalf("summary = %#v, want Need more context", got)
	}
}

func TestTaskOutputShouldHideInWorkspace(t *testing.T) {
	output := &TaskOutput{
		OutputType: TaskOutputTypeEmail,
		Status:     TaskOutputStatusActive,
		Data: map[string]any{
			"draft_id": "draft-123",
		},
	}
	if !output.ShouldHideInWorkspace() {
		t.Fatal("expected draft email artifact to be hidden in workspace")
	}
}

func TestTaskOutputIsApprovalArtifact(t *testing.T) {
	output := &TaskOutput{
		Status: TaskOutputStatusPending,
	}
	output.SetBlocking(TaskOutputBlockingMetadata{
		Kind:            TaskOutputBlockingKindApproval,
		InputKind:       InputKindApproveReject,
		ApprovalSurface: true,
	})
	if !output.IsApprovalArtifact() {
		t.Fatal("expected pending approval artifact")
	}
}
