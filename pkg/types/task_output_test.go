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

func TestTaskOutputShouldHideInWorkspaceSupportingRole(t *testing.T) {
	output := &TaskOutput{
		OutputType: "json",
		Status:     TaskOutputStatusActive,
		Metadata: map[string]any{
			TaskOutputMetadataArtifactRole: TaskOutputArtifactRoleSupporting,
			TaskOutputMetadataArtifactKind: "config",
		},
	}
	if !output.ShouldHideInWorkspace() {
		t.Fatal("expected supporting-role output to be hidden in workspace inbox")
	}
}

func TestTaskOutputShouldHideInWorkspaceIncidentalRole(t *testing.T) {
	output := &TaskOutput{
		OutputType: "json",
		Status:     TaskOutputStatusActive,
		Metadata: map[string]any{
			TaskOutputMetadataArtifactRole: TaskOutputArtifactRoleIncidental,
		},
	}
	if !output.ShouldHideInWorkspace() {
		t.Fatal("expected incidental-role output to be hidden in workspace inbox")
	}
}

func TestTaskOutputShouldShowInWorkspacePrimaryRole(t *testing.T) {
	output := &TaskOutput{
		OutputType: "json",
		Status:     TaskOutputStatusActive,
		Metadata: map[string]any{
			TaskOutputMetadataArtifactRole: TaskOutputArtifactRolePrimary,
		},
	}
	if output.ShouldHideInWorkspace() {
		t.Fatal("expected primary-role output to be visible in workspace inbox")
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

func TestCanonicalArtifactLifecycleKeyStripsLifecyclePhase(t *testing.T) {
	cases := []struct {
		key  string
		want string
	}{
		{"approval-report", "report"},
		{"blocked-email", "email"},
		{"email-sent", "email"},
		{"sales-email", "sales-email"},
		{"", ""},
	}
	for _, tc := range cases {
		if got := CanonicalArtifactLifecycleKey(tc.key); got != tc.want {
			t.Fatalf("CanonicalArtifactLifecycleKey(%q) = %q, want %q", tc.key, got, tc.want)
		}
	}
}

func TestCanonicalArtifactFamilyKeyPrefersBaseKindWhenKeyWrapsIt(t *testing.T) {
	cases := []struct {
		key, kind, outputType string
		want                  string
	}{
		{"sales-email", "email", "email", "email"},
		{"approval-report", "report", "text", "report"},
		{"drive-link", "drive-link", "link", "drive-link"},
		{"", "json", "json", "json"},
	}
	for _, tc := range cases {
		if got := CanonicalArtifactFamilyKey(tc.key, tc.kind, tc.outputType); got != tc.want {
			t.Fatalf(
				"CanonicalArtifactFamilyKey(%q, %q, %q) = %q, want %q",
				tc.key, tc.kind, tc.outputType, got, tc.want,
			)
		}
	}
}
