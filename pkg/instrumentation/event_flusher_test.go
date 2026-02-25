package instrumentation

import "testing"

func TestAccessWorkspaceStreamNameDefaultsSessionToWorkspace(t *testing.T) {
	got := AccessWorkspaceStreamName("ws-123", "")
	want := "access.ws-123.ws-123.events"
	if got != want {
		t.Fatalf("expected %q, got %q", want, got)
	}
}

func TestAccessWorkspaceStreamNameRoundTrip(t *testing.T) {
	workspaceID := "ws-abc"
	sessionID := "custom.session.v1"
	stream := AccessWorkspaceStreamName(workspaceID, sessionID)

	if got := SessionIDFromWorkspaceStreamName(stream, workspaceID); got != sessionID {
		t.Fatalf("expected session %q, got %q", sessionID, got)
	}
	if got := SessionIDFromWorkspaceStreamName(stream, "other-workspace"); got != "" {
		t.Fatalf("expected empty session for wrong workspace, got %q", got)
	}
}

func TestAccessWorkspaceStreamNameFallsBackToLegacy(t *testing.T) {
	got := AccessWorkspaceStreamName("", "legacy-session")
	want := AccessStreamName("legacy-session")
	if got != want {
		t.Fatalf("expected legacy stream %q, got %q", want, got)
	}
}

func TestSessionIDFromWorkspaceStreamNameIgnoresLegacyWorkspaceStream(t *testing.T) {
	workspaceID := "c1b6bd1e-12dc-43e4-af2c-664f61b7f094"
	legacyWorkspaceStream := AccessStreamName(workspaceID) // access.{workspace}.events

	if got := SessionIDFromWorkspaceStreamName(legacyWorkspaceStream, workspaceID); got != "" {
		t.Fatalf("expected empty session for legacy workspace stream, got %q", got)
	}
}

func TestSessionIDFromWorkspaceStreamNameIgnoresEmptySessionSegment(t *testing.T) {
	workspaceID := "ws-123"
	malformed := "access.ws-123..events"

	if got := SessionIDFromWorkspaceStreamName(malformed, workspaceID); got != "" {
		t.Fatalf("expected empty session for malformed stream, got %q", got)
	}
}
