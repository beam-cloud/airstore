package apiv1

import (
	"testing"

	"github.com/beam-cloud/airstore/pkg/instrumentation"
)

func TestNormalizeAccessSession(t *testing.T) {
	if got := normalizeAccessSession("ws-1", ""); got != "ws-1" {
		t.Fatalf("expected workspace default session, got %q", got)
	}
	if got := normalizeAccessSession("ws-1", "  "); got != "ws-1" {
		t.Fatalf("expected trimmed empty session to default, got %q", got)
	}
	if got := normalizeAccessSession("ws-1", "custom"); got != "custom" {
		t.Fatalf("expected explicit session, got %q", got)
	}
}

func TestAccessEventInScope(t *testing.T) {
	workspaceID := "ws-1"
	sessionID := "custom"

	if accessEventInScope(instrumentation.AccessEvent{}, workspaceID, sessionID) {
		t.Fatalf("expected empty event to be rejected")
	}
	if accessEventInScope(instrumentation.AccessEvent{WorkspaceID: "ws-2", SessionID: sessionID}, workspaceID, sessionID) {
		t.Fatalf("expected wrong workspace event to be rejected")
	}
	if !accessEventInScope(instrumentation.AccessEvent{WorkspaceID: workspaceID, SessionID: sessionID}, workspaceID, sessionID) {
		t.Fatalf("expected matching workspace/session event to be accepted")
	}
	if !accessEventInScope(instrumentation.AccessEvent{WorkspaceID: workspaceID}, workspaceID, workspaceID) {
		t.Fatalf("expected empty session_id to default to workspace session")
	}
}
