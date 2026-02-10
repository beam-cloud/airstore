package types

import (
	"testing"
)

func TestWorkspaceBucketName(t *testing.T) {
	tests := []struct {
		prefix      string
		workspaceId string
		want        string
	}{
		{"airstore", "abc123", "airstore-abc123"},
		{"airstore", "ABC123", "airstore-abc123"}, // lowercased
		{"MyPrefix", "WS-001", "myprefix-ws-001"}, // both parts lowercased
		{"", "test", "-test"},                      // empty prefix
		{"prefix", "", "prefix-"},                  // empty workspace
	}

	for _, tt := range tests {
		got := WorkspaceBucketName(tt.prefix, tt.workspaceId)
		if got != tt.want {
			t.Errorf("WorkspaceBucketName(%q, %q) = %q, want %q",
				tt.prefix, tt.workspaceId, got, tt.want)
		}
	}
}

func TestWorkspaceToolSettings(t *testing.T) {
	settings := NewWorkspaceToolSettings(1)

	// All tools enabled by default
	if !settings.IsEnabled("some-tool") {
		t.Error("Expected tool to be enabled by default")
	}
	if settings.IsDisabled("some-tool") {
		t.Error("Expected tool to not be disabled by default")
	}

	// Disable a tool
	settings.SetEnabled("my-tool", false)
	if settings.IsEnabled("my-tool") {
		t.Error("Expected tool to be disabled after SetEnabled(false)")
	}
	if !settings.IsDisabled("my-tool") {
		t.Error("Expected IsDisabled to return true")
	}

	// Re-enable
	settings.SetEnabled("my-tool", true)
	if !settings.IsEnabled("my-tool") {
		t.Error("Expected tool to be enabled after SetEnabled(true)")
	}
}

func TestErrWorkspaceNotFound(t *testing.T) {
	err := &ErrWorkspaceNotFound{ExternalId: "abc-123"}
	if err.Error() != "workspace not found: abc-123" {
		t.Errorf("Unexpected error message: %s", err.Error())
	}

	err = &ErrWorkspaceNotFound{Name: "my-workspace"}
	if err.Error() != "workspace not found: my-workspace" {
		t.Errorf("Unexpected error message: %s", err.Error())
	}
}
