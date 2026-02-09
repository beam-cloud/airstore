package types

import (
	"testing"
)

func TestIsSystemRootPath(t *testing.T) {
	tests := []struct {
		path string
		want bool
	}{
		// System roots - should return true
		{"/Tasks", true},
		{"/Tasks/", true},
		{"/Tools", true},
		{"/Tools/", true},
		{"/Skills", true},
		{"/Skills/", true},
		{"/Sources", true},
		{"/Sources/", true},

		// Not system roots - should return false
		{"/Tasks/something", false},
		{"/Tools/my-tool", false},
		{"/Skills/my-skill", false},
		{"/Sources/gmail", false},
		{"/my-query", false},
		{"/", false},
		{"", false},
	}

	for _, tt := range tests {
		got := IsSystemRootPath(tt.path)
		if got != tt.want {
			t.Errorf("IsSystemRootPath(%q) = %v, want %v", tt.path, got, tt.want)
		}
	}
}

func TestIsRootLevelSource(t *testing.T) {
	tests := []struct {
		path string
		want bool
	}{
		// Root-level sources - should return true
		{"/Sources/gmail", true},
		{"/Sources/gmail/", true},
		{"/Sources/github", true},
		{"/Sources/gdrive", true},

		// Not root-level sources - should return false
		{"/Sources", false},
		{"/Sources/", false},
		{"/Sources/gmail/inbox", false},
		{"/Sources/gmail/my-query", false},
		{"/Sources/gdrive/folder/subfolder", false},
		{"/Tools/something", false},
		{"/my-query", false},
		{"", false},
	}

	for _, tt := range tests {
		got := IsRootLevelSource(tt.path)
		if got != tt.want {
			t.Errorf("IsRootLevelSource(%q) = %v, want %v", tt.path, got, tt.want)
		}
	}
}

func TestIsHookablePath(t *testing.T) {
	tests := []struct {
		path string
		want bool
	}{
		// Not hookable - system roots
		{"/Tasks", false},
		{"/Tools", false},
		{"/Skills", false},
		{"/Sources", false},

		// Not hookable - root-level sources
		{"/Sources/gmail", false},
		{"/Sources/github", false},

		// Hookable - smart query folders under sources
		{"/Sources/gmail/inbox", true},
		{"/Sources/gmail/new unread emails", true},
		{"/Sources/gdrive/my-folder", true},

		// Hookable - top-level queries
		{"/my-emails", true},
		{"/invoices", true},
	}

	for _, tt := range tests {
		got := IsHookablePath(tt.path)
		if got != tt.want {
			t.Errorf("IsHookablePath(%q) = %v, want %v", tt.path, got, tt.want)
		}
	}
}

func TestSystemPaths(t *testing.T) {
	paths := SystemPaths()
	if len(paths) != 4 {
		t.Errorf("SystemPaths() returned %d paths, want 4", len(paths))
	}

	expected := map[string]bool{
		"/Tasks":   true,
		"/Tools":   true,
		"/Skills":  true,
		"/Sources": true,
	}

	for _, p := range paths {
		if !expected[p] {
			t.Errorf("Unexpected path in SystemPaths(): %q", p)
		}
	}
}

func TestPathConstants(t *testing.T) {
	// Verify constants are what we expect (defined in virtualfile.go)
	if PathTools != "/Tools" {
		t.Errorf("PathTools = %q, want %q", PathTools, "/Tools")
	}
	if PathSkills != "/Skills" {
		t.Errorf("PathSkills = %q, want %q", PathSkills, "/Skills")
	}
	if PathTasks != "/Tasks" {
		t.Errorf("PathTasks = %q, want %q", PathTasks, "/Tasks")
	}
	if PathSources != "/Sources" {
		t.Errorf("PathSources = %q, want %q", PathSources, "/Sources")
	}
	if PathMemory != "/Memory" {
		t.Errorf("PathMemory = %q, want %q", PathMemory, "/Memory")
	}
}
