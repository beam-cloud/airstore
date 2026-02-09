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
		{"/tasks", true},
		{"/tasks/", true},
		{"/tools", true},
		{"/tools/", true},
		{"/skills", true},
		{"/skills/", true},
		{"/sources", true},
		{"/sources/", true},

		// Not system roots - should return false
		{"/tasks/something", false},
		{"/tools/my-tool", false},
		{"/skills/my-skill", false},
		{"/sources/gmail", false},
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
		{"/sources/gmail", true},
		{"/sources/gmail/", true},
		{"/sources/github", true},
		{"/sources/gdrive", true},

		// Not root-level sources - should return false
		{"/sources", false},
		{"/sources/", false},
		{"/sources/gmail/inbox", false},
		{"/sources/gmail/my-query", false},
		{"/sources/gdrive/folder/subfolder", false},
		{"/tools/something", false},
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
		{"/tasks", false},
		{"/tools", false},
		{"/skills", false},
		{"/sources", false},

		// Not hookable - root-level sources
		{"/sources/gmail", false},
		{"/sources/github", false},

		// Hookable - smart query folders under sources
		{"/sources/gmail/inbox", true},
		{"/sources/gmail/new unread emails", true},
		{"/sources/gdrive/my-folder", true},

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
		"/tasks":   true,
		"/tools":   true,
		"/skills":  true,
		"/sources": true,
	}

	for _, p := range paths {
		if !expected[p] {
			t.Errorf("Unexpected path in SystemPaths(): %q", p)
		}
	}
}

func TestPathConstants(t *testing.T) {
	// Verify constants are what we expect (defined in virtualfile.go)
	if PathTools != "/tools" {
		t.Errorf("PathTools = %q, want %q", PathTools, "/tools")
	}
	if PathSkills != "/skills" {
		t.Errorf("PathSkills = %q, want %q", PathSkills, "/skills")
	}
	if PathTasks != "/tasks" {
		t.Errorf("PathTasks = %q, want %q", PathTasks, "/tasks")
	}
	if PathSources != "/sources" {
		t.Errorf("PathSources = %q, want %q", PathSources, "/sources")
	}
}
