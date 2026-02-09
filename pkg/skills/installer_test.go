package skills

import (
	"testing"
)

func TestManifestKey(t *testing.T) {
	tests := []struct {
		name string
		want string
	}{
		{"email-triage", "Skills/email-triage/SKILL.md"},
		{"test", "Skills/test/SKILL.md"},
		{"my-skill", "Skills/my-skill/SKILL.md"},
	}

	for _, tt := range tests {
		got := ManifestKey(tt.name)
		if got != tt.want {
			t.Errorf("ManifestKey(%q) = %q, want %q", tt.name, got, tt.want)
		}
	}
}

func TestPathToName(t *testing.T) {
	tests := []struct {
		path string
		want string
	}{
		// Valid paths
		{"/Skills/email-triage", "email-triage"},
		{"/Skills/test", "test"},
		{"Skills/my-skill", "my-skill"}, // without leading slash
		{"/Skills/foo", "foo"},

		// Invalid paths - should return empty
		{"/Skills/nested/path", ""},      // nested not allowed
		{"/Sources/gmail", ""},           // wrong prefix
		{"/Tools/something", ""},         // wrong prefix
		{"/Skills/", ""},                 // no name
		{"/Skills", ""},                  // no trailing name
		{"random", ""},                   // no prefix
		{"", ""},                         // empty
	}

	for _, tt := range tests {
		got := PathToName(tt.path)
		if got != tt.want {
			t.Errorf("PathToName(%q) = %q, want %q", tt.path, got, tt.want)
		}
	}
}

func TestNameToPath(t *testing.T) {
	tests := []struct {
		name string
		want string
	}{
		{"email-triage", "/Skills/email-triage"},
		{"test", "/Skills/test"},
		{"my-skill", "/Skills/my-skill"},
	}

	for _, tt := range tests {
		got := NameToPath(tt.name)
		if got != tt.want {
			t.Errorf("NameToPath(%q) = %q, want %q", tt.name, got, tt.want)
		}
	}
}

func TestKeyToName(t *testing.T) {
	tests := []struct {
		key  string
		want string
	}{
		// Valid keys
		{"Skills/email-triage/SKILL.md", "email-triage"},
		{"Skills/test/SKILL.md", "test"},
		{"Skills/my-skill/SKILL.md", "my-skill"},

		// Invalid keys - should return empty
		{"Skills/nested/path/SKILL.md", ""},   // nested directory
		{"Skills/test/other.txt", ""},         // wrong filename
		{"Skills/test/skill.md", ""},          // wrong case
		{"other/test/SKILL.md", ""},           // wrong prefix
		{"Skills/SKILL.md", ""},               // no skill name
		{"SKILL.md", ""},                      // no path
		{"", ""},                              // empty
		{"Skills/test/", ""},                  // no filename
		{"Skills/test", ""},                   // no manifest
	}

	for _, tt := range tests {
		got := KeyToName(tt.key)
		if got != tt.want {
			t.Errorf("KeyToName(%q) = %q, want %q", tt.key, got, tt.want)
		}
	}
}

func TestSkillNameFromPath(t *testing.T) {
	tests := []struct {
		path string
		want string
	}{
		{"./email-triage/", "email-triage"},
		{"./email-triage", "email-triage"},
		{"/absolute/path/my-skill", "my-skill"},
		{"relative/path/test-skill/", "test-skill"},
		{"simple", "simple"},
	}

	for _, tt := range tests {
		got := SkillNameFromPath(tt.path)
		if got != tt.want {
			t.Errorf("SkillNameFromPath(%q) = %q, want %q", tt.path, got, tt.want)
		}
	}
}

// TestRoundTrip verifies that name -> path -> name and name -> key -> name are consistent
func TestRoundTrip(t *testing.T) {
	names := []string{"email-triage", "test", "my-skill", "foo123"}

	for _, name := range names {
		// Name -> Path -> Name
		path := NameToPath(name)
		gotName := PathToName(path)
		if gotName != name {
			t.Errorf("Path roundtrip failed: NameToPath(%q) = %q, PathToName(%q) = %q",
				name, path, path, gotName)
		}

		// Name -> Key -> Name
		key := ManifestKey(name)
		gotName = KeyToName(key)
		if gotName != name {
			t.Errorf("Key roundtrip failed: ManifestKey(%q) = %q, KeyToName(%q) = %q",
				name, key, key, gotName)
		}
	}
}

// TestConstants verifies the constants are what we expect
func TestConstants(t *testing.T) {
	if Dir != "Skills" {
		t.Errorf("Dir = %q, want %q", Dir, "Skills")
	}
	if ManifestFile != "SKILL.md" {
		t.Errorf("ManifestFile = %q, want %q", ManifestFile, "SKILL.md")
	}
}
