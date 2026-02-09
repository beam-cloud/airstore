package skills

import (
	"testing"
)

func TestManifestKey(t *testing.T) {
	tests := []struct {
		name string
		want string
	}{
		{"email-triage", "skills/email-triage/SKILL.md"},
		{"test", "skills/test/SKILL.md"},
		{"my-skill", "skills/my-skill/SKILL.md"},
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
		{"/skills/email-triage", "email-triage"},
		{"/skills/test", "test"},
		{"skills/my-skill", "my-skill"}, // without leading slash
		{"/skills/foo", "foo"},

		// Invalid paths - should return empty
		{"/skills/nested/path", ""},      // nested not allowed
		{"/sources/gmail", ""},           // wrong prefix
		{"/tools/something", ""},         // wrong prefix
		{"/skills/", ""},                 // no name
		{"/skills", ""},                  // no trailing name
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
		{"email-triage", "/skills/email-triage"},
		{"test", "/skills/test"},
		{"my-skill", "/skills/my-skill"},
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
		{"skills/email-triage/SKILL.md", "email-triage"},
		{"skills/test/SKILL.md", "test"},
		{"skills/my-skill/SKILL.md", "my-skill"},

		// Invalid keys - should return empty
		{"skills/nested/path/SKILL.md", ""},   // nested directory
		{"skills/test/other.txt", ""},         // wrong filename
		{"skills/test/skill.md", ""},          // wrong case
		{"other/test/SKILL.md", ""},           // wrong prefix
		{"skills/SKILL.md", ""},               // no skill name
		{"SKILL.md", ""},                      // no path
		{"", ""},                              // empty
		{"skills/test/", ""},                  // no filename
		{"skills/test", ""},                   // no manifest
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
	if Dir != "skills" {
		t.Errorf("Dir = %q, want %q", Dir, "skills")
	}
	if ManifestFile != "SKILL.md" {
		t.Errorf("ManifestFile = %q, want %q", ManifestFile, "SKILL.md")
	}
}
