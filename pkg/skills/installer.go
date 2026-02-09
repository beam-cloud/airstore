package skills

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

const (
	// InstalledMetaFile is written into a skill's directory to track install state.
	InstalledMetaFile = ".installed.json"
)

// ReadInstalledMeta loads the installed skill metadata from a directory.
func ReadInstalledMeta(skillDir string) (*InstalledSkill, error) {
	data, err := os.ReadFile(filepath.Join(skillDir, InstalledMetaFile))
	if err != nil {
		return nil, err
	}
	var meta InstalledSkill
	if err := json.Unmarshal(data, &meta); err != nil {
		return nil, fmt.Errorf("invalid %s: %w", InstalledMetaFile, err)
	}
	return &meta, nil
}

// WriteInstalledMeta saves installed skill metadata to a directory.
func WriteInstalledMeta(skillDir string, meta *InstalledSkill) error {
	data, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(skillDir, InstalledMetaFile), data, 0644)
}

// BuildPrompt constructs the task prompt from a SKILL.md file's instructions.
// The prompt includes the skill name and the full instruction body so the
// agent has complete context when the hook triggers.
func BuildPrompt(manifest *SkillManifest, skillContent string) string {
	instructions := ExtractInstructions([]byte(skillContent))
	if instructions == "" {
		return fmt.Sprintf("Run the %q skill.", manifest.Name)
	}
	return instructions
}

// SkillNameFromPath derives a skill name from a directory path.
// e.g., "./email-triage/" → "email-triage"
func SkillNameFromPath(path string) string {
	path = strings.TrimSuffix(filepath.Clean(path), "/")
	return filepath.Base(path)
}

// FindSkillMD looks for a SKILL.md file in a directory.
func FindSkillMD(dir string) (string, error) {
	path := filepath.Join(dir, "SKILL.md")
	if _, err := os.Stat(path); err != nil {
		return "", fmt.Errorf("no SKILL.md found in %s", dir)
	}
	return path, nil
}
