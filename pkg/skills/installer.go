package skills

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

const (
	Dir               = "skills"          // S3 prefix (no leading slash)
	ManifestFile      = "SKILL.md"        // manifest filename
	InstalledMetaFile = ".installed.json" // install state file
)

// ManifestKey returns the S3 key for a skill's manifest: "skills/{name}/SKILL.md"
func ManifestKey(name string) string {
	return Dir + "/" + name + "/" + ManifestFile
}

// PathToName extracts skill name from an airstore path like "/skills/email-triage".
func PathToName(path string) string {
	path = strings.TrimPrefix(path, "/")
	if !strings.HasPrefix(path, Dir+"/") {
		return ""
	}
	name := strings.TrimPrefix(path, Dir+"/")
	if strings.Contains(name, "/") {
		return "" // nested paths not allowed
	}
	return name
}

// NameToPath converts a skill name to airstore path: "/skills/{name}"
func NameToPath(name string) string {
	return "/" + Dir + "/" + name
}

// KeyToName extracts skill name from S3 key like "skills/email-triage/SKILL.md".
func KeyToName(key string) string {
	if !strings.HasSuffix(key, "/"+ManifestFile) {
		return ""
	}
	dir := strings.TrimSuffix(key, "/"+ManifestFile)
	if !strings.HasPrefix(dir, Dir+"/") {
		return ""
	}
	name := strings.TrimPrefix(dir, Dir+"/")
	if strings.Contains(name, "/") {
		return ""
	}
	return name
}

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
