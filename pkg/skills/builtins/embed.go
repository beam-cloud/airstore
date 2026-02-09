package builtins

import (
	"embed"
	"fmt"
	"io/fs"
	"path/filepath"
	"strings"

	"github.com/beam-cloud/airstore/pkg/skills"
)

//go:embed email-triage slack-actions pr-reviewer issue-triage
var skillsFS embed.FS

// BuiltinSkill represents a pre-built skill that ships with airstore.
type BuiltinSkill struct {
	Name     string
	Manifest *skills.SkillManifest
	Content  string // full SKILL.md content
}

// List returns all built-in skills.
func List() ([]BuiltinSkill, error) {
	entries, err := skillsFS.ReadDir(".")
	if err != nil {
		return nil, err
	}

	var result []BuiltinSkill
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		bs, err := Get(entry.Name())
		if err != nil {
			continue
		}
		result = append(result, *bs)
	}
	return result, nil
}

// Get returns a specific built-in skill by name.
func Get(name string) (*BuiltinSkill, error) {
	data, err := skillsFS.ReadFile(filepath.Join(name, "SKILL.md"))
	if err != nil {
		return nil, fmt.Errorf("built-in skill %q not found", name)
	}

	manifest, err := skills.Parse(data)
	if err != nil {
		return nil, fmt.Errorf("invalid built-in skill %q: %w", name, err)
	}

	return &BuiltinSkill{
		Name:     name,
		Manifest: manifest,
		Content:  string(data),
	}, nil
}

// ExtractFiles returns all files for a built-in skill as a map of relative path → content.
func ExtractFiles(name string) (map[string][]byte, error) {
	files := make(map[string][]byte)

	err := fs.WalkDir(skillsFS, name, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}

		data, err := skillsFS.ReadFile(path)
		if err != nil {
			return err
		}

		// Get path relative to the skill directory
		relPath := strings.TrimPrefix(path, name+"/")
		files[relPath] = data
		return nil
	})
	if err != nil {
		return nil, err
	}

	return files, nil
}
