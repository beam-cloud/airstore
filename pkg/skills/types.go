package skills

// SkillManifest represents the parsed frontmatter of a SKILL.md file,
// aligned with the Agent Skills specification (https://agentskills.io/specification).
type SkillManifest struct {
	Name          string            `yaml:"name"`
	Description   string            `yaml:"description"`
	License       string            `yaml:"license,omitempty"`
	Compatibility string            `yaml:"compatibility,omitempty"`
	Metadata      map[string]any    `yaml:"metadata,omitempty"`
	AllowedTools  string            `yaml:"allowed-tools,omitempty"`
}

// AirstoreMetadata returns Airstore-specific extensions from metadata.airstore.
func (m *SkillManifest) AirstoreMetadata() *AirstoreSkillMeta {
	if m.Metadata == nil {
		return &AirstoreSkillMeta{}
	}
	raw, ok := m.Metadata["airstore"]
	if !ok {
		return &AirstoreSkillMeta{}
	}
	asMap, ok := raw.(map[string]any)
	if !ok {
		return &AirstoreSkillMeta{}
	}

	meta := &AirstoreSkillMeta{}

	if needs, ok := asMap["needs"]; ok {
		if needsList, ok := needs.([]any); ok {
			for _, n := range needsList {
				if s, ok := n.(string); ok {
					meta.Needs = append(meta.Needs, s)
				}
			}
		}
	}

	if writes, ok := asMap["writes"]; ok {
		if writesList, ok := writes.([]any); ok {
			for _, w := range writesList {
				if s, ok := w.(string); ok {
					meta.Writes = append(meta.Writes, s)
				}
			}
		}
	}

	return meta
}

// AirstoreSkillMeta holds Airstore-specific metadata extensions.
type AirstoreSkillMeta struct {
	Needs  []string // integration names (gmail, slack, github, etc.)
	Writes []string // output paths (created on install)
}

// InstalledSkill holds runtime info about an installed skill.
type InstalledSkill struct {
	Name        string   `json:"name"`
	Description string   `json:"description"`
	Needs       []string `json:"needs"`
	Source      string   `json:"source"` // where the skill was installed from (airstore://, github.com/, or local path)
}
