package skills

// SkillManifest represents the parsed frontmatter of a SKILL.md file.
type SkillManifest struct {
	Name        string         `yaml:"name"`
	Description string         `yaml:"description"`
	Needs       []string       `yaml:"needs"`    // integration names (gmail, slack, github, etc.)
	Triggers    []SkillTrigger `yaml:"triggers"` // when the skill auto-runs
	Writes      []string       `yaml:"writes"`   // output paths (created on install)
}

// SkillTrigger defines when a skill should auto-run.
type SkillTrigger struct {
	On   string `yaml:"on"`   // event type: source.change, fs.create, fs.write, fs.delete
	Path string `yaml:"path"` // filesystem path to watch
}

// InstalledSkill holds runtime info about an installed skill.
type InstalledSkill struct {
	Name        string   `json:"name"`
	Description string   `json:"description"`
	Needs       []string `json:"needs"`
	HookIds     []string `json:"hook_ids"` // external IDs of hooks created by this skill
}
