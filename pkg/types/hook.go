package types

import (
	"strings"
	"time"
)

// Hook is metadata on a filesystem path that creates tasks when something
// changes at that path. A hook on "/skills" fires when files are created
// or modified there. A hook on "/sources/gmail/inbox" fires when new
// query results appear. The event type (create, modify, source change)
// is passed as context in the task prompt -- not as a filter.
type Hook struct {
	Id                uint           `json:"id" db:"id"`
	ExternalId        string         `json:"external_id" db:"external_id"`
	WorkspaceId       uint           `json:"workspace_id" db:"workspace_id"`
	Path              string         `json:"path" db:"path"`
	Prompt            string         `json:"prompt" db:"prompt"`
	SkillPath         string         `json:"skill_path" db:"skill_path"`
	SkillPaths        []string       `json:"skill_paths,omitempty" db:"skill_paths"`
	AgentId           *string        `json:"agent_id,omitempty" db:"agent_id"`
	AgentKey          string         `json:"agent_key,omitempty" db:"-"`
	AgentName         string         `json:"agent_name,omitempty" db:"-"`
	AgentConfig       map[string]any `json:"agent_config,omitempty" db:"-"`
	Active            bool           `json:"active" db:"active"`
	EventTypes        []string       `json:"event_types,omitempty" db:"event_types"`
	CreatedByMemberId *uint          `json:"created_by_member_id,omitempty" db:"created_by_member_id"`
	TokenId           *uint          `json:"-" db:"token_id"`
	EncryptedToken    []byte         `json:"-" db:"encrypted_token"`
	CreatedAt         time.Time      `json:"created_at" db:"created_at"`
	UpdatedAt         time.Time      `json:"updated_at" db:"updated_at"`
}

func (h *Hook) NormalizeSkills() {
	normalized := normalizeSkillPaths(h.SkillPaths)
	if len(normalized) == 0 {
		legacy := strings.TrimSpace(h.SkillPath)
		if legacy != "" {
			normalized = []string{legacy}
		}
	}

	if normalized == nil {
		normalized = []string{}
	}
	h.SkillPaths = normalized
	if len(normalized) > 0 {
		h.SkillPath = normalized[0]
	} else {
		h.SkillPath = ""
	}
}

func NormalizeSkillPaths(paths []string, legacy string) []string {
	normalized := normalizeSkillPaths(paths)
	if len(normalized) > 0 {
		return normalized
	}
	legacy = strings.TrimSpace(legacy)
	if legacy == "" {
		return []string{}
	}
	return []string{legacy}
}

func normalizeSkillPaths(paths []string) []string {
	seen := make(map[string]struct{}, len(paths))
	out := make([]string, 0, len(paths))
	for _, raw := range paths {
		path := strings.TrimSpace(raw)
		if path == "" {
			continue
		}
		if _, exists := seen[path]; exists {
			continue
		}
		seen[path] = struct{}{}
		out = append(out, path)
	}
	return out
}

// ErrHookNotFound is returned when a hook cannot be found.
type ErrHookNotFound struct {
	ExternalId string
}

func (e *ErrHookNotFound) Error() string {
	return "hook not found: " + e.ExternalId
}
