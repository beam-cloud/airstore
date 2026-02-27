package hooks

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"reflect"
	"strings"
	"unicode"

	"github.com/beam-cloud/airstore/pkg/orchestration"
	"github.com/beam-cloud/airstore/pkg/repository"
	"github.com/beam-cloud/airstore/pkg/types"
)

type AgentConfigPatch struct {
	Name             *string
	Runner           *string
	Model            *string
	SystemPrompt     *string
	SystemPromptMode *string
	WorkspaceDir     *string
}

func HookAgentKey(path string) string {
	normalized := NormalizePath(path)
	trimmed := strings.Trim(normalized, "/")
	if trimmed == "" {
		trimmed = "root"
	}

	slug := slugify(trimmed)
	if slug == "" {
		slug = "hook"
	}
	if len(slug) > 36 {
		slug = strings.Trim(slug[:36], "-")
	}
	if slug == "" {
		slug = "hook"
	}

	sum := sha1.Sum([]byte(normalized))
	suffix := hex.EncodeToString(sum[:6])
	return fmt.Sprintf("hook-%s-%s", slug, suffix)
}

func ResolveHookAgent(
	ctx context.Context,
	backend repository.BackendRepository,
	workspaceID uint,
	path string,
	existingAgentID *string,
	patch *AgentConfigPatch,
) (*types.AgentProfile, error) {
	if backend == nil {
		return nil, fmt.Errorf("backend is required")
	}

	agentKey := HookAgentKey(path)
	defaultName := defaultHookAgentName(path)

	profile, err := lookupExistingHookAgent(ctx, backend, workspaceID, agentKey, existingAgentID)
	if err != nil {
		return nil, err
	}

	if profile == nil {
		name := defaultName
		if patch != nil && patch.Name != nil {
			if v := strings.TrimSpace(*patch.Name); v != "" {
				name = v
			}
		}
		config, err := normalizeHookAgentConfig(nil, agentKey, patch)
		if err != nil {
			return nil, err
		}
		profile = &types.AgentProfile{
			WorkspaceID: workspaceID,
			AgentKey:    agentKey,
			Name:        name,
			ConfigJSON:  config,
			Active:      true,
		}
		if err := backend.CreateAgentProfile(ctx, profile); err != nil {
			if !isUniqueConstraintErr(err) {
				return nil, err
			}
			// Race on key creation: fetch the winner and continue.
			existing, fetchErr := backend.GetAgentProfileByKey(ctx, workspaceID, agentKey)
			if fetchErr == nil {
				profile = existing
			} else if _, notFound := fetchErr.(*types.ErrAgentProfileNotFound); notFound {
				return nil, formatHookAgentConflictErr(err, name)
			} else {
				return nil, err
			}
		}
	}

	desiredName := strings.TrimSpace(profile.Name)
	if desiredName == "" {
		desiredName = defaultName
	}
	if patch != nil && patch.Name != nil {
		if v := strings.TrimSpace(*patch.Name); v != "" {
			desiredName = v
		} else {
			desiredName = defaultName
		}
	}

	desiredConfig, err := normalizeHookAgentConfig(profile.ConfigJSON, agentKey, patch)
	if err != nil {
		return nil, err
	}

	needsUpdate := profile.AgentKey != agentKey ||
		!profile.Active ||
		profile.Name != desiredName ||
		!reflect.DeepEqual(profile.ConfigJSON, desiredConfig)
	if needsUpdate {
		profile.AgentKey = agentKey
		profile.Name = desiredName
		profile.Active = true
		profile.ConfigJSON = desiredConfig
		if err := backend.UpdateAgentProfile(ctx, profile); err != nil {
			if isUniqueConstraintErr(err) {
				return nil, formatHookAgentConflictErr(err, desiredName)
			}
			return nil, err
		}
	}

	return profile, nil
}

func HydrateHookAgent(
	ctx context.Context,
	backend repository.BackendRepository,
	hook *types.Hook,
) {
	if backend == nil || hook == nil || hook.AgentId == nil || strings.TrimSpace(*hook.AgentId) == "" {
		return
	}

	profile, err := backend.GetAgentProfile(ctx, hook.WorkspaceId, strings.TrimSpace(*hook.AgentId))
	if err != nil || profile == nil {
		return
	}

	hook.AgentKey = profile.AgentKey
	hook.AgentName = profile.Name
	hook.AgentConfig = cloneAnyMap(profile.ConfigJSON)
}

func lookupExistingHookAgent(
	ctx context.Context,
	backend repository.BackendRepository,
	workspaceID uint,
	agentKey string,
	existingAgentID *string,
) (*types.AgentProfile, error) {
	if existingAgentID != nil && strings.TrimSpace(*existingAgentID) != "" {
		profile, err := backend.GetAgentProfile(ctx, workspaceID, strings.TrimSpace(*existingAgentID))
		if err == nil {
			return profile, nil
		}
		if _, ok := err.(*types.ErrAgentProfileNotFound); !ok {
			return nil, err
		}
	}

	profile, err := backend.GetAgentProfileByKey(ctx, workspaceID, agentKey)
	if err == nil {
		return profile, nil
	}
	if _, ok := err.(*types.ErrAgentProfileNotFound); ok {
		return nil, nil
	}
	return nil, err
}

func normalizeHookAgentConfig(
	base map[string]any,
	agentKey string,
	patch *AgentConfigPatch,
) (map[string]any, error) {
	defaults := orchestration.DefaultAgentConfig(agentKey)
	cfg := cloneAnyMap(defaults)
	for k, v := range base {
		cfg[k] = v
	}

	if patch != nil {
		applyStringPatch(cfg, "runner", patch.Runner, true)
		applyStringPatch(cfg, "model", patch.Model, false)
		applyStringPatch(cfg, "system_prompt", patch.SystemPrompt, true)
		applyStringPatch(cfg, "system_prompt_mode", patch.SystemPromptMode, false)
		applyStringPatch(cfg, "workspace_dir", patch.WorkspaceDir, true)
	}

	runner := strings.ToLower(strings.TrimSpace(anyToString(cfg["runner"])))
	provider := strings.ToLower(strings.TrimSpace(anyToString(cfg["provider"])))
	if runner == "" && provider == "" {
		runner = orchestration.AgentRunnerClaudeCode
	}
	if runner == "" {
		runner = orchestration.AgentRunnerClaudeCode
	}
	if runner != orchestration.AgentRunnerClaudeCode {
		return nil, fmt.Errorf("runner %q is not supported", runner)
	}
	if provider == "" {
		provider = orchestration.AgentProviderClaude
	}
	if provider != orchestration.AgentProviderClaude {
		return nil, fmt.Errorf("provider %q is not supported", provider)
	}
	cfg["runner"] = runner
	cfg["provider"] = provider

	for _, key := range []string{"workspace_dir", "system_prompt"} {
		if strings.TrimSpace(anyToString(cfg[key])) == "" {
			cfg[key] = defaults[key]
		}
	}

	if model := strings.TrimSpace(anyToString(cfg["model"])); model == "" {
		delete(cfg, "model")
	} else {
		cfg["model"] = model
	}
	if mode := strings.TrimSpace(anyToString(cfg["system_prompt_mode"])); mode == "" {
		delete(cfg, "system_prompt_mode")
	} else {
		cfg["system_prompt_mode"] = mode
	}

	return cfg, nil
}

func applyStringPatch(cfg map[string]any, key string, value *string, keepEmpty bool) {
	if value == nil {
		return
	}

	trimmed := strings.TrimSpace(*value)
	if trimmed == "" && !keepEmpty {
		delete(cfg, key)
		return
	}
	cfg[key] = trimmed
	if key == "runner" {
		// Provider is derived from runner in our current implementation.
		delete(cfg, "provider")
	}
}

func defaultHookAgentName(path string) string {
	normalized := NormalizePath(path)
	trimmed := strings.Trim(normalized, "/")
	if trimmed == "" {
		return "Hook"
	}
	parts := strings.Split(trimmed, "/")
	leaf := parts[len(parts)-1]
	if leaf == "" {
		leaf = trimmed
	}
	leaf = strings.ReplaceAll(leaf, "-", " ")
	leaf = strings.ReplaceAll(leaf, "_", " ")
	// Title-case the leaf: "airstore prs" -> "Airstore Prs"
	words := strings.Fields(leaf)
	for i, w := range words {
		if len(w) > 0 {
			words[i] = strings.ToUpper(w[:1]) + w[1:]
		}
	}
	name := strings.Join(words, " ")
	if name == "" {
		return "Hook"
	}
	const maxHookNameLen = 36
	runes := []rune(name)
	if len(runes) > maxHookNameLen {
		name = strings.TrimSpace(string(runes[:maxHookNameLen]))
	}
	return name
}

func isUniqueConstraintErr(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "duplicate key") ||
		strings.Contains(msg, "unique constraint") ||
		strings.Contains(msg, "violates unique")
}

func formatHookAgentConflictErr(err error, requestedName string) error {
	if err == nil {
		return nil
	}
	msg := strings.ToLower(err.Error())
	if strings.Contains(msg, "agent_profile") || strings.Contains(msg, "agent profile") {
		if strings.Contains(msg, "name") || strings.Contains(msg, "workspace_id, name") {
			trimmed := strings.TrimSpace(requestedName)
			if trimmed != "" {
				return fmt.Errorf("agent name %q is already in use in this workspace", trimmed)
			}
			return fmt.Errorf("agent name is already in use in this workspace")
		}
	}
	return fmt.Errorf("hook agent configuration conflicts with an existing agent")
}

func slugify(value string) string {
	var out []rune
	lastDash := false
	for _, r := range strings.ToLower(strings.TrimSpace(value)) {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			out = append(out, r)
			lastDash = false
			continue
		}
		if lastDash {
			continue
		}
		out = append(out, '-')
		lastDash = true
	}
	return strings.Trim(string(out), "-")
}

func cloneAnyMap(in map[string]any) map[string]any {
	if len(in) == 0 {
		return map[string]any{}
	}
	out := make(map[string]any, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func anyToString(v any) string {
	switch t := v.(type) {
	case string:
		return t
	case nil:
		return ""
	default:
		return fmt.Sprintf("%v", t)
	}
}
