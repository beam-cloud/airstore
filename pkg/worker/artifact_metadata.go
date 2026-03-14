package worker

import (
	"encoding/json"
	"strings"

	"github.com/beam-cloud/airstore/pkg/types"
)

// defaultArtifactMetadata normalizes BAML-provided artifact values and sets
// defaults for role. BAML is the source of truth for artifact_key,
// artifact_label, and artifact_kind — this function only normalizes tokens
// and fills in the role default when the model omits it.
func defaultArtifactMetadata(metadata map[string]any, role string) map[string]any {
	m := cloneAnyMap(metadata)

	if key := anyToTrimmedString(m[types.TaskOutputMetadataArtifactKey]); key != "" {
		m[types.TaskOutputMetadataArtifactKey] = normalizeArtifactToken(key)
	}
	if kind := anyToTrimmedString(m[types.TaskOutputMetadataArtifactKind]); kind != "" {
		m[types.TaskOutputMetadataArtifactKind] = normalizeArtifactToken(kind)
	}

	if r := normalizeArtifactRole(firstNonEmptyTrimmed(anyToTrimmedString(m[types.TaskOutputMetadataArtifactRole]), role)); r != "" {
		m[types.TaskOutputMetadataArtifactRole] = r
	} else {
		m[types.TaskOutputMetadataArtifactRole] = types.TaskOutputArtifactRoleSupporting
	}

	return m
}

func normalizeArtifactToken(value string) string {
	value = strings.TrimSpace(strings.ToLower(value))
	if value == "" {
		return ""
	}
	var b strings.Builder
	lastDash := false
	for _, r := range value {
		switch {
		case (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9'):
			b.WriteRune(r)
			lastDash = false
		default:
			if !lastDash {
				b.WriteByte('-')
				lastDash = true
			}
		}
	}
	return strings.Trim(b.String(), "-")
}

func normalizeArtifactRole(value string) string {
	switch strings.TrimSpace(strings.ToLower(value)) {
	case types.TaskOutputArtifactRolePrimary:
		return types.TaskOutputArtifactRolePrimary
	case types.TaskOutputArtifactRoleSupporting:
		return types.TaskOutputArtifactRoleSupporting
	case types.TaskOutputArtifactRoleIncidental:
		return types.TaskOutputArtifactRoleIncidental
	default:
		return ""
	}
}

func cloneAnyMap(metadata map[string]any) map[string]any {
	if len(metadata) == 0 {
		return map[string]any{}
	}
	cloned := make(map[string]any, len(metadata))
	for key, value := range metadata {
		cloned[key] = value
	}
	return cloned
}

func boolFromAny(value any) (bool, bool) {
	switch typed := value.(type) {
	case bool:
		return typed, true
	case string:
		switch strings.ToLower(strings.TrimSpace(typed)) {
		case "true", "1", "yes":
			return true, true
		case "false", "0", "no":
			return false, true
		}
	}
	return false, false
}

func firstNonEmptyTrimmed(values ...string) string {
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			return trimmed
		}
	}
	return ""
}

func mapFromAny(value any) map[string]any {
	switch typed := value.(type) {
	case nil:
		return nil
	case map[string]any:
		return cloneAnyMap(typed)
	default:
		raw, err := json.Marshal(value)
		if err != nil {
			return nil
		}
		var decoded map[string]any
		if err := json.Unmarshal(raw, &decoded); err != nil {
			return nil
		}
		return decoded
	}
}
