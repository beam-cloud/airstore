package types

import "strings"

// Path validation helpers for hooks and other features.
// Path constants are defined in virtualfile.go (PathSkills, PathSources, etc.)

// SystemPaths returns all system root paths that cannot have hooks attached.
func SystemPaths() []string {
	return []string{PathTasks, PathTools, PathSkills, PathSources, PathMemory}
}

// IsSystemRootPath returns true if the path is a system root directory.
// Comparison is case-insensitive to handle paths from different sources.
func IsSystemRootPath(p string) bool {
	p = strings.TrimSuffix(p, "/")
	pLower := strings.ToLower(p)
	for _, sys := range SystemPaths() {
		if pLower == strings.ToLower(sys) {
			return true
		}
	}
	return false
}

// IsRootLevelSource returns true if path is a root-level source like /Sources/gmail
// but not a nested path like /Sources/gmail/query.
// Comparison is case-insensitive to handle paths from different sources.
func IsRootLevelSource(p string) bool {
	p = strings.TrimSuffix(p, "/")
	pLower := strings.ToLower(p)
	sourcesPrefix := strings.ToLower(PathSources) + "/"
	if !strings.HasPrefix(pLower, sourcesPrefix) {
		return false
	}
	rest := pLower[len(sourcesPrefix):]
	// Root-level source has no slash in the rest (e.g., "gmail")
	// Nested has at least one slash (e.g., "gmail/my-query")
	return rest != "" && !strings.Contains(rest, "/")
}

// IsHookablePath returns true if a path can have a hook attached.
// This is a basic check; the frontend also validates via external_id.
func IsHookablePath(p string) bool {
	if IsSystemRootPath(p) {
		return false
	}
	if IsRootLevelSource(p) {
		return false
	}
	return true
}

// IsHiddenDotPath returns true when any path segment starts with ".".
// Examples: "/.claude/settings.json", "skills/.cache/index.json".
func IsHiddenDotPath(p string) bool {
	p = strings.TrimSpace(strings.TrimPrefix(p, "/"))
	if p == "" {
		return false
	}

	for _, segment := range strings.Split(p, "/") {
		if segment == "" || segment == "." || segment == ".." {
			continue
		}
		if strings.HasPrefix(segment, ".") {
			return true
		}
	}

	return false
}
