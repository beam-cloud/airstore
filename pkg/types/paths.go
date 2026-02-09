package types

import "strings"

// Path validation helpers for hooks and other features.
// Path constants are defined in virtualfile.go (PathSkills, PathSources, etc.)

// SystemPaths returns all system root paths that cannot have hooks attached.
func SystemPaths() []string {
	return []string{PathTasks, PathTools, PathSkills, PathSources, PathMemory}
}

// IsSystemRootPath returns true if the path is a system root directory.
func IsSystemRootPath(p string) bool {
	p = strings.TrimSuffix(p, "/")
	for _, sys := range SystemPaths() {
		if p == sys {
			return true
		}
	}
	return false
}

// IsRootLevelSource returns true if path is a root-level source like /sources/gmail
// but not a nested path like /sources/gmail/query.
func IsRootLevelSource(p string) bool {
	p = strings.TrimSuffix(p, "/")
	if !strings.HasPrefix(p, PathSources+"/") {
		return false
	}
	rest := strings.TrimPrefix(p, PathSources+"/")
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
