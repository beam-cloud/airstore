package worker

import (
	"os"
	"sort"
	"strings"

	"github.com/beam-cloud/airstore/pkg/types"
)

var (
	requiredFilesystemRoots   = buildRequiredFilesystemRoots()
	requiredFilesystemRootSet = buildRequiredFilesystemRootSet(requiredFilesystemRoots)
)

func buildRequiredFilesystemRoots() []string {
	roots := make([]string, 0, len(types.SystemPaths()))
	for _, path := range types.SystemPaths() {
		root := strings.TrimPrefix(strings.ToLower(path), "/")
		if root != "" {
			roots = append(roots, root)
		}
	}
	sort.Strings(roots)
	return roots
}

func buildRequiredFilesystemRootSet(roots []string) map[string]struct{} {
	out := make(map[string]struct{}, len(roots))
	for _, root := range roots {
		out[root] = struct{}{}
	}
	return out
}

// checkFilesystemMountReady validates that the mount path contains all expected
// system root directories (/memory, /skills, /sources, /tasks, /tools).
func checkFilesystemMountReady(mountPath string) (bool, []string, error) {
	entries, err := os.ReadDir(mountPath)
	if err != nil {
		return false, nil, err
	}

	present := make(map[string]struct{}, len(entries))
	for _, entry := range entries {
		name := strings.ToLower(strings.TrimSpace(entry.Name()))
		if name == "" {
			continue
		}

		if _, tracked := requiredFilesystemRootSet[name]; !tracked {
			continue
		}
		if entry.IsDir() {
			present[name] = struct{}{}
		}
	}

	missing := make([]string, 0)
	for _, root := range requiredFilesystemRoots {
		if _, ok := present[root]; !ok {
			missing = append(missing, root)
		}
	}
	return len(missing) == 0, missing, nil
}
