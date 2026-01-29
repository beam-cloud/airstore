package providers

import (
	"context"
	"regexp"
	"strings"
	"time"

	"github.com/beam-cloud/airstore/pkg/index"
)

// Common helper functions for index providers

// getString extracts a string value from a map
func getString(m map[string]any, key string) string {
	if v, ok := m[key].(string); ok {
		return v
	}
	return ""
}

// unsafeFilenameChars matches characters that are unsafe in filenames
var unsafeFilenameChars = regexp.MustCompile(`[<>:"/\\|?*\x00-\x1f]`)

// sanitizeFilename makes a filename safe for the filesystem
func sanitizeFilename(name string) string {
	name = unsafeFilenameChars.ReplaceAllString(name, "_")
	name = strings.TrimSpace(name)
	if name == "" {
		name = "unnamed"
	}
	if len(name) > 200 {
		name = name[:200]
	}
	return name
}

// ensureParentDirs creates virtual directory entries for all parent paths.
// This enables directory listing to work from the index.
func ensureParentDirs(ctx context.Context, store index.IndexStore, integration, path string) {
	parts := strings.Split(path, "/")

	// Create entries for each parent directory
	for i := 1; i < len(parts); i++ {
		dirPath := strings.Join(parts[:i], "/")
		parentPath := ""
		if i > 1 {
			parentPath = strings.Join(parts[:i-1], "/")
		}
		dirName := parts[i-1]

		// Use a stable entity ID for this virtual directory
		entityID := "vdir:" + dirPath

		entry := &index.IndexEntry{
			Integration: integration,
			EntityID:    entityID,
			Path:        dirPath,
			ParentPath:  parentPath,
			Name:        dirName,
			Type:        index.EntryTypeDir,
			Size:        0,
			ModTime:     time.Now(),
		}

		// Upsert - this is idempotent
		store.Upsert(ctx, entry)
	}
}
