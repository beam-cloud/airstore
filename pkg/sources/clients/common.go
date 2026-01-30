package clients

import (
	"encoding/json"
	"regexp"
	"strings"
)

// Common helper functions for API clients

// getString extracts a string value from a map
func getString(m map[string]any, key string) string {
	if v, ok := m[key].(string); ok {
		return v
	}
	return ""
}

// getInt64 extracts an int64 value from a map (handles both int and float64)
func getInt64(m map[string]any, key string) int64 {
	switch v := m[key].(type) {
	case int64:
		return v
	case float64:
		return int64(v)
	case int:
		return int64(v)
	case string:
		var n int64
		if _, err := json.Number(v).Int64(); err == nil {
			return n
		}
	}
	return 0
}

// UnsafeFilenameChars matches characters unsafe for filenames
var UnsafeFilenameChars = regexp.MustCompile(`[<>:"/\\|?*\x00-\x1f]`)

// SanitizeFilename makes a filename safe for filesystem use
func SanitizeFilename(name string) string {
	name = UnsafeFilenameChars.ReplaceAllString(name, "_")
	name = strings.TrimSpace(name)
	if name == "" {
		name = "unnamed"
	}
	if len(name) > 200 {
		name = name[:200]
	}
	return name
}

// SanitizeFolderName makes a folder name safe with shorter limit
func SanitizeFolderName(name string) string {
	name = UnsafeFilenameChars.ReplaceAllString(name, "_")
	name = strings.TrimSpace(name)
	if name == "" {
		name = "unknown"
	}
	if len(name) > 100 {
		name = name[:100]
	}
	return name
}

// JSONMarshalIndent marshals to indented JSON
func JSONMarshalIndent(v any) ([]byte, error) {
	return json.MarshalIndent(v, "", "  ")
}

// JSONMarshal marshals to JSON
func JSONMarshal(v any) ([]byte, error) {
	return json.Marshal(v)
}
