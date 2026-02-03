package vnode

import (
	"path/filepath"
	"strings"
)

// isAppleDoublePath returns true when the path targets a macOS AppleDouble file.
// These files store extended attributes/resource forks as "._<name>".
func isAppleDoublePath(path string) bool {
	name := filepath.Base(path)
	return strings.HasPrefix(name, "._")
}
