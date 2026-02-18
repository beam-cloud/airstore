package static

import "embed"

// Files contains the built desktop frontend.
//
//go:embed dist
var Files embed.FS
