package definitions

import "embed"

// FS contains the embedded tool definition YAML files
//
//go:embed *.yaml
var FS embed.FS
