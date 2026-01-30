package vnode

// Reserved paths in the virtual filesystem
const (
	// ToolsPath is the virtual directory containing tool binaries
	ToolsPath = "/tools"

	// ToolsPathPrefix is the prefix for tool binary paths (with trailing slash)
	ToolsPathPrefix = "/tools/"

	// ContextPath is the virtual directory for context/workspace files
	ContextPath = "/context"

	// ContextPathPrefix is the prefix for context paths (with trailing slash)
	ContextPathPrefix = "/context/"

	// ConfigDir is the virtual directory for airstore configuration
	ConfigDir = "/.airstore"

	// ConfigFile is the virtual file containing gateway settings
	ConfigFile = "/.airstore/config"

	// SourcesPath is the virtual directory for integration sources
	SourcesPath = "/sources"

	// SourcesPathPrefix is the prefix for sources paths (with trailing slash)
	SourcesPathPrefix = "/sources/"
)
