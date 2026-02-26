package vnode

import "github.com/beam-cloud/airstore/pkg/types"

// Re-export path constants from types for backward compatibility.
// New code should import from types directly.
const (
	ToolsPath         = types.PathTools
	ToolsPathPrefix   = types.PathTools + "/"
	SkillsPath        = types.PathSkills
	SkillsPathPrefix  = types.PathSkills + "/"
	TasksPath         = types.PathTasks
	TasksPathPrefix   = types.PathTasks + "/"
	SourcesPath       = types.PathSources
	SourcesPathPrefix = types.PathSources + "/"
	ConfigDir         = "/.airstore"
	ConfigFile        = "/.airstore/config"
	ConfigToolShim    = "/.airstore/tool-shim"
)
