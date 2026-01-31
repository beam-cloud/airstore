package types

import (
	"path"
	"time"
)

// VirtualFileType determines UI behavior and available operations
type VirtualFileType string

const (
	VFTypeStorage VirtualFileType = "storage" // S3-backed files
	VFTypeSource  VirtualFileType = "source"  // Integration sources (github, gmail, etc)
	VFTypeTool    VirtualFileType = "tool"    // Available tools
	VFTypeRoot    VirtualFileType = "root"    // Virtual root directories
)

// Directory names and paths
const (
	DirNameSkills  = "skills"
	DirNameSources = "sources"
	DirNameTools   = "tools"
	DirNameTasks   = "tasks"

	PathRoot    = "/"
	PathSkills  = "/skills"
	PathSources = "/sources"
	PathTools   = "/tools"
	PathTasks   = "/tasks"
)

// Source integration files
const (
	SourceStatusFile = "README.md" // Status and description file at integration root
)

// Reserved folders cannot be deleted. Virtual folders are not S3-backed.
var (
	ReservedFolders = map[string]struct{}{
		DirNameSkills:  {},
		DirNameSources: {},
		DirNameTools:   {},
		DirNameTasks:   {},
	}

	VirtualFolders = map[string]struct{}{
		DirNameSources: {},
		DirNameTools:   {},
		DirNameTasks:   {},
	}
)

func IsReservedFolder(name string) bool {
	_, ok := ReservedFolders[name]
	return ok
}

func IsVirtualFolder(name string) bool {
	_, ok := VirtualFolders[name]
	return ok
}

// Metadata keys
const (
	MetaKeyProvider   = "provider"
	MetaKeyExternalID = "external_id"
	MetaKeyGuidance   = "guidance"
	MetaKeyHidden     = "hidden"
)

// Path helpers
func JoinPath(segments ...string) string {
	return path.Clean(path.Join(segments...))
}

func SourcePath(subpath string) string {
	if subpath == "" {
		return PathSources
	}
	return JoinPath(PathSources, subpath)
}

func SkillsPath(subpath string) string {
	if subpath == "" {
		return PathSkills
	}
	return JoinPath(PathSkills, subpath)
}

func ToolsPath(subpath string) string {
	if subpath == "" {
		return PathTools
	}
	return JoinPath(PathTools, subpath)
}

// VirtualFile represents a file or folder in the virtual filesystem
type VirtualFile struct {
	ID         string                 `json:"id"`
	Name       string                 `json:"name"`
	Path       string                 `json:"path"`
	Type       VirtualFileType        `json:"type"`
	IsFolder   bool                   `json:"is_folder"`
	IsSymlink  bool                   `json:"is_symlink,omitempty"`
	IsReadOnly bool                   `json:"is_readonly,omitempty"`
	Size       int64                  `json:"size,omitempty"`
	ModifiedAt *time.Time             `json:"modified_at,omitempty"`
	ChildCount int                    `json:"child_count,omitempty"`
	Metadata   map[string]interface{} `json:"metadata,omitempty"`
}

type VirtualFileListResponse struct {
	Path    string        `json:"path"`
	Entries []VirtualFile `json:"entries"`
}

type VirtualFileTreeResponse struct {
	Path    string        `json:"path"`
	Depth   int           `json:"depth"`
	Entries []VirtualFile `json:"entries"`
}

func NewVirtualFile(id, name, path string, fileType VirtualFileType) *VirtualFile {
	return &VirtualFile{
		ID:       id,
		Name:     name,
		Path:     path,
		Type:     fileType,
		Metadata: make(map[string]interface{}),
	}
}

func NewRootFolder(name, path string) *VirtualFile {
	return &VirtualFile{
		ID:       "root-" + name,
		Name:     name,
		Path:     path,
		Type:     VFTypeRoot,
		IsFolder: true,
		Metadata: make(map[string]interface{}),
	}
}

// Builder methods for fluent construction
func (f *VirtualFile) WithFolder(v bool) *VirtualFile   { f.IsFolder = v; return f }
func (f *VirtualFile) WithReadOnly(v bool) *VirtualFile { f.IsReadOnly = v; return f }
func (f *VirtualFile) WithSize(v int64) *VirtualFile    { f.Size = v; return f }
func (f *VirtualFile) WithChildCount(v int) *VirtualFile { f.ChildCount = v; return f }

func (f *VirtualFile) WithModifiedAt(t time.Time) *VirtualFile {
	f.ModifiedAt = &t
	return f
}

func (f *VirtualFile) WithMetadata(key string, value interface{}) *VirtualFile {
	if f.Metadata == nil {
		f.Metadata = make(map[string]interface{})
	}
	f.Metadata[key] = value
	return f
}
