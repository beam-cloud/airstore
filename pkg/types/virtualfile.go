package types

import (
	"path"
	"time"
)

// VirtualFileType represents the type of virtual file in the filesystem.
// This determines UI behavior, available operations, and icon presentation.
type VirtualFileType string

const (
	// VFTypeContext represents user-uploaded context files (S3-backed)
	VFTypeContext VirtualFileType = "context"
	// VFTypeSource represents integration sources (github, gmail, etc)
	VFTypeSource VirtualFileType = "source"
	// VFTypeTool represents available tools
	VFTypeTool VirtualFileType = "tool"
	// VFTypeRoot represents virtual root directories
	VFTypeRoot VirtualFileType = "root"

	// Future types can be added here:
	// VFTypeAgent VirtualFileType = "agent"
	// VFTypeOutput VirtualFileType = "output"
)

// Root directory paths for the virtual filesystem
const (
	// PathRoot is the filesystem root
	PathRoot = "/"
	// PathContext is the context files root directory
	PathContext = "/context"
	// PathSources is the sources/integrations root directory
	PathSources = "/sources"
	// PathTools is the tools root directory
	PathTools = "/tools"
)

// Root directory names (without leading slash)
const (
	DirNameContext = "context"
	DirNameSources = "sources"
	DirNameTools   = "tools"
)

// Metadata keys for VirtualFile.Metadata
const (
	MetaKeyProvider   = "provider"
	MetaKeyExternalID = "external_id"
	MetaKeyGuidance   = "guidance"
	MetaKeyHidden     = "hidden"
)

// JoinPath safely joins path segments, ensuring clean paths
func JoinPath(segments ...string) string {
	return path.Clean(path.Join(segments...))
}

// SourcePath returns a path within the sources directory
func SourcePath(subpath string) string {
	if subpath == "" {
		return PathSources
	}
	return JoinPath(PathSources, subpath)
}

// ContextPath returns a path within the context directory
func ContextPath(subpath string) string {
	if subpath == "" {
		return PathContext
	}
	return JoinPath(PathContext, subpath)
}

// ToolsPath returns a path within the tools directory
func ToolsPath(subpath string) string {
	if subpath == "" {
		return PathTools
	}
	return JoinPath(PathTools, subpath)
}

// VirtualFile represents a file or folder in the virtual filesystem.
// It contains base metadata plus a Type field that determines available
// operations and UI presentation.
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

// VirtualFileListResponse is the response for listing directory contents
type VirtualFileListResponse struct {
	Path    string        `json:"path"`
	Entries []VirtualFile `json:"entries"`
}

// VirtualFileTreeResponse is the response for listing a directory tree
type VirtualFileTreeResponse struct {
	Path    string        `json:"path"`
	Depth   int           `json:"depth"`
	Entries []VirtualFile `json:"entries"`
}

// NewVirtualFile creates a new VirtualFile with the given parameters
func NewVirtualFile(id, name, path string, fileType VirtualFileType) *VirtualFile {
	return &VirtualFile{
		ID:       id,
		Name:     name,
		Path:     path,
		Type:     fileType,
		Metadata: make(map[string]interface{}),
	}
}

// NewRootFolder creates a virtual root folder
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

// WithFolder marks the file as a folder
func (f *VirtualFile) WithFolder(isFolder bool) *VirtualFile {
	f.IsFolder = isFolder
	return f
}

// WithReadOnly marks the file as read-only
func (f *VirtualFile) WithReadOnly(readOnly bool) *VirtualFile {
	f.IsReadOnly = readOnly
	return f
}

// WithSize sets the file size
func (f *VirtualFile) WithSize(size int64) *VirtualFile {
	f.Size = size
	return f
}

// WithModifiedAt sets the modification time
func (f *VirtualFile) WithModifiedAt(t time.Time) *VirtualFile {
	f.ModifiedAt = &t
	return f
}

// WithChildCount sets the child count for folders
func (f *VirtualFile) WithChildCount(count int) *VirtualFile {
	f.ChildCount = count
	return f
}

// WithMetadata adds metadata to the file
func (f *VirtualFile) WithMetadata(key string, value interface{}) *VirtualFile {
	if f.Metadata == nil {
		f.Metadata = make(map[string]interface{})
	}
	f.Metadata[key] = value
	return f
}
