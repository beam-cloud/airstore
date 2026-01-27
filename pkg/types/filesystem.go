package types

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"time"
)

// DirMeta holds metadata for a directory
type DirMeta struct {
	Path     string    `json:"path"`
	Mode     uint32    `json:"mode"`
	Uid      uint32    `json:"uid"`
	Gid      uint32    `json:"gid"`
	Mtime    time.Time `json:"mtime"`
	Children []string  `json:"children"` // Entry names in this directory
}

// FileMeta holds metadata for a file or symlink
type FileMeta struct {
	Path    string    `json:"path"`
	Size    int64     `json:"size"`
	Mode    uint32    `json:"mode"`
	Uid     uint32    `json:"uid"`
	Gid     uint32    `json:"gid"`
	Mtime   time.Time `json:"mtime"`
	S3Key   string    `json:"s3_key,omitempty"`  // Object storage key (for context files)
	Symlink string    `json:"symlink,omitempty"` // Target path if symlink (empty otherwise)
}

// IsSymlink returns true if this file metadata represents a symlink
func (f *FileMeta) IsSymlink() bool {
	return f.Symlink != ""
}

// DirEntry represents an entry in a directory listing
type DirEntry struct {
	Name   string `json:"name"`
	Mode   uint32 `json:"mode"`
	IsLink bool   `json:"is_link,omitempty"`
}

// IsDir returns true if this entry is a directory
func (e *DirEntry) IsDir() bool {
	return e.Mode&0040000 != 0 // S_IFDIR
}

// GeneratePathID generates a unique ID for a path (for Redis keys)
func GeneratePathID(path string) string {
	hash := sha256.Sum256([]byte(path))
	return hex.EncodeToString(hash[:16]) // 32 hex chars
}

// GenerateS3Key generates an S3 object key for a context file
func GenerateS3Key(workspace, path string) string {
	return fmt.Sprintf("context/%s%s", workspace, path)
}
