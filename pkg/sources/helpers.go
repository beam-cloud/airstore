package sources

import (
	"encoding/json"
	"fmt"
	"syscall"
	"time"
)

// Common file modes
const (
	ModeDir  = syscall.S_IFDIR | 0755
	ModeFile = syscall.S_IFREG | 0644
	ModeLink = syscall.S_IFLNK | 0777
)

// StatusInfo represents the status.json content for an integration
type StatusInfo struct {
	Integration string `json:"integration"`
	Connected   bool   `json:"connected"`
	Scope       string `json:"scope,omitempty"` // "shared" or "personal"
	Hint        string `json:"hint,omitempty"`  // CLI hint when disconnected
	Error       string `json:"error,omitempty"`
}

// GenerateStatusJSON creates the status.json content for an integration
func GenerateStatusJSON(integration string, connected bool, scope string, workspaceId string) []byte {
	status := StatusInfo{
		Integration: integration,
		Connected:   connected,
		Scope:       scope,
	}

	if !connected {
		status.Hint = fmt.Sprintf("cli connection add %s %s --token <your-token>", workspaceId, integration)
	}

	data, _ := json.MarshalIndent(status, "", "  ")
	return append(data, '\n')
}

// GenerateErrorJSON creates a JSON error response
func GenerateErrorJSON(err error) []byte {
	data, _ := json.MarshalIndent(map[string]any{
		"error":   true,
		"message": err.Error(),
	}, "", "  ")
	return append(data, '\n')
}

// NowUnix returns the current Unix timestamp
func NowUnix() int64 {
	return time.Now().Unix()
}

// DirInfo creates FileInfo for a directory
func DirInfo() *FileInfo {
	return &FileInfo{
		Size:  0,
		Mode:  ModeDir,
		Mtime: NowUnix(),
		IsDir: true,
	}
}

// FileInfoFromBytes creates FileInfo for file content
func FileInfoFromBytes(data []byte) *FileInfo {
	return &FileInfo{
		Size:  int64(len(data)),
		Mode:  ModeFile,
		Mtime: NowUnix(),
		IsDir: false,
	}
}

// ErrNotFound is returned when a path doesn't exist
var ErrNotFound = fmt.Errorf("not found")

// ErrNotConnected is returned when the integration is not connected
var ErrNotConnected = fmt.Errorf("integration not connected")

// ErrNotDir is returned when path is not a directory
var ErrNotDir = fmt.Errorf("not a directory")

// ErrIsDir is returned when path is a directory but file was expected
var ErrIsDir = fmt.Errorf("is a directory")
