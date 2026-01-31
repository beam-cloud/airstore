package sources

import (
	"encoding/json"
	"fmt"
	"strings"
	"syscall"
	"time"
	"unicode"

	"github.com/beam-cloud/airstore/pkg/types"
)

// Common file modes
const (
	ModeDir  = syscall.S_IFDIR | 0755
	ModeFile = syscall.S_IFREG | 0644
	ModeLink = syscall.S_IFLNK | 0777
)

// SourceStatus represents the status information for an integration
type SourceStatus struct {
	Integration string
	DisplayName string
	Description string
	Connected   bool
	Scope       string // "shared" or "personal"
	Hint        string // CLI hint when disconnected
}

// oauthIntegrations lists integrations that use OAuth
var oauthIntegrations = map[string]bool{
	"gmail":  true,
	"gdrive": true,
	"github": true,
	"notion": true,
	"slack":  true,
}

// GenerateSourceReadme creates the README.md content for an integration
func GenerateSourceReadme(integration string, connected bool, scope string, workspaceId string) []byte {
	status := SourceStatus{
		Integration: integration,
		Connected:   connected,
		Scope:       scope,
	}

	// Get integration metadata for display name and description
	if meta, ok := types.GetIntegrationMeta(types.ToolName(integration)); ok {
		status.DisplayName = meta.DisplayName
		status.Description = meta.Description
	} else {
		// Fallback: capitalize the integration name
		status.DisplayName = strings.Title(integration)
		status.Description = fmt.Sprintf("%s integration", status.DisplayName)
	}

	// Generate CLI hint when disconnected
	if !connected {
		if oauthIntegrations[integration] {
			// OAuth integrations use 'connection connect' for browser-based auth
			status.Hint = fmt.Sprintf("beta9 connection connect %s", integration)
		} else {
			// Token/API-key integrations use 'connection add'
			status.Hint = fmt.Sprintf("beta9 connection add %s %s --token <your-token>", workspaceId, integration)
		}
	}

	return generateReadmeMarkdown(status)
}

// generateReadmeMarkdown creates the markdown content for the README
func generateReadmeMarkdown(status SourceStatus) []byte {
	var b strings.Builder

	// Title
	b.WriteString("# ")
	b.WriteString(status.DisplayName)
	b.WriteString("\n\n")

	// Description
	b.WriteString(status.Description)
	b.WriteString("\n\n")

	// Status section
	b.WriteString("## Status\n\n")
	b.WriteString("| Key | Value |\n")
	b.WriteString("|-----|-------|\n")

	// Connected status
	connectedStr := "No"
	if status.Connected {
		connectedStr = "Yes"
	}
	b.WriteString("| Connected | ")
	b.WriteString(connectedStr)
	b.WriteString(" |\n")

	// Scope (only show if connected)
	if status.Connected && status.Scope != "" {
		b.WriteString("| Scope | ")
		b.WriteString(status.Scope)
		b.WriteString(" |\n")
	}

	b.WriteString("\n")

	// Getting started or connect hint
	if status.Connected {
		b.WriteString("## Getting Started\n\n")
		b.WriteString("Create smart queries to access your data:\n\n")
		b.WriteString("```bash\n")
		b.WriteString(fmt.Sprintf("mkdir /sources/%s/my-query\n", status.Integration))
		b.WriteString(fmt.Sprintf("ls /sources/%s/my-query/\n", status.Integration))
		b.WriteString("```\n")
	} else {
		b.WriteString("## Connect\n\n")
		b.WriteString("This integration is not connected. To connect, run:\n\n")
		b.WriteString("```bash\n")
		b.WriteString(status.Hint)
		b.WriteString("\n```\n")
	}

	return []byte(b.String())
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

// SanitizeFilename makes a string safe for use as a filename.
// It removes emojis, non-ASCII characters, and filesystem-unsafe characters,
// keeping only alphanumeric, underscores, hyphens, and dots.
// This is the canonical sanitization function for all providers.
func SanitizeFilename(s string) string {
	if s == "" {
		return "_unknown_"
	}

	var result strings.Builder
	result.Grow(len(s))

	prevUnderscore := false
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z',
			r >= 'A' && r <= 'Z',
			r >= '0' && r <= '9':
			// Keep alphanumeric ASCII
			result.WriteRune(r)
			prevUnderscore = false

		case r == '-' || r == '.':
			// Keep hyphens and dots as-is
			result.WriteRune(r)
			prevUnderscore = false

		case r == '_':
			// Keep underscores, but prevent doubles
			if !prevUnderscore {
				result.WriteRune('_')
				prevUnderscore = true
			}

		case r == ' ' || r == '\t' || r == '\n' || r == '\r':
			// Convert whitespace to underscore
			if !prevUnderscore {
				result.WriteRune('_')
				prevUnderscore = true
			}

		case unicode.IsLetter(r):
			// Non-ASCII letters: try to keep if they're "safe" Latin-like characters
			// For now, replace with underscore to ensure ASCII-safe filenames
			if !prevUnderscore {
				result.WriteRune('_')
				prevUnderscore = true
			}

		default:
			// Drop everything else: emojis, symbols, control chars, etc.
			// But add underscore to mark the gap (prevents word collision)
			if !prevUnderscore && result.Len() > 0 {
				result.WriteRune('_')
				prevUnderscore = true
			}
		}
	}

	s = result.String()

	// Trim leading/trailing underscores and dots
	s = strings.Trim(s, "_.")

	// Final safety check
	if s == "" {
		return "_unknown_"
	}

	return s
}
