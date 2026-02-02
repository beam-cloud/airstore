package sources

import (
	"context"
	"errors"
	"sync"

	"github.com/beam-cloud/airstore/pkg/types"
)

// ErrSearchNotSupported is returned when a provider doesn't support search
var ErrSearchNotSupported = errors.New("search not supported")

// FileInfo contains file/directory metadata
type FileInfo struct {
	Size   int64
	Mode   uint32
	Mtime  int64 // Unix timestamp
	IsDir  bool
	IsLink bool
}

// DirEntry represents a directory entry
type DirEntry struct {
	Name  string
	Mode  uint32
	IsDir bool
	Size  int64
	Mtime int64
}

// SearchResult represents a single search result from a provider
type SearchResult struct {
	Name    string // filename for the result (e.g., "2026-01-28_invoice_abc123.txt")
	Id      string // internal ID for fetching content (e.g., message ID, file ID)
	Mode    uint32 // file mode
	Size    int64  // file size if known, 0 otherwise
	Mtime   int64  // modification time (Unix timestamp)
	Preview string // optional preview/snippet of content
}

// ProviderContext contains workspace and credential info for provider operations
type ProviderContext struct {
	WorkspaceId uint
	MemberId    uint
	Credentials *types.IntegrationCredentials
}

// QuerySpec contains the parsed query specification from a FilesystemQuery.
type QuerySpec struct {
	Query          string            // Provider-specific query string (e.g., "is:unread", "mimeType='application/pdf'")
	Limit          int               // Page size - number of results per page (default 50)
	MaxResults     int               // Total cap on results across all pages (default 500)
	FilenameFormat string            // Format template for generating filenames (e.g., "{date}_{subject}_{id}.txt")
	PageToken      string            // Pagination token for fetching subsequent pages
	Metadata       map[string]string // Additional provider-specific metadata (e.g., content_type for GitHub)
}

// QueryResult represents a single result from executing a filesystem query.
// This is used by QueryExecutor to return search results with metadata.
type QueryResult struct {
	ID       string            // Provider-specific ID (e.g., Gmail message ID, GDrive file ID)
	Filename string            // Generated filename using FilenameFormat
	Metadata map[string]string // Key-value metadata (date, subject, from, etc.)
	Size     int64             // Content size in bytes (0 if unknown)
	Mtime    int64             // Last modified time (Unix timestamp)
}

// QueryResponse wraps query results with pagination metadata.
// Providers return this to indicate whether more results are available.
type QueryResponse struct {
	Results       []QueryResult // Results for this page
	NextPageToken string        // Token for fetching the next page (empty if no more pages)
	HasMore       bool          // True if there are more results beyond this page
}

// QueryExecutor is an optional interface implemented by providers that support
// filesystem query operations. This enables the smart query filesystem feature
// where users create virtual folders/files that execute queries on access.
type QueryExecutor interface {
	// ExecuteQuery runs a query and returns results with pagination metadata.
	// The spec contains the provider-specific query string, filename format,
	// and optional PageToken for fetching subsequent pages.
	// Returns a QueryResponse containing results and pagination info.
	ExecuteQuery(ctx context.Context, pctx *ProviderContext, spec QuerySpec) (*QueryResponse, error)

	// ReadResult fetches content for a specific result by its provider ID.
	// This is called when a user reads a file from a smart query folder.
	ReadResult(ctx context.Context, pctx *ProviderContext, resultID string) ([]byte, error)

	// FormatFilename generates a filename from metadata using the format template.
	// Supported placeholders vary by provider but typically include:
	// - {id}: Unique identifier
	// - {date}: Date in YYYY-MM-DD format
	// - {subject}, {from}, {to}: Email-specific
	// - {title}, {name}: Document-specific
	// The result should be sanitized for filesystem use.
	FormatFilename(format string, metadata map[string]string) string
}

// DefaultFilenameFormat returns a sensible default filename format for an integration.
func DefaultFilenameFormat(integration string) string {
	switch integration {
	case "gmail":
		return "{date}_{from}_{subject}_{id}.txt"
	case "gdrive":
		return "{name}_{id}"
	case "notion":
		return "{title}_{id}.md"
	case "github":
		return "{repo}_{type}_{number}_{id}.json"
	case "slack":
		return "{date}_{channel}_{user}_{id}.txt"
	case "linear":
		return "{identifier}_{title}.md"
	default:
		return "{id}"
	}
}

// Provider defines the interface for source integrations.
// Each integration (github, gmail, notion, etc.) implements this interface.
type Provider interface {
	// Name returns the integration name (e.g., "github", "gmail")
	Name() string

	// Stat returns file/directory attributes for a path within the integration
	// Path is relative to the integration root (e.g., "views/repos.json" not "github/views/repos.json")
	Stat(ctx context.Context, pctx *ProviderContext, path string) (*FileInfo, error)

	// ReadDir lists directory contents
	ReadDir(ctx context.Context, pctx *ProviderContext, path string) ([]DirEntry, error)

	// Read reads file content
	// Returns the data and any error
	Read(ctx context.Context, pctx *ProviderContext, path string, offset, length int64) ([]byte, error)

	// Readlink reads a symbolic link target (optional, return empty string if not supported)
	Readlink(ctx context.Context, pctx *ProviderContext, path string) (string, error)

	// Search executes a provider-specific query and returns results
	// The query format is provider-specific (e.g., Gmail search syntax, Drive query syntax)
	// Returns ErrSearchNotSupported if the provider doesn't support search
	Search(ctx context.Context, pctx *ProviderContext, query string, limit int) ([]SearchResult, error)
}

// Registry manages registered source providers
type Registry struct {
	mu        sync.RWMutex
	providers map[string]Provider
}

// NewRegistry creates a new provider registry
func NewRegistry() *Registry {
	return &Registry{
		providers: make(map[string]Provider),
	}
}

// Register adds a provider to the registry
func (r *Registry) Register(p Provider) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.providers[p.Name()] = p
}

// Get returns a provider by name
func (r *Registry) Get(name string) Provider {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.providers[name]
}

// List returns all registered provider names
func (r *Registry) List() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	names := make([]string, 0, len(r.providers))
	for name := range r.providers {
		names = append(names, name)
	}
	return names
}

// Has returns true if a provider is registered
func (r *Registry) Has(name string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	_, ok := r.providers[name]
	return ok
}
