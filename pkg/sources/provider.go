package sources

import (
	"context"
	"sync"

	"github.com/beam-cloud/airstore/pkg/types"
)

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

// ProviderContext contains workspace and credential info for provider operations
type ProviderContext struct {
	WorkspaceId uint
	MemberId    uint
	Credentials *types.IntegrationCredentials
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
