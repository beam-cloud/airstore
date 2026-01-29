package index

import (
	"context"
	"time"

	"github.com/beam-cloud/airstore/pkg/types"
)

// IndexProvider defines the interface for source-specific indexing.
// Each integration (gmail, gdrive, notion) implements this interface
// to sync its data into the local index.
type IndexProvider interface {
	// Integration returns the integration name (e.g., "gmail", "gdrive", "notion")
	Integration() string

	// Sync fetches changes from the source and updates the index.
	// The `since` parameter allows for incremental sync - only fetch changes
	// since the last sync time. If `since` is zero, do a full sync.
	// Called continuously by the background syncer.
	Sync(ctx context.Context, store IndexStore, creds *types.IntegrationCredentials, since time.Time) error

	// FetchContent retrieves actual file content for large files not stored in the index.
	// Returns the content bytes for the given entity ID.
	// This is called when a user reads a file that has ContentRef set.
	FetchContent(ctx context.Context, creds *types.IntegrationCredentials, entityID string) ([]byte, error)

	// SyncInterval returns the recommended sync interval for this provider.
	// Different providers may have different rate limits and update frequencies.
	SyncInterval() time.Duration
}

// ProviderRegistry manages registered index providers
type ProviderRegistry struct {
	providers map[string]IndexProvider
}

// NewProviderRegistry creates a new provider registry
func NewProviderRegistry() *ProviderRegistry {
	return &ProviderRegistry{
		providers: make(map[string]IndexProvider),
	}
}

// Register adds a provider to the registry
func (r *ProviderRegistry) Register(p IndexProvider) {
	r.providers[p.Integration()] = p
}

// Get returns a provider by integration name
func (r *ProviderRegistry) Get(integration string) IndexProvider {
	return r.providers[integration]
}

// List returns all registered providers
func (r *ProviderRegistry) List() []IndexProvider {
	providers := make([]IndexProvider, 0, len(r.providers))
	for _, p := range r.providers {
		providers = append(providers, p)
	}
	return providers
}

// Has returns true if a provider is registered for the integration
func (r *ProviderRegistry) Has(integration string) bool {
	_, ok := r.providers[integration]
	return ok
}
