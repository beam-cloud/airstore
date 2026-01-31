package oauth

import (
	"context"
	"fmt"

	"github.com/beam-cloud/airstore/pkg/types"
)

// Provider defines the interface for OAuth providers
type Provider interface {
	// Name returns the provider name (e.g., "google", "github")
	Name() string

	// IsConfigured returns true if the provider has valid credentials
	IsConfigured() bool

	// SupportsIntegration returns true if this provider handles the given integration type
	SupportsIntegration(integrationType string) bool

	// AuthorizeURL generates the OAuth authorization URL for the given integration
	AuthorizeURL(state, integrationType string) (string, error)

	// Exchange exchanges an authorization code for tokens
	Exchange(ctx context.Context, code, integrationType string) (*types.IntegrationCredentials, error)

	// Refresh refreshes an access token using a refresh token
	Refresh(ctx context.Context, refreshToken string) (*types.IntegrationCredentials, error)
}

// Registry manages OAuth providers and maps integration types to providers
type Registry struct {
	providers     map[string]Provider // provider name -> provider
	byIntegration map[string]string   // integration type -> provider name
}

// NewRegistry creates a new provider registry
func NewRegistry() *Registry {
	return &Registry{
		providers:     make(map[string]Provider),
		byIntegration: make(map[string]string),
	}
}

// Register adds a provider to the registry
func (r *Registry) Register(p Provider) {
	if p == nil || !p.IsConfigured() {
		return
	}
	r.providers[p.Name()] = p
}

// RegisterIntegration maps an integration type to a provider
func (r *Registry) RegisterIntegration(integrationType, providerName string) {
	r.byIntegration[integrationType] = providerName
}

// GetProvider returns a provider by name
func (r *Registry) GetProvider(name string) (Provider, bool) {
	p, ok := r.providers[name]
	return p, ok
}

// GetProviderForIntegration returns the provider that handles the given integration type
func (r *Registry) GetProviderForIntegration(integrationType string) (Provider, error) {
	providerName, ok := r.byIntegration[integrationType]
	if !ok {
		return nil, fmt.Errorf("no provider registered for integration: %s", integrationType)
	}

	provider, ok := r.providers[providerName]
	if !ok {
		return nil, fmt.Errorf("provider %s not configured for integration: %s", providerName, integrationType)
	}

	return provider, nil
}

// IsOAuthIntegration returns true if the integration type uses OAuth
func (r *Registry) IsOAuthIntegration(integrationType string) bool {
	_, ok := r.byIntegration[integrationType]
	return ok
}

// ListIntegrations returns all registered integration types
func (r *Registry) ListIntegrations() []string {
	integrations := make([]string, 0, len(r.byIntegration))
	for integration := range r.byIntegration {
		integrations = append(integrations, integration)
	}
	return integrations
}

// ListConfiguredProviders returns names of all configured providers
func (r *Registry) ListConfiguredProviders() []string {
	names := make([]string, 0, len(r.providers))
	for name := range r.providers {
		names = append(names, name)
	}
	return names
}
