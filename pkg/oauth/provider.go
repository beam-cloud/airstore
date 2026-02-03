package oauth

import (
	"context"
	"errors"

	"github.com/beam-cloud/airstore/pkg/types"
)

var (
	ErrProviderNotFound     = errors.New("provider not found")
	ErrIntegrationNotFound  = errors.New("integration not found")
	ErrProviderNotConfigured = errors.New("provider not configured")
)

// Provider defines the interface for OAuth providers.
type Provider interface {
	// Name returns the provider name (e.g., "google", "github")
	Name() string

	// IsConfigured returns true if the provider has valid credentials
	IsConfigured() bool

	// Integrations returns the integration types this provider handles
	Integrations() []string

	// AuthorizeURL generates the OAuth authorization URL for the given integration
	AuthorizeURL(state, integrationType string) (string, error)

	// Exchange exchanges an authorization code for tokens
	Exchange(ctx context.Context, code, integrationType string) (*types.IntegrationCredentials, error)

	// Refresh refreshes an access token using a refresh token
	Refresh(ctx context.Context, refreshToken string) (*types.IntegrationCredentials, error)
}

// Registry manages OAuth providers and maps integration types to providers.
type Registry struct {
	providers     map[string]Provider
	byIntegration map[string]string
}

// NewRegistry creates a new provider registry.
func NewRegistry() *Registry {
	return &Registry{
		providers:     make(map[string]Provider),
		byIntegration: make(map[string]string),
	}
}

// Register adds a provider to the registry if configured.
// Automatically registers all integrations the provider supports.
func (r *Registry) Register(p Provider) {
	if p == nil || !p.IsConfigured() {
		return
	}

	r.providers[p.Name()] = p

	for _, integration := range p.Integrations() {
		r.byIntegration[integration] = p.Name()
	}
}

// GetProvider returns a provider by name.
func (r *Registry) GetProvider(name string) (Provider, error) {
	p, ok := r.providers[name]
	if !ok {
		return nil, ErrProviderNotFound
	}
	return p, nil
}

// GetProviderForIntegration returns the provider that handles the given integration type.
func (r *Registry) GetProviderForIntegration(integrationType string) (Provider, error) {
	providerName, ok := r.byIntegration[integrationType]
	if !ok {
		return nil, ErrIntegrationNotFound
	}

	provider, ok := r.providers[providerName]
	if !ok {
		return nil, ErrProviderNotConfigured
	}

	return provider, nil
}

// IsOAuthIntegration returns true if the integration type uses OAuth.
func (r *Registry) IsOAuthIntegration(integrationType string) bool {
	_, ok := r.byIntegration[integrationType]
	return ok
}

// ListIntegrations returns all registered integration types.
func (r *Registry) ListIntegrations() []string {
	integrations := make([]string, 0, len(r.byIntegration))
	for integration := range r.byIntegration {
		integrations = append(integrations, integration)
	}
	return integrations
}

// ListConfiguredProviders returns names of all configured providers.
func (r *Registry) ListConfiguredProviders() []string {
	names := make([]string, 0, len(r.providers))
	for name := range r.providers {
		names = append(names, name)
	}
	return names
}
