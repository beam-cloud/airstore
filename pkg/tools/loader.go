package tools

import (
	"embed"
	"fmt"
	"io/fs"
	"path/filepath"
	"strings"

	"github.com/beam-cloud/airstore/pkg/types"
	"github.com/rs/zerolog/log"
)

// ClientRegistry holds registered tool clients that can be paired with schemas
type ClientRegistry struct {
	clients map[types.ToolName]ToolClient
}

// NewClientRegistry creates a new client registry
func NewClientRegistry() *ClientRegistry {
	return &ClientRegistry{
		clients: make(map[types.ToolName]ToolClient),
	}
}

// Register adds a self-describing client to the registry using its Name()
func (r *ClientRegistry) Register(client ToolClient) {
	r.clients[client.Name()] = client
}

// Get returns a client by name
func (r *ClientRegistry) Get(name string) (ToolClient, bool) {
	client, ok := r.clients[types.ToolName(name)]
	return client, ok
}

// RegisterAll registers multiple clients at once
func (r *ClientRegistry) RegisterAll(clients ...ToolClient) {
	for _, c := range clients {
		r.Register(c)
	}
}

// Loader handles discovering tool schemas and creating providers
type Loader struct {
	clients *ClientRegistry
}

// NewLoader creates a new tool loader
func NewLoader(clients *ClientRegistry) *Loader {
	return &Loader{
		clients: clients,
	}
}

// LoadFromFS loads tool schemas from an embedded filesystem
func (l *Loader) LoadFromFS(toolsFS embed.FS, dir string) ([]*SchemaProvider, error) {
	var providers []*SchemaProvider

	// Get already self-registered tools to avoid duplicates/warnings
	selfRegistered := GetRegisteredTools()

	err := fs.WalkDir(toolsFS, dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if d.IsDir() {
			return nil
		}

		ext := filepath.Ext(path)
		if ext != ".yaml" && ext != ".yml" {
			return nil
		}

		data, err := toolsFS.ReadFile(path)
		if err != nil {
			return fmt.Errorf("read %s: %w", path, err)
		}

		schema, err := ParseSchema(data)
		if err != nil {
			return fmt.Errorf("parse %s: %w", path, err)
		}

		// Skip if already registered via self-registration (builtin tools)
		if _, exists := selfRegistered[schema.Name]; exists {
			log.Debug().Str("tool", schema.Name).Msg("tool already self-registered, skipping YAML schema")
			return nil
		}

		// Find matching client
		client, ok := l.clients.Get(schema.Name)
		if !ok {
			log.Debug().Str("tool", schema.Name).Msg("no client registered for tool, skipping")
			return nil
		}

		provider := NewSchemaProvider(schema, client)
		providers = append(providers, provider)

		log.Info().
			Str("tool", schema.Name).
			Int("commands", len(schema.Commands)).
			Msg("loaded tool")

		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("walk tools directory: %w", err)
	}

	return providers, nil
}

// LoadFromDir loads tool schemas from a filesystem directory
func (l *Loader) LoadFromDir(dir string) ([]*SchemaProvider, error) {
	schemas, err := DiscoverSchemas(dir)
	if err != nil {
		return nil, err
	}

	// Get already self-registered tools to avoid duplicates/warnings
	selfRegistered := GetRegisteredTools()

	var providers []*SchemaProvider

	for _, schema := range schemas {
		// Skip if already registered via self-registration (builtin tools)
		if _, exists := selfRegistered[schema.Name]; exists {
			log.Debug().Str("tool", schema.Name).Msg("tool already self-registered, skipping YAML schema")
			continue
		}

		client, ok := l.clients.Get(schema.Name)
		if !ok {
			log.Debug().Str("tool", schema.Name).Msg("no client registered for tool, skipping")
			continue
		}

		provider := NewSchemaProvider(schema, client)
		providers = append(providers, provider)

		log.Info().
			Str("tool", schema.Name).
			Int("commands", len(schema.Commands)).
			Msg("loaded tool")
	}

	return providers, nil
}

// LoadFromBytes loads a single tool from YAML bytes
func (l *Loader) LoadFromBytes(data []byte) (*SchemaProvider, error) {
	schema, err := ParseSchema(data)
	if err != nil {
		return nil, err
	}

	client, ok := l.clients.Get(schema.Name)
	if !ok {
		return nil, fmt.Errorf("no client registered for tool: %s", schema.Name)
	}

	return NewSchemaProvider(schema, client), nil
}

// RegisterProviders loads tools and registers them with a provider registry
func (l *Loader) RegisterProviders(toolsFS embed.FS, dir string, registry *Registry) error {
	providers, err := l.LoadFromFS(toolsFS, dir)
	if err != nil {
		return err
	}

	for _, p := range providers {
		registry.Register(p)
	}

	return nil
}

// ListAvailableTools returns the names of tools that have schemas but may or may not have clients
func ListAvailableTools(toolsFS embed.FS, dir string) ([]string, error) {
	var names []string

	err := fs.WalkDir(toolsFS, dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if d.IsDir() {
			return nil
		}

		ext := filepath.Ext(path)
		if ext != ".yaml" && ext != ".yml" {
			return nil
		}

		// Extract tool name from filename (e.g., "wikipedia.yaml" -> "wikipedia")
		name := strings.TrimSuffix(filepath.Base(path), ext)
		names = append(names, name)

		return nil
	})

	return names, err
}
