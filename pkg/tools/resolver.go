package tools

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/beam-cloud/airstore/pkg/auth"
	"github.com/beam-cloud/airstore/pkg/types"
	"github.com/rs/zerolog/log"
)

// WorkspaceToolResolver provides workspace-aware tool resolution.
// It merges global tools with workspace-specific tools and enforces
// per-workspace enable/disable settings.
type WorkspaceToolResolver struct {
	globalRegistry *Registry
	backend        BackendRepository

	// Per-workspace MCP client cache
	mu      sync.RWMutex
	clients map[workspaceToolKey]*mcpClientEntry
}

// BackendRepository is the subset of repository.BackendRepository needed by the resolver
type BackendRepository interface {
	ListWorkspaceTools(ctx context.Context, workspaceId uint) ([]*types.WorkspaceTool, error)
	GetWorkspaceToolByName(ctx context.Context, workspaceId uint, name string) (*types.WorkspaceTool, error)
	GetWorkspaceToolSettings(ctx context.Context, workspaceId uint) (*types.WorkspaceToolSettings, error)
}

type workspaceToolKey struct {
	workspaceId uint
	toolName    string
}

type mcpClientEntry struct {
	client   MCPClient
	provider *MCPServerProvider
}

// NewWorkspaceToolResolver creates a new resolver
func NewWorkspaceToolResolver(globalRegistry *Registry, backend BackendRepository) *WorkspaceToolResolver {
	return &WorkspaceToolResolver{
		globalRegistry: globalRegistry,
		backend:        backend,
		clients:        make(map[workspaceToolKey]*mcpClientEntry),
	}
}

// List returns all tools available to the workspace (global + workspace-specific),
// filtered by workspace tool settings
func (r *WorkspaceToolResolver) List(ctx context.Context) ([]types.ResolvedTool, error) {
	workspaceId := auth.WorkspaceId(ctx)

	// Get tool settings for filtering
	var settings *types.WorkspaceToolSettings
	if workspaceId > 0 && r.backend != nil {
		var err error
		settings, err = r.backend.GetWorkspaceToolSettings(ctx, workspaceId)
		if err != nil {
			log.Warn().Err(err).Uint("workspace_id", workspaceId).Msg("failed to get tool settings")
		}
	}

	var result []types.ResolvedTool

	// Add global tools
	for _, name := range r.globalRegistry.List() {
		provider := r.globalRegistry.Get(name)
		if provider == nil {
			continue
		}

		enabled := settings == nil || settings.IsEnabled(name)
		result = append(result, types.ResolvedTool{
			Name:    name,
			Help:    provider.Help(),
			Origin:  types.ToolOriginGlobal,
			Enabled: enabled,
		})
	}

	// Add workspace-specific tools
	if workspaceId > 0 && r.backend != nil {
		workspaceTools, err := r.backend.ListWorkspaceTools(ctx, workspaceId)
		if err != nil {
			log.Warn().Err(err).Uint("workspace_id", workspaceId).Msg("failed to list workspace tools")
		} else {
			for _, wt := range workspaceTools {
				// Skip if a global tool with the same name exists
				if r.globalRegistry.Has(wt.Name) {
					log.Debug().Str("tool", wt.Name).Msg("workspace tool shadowed by global tool")
					continue
				}

				enabled := settings == nil || settings.IsEnabled(wt.Name)
				toolCount := r.getToolCount(wt)

				result = append(result, types.ResolvedTool{
					Name:       wt.Name,
					Help:       r.getWorkspaceToolHelp(wt),
					Origin:     types.ToolOriginWorkspace,
					ExternalId: wt.ExternalId,
					Enabled:    enabled,
					ToolCount:  toolCount,
				})
			}
		}
	}

	return result, nil
}

// ListEnabled returns only enabled tools for the workspace
func (r *WorkspaceToolResolver) ListEnabled(ctx context.Context) ([]types.ResolvedTool, error) {
	all, err := r.List(ctx)
	if err != nil {
		return nil, err
	}

	var enabled []types.ResolvedTool
	for _, t := range all {
		if t.Enabled {
			enabled = append(enabled, t)
		}
	}
	return enabled, nil
}

// Get returns a ToolProvider for the given tool name, checking both global
// and workspace-specific tools. Returns nil if disabled or not found.
func (r *WorkspaceToolResolver) Get(ctx context.Context, name string) (ToolProvider, error) {
	workspaceId := auth.WorkspaceId(ctx)

	// Check if disabled
	if workspaceId > 0 && r.backend != nil {
		settings, err := r.backend.GetWorkspaceToolSettings(ctx, workspaceId)
		if err != nil {
			log.Warn().Err(err).Uint("workspace_id", workspaceId).Msg("failed to get tool settings")
		} else if settings != nil && settings.IsDisabled(name) {
			return nil, nil // Tool is disabled
		}
	}

	// Check global registry first
	if provider := r.globalRegistry.Get(name); provider != nil {
		return provider, nil
	}

	// Check workspace-specific tools
	if workspaceId > 0 && r.backend != nil {
		wt, err := r.backend.GetWorkspaceToolByName(ctx, workspaceId, name)
		if err != nil {
			if _, ok := err.(*types.ErrWorkspaceToolNotFound); ok {
				return nil, nil // Not found
			}
			return nil, fmt.Errorf("get workspace tool: %w", err)
		}

		// Get or create provider for this workspace tool
		return r.getOrCreateProvider(ctx, workspaceId, wt)
	}

	return nil, nil
}

// Has checks if a tool exists (in global or workspace scope)
func (r *WorkspaceToolResolver) Has(ctx context.Context, name string) bool {
	if r.globalRegistry.Has(name) {
		return true
	}

	workspaceId := auth.WorkspaceId(ctx)
	if workspaceId == 0 || r.backend == nil {
		return false
	}

	_, err := r.backend.GetWorkspaceToolByName(ctx, workspaceId, name)
	return err == nil
}

// IsGlobal returns true if the tool is from the global registry
func (r *WorkspaceToolResolver) IsGlobal(name string) bool {
	return r.globalRegistry.Has(name)
}

// Invalidate removes cached clients for a workspace tool
func (r *WorkspaceToolResolver) Invalidate(workspaceId uint, toolName string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	key := workspaceToolKey{workspaceId: workspaceId, toolName: toolName}
	if entry, ok := r.clients[key]; ok {
		if entry.client != nil {
			entry.client.Close()
		}
		delete(r.clients, key)
		log.Debug().Uint("workspace_id", workspaceId).Str("tool", toolName).Msg("invalidated workspace tool cache")
	}
}

// InvalidateWorkspace removes all cached clients for a workspace
func (r *WorkspaceToolResolver) InvalidateWorkspace(workspaceId uint) {
	r.mu.Lock()
	defer r.mu.Unlock()

	for key, entry := range r.clients {
		if key.workspaceId == workspaceId {
			if entry.client != nil {
				entry.client.Close()
			}
			delete(r.clients, key)
		}
	}
	log.Debug().Uint("workspace_id", workspaceId).Msg("invalidated all workspace tool caches")
}

// Close shuts down all cached MCP clients
func (r *WorkspaceToolResolver) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	for key, entry := range r.clients {
		if entry.client != nil {
			entry.client.Close()
		}
		delete(r.clients, key)
	}
	return nil
}

// getOrCreateProvider returns a cached provider or creates a new one
func (r *WorkspaceToolResolver) getOrCreateProvider(ctx context.Context, workspaceId uint, wt *types.WorkspaceTool) (ToolProvider, error) {
	key := workspaceToolKey{workspaceId: workspaceId, toolName: wt.Name}

	// Fast path: check cache
	r.mu.RLock()
	if entry, ok := r.clients[key]; ok {
		r.mu.RUnlock()
		return entry.provider, nil
	}
	r.mu.RUnlock()

	// Slow path: create new client
	r.mu.Lock()
	defer r.mu.Unlock()

	// Double-check after acquiring write lock
	if entry, ok := r.clients[key]; ok {
		return entry.provider, nil
	}

	// Create MCP client based on config
	if wt.ProviderType != types.ProviderTypeMCP {
		return nil, fmt.Errorf("unsupported provider type: %s", wt.ProviderType)
	}

	cfg, err := wt.GetMCPConfig()
	if err != nil {
		return nil, fmt.Errorf("parse MCP config: %w", err)
	}

	// Create client
	var client MCPClient
	if cfg.IsRemote() {
		client = NewMCPRemoteClient(wt.Name, *cfg)
	} else {
		client = NewMCPStdioClient(wt.Name, *cfg)
	}

	// Start client
	if err := client.Start(ctx); err != nil {
		return nil, fmt.Errorf("start MCP client: %w", err)
	}

	// List tools
	tools, err := client.ListTools(ctx)
	if err != nil {
		client.Close()
		return nil, fmt.Errorf("list MCP tools: %w", err)
	}

	// Create provider
	provider := NewMCPServerProvider(wt.Name, client)
	for _, tool := range tools {
		provider.AddTool(tool.Name, tool.Description, tool.InputSchema)
	}

	// Cache
	r.clients[key] = &mcpClientEntry{
		client:   client,
		provider: provider,
	}

	log.Info().
		Uint("workspace_id", workspaceId).
		Str("tool", wt.Name).
		Int("sub_tools", len(tools)).
		Msg("created workspace MCP provider")

	return provider, nil
}

// getWorkspaceToolHelp generates help text for a workspace tool
func (r *WorkspaceToolResolver) getWorkspaceToolHelp(wt *types.WorkspaceTool) string {
	if wt.ProviderType != types.ProviderTypeMCP {
		return fmt.Sprintf("Workspace tool: %s", wt.Name)
	}

	cfg, err := wt.GetMCPConfig()
	if err != nil {
		return fmt.Sprintf("MCP Server: %s", wt.Name)
	}

	if cfg.IsRemote() {
		return fmt.Sprintf("MCP Server (remote): %s", wt.Name)
	}
	return fmt.Sprintf("MCP Server (stdio): %s %v", cfg.Command, cfg.Args)
}

// getToolCount returns the number of tools in an MCP manifest
func (r *WorkspaceToolResolver) getToolCount(wt *types.WorkspaceTool) int {
	if len(wt.Manifest) == 0 {
		return 0
	}

	var manifest struct {
		Tools []json.RawMessage `json:"tools"`
	}
	if err := json.Unmarshal(wt.Manifest, &manifest); err != nil {
		return 0
	}
	return len(manifest.Tools)
}
