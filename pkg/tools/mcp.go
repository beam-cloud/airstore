package tools

import (
	"context"
	"strings"
	"time"

	"github.com/beam-cloud/airstore/pkg/types"
	"github.com/rs/zerolog/log"
)

const (
	// MCPInitTimeout is the timeout for MCP server initialization (includes NPX download time)
	MCPInitTimeout = 120 * time.Second
	// MCPRetryDelay is the delay between retry attempts
	MCPRetryDelay = 2 * time.Second
	// MCPMaxRetries is the maximum number of retry attempts
	MCPMaxRetries = 1
)

// MCPManager manages MCP server clients and their lifecycle
type MCPManager struct {
	clients []MCPClient
}

// NewMCPManager creates a new MCP manager
func NewMCPManager() *MCPManager {
	return &MCPManager{
		clients: make([]MCPClient, 0),
	}
}

// LoadServers starts MCP servers from config and registers their tools
func (m *MCPManager) LoadServers(config map[string]types.MCPServerConfig, registry *Registry) error {
	for name, serverCfg := range config {
		if err := m.loadServerWithRetry(name, serverCfg, registry); err != nil {
			log.Warn().Err(err).Str("server", name).Msg("failed to load MCP server")
			continue
		}
	}
	return nil
}

// loadServerWithRetry attempts to load an MCP server with retry logic
func (m *MCPManager) loadServerWithRetry(name string, cfg types.MCPServerConfig, registry *Registry) error {
	var lastErr error

	for attempt := 0; attempt <= MCPMaxRetries; attempt++ {
		if attempt > 0 {
			log.Info().
				Str("server", name).
				Int("attempt", attempt+1).
				Msg("retrying MCP server initialization...")
			time.Sleep(MCPRetryDelay)
		}

		err := m.loadServer(name, cfg, registry)
		if err == nil {
			return nil
		}

		lastErr = err

		// Check if the error suggests a download/network issue
		if strings.Contains(err.Error(), "context deadline exceeded") {
			log.Warn().
				Str("server", name).
				Msg("MCP server timed out - this may be due to NPX downloading packages for the first time")
		}
	}

	return lastErr
}

// loadServer starts a single MCP server and registers it as a single provider
func (m *MCPManager) loadServer(name string, cfg types.MCPServerConfig, registry *Registry) error {
	// Select client type based on configuration
	var client MCPClient
	if cfg.IsRemote() {
		client = NewMCPRemoteClient(name, cfg)
		log.Debug().Str("server", name).Str("url", cfg.URL).Msg("using remote HTTP/SSE transport")
	} else {
		client = NewMCPStdioClient(name, cfg)
		log.Debug().Str("server", name).Str("command", cfg.Command).Msg("using stdio transport")
	}

	// Start with extended timeout (120s to allow for NPX downloads or remote connection)
	ctx, cancel := context.WithTimeout(context.Background(), MCPInitTimeout)
	defer cancel()

	if err := client.Start(ctx); err != nil {
		return err
	}

	// Get available tools
	tools, err := client.ListTools(ctx)
	if err != nil {
		client.Close()
		return err
	}

	// Track the client for cleanup
	m.clients = append(m.clients, client)

	// Create a single server provider and add all tools to it
	serverProvider := NewMCPServerProvider(name, client)
	for _, tool := range tools {
		serverProvider.AddTool(tool.Name, tool.Description, tool.InputSchema)
		log.Debug().
			Str("server", name).
			Str("tool", tool.Name).
			Msg("added MCP tool to server")
	}

	// Register the server as a single provider (one binary per server)
	registry.Register(serverProvider)

	log.Info().
		Str("server", name).
		Int("tools", len(tools)).
		Msg("MCP server loaded")

	return nil
}

// Close shuts down all MCP clients
func (m *MCPManager) Close() error {
	for _, client := range m.clients {
		client.Close()
	}
	m.clients = nil
	return nil
}

// Clients returns the list of active MCP clients
func (m *MCPManager) Clients() []MCPClient {
	return m.clients
}
