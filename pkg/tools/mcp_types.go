package tools

import (
	"context"
	"encoding/json"
)

// MCPToolExecutor is the interface for executing MCP tool calls.
// Implemented by MCPClient.
type MCPToolExecutor interface {
	CallTool(ctx context.Context, name string, args map[string]any) (MCPToolResult, error)
}

// MCPToolResult represents the result of an MCP tool call
type MCPToolResult struct {
	Content []MCPContent `json:"content"`
	IsError bool         `json:"isError,omitempty"`
}

// MCPContent represents content in a tool result
type MCPContent struct {
	Type string `json:"type"`
	Text string `json:"text,omitempty"`
}

// MCPToolSchema represents the input schema for an MCP tool (JSON Schema subset)
type MCPToolSchema struct {
	Type       string                       `json:"type"`
	Properties map[string]MCPPropertySchema `json:"properties,omitempty"`
	Required   []string                     `json:"required,omitempty"`
}

// MCPPropertySchema represents a property in the tool schema
type MCPPropertySchema struct {
	Type        string              `json:"type"`
	Description string              `json:"description,omitempty"`
	Items       *MCPPropertySchema  `json:"items,omitempty"`       // For arrays
	Properties  map[string]MCPPropertySchema `json:"properties,omitempty"` // For objects
}

// MCPServerInfo contains server identification from initialization
type MCPServerInfo struct {
	Name    string `json:"name"`
	Version string `json:"version"`
}

// MCPCapabilities describes what the MCP server supports
type MCPCapabilities struct {
	Tools     *MCPToolsCapability     `json:"tools,omitempty"`
	Resources *MCPResourcesCapability `json:"resources,omitempty"`
	Prompts   *MCPPromptsCapability   `json:"prompts,omitempty"`
}

// MCPToolsCapability indicates tools support
type MCPToolsCapability struct {
	ListChanged bool `json:"listChanged,omitempty"`
}

// MCPResourcesCapability indicates resources support
type MCPResourcesCapability struct {
	Subscribe   bool `json:"subscribe,omitempty"`
	ListChanged bool `json:"listChanged,omitempty"`
}

// MCPPromptsCapability indicates prompts support
type MCPPromptsCapability struct {
	ListChanged bool `json:"listChanged,omitempty"`
}

// MCPToolInfo represents a tool exposed by an MCP server
type MCPToolInfo struct {
	Name        string          `json:"name"`
	Description string          `json:"description,omitempty"`
	InputSchema json.RawMessage `json:"inputSchema"`
}
