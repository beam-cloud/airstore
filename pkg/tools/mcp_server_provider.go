package tools

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"
)

// MCPServerTool represents a single tool within an MCP server
type MCPServerTool struct {
	Name        string
	Description string
	InputSchema json.RawMessage
}

// MCPServerProvider implements ToolProvider for an entire MCP server.
// The server becomes a single binary, with each tool as a subcommand.
// Usage: /tools/<server> <tool> [args...]
type MCPServerProvider struct {
	serverName string
	tools      map[string]*MCPServerTool
	toolOrder  []string // for consistent ordering in help
	executor   MCPToolExecutor
}

// NewMCPServerProvider creates a new provider for an MCP server
func NewMCPServerProvider(serverName string, executor MCPToolExecutor) *MCPServerProvider {
	return &MCPServerProvider{
		serverName: serverName,
		tools:      make(map[string]*MCPServerTool),
		toolOrder:  []string{},
		executor:   executor,
	}
}

// AddTool registers a tool with this server provider
func (p *MCPServerProvider) AddTool(name, description string, inputSchema json.RawMessage) {
	p.tools[name] = &MCPServerTool{
		Name:        name,
		Description: description,
		InputSchema: inputSchema,
	}
	p.toolOrder = append(p.toolOrder, name)
	sort.Strings(p.toolOrder)
}

// Name returns the server name (used as the binary name in /tools/)
func (p *MCPServerProvider) Name() string {
	return p.serverName
}

// Help returns usage information listing all available tools as subcommands
func (p *MCPServerProvider) Help() string {
	var sb strings.Builder

	sb.WriteString(fmt.Sprintf("NAME\n    %s - MCP server with %d tools\n\n", p.serverName, len(p.tools)))

	sb.WriteString("SYNOPSIS\n")
	sb.WriteString(fmt.Sprintf("    %s <command> [arguments...]\n", p.serverName))
	sb.WriteString(fmt.Sprintf("    %s <command> --help\n\n", p.serverName))

	sb.WriteString("COMMANDS\n")
	for _, name := range p.toolOrder {
		tool := p.tools[name]
		if tool.Description != "" {
			sb.WriteString(fmt.Sprintf("    %-24s %s\n", name, tool.Description))
		} else {
			sb.WriteString(fmt.Sprintf("    %s\n", name))
		}
	}
	sb.WriteString("\n")

	sb.WriteString("EXAMPLES\n")
	if len(p.toolOrder) > 0 {
		firstTool := p.toolOrder[0]
		sb.WriteString(fmt.Sprintf("    %s %s --help\n", p.serverName, firstTool))
		sb.WriteString(fmt.Sprintf("    %s %s <arg>\n", p.serverName, firstTool))
		sb.WriteString(fmt.Sprintf("    %s %s '{\"key\": \"value\"}'\n", p.serverName, firstTool))
	}
	sb.WriteString("\n")

	sb.WriteString(fmt.Sprintf("SOURCE\n    MCP Server: %s\n", p.serverName))

	return sb.String()
}

// Execute runs an MCP tool as a subcommand
func (p *MCPServerProvider) Execute(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	return p.ExecuteWithContext(ctx, nil, args, stdout, stderr)
}

// ExecuteWithContext runs an MCP tool with execution context
func (p *MCPServerProvider) ExecuteWithContext(ctx context.Context, execCtx *ExecutionContext, args []string, stdout, stderr io.Writer) error {
	// No args = show help
	if len(args) == 0 {
		fmt.Fprint(stdout, p.Help())
		return nil
	}

	toolName := args[0]
	toolArgs := args[1:]

	// Handle --help for the server itself
	if toolName == "--help" || toolName == "-h" {
		fmt.Fprint(stdout, p.Help())
		return nil
	}

	// Look up the tool
	tool, ok := p.tools[toolName]
	if !ok {
		fmt.Fprintf(stderr, "Unknown command: %s\n\n", toolName)
		fmt.Fprint(stderr, p.Help())
		return fmt.Errorf("unknown command: %s", toolName)
	}

	// Handle --help for specific tool
	if len(toolArgs) > 0 && (toolArgs[0] == "--help" || toolArgs[0] == "-h") {
		fmt.Fprint(stdout, p.toolHelp(tool))
		return nil
	}

	// Parse arguments for the tool
	parsedArgs, err := p.parseToolArgs(tool, toolArgs)
	if err != nil {
		return err
	}

	// Call the MCP tool
	result, err := p.executor.CallTool(ctx, toolName, parsedArgs)
	if err != nil {
		return fmt.Errorf("MCP call failed: %w", err)
	}

	// Handle error results
	if result.IsError {
		for _, content := range result.Content {
			if content.Type == "text" {
				fmt.Fprintln(stderr, content.Text)
			}
		}
		return fmt.Errorf("tool returned error")
	}

	// Write content to stdout
	for _, content := range result.Content {
		switch content.Type {
		case "text":
			fmt.Fprintln(stdout, content.Text)
		default:
			// For other content types, output as JSON
			data, _ := json.MarshalIndent(content, "", "  ")
			fmt.Fprintln(stdout, string(data))
		}
	}

	return nil
}

// toolHelp generates help text for a specific tool
func (p *MCPServerProvider) toolHelp(tool *MCPServerTool) string {
	var sb strings.Builder

	sb.WriteString(fmt.Sprintf("NAME\n    %s %s", p.serverName, tool.Name))
	if tool.Description != "" {
		sb.WriteString(fmt.Sprintf(" - %s", tool.Description))
	}
	sb.WriteString("\n\n")

	var schema MCPToolSchema
	if len(tool.InputSchema) > 0 {
		json.Unmarshal(tool.InputSchema, &schema)
	}

	// Categorize properties
	var stringParams, complexParams []string
	for _, name := range schema.Required {
		if prop, ok := schema.Properties[name]; ok {
			if prop.Type == "string" || prop.Type == "integer" || prop.Type == "number" || prop.Type == "boolean" {
				stringParams = append(stringParams, name)
			} else {
				complexParams = append(complexParams, name)
			}
		}
	}

	sb.WriteString("SYNOPSIS\n")
	if len(complexParams) > 0 {
		// Has complex types - recommend JSON
		sb.WriteString(fmt.Sprintf("    %s %s '<json>'\n", p.serverName, tool.Name))
	} else if len(stringParams) > 0 {
		sb.WriteString(fmt.Sprintf("    %s %s", p.serverName, tool.Name))
		for _, name := range stringParams {
			sb.WriteString(fmt.Sprintf(" <%s>", name))
		}
		sb.WriteString("\n")
	} else {
		sb.WriteString(fmt.Sprintf("    %s %s [arguments...]\n", p.serverName, tool.Name))
	}
	sb.WriteString("\n")

	if tool.Description != "" {
		sb.WriteString("DESCRIPTION\n")
		sb.WriteString(fmt.Sprintf("    %s\n\n", tool.Description))
	}

	if len(schema.Properties) > 0 {
		sb.WriteString("ARGUMENTS\n")
		for _, name := range schema.Required {
			if prop, ok := schema.Properties[name]; ok {
				sb.WriteString(fmt.Sprintf("    %s (%s, required)\n", name, prop.Type))
				if prop.Description != "" {
					sb.WriteString(fmt.Sprintf("        %s\n", prop.Description))
				}
				sb.WriteString("\n")
			}
		}
		for name, prop := range schema.Properties {
			if !contains(schema.Required, name) {
				sb.WriteString(fmt.Sprintf("    %s (%s, optional)\n", name, prop.Type))
				if prop.Description != "" {
					sb.WriteString(fmt.Sprintf("        %s\n", prop.Description))
				}
				sb.WriteString("\n")
			}
		}
	}

	sb.WriteString("EXAMPLES\n")
	sb.WriteString(p.generateExamples(tool.Name, schema))

	return sb.String()
}

func (p *MCPServerProvider) generateExamples(toolName string, schema MCPToolSchema) string {
	var sb strings.Builder

	// Check if all required params are simple types
	allSimple := true
	for _, name := range schema.Required {
		if prop, ok := schema.Properties[name]; ok {
			if prop.Type == "array" || prop.Type == "object" {
				allSimple = false
				break
			}
		}
	}

	if allSimple && len(schema.Required) > 0 {
		// Simple positional example
		sb.WriteString(fmt.Sprintf("    %s %s", p.serverName, toolName))
		for _, name := range schema.Required {
			sb.WriteString(fmt.Sprintf(" \"<%s>\"", name))
		}
		sb.WriteString("\n")
	}

	// Always show JSON example with actual schema structure
	example := p.buildJSONExample(schema)
	sb.WriteString(fmt.Sprintf("    %s %s '%s'\n", p.serverName, toolName, example))

	return sb.String()
}

func (p *MCPServerProvider) buildJSONExample(schema MCPToolSchema) string {
	example := make(map[string]any)
	for name, prop := range schema.Properties {
		example[name] = p.exampleValue(prop)
	}
	data, _ := json.Marshal(example)
	return string(data)
}

func (p *MCPServerProvider) exampleValue(prop MCPPropertySchema) any {
	switch prop.Type {
	case "string":
		return "..."
	case "integer", "number":
		return 0
	case "boolean":
		return true
	case "array":
		if prop.Items != nil {
			return []any{p.exampleValue(*prop.Items)}
		}
		return []any{"..."}
	case "object":
		if len(prop.Properties) > 0 {
			obj := make(map[string]any)
			for k, v := range prop.Properties {
				obj[k] = p.exampleValue(v)
			}
			return obj
		}
		return map[string]any{"key": "value"}
	default:
		return "..."
	}
}

func contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}

// parseToolArgs parses command-line arguments for a tool
func (p *MCPServerProvider) parseToolArgs(tool *MCPServerTool, args []string) (map[string]any, error) {
	if len(args) == 0 {
		return make(map[string]any), nil
	}

	// Parse schema
	var schema MCPToolSchema
	if len(tool.InputSchema) > 0 {
		json.Unmarshal(tool.InputSchema, &schema)
	}

	// Single JSON argument - try to parse it intelligently
	if len(args) == 1 && (strings.HasPrefix(args[0], "{") || strings.HasPrefix(args[0], "[")) {
		return p.parseJSONArg(args[0], schema)
	}

	// Multiple args or non-JSON - parse as key=value or positional
	return p.parsePositionalArgs(args, schema)
}

// parseJSONArg parses a JSON argument, mapping it to schema params
func (p *MCPServerProvider) parseJSONArg(arg string, schema MCPToolSchema) (map[string]any, error) {
	var parsed any
	if err := json.Unmarshal([]byte(arg), &parsed); err != nil {
		return nil, fmt.Errorf("invalid JSON: %w", err)
	}

	// If it's already an object, check if it matches the schema
	if obj, ok := parsed.(map[string]any); ok {
		// Check if the object has keys that match schema properties
		hasSchemaKey := false
		for key := range obj {
			if _, exists := schema.Properties[key]; exists {
				hasSchemaKey = true
				break
			}
		}
		if hasSchemaKey {
			return obj, nil
		}
	}

	// If there's exactly one required param, wrap the value in it
	if len(schema.Required) == 1 {
		paramName := schema.Required[0]
		return map[string]any{paramName: parsed}, nil
	}

	// If it's an object but doesn't match schema, return as-is (let MCP server validate)
	if obj, ok := parsed.(map[string]any); ok {
		return obj, nil
	}

	// For arrays with no clear mapping, return error with guidance
	return nil, fmt.Errorf("cannot map JSON to parameters; use format: '{\"paramName\": <value>}'")
}

// parsePositionalArgs parses positional and flag arguments
func (p *MCPServerProvider) parsePositionalArgs(args []string, schema MCPToolSchema) (map[string]any, error) {
	toolArgs := make(map[string]any)
	positionalIdx := 0

	for i := 0; i < len(args); i++ {
		arg := args[i]

		// Handle --key=value
		if strings.HasPrefix(arg, "--") && strings.Contains(arg, "=") {
			parts := strings.SplitN(arg[2:], "=", 2)
			toolArgs[parts[0]] = p.parseValue(parts[1], schema.Properties[parts[0]])
			continue
		}

		// Handle --key value
		if strings.HasPrefix(arg, "--") {
			key := arg[2:]
			if i+1 < len(args) && !strings.HasPrefix(args[i+1], "--") {
				i++
				toolArgs[key] = p.parseValue(args[i], schema.Properties[key])
			} else {
				toolArgs[key] = true
			}
			continue
		}

		// Handle key=value
		if strings.Contains(arg, "=") && !strings.HasPrefix(arg, "{") && !strings.HasPrefix(arg, "[") {
			parts := strings.SplitN(arg, "=", 2)
			toolArgs[parts[0]] = p.parseValue(parts[1], schema.Properties[parts[0]])
			continue
		}

		// Positional argument
		if positionalIdx < len(schema.Required) {
			paramName := schema.Required[positionalIdx]
			toolArgs[paramName] = p.parseValue(arg, schema.Properties[paramName])
			positionalIdx++
		}
	}

	return toolArgs, nil
}

// parseValue converts a string value based on the schema type
func (p *MCPServerProvider) parseValue(value string, prop MCPPropertySchema) any {
	// Try JSON parsing for complex types or JSON-looking strings
	if prop.Type == "array" || prop.Type == "object" || strings.HasPrefix(value, "[") || strings.HasPrefix(value, "{") {
		var parsed any
		if err := json.Unmarshal([]byte(value), &parsed); err == nil {
			return parsed
		}
	}

	switch prop.Type {
	case "integer":
		var n int
		if _, err := fmt.Sscanf(value, "%d", &n); err == nil {
			return n
		}
	case "number":
		var f float64
		if _, err := fmt.Sscanf(value, "%f", &f); err == nil {
			return f
		}
	case "boolean":
		return value == "true" || value == "1"
	}

	return value
}

// ToolCount returns the number of tools in this server
func (p *MCPServerProvider) ToolCount() int {
	return len(p.tools)
}

// HasTool checks if a tool exists in this server
func (p *MCPServerProvider) HasTool(name string) bool {
	_, ok := p.tools[name]
	return ok
}
