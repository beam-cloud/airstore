package tools

import (
	"context"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/beam-cloud/airstore/pkg/types"
)

// SchemaProvider wraps a ToolClient with a schema to create a ToolProvider.
// It handles argument parsing, validation, and help generation.
type SchemaProvider struct {
	schema *ToolSchema
	client ToolClient
}

// NewSchemaProvider creates a new schema-driven provider
func NewSchemaProvider(schema *ToolSchema, client ToolClient) *SchemaProvider {
	return &SchemaProvider{
		schema: schema,
		client: client,
	}
}

// Name returns the tool name from the schema
func (p *SchemaProvider) Name() string {
	return p.schema.Name
}

// Help generates help text from the schema
func (p *SchemaProvider) Help() string {
	var sb strings.Builder

	// Tool header
	sb.WriteString(fmt.Sprintf("%s - %s\n\n", p.schema.Name, p.schema.Description))

	// Commands list
	sb.WriteString("Commands:\n")
	for name, cmd := range p.schema.Commands {
		sb.WriteString(fmt.Sprintf("  %-16s %s\n", name, cmd.Description))
	}

	sb.WriteString(fmt.Sprintf("\nRun '%s <command> --help' for command details.\n", p.schema.Name))

	return sb.String()
}

// Execute parses arguments and delegates to the client (without credentials)
func (p *SchemaProvider) Execute(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	return p.ExecuteWithContext(ctx, nil, args, stdout, stderr)
}

// ExecuteWithContext parses arguments and delegates to the client with credentials
func (p *SchemaProvider) ExecuteWithContext(ctx context.Context, execCtx *ExecutionContext, args []string, stdout, stderr io.Writer) error {
	// Need at least a command
	if len(args) == 0 {
		fmt.Fprint(stderr, p.Help())
		return fmt.Errorf("no command specified")
	}

	command := args[0]
	cmdArgs := args[1:]

	// Check for command-level help
	if command == "--help" || command == "-h" {
		fmt.Fprint(stdout, p.Help())
		return nil
	}

	// Find command schema
	cmdSchema, ok := p.schema.Commands[command]
	if !ok {
		fmt.Fprint(stderr, p.Help())
		return fmt.Errorf("unknown command: %s", command)
	}

	// Check for command-specific help
	for _, arg := range cmdArgs {
		if arg == "--help" || arg == "-h" {
			fmt.Fprint(stdout, p.commandHelp(command, cmdSchema))
			return nil
		}
	}

	// Parse arguments
	parsedArgs, err := p.parseArgs(cmdSchema, cmdArgs)
	if err != nil {
		fmt.Fprintf(stderr, "Error: %v\n\n", err)
		fmt.Fprint(stderr, p.commandHelp(command, cmdSchema))
		return err
	}

	// Extract credentials from execution context
	var creds *types.IntegrationCredentials
	if execCtx != nil {
		creds = execCtx.Credentials
	}

	// Execute via client
	return p.client.Execute(ctx, command, parsedArgs, creds, stdout, stderr)
}

// commandHelp generates help for a specific command
func (p *SchemaProvider) commandHelp(cmdName string, cmd *CommandSchema) string {
	var sb strings.Builder

	// Usage line
	sb.WriteString(fmt.Sprintf("Usage: %s %s", p.schema.Name, cmdName))

	positional := cmd.GetPositionalParams()
	for _, param := range positional {
		if param.Required {
			sb.WriteString(fmt.Sprintf(" <%s>", param.Name))
		} else {
			sb.WriteString(fmt.Sprintf(" [%s]", param.Name))
		}
	}

	flags := cmd.GetFlagParams()
	if len(flags) > 0 {
		sb.WriteString(" [flags]")
	}
	sb.WriteString("\n\n")

	// Description
	sb.WriteString(fmt.Sprintf("%s\n", cmd.Description))

	// Positional arguments
	if len(positional) > 0 {
		sb.WriteString("\nArguments:\n")
		for _, param := range positional {
			req := ""
			if param.Required {
				req = " (required)"
			}
			sb.WriteString(fmt.Sprintf("  %-16s %s%s\n", param.Name, param.Description, req))
		}
	}

	// Flags
	if len(flags) > 0 {
		sb.WriteString("\nFlags:\n")
		for _, param := range flags {
			defaultStr := ""
			if param.Default != nil {
				defaultStr = fmt.Sprintf(" (default: %v)", param.Default)
			}

			// Show both short and long flags
			flagStr := param.Flag
			if param.Short != "" {
				flagStr = param.Short + ", " + param.Flag
			}
			sb.WriteString(fmt.Sprintf("  %-20s %s%s\n", flagStr, param.Description, defaultStr))
		}
	}

	return sb.String()
}

// parseArgs parses CLI arguments according to the command schema
func (p *SchemaProvider) parseArgs(cmd *CommandSchema, args []string) (map[string]any, error) {
	result := make(map[string]any)

	// Apply defaults first
	for _, param := range cmd.Params {
		if param.Default != nil {
			result[param.Name] = param.Default
		}
	}

	// Build flag lookup (both long and short flags)
	flagParams := make(map[string]*ParamSchema)
	for _, param := range cmd.Params {
		if param.Flag != "" {
			flagParams[param.Flag] = param
		}
		if param.Short != "" {
			flagParams[param.Short] = param
		}
	}

	// Get positional params in order
	positional := cmd.GetPositionalParams()
	posIdx := 0

	// Parse arguments
	for i := 0; i < len(args); i++ {
		arg := args[i]

		// Check if it's a flag (-- or - prefix)
		if strings.HasPrefix(arg, "-") {
			// Handle --flag=value or -f=value syntax
			var flagName, flagValue string
			if eqIdx := strings.Index(arg, "="); eqIdx != -1 {
				flagName = arg[:eqIdx]
				flagValue = arg[eqIdx+1:]
			} else {
				flagName = arg
				// For non-bool flags, consume next arg as value
				param, ok := flagParams[flagName]
				if !ok {
					return nil, fmt.Errorf("unknown flag: %s", flagName)
				}
				if param.Type == "bool" {
					flagValue = "true"
				} else {
					if i+1 >= len(args) {
						return nil, fmt.Errorf("flag %s requires a value", flagName)
					}
					i++
					flagValue = args[i]
				}
			}

			param, ok := flagParams[flagName]
			if !ok {
				return nil, fmt.Errorf("unknown flag: %s", flagName)
			}

			value, err := parseValue(flagValue, param.Type)
			if err != nil {
				return nil, fmt.Errorf("flag %s: %w", flagName, err)
			}
			result[param.Name] = value
		} else {
			// Positional argument
			if posIdx >= len(positional) {
				return nil, fmt.Errorf("unexpected argument: %s", arg)
			}

			param := positional[posIdx]
			value, err := parseValue(arg, param.Type)
			if err != nil {
				return nil, fmt.Errorf("argument %s: %w", param.Name, err)
			}
			result[param.Name] = value
			posIdx++
		}
	}

	// Check required parameters
	for _, param := range cmd.Params {
		if param.Required {
			if _, ok := result[param.Name]; !ok {
				return nil, fmt.Errorf("missing required parameter: %s", param.Name)
			}
		}
	}

	return result, nil
}

// parseValue converts a string value to the appropriate type
func parseValue(s string, typ string) (any, error) {
	switch typ {
	case "string":
		return s, nil
	case "int":
		return strconv.Atoi(s)
	case "float":
		return strconv.ParseFloat(s, 64)
	case "bool":
		return strconv.ParseBool(s)
	default:
		return nil, fmt.Errorf("unknown type: %s", typ)
	}
}
