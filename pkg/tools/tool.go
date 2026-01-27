package tools

import (
	"context"
	"fmt"
	"io"
	"strconv"
	"sync"

	"github.com/beam-cloud/airstore/pkg/types"
)

// Tool is the unified interface for tool implementations.
type Tool interface {
	Name() string
	Description() string
	Commands() map[string]*CommandDef
	Execute(ctx context.Context, execCtx *ExecutionContext, command string, args map[string]any, stdout, stderr io.Writer) error
}

// CommandDef defines a command within a tool
type CommandDef struct {
	Description string
	Params      []*ParamDef
}

// ParamDef defines a parameter for a command
type ParamDef struct {
	Name        string
	Type        string // string, int, bool, float
	Required    bool
	Position    *int   // positional arg index (nil = flag only)
	Short       string // short flag (e.g., "-n")
	Flag        string // long flag (e.g., "--limit")
	Default     any
	Description string
}

// RequiresCredentials returns true if the tool needs stored credentials
func RequiresCredentials(name string) bool {
	return types.RequiresAuth(types.ToolName(name))
}

// Global tool registry
var (
	globalRegistry     = make(map[string]Tool)
	globalRegistryLock sync.RWMutex
)

func RegisterTool(t Tool) {
	globalRegistryLock.Lock()
	defer globalRegistryLock.Unlock()
	globalRegistry[t.Name()] = t
}

func GetRegisteredTools() map[string]Tool {
	globalRegistryLock.RLock()
	defer globalRegistryLock.RUnlock()
	result := make(map[string]Tool, len(globalRegistry))
	for k, v := range globalRegistry {
		result[k] = v
	}
	return result
}

func GetRegisteredTool(name string) (Tool, bool) {
	globalRegistryLock.RLock()
	defer globalRegistryLock.RUnlock()
	t, ok := globalRegistry[name]
	return t, ok
}

// ToolAdapter wraps a Tool to implement ToolProvider
type ToolAdapter struct {
	tool Tool
}

func NewToolAdapter(t Tool) ToolProvider {
	return &ToolAdapter{tool: t}
}

func (a *ToolAdapter) Name() string {
	return a.tool.Name()
}

func (a *ToolAdapter) Help() string {
	return a.toolHelp()
}

func (a *ToolAdapter) Execute(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	return a.ExecuteWithContext(ctx, nil, args, stdout, stderr)
}

func (a *ToolAdapter) ExecuteWithContext(ctx context.Context, execCtx *ExecutionContext, args []string, stdout, stderr io.Writer) error {
	// No args or help flag -> show tool help
	if len(args) == 0 || args[0] == "--help" || args[0] == "-h" {
		fmt.Fprint(stdout, a.toolHelp())
		return nil
	}

	command := args[0]
	cmdDef, ok := a.tool.Commands()[command]
	if !ok {
		fmt.Fprintf(stderr, "Unknown command: %s\n\n", command)
		fmt.Fprint(stderr, a.toolHelp())
		return fmt.Errorf("unknown command: %s", command)
	}

	// Help for specific command
	cmdArgs := args[1:]
	if hasHelpFlag(cmdArgs) {
		fmt.Fprint(stdout, a.commandHelp(command, cmdDef))
		return nil
	}

	// Parse and validate args
	parsed, err := parseAndValidate(cmdDef, cmdArgs)
	if err != nil {
		fmt.Fprintf(stderr, "Error: %s\n\n", err)
		fmt.Fprint(stderr, a.commandHelp(command, cmdDef))
		return err
	}

	return a.tool.Execute(ctx, execCtx, command, parsed, stdout, stderr)
}

func hasHelpFlag(args []string) bool {
	for _, arg := range args {
		if arg == "--help" || arg == "-h" {
			return true
		}
	}
	return false
}

// toolHelp generates help for the tool (list of commands)
func (a *ToolAdapter) toolHelp() string {
	var out string
	out += fmt.Sprintf("%s - %s\n\n", a.tool.Name(), a.tool.Description())
	out += "Commands:\n"
	for name, cmd := range a.tool.Commands() {
		out += fmt.Sprintf("  %-16s %s\n", name, cmd.Description)
	}
	out += fmt.Sprintf("\nRun '%s <command> --help' for command usage.\n", a.tool.Name())
	return out
}

// commandHelp generates help for a specific command
func (a *ToolAdapter) commandHelp(cmdName string, cmd *CommandDef) string {
	var positional, flags []*ParamDef
	for _, p := range cmd.Params {
		if p.Position != nil {
			positional = append(positional, p)
		} else {
			flags = append(flags, p)
		}
	}

	// Usage line
	out := fmt.Sprintf("Usage: %s %s", a.tool.Name(), cmdName)
	for _, p := range positional {
		if p.Required {
			out += fmt.Sprintf(" <%s>", p.Name)
		} else {
			out += fmt.Sprintf(" [%s]", p.Name)
		}
	}
	if len(flags) > 0 {
		out += " [flags]"
	}
	out += "\n\n"

	if cmd.Description != "" {
		out += cmd.Description + "\n\n"
	}

	if len(positional) > 0 {
		out += "Arguments:\n"
		for _, p := range positional {
			req := ""
			if p.Required {
				req = " (required)"
			}
			out += fmt.Sprintf("  %-16s %s%s\n", p.Name, p.Description, req)
		}
		out += "\n"
	}

	if len(flags) > 0 {
		out += "Flags:\n"
		for _, p := range flags {
			flag := ""
			if p.Short != "" && p.Flag != "" {
				flag = fmt.Sprintf("%s, %s", p.Short, p.Flag)
			} else if p.Flag != "" {
				flag = p.Flag
			} else if p.Short != "" {
				flag = p.Short
			}
			def := ""
			if p.Default != nil {
				def = fmt.Sprintf(" (default: %v)", p.Default)
			}
			out += fmt.Sprintf("  %-16s %s%s\n", flag, p.Description, def)
		}
	}

	return out
}

// parseAndValidate parses args and validates required params
func parseAndValidate(cmd *CommandDef, args []string) (map[string]any, error) {
	result := make(map[string]any)

	// Apply defaults
	for _, p := range cmd.Params {
		if p.Default != nil {
			result[p.Name] = p.Default
		}
	}

	// Build lookups
	flagParams := make(map[string]*ParamDef)
	var positional []*ParamDef
	for _, p := range cmd.Params {
		if p.Flag != "" {
			flagParams[p.Flag] = p
		}
		if p.Short != "" {
			flagParams[p.Short] = p
		}
		if p.Position != nil {
			positional = append(positional, p)
		}
	}

	// Parse args
	posIdx := 0
	for i := 0; i < len(args); i++ {
		arg := args[i]

		if len(arg) > 0 && arg[0] == '-' {
			param, ok := flagParams[arg]
			if !ok {
				continue // ignore unknown flags
			}
			if param.Type == "bool" {
				result[param.Name] = true
			} else if i+1 < len(args) {
				i++
				result[param.Name] = convertValue(args[i], param.Type)
			}
		} else if posIdx < len(positional) {
			param := positional[posIdx]
			result[param.Name] = convertValue(arg, param.Type)
			posIdx++
		}
	}

	// Validate required params
	for _, p := range cmd.Params {
		if p.Required {
			if _, ok := result[p.Name]; !ok {
				return nil, fmt.Errorf("missing required argument: %s", p.Name)
			}
		}
	}

	return result, nil
}

func convertValue(s string, typ string) any {
	switch typ {
	case "int":
		if v, err := strconv.Atoi(s); err == nil {
			return v
		}
		return 0
	case "bool":
		return s == "true" || s == "1"
	case "float":
		if v, err := strconv.ParseFloat(s, 64); err == nil {
			return v
		}
		return 0.0
	default:
		return s
	}
}
