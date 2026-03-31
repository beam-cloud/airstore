package tools

import (
	"context"
	"io"
	"io/fs"
	"path/filepath"
	"strings"
	"sync"

	"github.com/beam-cloud/airstore/pkg/tools/definitions"
	"github.com/beam-cloud/airstore/pkg/types"
)

// ExecutionContext contains identity and credentials for tool execution
type ExecutionContext struct {
	WorkspaceId   uint
	WorkspaceName string
	MemberId      uint
	MemberEmail   string
	Credentials   *types.IntegrationCredentials
}

// ToolProvider defines the interface for tool implementations
type ToolProvider interface {
	Name() string
	Help() string
	Execute(ctx context.Context, args []string, stdout, stderr io.Writer) error
	ExecuteWithContext(ctx context.Context, execCtx *ExecutionContext, args []string, stdout, stderr io.Writer) error
}

// LocalToolProvider is an optional interface for tools that execute locally
// inside the sandbox rather than proxying through the gateway.
// The FUSE layer serves a wrapper script for these tools instead of the gRPC shim.
type LocalToolProvider interface {
	LocalCommand() string
}

// ToolClient is implemented by tool backends
type ToolClient interface {
	Name() types.IntegrationName
	Execute(ctx context.Context, command string, args map[string]any, creds *types.IntegrationCredentials, stdout, stderr io.Writer) error
}

// Registry manages registered tool providers
type Registry struct {
	mu        sync.RWMutex
	providers map[string]ToolProvider
	schemas   map[string]*ToolSchema
}

func NewRegistry() *Registry {
	return &Registry{
		providers: make(map[string]ToolProvider),
		schemas:   make(map[string]*ToolSchema),
	}
}

func (r *Registry) Register(p ToolProvider) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.providers[p.Name()] = p
}

func (r *Registry) RegisterSchema(name string, schema *ToolSchema) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.schemas[name] = schema
}

// IsValidWriteCommand returns true if the tool+subcommand is marked write in
// its schema AND the supplied args satisfy all required params. Invalid calls
// (missing args, --help probes) pass through ungated so the tool returns a
// proper error and the agent self-corrects.
func (r *Registry) IsValidWriteCommand(toolName string, args []string) bool {
	if len(args) == 0 {
		return false
	}
	r.mu.RLock()
	schema := r.schemas[toolName]
	r.mu.RUnlock()
	if schema == nil {
		return false
	}
	cmd, ok := schema.Commands[args[0]]
	if !ok || !cmd.Write {
		return false
	}
	return cmd.HasRequiredArgs(args[1:])
}

func (r *Registry) Get(name string) ToolProvider {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.providers[name]
}

func (r *Registry) List() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	names := make([]string, 0, len(r.providers))
	for name := range r.providers {
		names = append(names, name)
	}
	return names
}

func (r *Registry) Has(name string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	_, ok := r.providers[name]
	return ok
}

// GetCommandSchema returns the schema for a specific tool command, or nil.
func (r *Registry) GetCommandSchema(toolName, command string) *CommandSchema {
	r.mu.RLock()
	schema := r.schemas[toolName]
	r.mu.RUnlock()
	if schema == nil {
		return nil
	}
	return schema.Commands[command]
}

// ---------------------------------------------------------------------------
// Global output-type lookup from embedded definitions
// ---------------------------------------------------------------------------

var (
	outputTypeMap     map[string]map[string]string // toolName -> command -> outputType
	outputTypeMapOnce sync.Once
)

func loadOutputTypeMap() map[string]map[string]string {
	m := make(map[string]map[string]string)
	_ = fs.WalkDir(definitions.FS, ".", func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}
		ext := filepath.Ext(path)
		if ext != ".yaml" && ext != ".yml" {
			return nil
		}
		data, readErr := definitions.FS.ReadFile(path)
		if readErr != nil {
			return nil
		}
		schema, parseErr := ParseSchema(data)
		if parseErr != nil {
			return nil
		}
		for cmdName, cmd := range schema.Commands {
			if cmd.OutputType != "" {
				if m[schema.Name] == nil {
					m[schema.Name] = make(map[string]string)
				}
				m[schema.Name][cmdName] = cmd.OutputType
			}
		}
		return nil
	})
	return m
}

// CommandOutputType returns the deterministic output type declared in a tool
// definition, or "" if none is declared. Safe to call from any package; lazily
// loads and caches the embedded YAML definitions on first call.
func CommandOutputType(toolName, command string) string {
	outputTypeMapOnce.Do(func() { outputTypeMap = loadOutputTypeMap() })
	if cmds, ok := outputTypeMap[toolName]; ok {
		return cmds[strings.TrimSpace(command)]
	}
	return ""
}
