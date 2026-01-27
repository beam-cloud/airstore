package tools

import (
	"context"
	"io"
	"sync"

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

// ToolClient is implemented by tool backends
type ToolClient interface {
	Name() types.ToolName
	Execute(ctx context.Context, command string, args map[string]any, creds *types.IntegrationCredentials, stdout, stderr io.Writer) error
}

// Registry manages registered tool providers
type Registry struct {
	mu        sync.RWMutex
	providers map[string]ToolProvider
}

func NewRegistry() *Registry {
	return &Registry{providers: make(map[string]ToolProvider)}
}

func (r *Registry) Register(p ToolProvider) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.providers[p.Name()] = p
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
