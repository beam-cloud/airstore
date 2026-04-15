package tools

import (
	"context"
	"encoding/json"
	"io"
	"testing"

	"github.com/beam-cloud/airstore/pkg/auth"
	"github.com/beam-cloud/airstore/pkg/types"
)

type resolverTestBackend struct {
	settings    *types.WorkspaceToolSettings
	connections map[string]*types.IntegrationConnection
}

func (b *resolverTestBackend) ListWorkspaceTools(ctx context.Context, workspaceId uint) ([]*types.WorkspaceTool, error) {
	return nil, nil
}

func (b *resolverTestBackend) GetWorkspaceToolByName(ctx context.Context, workspaceId uint, name string) (*types.WorkspaceTool, error) {
	return nil, &types.ErrWorkspaceToolNotFound{Name: name}
}

func (b *resolverTestBackend) GetWorkspaceToolSettings(ctx context.Context, workspaceId uint) (*types.WorkspaceToolSettings, error) {
	return b.settings, nil
}

func (b *resolverTestBackend) GetConnection(ctx context.Context, workspaceId uint, memberId uint, integrationType string) (*types.IntegrationConnection, error) {
	if b.connections == nil {
		return nil, nil
	}
	return b.connections[integrationType], nil
}

type noopProvider struct {
	name string
}

func (p *noopProvider) Name() string { return p.name }
func (p *noopProvider) Help() string { return "noop" }
func (p *noopProvider) Execute(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	return nil
}
func (p *noopProvider) ExecuteWithContext(ctx context.Context, execCtx *ExecutionContext, args []string, stdout, stderr io.Writer) error {
	return nil
}

func resolverAuthCtx() context.Context {
	info := &types.AuthInfo{
		TokenType: types.TokenTypeWorkspaceMember,
		Workspace: &types.WorkspaceInfo{Id: 1, ExternalId: "ws-1", Name: "ws"},
		Member:    &types.MemberInfo{Id: 7, ExternalId: "m-7", Email: "m@example.com", Role: types.RoleAdmin},
	}
	return auth.WithAuthInfo(context.Background(), info)
}

func TestWorkspaceToolResolver_GlobalToolGatedByCapability(t *testing.T) {
	registry := NewRegistry()
	registry.Register(&noopProvider{name: "github"})

	creds := &types.IntegrationCredentials{
		AccessToken: "token",
		Extra: map[string]string{
			types.CredentialMetaCapabilities: "source_read,source_write",
		},
	}
	data, _ := json.Marshal(creds)

	backend := &resolverTestBackend{
		settings: types.NewWorkspaceToolSettings(1),
		connections: map[string]*types.IntegrationConnection{
			"github": {IntegrationType: "github", Credentials: data},
		},
	}
	resolver := NewWorkspaceToolResolver(registry, backend)

	list, err := resolver.List(resolverAuthCtx())
	if err != nil {
		t.Fatalf("resolver list: %v", err)
	}
	if len(list) != 1 || !list[0].Enabled {
		t.Fatalf("expected github tool to be enabled, got %#v", list)
	}

	provider, err := resolver.Get(resolverAuthCtx(), "github")
	if err != nil {
		t.Fatalf("resolver get: %v", err)
	}
	if provider == nil {
		t.Fatalf("expected provider for github when capability is present")
	}
}

func TestWorkspaceToolResolver_AuthNoneToolEnabled(t *testing.T) {
	registry := NewRegistry()
	registry.Register(&noopProvider{name: "agentmail"})

	// No connections — AgentMail uses server-level auth, not per-user OAuth.
	backend := &resolverTestBackend{
		settings: types.NewWorkspaceToolSettings(1),
	}
	resolver := NewWorkspaceToolResolver(registry, backend)

	list, err := resolver.List(resolverAuthCtx())
	if err != nil {
		t.Fatalf("resolver list: %v", err)
	}
	if len(list) != 1 || !list[0].Enabled {
		t.Fatalf("expected agentmail tool to be enabled (AuthNone), got %#v", list)
	}

	provider, err := resolver.Get(resolverAuthCtx(), "agentmail")
	if err != nil {
		t.Fatalf("resolver get: %v", err)
	}
	if provider == nil {
		t.Fatalf("expected provider for agentmail — AuthNone tools should not require a connection")
	}
}

func TestWorkspaceToolResolver_GlobalToolDisabledWithoutCapability(t *testing.T) {
	registry := NewRegistry()
	registry.Register(&noopProvider{name: "github"})

	creds := &types.IntegrationCredentials{
		AccessToken: "token",
		Extra: map[string]string{
			types.CredentialMetaCapabilities: "source_read",
		},
	}
	data, _ := json.Marshal(creds)

	backend := &resolverTestBackend{
		settings: types.NewWorkspaceToolSettings(1),
		connections: map[string]*types.IntegrationConnection{
			"github": {IntegrationType: "github", Credentials: data},
		},
	}
	resolver := NewWorkspaceToolResolver(registry, backend)

	list, err := resolver.List(resolverAuthCtx())
	if err != nil {
		t.Fatalf("resolver list: %v", err)
	}
	if len(list) != 1 || list[0].Enabled {
		t.Fatalf("expected github tool to be disabled without source_write capability, got %#v", list)
	}

	provider, err := resolver.Get(resolverAuthCtx(), "github")
	if err != nil {
		t.Fatalf("resolver get: %v", err)
	}
	if provider != nil {
		t.Fatalf("expected nil provider when source_write capability is missing")
	}
}
