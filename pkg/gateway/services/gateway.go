package services

import (
	"context"
	"time"

	"github.com/beam-cloud/airstore/pkg/auth"
	"github.com/beam-cloud/airstore/pkg/repository"
	"github.com/beam-cloud/airstore/pkg/types"
	pb "github.com/beam-cloud/airstore/proto"
)

type GatewayService struct {
	pb.UnimplementedGatewayServiceServer
	backend repository.BackendRepository
}

func NewGatewayService(backend repository.BackendRepository) *GatewayService {
	return &GatewayService{backend: backend}
}

// requireGatewayAuth checks if the request is authenticated with the gateway/admin token
func requireGatewayAuth(ctx context.Context) error {
	rc := auth.FromContext(ctx)
	if rc == nil || !rc.IsGatewayAuth {
		return &authError{message: "admin access required"}
	}
	return nil
}

// requireWorkspaceAccess checks if the request has access to the specified workspace
func requireWorkspaceAccess(ctx context.Context, workspaceExtId string) error {
	rc := auth.FromContext(ctx)
	if rc == nil {
		return &authError{message: "authentication required"}
	}
	// Gateway token can access any workspace
	if rc.IsGatewayAuth {
		return nil
	}
	// Workspace token must match the workspace
	if rc.WorkspaceExt != workspaceExtId {
		return &authError{message: "token does not have access to this workspace"}
	}
	return nil
}

// requireAdminRole checks if the request has admin role within the workspace
func requireAdminRole(ctx context.Context) error {
	if !auth.IsAdmin(ctx) {
		return &authError{message: "admin access required"}
	}
	return nil
}

// authError is a simple error type for authorization failures
type authError struct {
	message string
}

func (e *authError) Error() string {
	return e.message
}

// Workspaces

func (s *GatewayService) CreateWorkspace(ctx context.Context, req *pb.CreateWorkspaceRequest) (*pb.WorkspaceResponse, error) {
	// Workspace creation requires gateway/admin auth
	if err := requireGatewayAuth(ctx); err != nil {
		return &pb.WorkspaceResponse{Ok: false, Error: err.Error()}, nil
	}

	ws, err := s.backend.CreateWorkspace(ctx, req.Name)
	if err != nil {
		return &pb.WorkspaceResponse{Ok: false, Error: err.Error()}, nil
	}
	return &pb.WorkspaceResponse{Ok: true, Workspace: workspaceToPb(ws)}, nil
}

func (s *GatewayService) ListWorkspaces(ctx context.Context, req *pb.ListWorkspacesRequest) (*pb.ListWorkspacesResponse, error) {
	// Listing all workspaces requires gateway/admin auth
	if err := requireGatewayAuth(ctx); err != nil {
		return &pb.ListWorkspacesResponse{Ok: false, Error: err.Error()}, nil
	}

	workspaces, err := s.backend.ListWorkspaces(ctx)
	if err != nil {
		return &pb.ListWorkspacesResponse{Ok: false, Error: err.Error()}, nil
	}

	pbWorkspaces := make([]*pb.Workspace, 0, len(workspaces))
	for _, ws := range workspaces {
		pbWorkspaces = append(pbWorkspaces, workspaceToPb(ws))
	}
	return &pb.ListWorkspacesResponse{Ok: true, Workspaces: pbWorkspaces}, nil
}

func (s *GatewayService) GetWorkspace(ctx context.Context, req *pb.GetWorkspaceRequest) (*pb.WorkspaceResponse, error) {
	// Getting a workspace requires gateway auth or workspace token for that workspace
	if err := requireWorkspaceAccess(ctx, req.Id); err != nil {
		return &pb.WorkspaceResponse{Ok: false, Error: err.Error()}, nil
	}

	ws, err := s.backend.GetWorkspaceByExternalId(ctx, req.Id)
	if err != nil {
		return &pb.WorkspaceResponse{Ok: false, Error: err.Error()}, nil
	}
	if ws == nil {
		return &pb.WorkspaceResponse{Ok: false, Error: "workspace not found"}, nil
	}
	return &pb.WorkspaceResponse{Ok: true, Workspace: workspaceToPb(ws)}, nil
}

func (s *GatewayService) DeleteWorkspace(ctx context.Context, req *pb.DeleteWorkspaceRequest) (*pb.DeleteResponse, error) {
	// Workspace deletion requires gateway/admin auth
	if err := requireGatewayAuth(ctx); err != nil {
		return &pb.DeleteResponse{Ok: false, Error: err.Error()}, nil
	}

	ws, err := s.backend.GetWorkspaceByExternalId(ctx, req.Id)
	if err != nil || ws == nil {
		return &pb.DeleteResponse{Ok: false, Error: "workspace not found"}, nil
	}
	if err := s.backend.DeleteWorkspace(ctx, ws.Id); err != nil {
		return &pb.DeleteResponse{Ok: false, Error: err.Error()}, nil
	}
	return &pb.DeleteResponse{Ok: true}, nil
}

// Members

func (s *GatewayService) AddMember(ctx context.Context, req *pb.AddMemberRequest) (*pb.MemberResponse, error) {
	// Adding members requires workspace access + admin role
	if err := requireWorkspaceAccess(ctx, req.WorkspaceId); err != nil {
		return &pb.MemberResponse{Ok: false, Error: err.Error()}, nil
	}
	if err := requireAdminRole(ctx); err != nil {
		return &pb.MemberResponse{Ok: false, Error: err.Error()}, nil
	}

	ws, err := s.backend.GetWorkspaceByExternalId(ctx, req.WorkspaceId)
	if err != nil || ws == nil {
		return &pb.MemberResponse{Ok: false, Error: "workspace not found"}, nil
	}

	role := types.MemberRole(req.Role)
	if role == "" {
		role = types.RoleMember
	}

	member, err := s.backend.CreateMember(ctx, ws.Id, req.Email, req.Name, role)
	if err != nil {
		return &pb.MemberResponse{Ok: false, Error: err.Error()}, nil
	}
	return &pb.MemberResponse{Ok: true, Member: memberToPb(member, ws.ExternalId)}, nil
}

func (s *GatewayService) ListMembers(ctx context.Context, req *pb.ListMembersRequest) (*pb.ListMembersResponse, error) {
	// Listing members requires workspace access + admin role
	if err := requireWorkspaceAccess(ctx, req.WorkspaceId); err != nil {
		return &pb.ListMembersResponse{Ok: false, Error: err.Error()}, nil
	}
	if err := requireAdminRole(ctx); err != nil {
		return &pb.ListMembersResponse{Ok: false, Error: err.Error()}, nil
	}

	ws, err := s.backend.GetWorkspaceByExternalId(ctx, req.WorkspaceId)
	if err != nil || ws == nil {
		return &pb.ListMembersResponse{Ok: false, Error: "workspace not found"}, nil
	}

	members, err := s.backend.ListMembers(ctx, ws.Id)
	if err != nil {
		return &pb.ListMembersResponse{Ok: false, Error: err.Error()}, nil
	}

	pbMembers := make([]*pb.Member, 0, len(members))
	for _, m := range members {
		pbMembers = append(pbMembers, memberToPb(&m, ws.ExternalId))
	}
	return &pb.ListMembersResponse{Ok: true, Members: pbMembers}, nil
}

func (s *GatewayService) RemoveMember(ctx context.Context, req *pb.RemoveMemberRequest) (*pb.DeleteResponse, error) {
	// Removing members requires admin role
	// Note: We can't verify workspace access here since we only have the member ID
	// The admin check ensures only admins can remove members
	if err := requireAdminRole(ctx); err != nil {
		return &pb.DeleteResponse{Ok: false, Error: err.Error()}, nil
	}

	if err := s.backend.DeleteMember(ctx, req.Id); err != nil {
		return &pb.DeleteResponse{Ok: false, Error: err.Error()}, nil
	}
	return &pb.DeleteResponse{Ok: true}, nil
}

// Tokens

func (s *GatewayService) CreateToken(ctx context.Context, req *pb.CreateTokenRequest) (*pb.CreateTokenResponse, error) {
	// Creating tokens requires workspace access + admin role
	if err := requireWorkspaceAccess(ctx, req.WorkspaceId); err != nil {
		return &pb.CreateTokenResponse{Ok: false, Error: err.Error()}, nil
	}
	if err := requireAdminRole(ctx); err != nil {
		return &pb.CreateTokenResponse{Ok: false, Error: err.Error()}, nil
	}

	ws, err := s.backend.GetWorkspaceByExternalId(ctx, req.WorkspaceId)
	if err != nil || ws == nil {
		return &pb.CreateTokenResponse{Ok: false, Error: "workspace not found"}, nil
	}

	member, err := s.backend.GetMember(ctx, req.MemberId)
	if err != nil || member == nil {
		return &pb.CreateTokenResponse{Ok: false, Error: "member not found"}, nil
	}
	if member.WorkspaceId != ws.Id {
		return &pb.CreateTokenResponse{Ok: false, Error: "member not in workspace"}, nil
	}

	var expiresAt *time.Time
	if req.ExpiresInSeconds > 0 {
		t := time.Now().Add(time.Duration(req.ExpiresInSeconds) * time.Second)
		expiresAt = &t
	}

	name := req.Name
	if name == "" {
		name = "API Token"
	}

	token, raw, err := s.backend.CreateToken(ctx, ws.Id, member.Id, name, expiresAt, types.TokenTypeWorkspaceMember)
	if err != nil {
		return &pb.CreateTokenResponse{Ok: false, Error: err.Error()}, nil
	}

	return &pb.CreateTokenResponse{
		Ok:    true,
		Token: raw,
		Info:  tokenToPb(token),
	}, nil
}

func (s *GatewayService) ListTokens(ctx context.Context, req *pb.ListTokensRequest) (*pb.ListTokensResponse, error) {
	// Listing tokens requires workspace access + admin role
	if err := requireWorkspaceAccess(ctx, req.WorkspaceId); err != nil {
		return &pb.ListTokensResponse{Ok: false, Error: err.Error()}, nil
	}
	if err := requireAdminRole(ctx); err != nil {
		return &pb.ListTokensResponse{Ok: false, Error: err.Error()}, nil
	}

	ws, err := s.backend.GetWorkspaceByExternalId(ctx, req.WorkspaceId)
	if err != nil || ws == nil {
		return &pb.ListTokensResponse{Ok: false, Error: "workspace not found"}, nil
	}

	tokens, err := s.backend.ListTokens(ctx, ws.Id)
	if err != nil {
		return &pb.ListTokensResponse{Ok: false, Error: err.Error()}, nil
	}

	pbTokens := make([]*pb.Token, 0, len(tokens))
	for _, t := range tokens {
		pbTokens = append(pbTokens, tokenToPb(&t))
	}
	return &pb.ListTokensResponse{Ok: true, Tokens: pbTokens}, nil
}

func (s *GatewayService) RevokeToken(ctx context.Context, req *pb.RevokeTokenRequest) (*pb.DeleteResponse, error) {
	// Revoking tokens requires admin role
	if err := requireAdminRole(ctx); err != nil {
		return &pb.DeleteResponse{Ok: false, Error: err.Error()}, nil
	}

	if err := s.backend.RevokeToken(ctx, req.Id); err != nil {
		return &pb.DeleteResponse{Ok: false, Error: err.Error()}, nil
	}
	return &pb.DeleteResponse{Ok: true}, nil
}

// Connections

func (s *GatewayService) AddConnection(ctx context.Context, req *pb.AddConnectionRequest) (*pb.ConnectionResponse, error) {
	// Adding connections requires workspace access
	if err := requireWorkspaceAccess(ctx, req.WorkspaceId); err != nil {
		return &pb.ConnectionResponse{Ok: false, Error: err.Error()}, nil
	}

	ws, err := s.backend.GetWorkspaceByExternalId(ctx, req.WorkspaceId)
	if err != nil || ws == nil {
		return &pb.ConnectionResponse{Ok: false, Error: "workspace not found"}, nil
	}

	var memberId *uint
	if req.MemberId != "" {
		// Personal connection - require admin or self
		member, err := s.backend.GetMember(ctx, req.MemberId)
		if err != nil || member == nil {
			return &pb.ConnectionResponse{Ok: false, Error: "member not found"}, nil
		}
		if member.WorkspaceId != ws.Id {
			return &pb.ConnectionResponse{Ok: false, Error: "member not in workspace"}, nil
		}

		// Check authorization: admin can create for anyone, members only for themselves
		if !auth.IsAdmin(ctx) && member.Id != auth.MemberId(ctx) {
			return &pb.ConnectionResponse{Ok: false, Error: "cannot create connection for another member"}, nil
		}

		memberId = &member.Id
	} else {
		// Shared connection - require admin
		if err := requireAdminRole(ctx); err != nil {
			return &pb.ConnectionResponse{Ok: false, Error: "admin access required for shared connections"}, nil
		}
	}

	creds := &types.IntegrationCredentials{
		AccessToken: req.AccessToken,
		APIKey:      req.ApiKey,
	}

	conn, err := s.backend.SaveConnection(ctx, ws.Id, memberId, req.IntegrationType, creds, req.Scope)
	if err != nil {
		return &pb.ConnectionResponse{Ok: false, Error: err.Error()}, nil
	}

	return &pb.ConnectionResponse{Ok: true, Connection: connectionToPb(conn, ws.ExternalId)}, nil
}

func (s *GatewayService) ListConnections(ctx context.Context, req *pb.ListConnectionsRequest) (*pb.ListConnectionsResponse, error) {
	// Listing connections requires workspace access
	if err := requireWorkspaceAccess(ctx, req.WorkspaceId); err != nil {
		return &pb.ListConnectionsResponse{Ok: false, Error: err.Error()}, nil
	}

	ws, err := s.backend.GetWorkspaceByExternalId(ctx, req.WorkspaceId)
	if err != nil || ws == nil {
		return &pb.ListConnectionsResponse{Ok: false, Error: "workspace not found"}, nil
	}

	conns, err := s.backend.ListConnections(ctx, ws.Id)
	if err != nil {
		return &pb.ListConnectionsResponse{Ok: false, Error: err.Error()}, nil
	}

	pbConns := make([]*pb.Connection, 0, len(conns))
	for _, c := range conns {
		pbConns = append(pbConns, connectionToPb(&c, ws.ExternalId))
	}
	return &pb.ListConnectionsResponse{Ok: true, Connections: pbConns}, nil
}

func (s *GatewayService) RemoveConnection(ctx context.Context, req *pb.RemoveConnectionRequest) (*pb.DeleteResponse, error) {
	// Fetch connection to check permissions
	conn, err := s.backend.GetConnectionByExternalId(ctx, req.Id)
	if err != nil {
		return &pb.DeleteResponse{Ok: false, Error: err.Error()}, nil
	}
	if conn == nil {
		return &pb.DeleteResponse{Ok: false, Error: "connection not found"}, nil
	}

	// Check authorization: shared connections require admin, personal require admin or owner
	if conn.IsShared() {
		if !auth.IsAdmin(ctx) {
			return &pb.DeleteResponse{Ok: false, Error: "admin access required for shared connections"}, nil
		}
	} else {
		// Personal connection
		if !auth.IsAdmin(ctx) && *conn.MemberId != auth.MemberId(ctx) {
			return &pb.DeleteResponse{Ok: false, Error: "cannot delete another member's connection"}, nil
		}
	}

	if err := s.backend.DeleteConnection(ctx, req.Id); err != nil {
		return &pb.DeleteResponse{Ok: false, Error: err.Error()}, nil
	}
	return &pb.DeleteResponse{Ok: true}, nil
}

// Helpers

func workspaceToPb(ws *types.Workspace) *pb.Workspace {
	return &pb.Workspace{
		Id:        ws.ExternalId,
		Name:      ws.Name,
		CreatedAt: ws.CreatedAt.Format(time.RFC3339),
		UpdatedAt: ws.UpdatedAt.Format(time.RFC3339),
	}
}

func memberToPb(m *types.WorkspaceMember, workspaceExtId string) *pb.Member {
	return &pb.Member{
		Id:          m.ExternalId,
		WorkspaceId: workspaceExtId,
		Email:       m.Email,
		Name:        m.Name,
		Role:        string(m.Role),
		CreatedAt:   m.CreatedAt.Format(time.RFC3339),
	}
}

func tokenToPb(t *types.WorkspaceToken) *pb.Token {
	token := &pb.Token{
		Id:        t.ExternalId,
		MemberId:  t.ExternalId, // Proto uses external ID
		Name:      t.Name,
		CreatedAt: t.CreatedAt.Format(time.RFC3339),
	}
	if t.ExpiresAt != nil {
		token.ExpiresAt = t.ExpiresAt.Format(time.RFC3339)
	}
	if t.LastUsedAt != nil {
		token.LastUsedAt = t.LastUsedAt.Format(time.RFC3339)
	}
	return token
}

func connectionToPb(c *types.IntegrationConnection, workspaceExtId string) *pb.Connection {
	conn := &pb.Connection{
		Id:              c.ExternalId,
		WorkspaceId:     workspaceExtId,
		IntegrationType: c.IntegrationType,
		Scope:           c.Scope,
		IsShared:        c.MemberId == nil,
		CreatedAt:       c.CreatedAt.Format(time.RFC3339),
	}
	return conn
}
