package services

import (
	"context"
	"time"

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

// Workspaces

func (s *GatewayService) CreateWorkspace(ctx context.Context, req *pb.CreateWorkspaceRequest) (*pb.WorkspaceResponse, error) {
	ws, err := s.backend.CreateWorkspace(ctx, req.Name)
	if err != nil {
		return &pb.WorkspaceResponse{Ok: false, Error: err.Error()}, nil
	}
	return &pb.WorkspaceResponse{Ok: true, Workspace: workspaceToPb(ws)}, nil
}

func (s *GatewayService) ListWorkspaces(ctx context.Context, req *pb.ListWorkspacesRequest) (*pb.ListWorkspacesResponse, error) {
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
	return &pb.MemberResponse{Ok: true, Member: memberToPb(member)}, nil
}

func (s *GatewayService) ListMembers(ctx context.Context, req *pb.ListMembersRequest) (*pb.ListMembersResponse, error) {
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
		pbMembers = append(pbMembers, memberToPb(&m))
	}
	return &pb.ListMembersResponse{Ok: true, Members: pbMembers}, nil
}

func (s *GatewayService) RemoveMember(ctx context.Context, req *pb.RemoveMemberRequest) (*pb.DeleteResponse, error) {
	if err := s.backend.DeleteMember(ctx, req.Id); err != nil {
		return &pb.DeleteResponse{Ok: false, Error: err.Error()}, nil
	}
	return &pb.DeleteResponse{Ok: true}, nil
}

// Tokens

func (s *GatewayService) CreateToken(ctx context.Context, req *pb.CreateTokenRequest) (*pb.CreateTokenResponse, error) {
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

	token, raw, err := s.backend.CreateToken(ctx, ws.Id, member.Id, name, expiresAt)
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
	if err := s.backend.RevokeToken(ctx, req.Id); err != nil {
		return &pb.DeleteResponse{Ok: false, Error: err.Error()}, nil
	}
	return &pb.DeleteResponse{Ok: true}, nil
}

// Connections

func (s *GatewayService) AddConnection(ctx context.Context, req *pb.AddConnectionRequest) (*pb.ConnectionResponse, error) {
	ws, err := s.backend.GetWorkspaceByExternalId(ctx, req.WorkspaceId)
	if err != nil || ws == nil {
		return &pb.ConnectionResponse{Ok: false, Error: "workspace not found"}, nil
	}

	var memberId *uint
	if req.MemberId != "" {
		member, err := s.backend.GetMember(ctx, req.MemberId)
		if err != nil || member == nil {
			return &pb.ConnectionResponse{Ok: false, Error: "member not found"}, nil
		}
		if member.WorkspaceId != ws.Id {
			return &pb.ConnectionResponse{Ok: false, Error: "member not in workspace"}, nil
		}
		memberId = &member.Id
	}

	creds := &types.IntegrationCredentials{
		AccessToken: req.AccessToken,
		APIKey:      req.ApiKey,
	}

	conn, err := s.backend.SaveConnection(ctx, ws.Id, memberId, req.IntegrationType, creds, req.Scope)
	if err != nil {
		return &pb.ConnectionResponse{Ok: false, Error: err.Error()}, nil
	}

	return &pb.ConnectionResponse{Ok: true, Connection: connectionToPb(conn)}, nil
}

func (s *GatewayService) ListConnections(ctx context.Context, req *pb.ListConnectionsRequest) (*pb.ListConnectionsResponse, error) {
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
		pbConns = append(pbConns, connectionToPb(&c))
	}
	return &pb.ListConnectionsResponse{Ok: true, Connections: pbConns}, nil
}

func (s *GatewayService) RemoveConnection(ctx context.Context, req *pb.RemoveConnectionRequest) (*pb.DeleteResponse, error) {
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

func memberToPb(m *types.WorkspaceMember) *pb.Member {
	return &pb.Member{
		Id:          m.ExternalId,
		WorkspaceId: m.ExternalId, // Will be looked up if needed
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

func connectionToPb(c *types.IntegrationConnection) *pb.Connection {
	conn := &pb.Connection{
		Id:              c.ExternalId,
		WorkspaceId:     c.ExternalId, // Will be proper external ID from lookup
		IntegrationType: c.IntegrationType,
		Scope:           c.Scope,
		IsShared:        c.MemberId == nil,
		CreatedAt:       c.CreatedAt.Format(time.RFC3339),
	}
	return conn
}
