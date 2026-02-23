package services

import (
	"context"
	"fmt"
	"time"

	"github.com/beam-cloud/airstore/pkg/auth"
	"github.com/beam-cloud/airstore/pkg/common"
	"github.com/beam-cloud/airstore/pkg/orchestration"
	"github.com/beam-cloud/airstore/pkg/repository"
	"github.com/beam-cloud/airstore/pkg/types"
	pb "github.com/beam-cloud/airstore/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

type AgentService struct {
	pb.UnimplementedAgentServiceServer
	backend  repository.BackendRepository
	api      *orchestration.AgentAPI
	s2Client *common.S2Client
}

func NewAgentService(
	backend repository.BackendRepository,
	api *orchestration.AgentAPI,
	s2Client *common.S2Client,
) *AgentService {
	return &AgentService{
		backend:  backend,
		api:      api,
		s2Client: s2Client,
	}
}

func (s *AgentService) resolveWorkspace(ctx context.Context, workspaceExtID string) (*types.Workspace, error) {
	resolved, err := auth.ResolveWorkspaceExtId(ctx, workspaceExtID)
	if err != nil {
		return nil, err
	}
	ws, err := s.backend.GetWorkspaceByExternalId(ctx, resolved)
	if err != nil || ws == nil {
		return nil, fmt.Errorf("workspace not found")
	}
	return ws, nil
}

func (s *AgentService) CreateAgentProfile(ctx context.Context, req *pb.CreateAgentProfileRequest) (*pb.AgentProfileResponse, error) {
	ws, err := s.resolveWorkspace(ctx, req.WorkspaceId)
	if err != nil {
		return &pb.AgentProfileResponse{Ok: false, Error: err.Error()}, nil
	}

	config := map[string]any{}
	if req.Config != nil {
		config = req.Config.AsMap()
	}
	var active *bool
	if req.HasActive {
		active = &req.Active
	}
	agent, err := s.api.CreateAgent(ctx, ws.Id, req.AgentKey, req.Name, config, active)
	if err != nil {
		return &pb.AgentProfileResponse{Ok: false, Error: err.Error()}, nil
	}

	return &pb.AgentProfileResponse{Ok: true, Agent: agentProfileToProto(agent, ws.ExternalId)}, nil
}

func (s *AgentService) ListAgentProfiles(ctx context.Context, req *pb.ListAgentProfilesRequest) (*pb.ListAgentProfilesResponse, error) {
	ws, err := s.resolveWorkspace(ctx, req.WorkspaceId)
	if err != nil {
		return &pb.ListAgentProfilesResponse{Ok: false, Error: err.Error()}, nil
	}

	agents, err := s.api.ListAgents(ctx, ws.Id)
	if err != nil {
		return &pb.ListAgentProfilesResponse{Ok: false, Error: err.Error()}, nil
	}

	out := make([]*pb.AgentProfile, 0, len(agents))
	for _, agent := range agents {
		out = append(out, agentProfileToProto(agent, ws.ExternalId))
	}
	return &pb.ListAgentProfilesResponse{Ok: true, Agents: out}, nil
}

func (s *AgentService) GetAgentProfile(ctx context.Context, req *pb.GetAgentProfileRequest) (*pb.AgentProfileResponse, error) {
	ws, err := s.resolveWorkspace(ctx, req.WorkspaceId)
	if err != nil {
		return &pb.AgentProfileResponse{Ok: false, Error: err.Error()}, nil
	}
	if err := auth.RequireWorkspaceAccess(ctx, ws.ExternalId); err != nil {
		return &pb.AgentProfileResponse{Ok: false, Error: err.Error()}, nil
	}

	agent, err := s.api.GetAgent(ctx, ws.Id, req.AgentId)
	if err != nil {
		return &pb.AgentProfileResponse{Ok: false, Error: err.Error()}, nil
	}
	return &pb.AgentProfileResponse{Ok: true, Agent: agentProfileToProto(agent, ws.ExternalId)}, nil
}

func agentProfileToProto(agent *types.AgentProfile, workspaceExternalID string) *pb.AgentProfile {
	if agent == nil {
		return nil
	}
	cfg := &structpb.Struct{}
	if len(agent.ConfigJSON) > 0 {
		if converted, err := structpb.NewStruct(agent.ConfigJSON); err == nil {
			cfg = converted
		}
	}
	return &pb.AgentProfile{
		Id:          agent.ID,
		WorkspaceId: workspaceExternalID,
		AgentKey:    agent.AgentKey,
		Name:        agent.Name,
		Config:      cfg,
		Active:      agent.Active,
		CreatedAt:   formatTime(agent.CreatedAt),
		UpdatedAt:   formatTime(agent.UpdatedAt),
	}
}

func formatTime(v time.Time) string {
	if v.IsZero() {
		return ""
	}
	return v.Format(time.RFC3339)
}
