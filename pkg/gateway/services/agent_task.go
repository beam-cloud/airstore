package services

import (
	"context"

	"github.com/beam-cloud/airstore/pkg/auth"
	"github.com/beam-cloud/airstore/pkg/orchestration"
	"github.com/beam-cloud/airstore/pkg/types"
	pb "github.com/beam-cloud/airstore/proto"
)

func (s *AgentService) CreateAgentTaskEnvelope(ctx context.Context, req *pb.CreateAgentTaskEnvelopeRequest) (*pb.AgentTaskEnvelopeAcceptedResponse, error) {
	if s.api == nil {
		return &pb.AgentTaskEnvelopeAcceptedResponse{Ok: false, Error: "orchestration unavailable"}, nil
	}

	ws, err := s.resolveWorkspace(ctx, req.WorkspaceId)
	if err != nil {
		return &pb.AgentTaskEnvelopeAcceptedResponse{Ok: false, Error: err.Error()}, nil
	}

	var agentID *string
	if req.AgentId != "" {
		agentID = &req.AgentId
	}
	var sessionKey *string
	if req.SessionKey != "" {
		sessionKey = &req.SessionKey
	}
	var lane *string
	if req.Lane != "" {
		lane = &req.Lane
	}
	var extraSystemPrompt *string
	if req.ExtraSystemPrompt != "" {
		extraSystemPrompt = &req.ExtraSystemPrompt
	}

	envelope, deduped, err := s.api.AcceptAgentCommand(ctx, ws.Id, orchestration.AgentCommandParams{
		Message:           req.Message,
		AgentID:           agentID,
		SessionID:         req.SessionId,
		SessionKey:        sessionKey,
		IdempotencyKey:    req.IdempotencyKey,
		Lane:              lane,
		ExtraSystemPrompt: extraSystemPrompt,
		Routing:           orchestration.RoutingContext{},
	})
	if err != nil {
		return &pb.AgentTaskEnvelopeAcceptedResponse{Ok: false, Error: err.Error()}, nil
	}

	return &pb.AgentTaskEnvelopeAcceptedResponse{
		Ok:            true,
		Accepted:      true,
		IdempotentHit: deduped,
		Envelope:      agentTaskEnvelopeToProto(envelope, ws.ExternalId),
		RunId:         stringOrEmpty(envelope.TargetRunID),
	}, nil
}

func (s *AgentService) GetAgentTaskEnvelope(ctx context.Context, req *pb.GetAgentTaskEnvelopeRequest) (*pb.AgentTaskEnvelopeResponse, error) {
	ws, err := s.resolveWorkspace(ctx, req.WorkspaceId)
	if err != nil {
		return &pb.AgentTaskEnvelopeResponse{Ok: false, Error: err.Error()}, nil
	}

	envelope, err := s.api.GetEnvelope(ctx, ws.Id, req.EnvelopeId)
	if err != nil {
		return &pb.AgentTaskEnvelopeResponse{Ok: false, Error: err.Error()}, nil
	}
	return &pb.AgentTaskEnvelopeResponse{Ok: true, Envelope: agentTaskEnvelopeToProto(envelope, ws.ExternalId)}, nil
}

func (s *AgentService) EnqueueRunInput(ctx context.Context, req *pb.EnqueueRunInputRequest) (*pb.AgentTaskEnvelopeAcceptedResponse, error) {
	if s.api == nil {
		return &pb.AgentTaskEnvelopeAcceptedResponse{Ok: false, Error: "orchestration unavailable"}, nil
	}

	ws, err := s.resolveWorkspace(ctx, req.WorkspaceId)
	if err != nil {
		return &pb.AgentTaskEnvelopeAcceptedResponse{Ok: false, Error: err.Error()}, nil
	}
	if err := auth.RequireWorkspaceAccess(ctx, ws.ExternalId); err != nil {
		return &pb.AgentTaskEnvelopeAcceptedResponse{Ok: false, Error: err.Error()}, nil
	}

	queueMode := types.AgentQueueMode(req.QueueMode)
	envelope, deduped, err := s.api.EnqueueRunInputEnvelope(ctx, ws.Id, req.RunId, queueMode, req.Message, req.IdempotencyKey)
	if err != nil {
		return &pb.AgentTaskEnvelopeAcceptedResponse{Ok: false, Error: err.Error()}, nil
	}

	return &pb.AgentTaskEnvelopeAcceptedResponse{
		Ok:            true,
		Accepted:      true,
		IdempotentHit: deduped,
		Envelope:      agentTaskEnvelopeToProto(envelope, ws.ExternalId),
		RunId:         stringOrEmpty(envelope.TargetRunID),
	}, nil
}

func agentTaskEnvelopeToProto(envelope *types.AgentTaskEnvelope, workspaceExternalID string) *pb.AgentTaskEnvelope {
	if envelope == nil {
		return nil
	}
	return &pb.AgentTaskEnvelope{
		Id:             envelope.ID,
		WorkspaceId:    workspaceExternalID,
		AgentId:        stringOrEmpty(envelope.AgentID),
		Kind:           string(envelope.Kind),
		QueueMode:      string(envelope.QueueMode),
		State:          string(envelope.State),
		IdempotencyKey: envelope.IdempotencyKey,
		TargetRunId:    stringOrEmpty(envelope.TargetRunID),
		DroppedReason:  stringOrEmpty(envelope.DroppedReason),
		CreatedAt:      formatTime(envelope.CreatedAt),
		UpdatedAt:      formatTime(envelope.UpdatedAt),
	}
}

func stringOrEmpty(v *string) string {
	if v == nil {
		return ""
	}
	return *v
}
