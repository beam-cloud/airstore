package services

import (
	"context"
	"encoding/json"
	"strconv"
	"time"

	"github.com/beam-cloud/airstore/pkg/auth"
	"github.com/beam-cloud/airstore/pkg/types"
	pb "github.com/beam-cloud/airstore/proto"
)

func (s *AgentService) ListAgentRuns(ctx context.Context, req *pb.ListAgentRunsRequest) (*pb.ListAgentRunsResponse, error) {
	ws, err := s.resolveWorkspace(ctx, req.WorkspaceId)
	if err != nil {
		return &pb.ListAgentRunsResponse{Ok: false, Error: err.Error()}, nil
	}

	runs, err := s.api.ListRuns(ctx, ws.Id, int(req.Limit))
	if err != nil {
		return &pb.ListAgentRunsResponse{Ok: false, Error: err.Error()}, nil
	}

	out := make([]*pb.AgentRun, 0, len(runs))
	for _, run := range runs {
		out = append(out, agentRunToProto(run, ws.ExternalId))
	}
	return &pb.ListAgentRunsResponse{Ok: true, Runs: out}, nil
}

func (s *AgentService) GetAgentRun(ctx context.Context, req *pb.GetAgentRunRequest) (*pb.AgentRunResponse, error) {
	ws, err := s.resolveWorkspace(ctx, req.WorkspaceId)
	if err != nil {
		return &pb.AgentRunResponse{Ok: false, Error: err.Error()}, nil
	}

	run, err := s.api.GetRun(ctx, ws.Id, req.RunId)
	if err != nil {
		return &pb.AgentRunResponse{Ok: false, Error: err.Error()}, nil
	}
	return &pb.AgentRunResponse{Ok: true, Run: agentRunToProto(run, ws.ExternalId)}, nil
}

func (s *AgentService) ListAgentRunSnapshots(ctx context.Context, req *pb.ListAgentRunSnapshotsRequest) (*pb.ListAgentRunSnapshotsResponse, error) {
	ws, err := s.resolveWorkspace(ctx, req.WorkspaceId)
	if err != nil {
		return &pb.ListAgentRunSnapshotsResponse{Ok: false, Error: err.Error()}, nil
	}
	snapshots, err := s.api.ListRunSnapshots(ctx, ws.Id, req.RunId, int(req.Limit))
	if err != nil {
		return &pb.ListAgentRunSnapshotsResponse{Ok: false, Error: err.Error()}, nil
	}

	out := make([]*pb.AgentRunSnapshot, 0, len(snapshots))
	for _, snapshot := range snapshots {
		out = append(out, agentRunSnapshotToProto(snapshot))
	}
	return &pb.ListAgentRunSnapshotsResponse{Ok: true, Snapshots: out}, nil
}

func (s *AgentService) ListAgentRunEvents(ctx context.Context, req *pb.ListAgentRunEventsRequest) (*pb.ListAgentRunEventsResponse, error) {
	ws, err := s.resolveWorkspace(ctx, req.WorkspaceId)
	if err != nil {
		return &pb.ListAgentRunEventsResponse{Ok: false, Error: err.Error()}, nil
	}
	events, err := s.api.ListRunEvents(ctx, ws.Id, req.RunId)
	if err != nil {
		return &pb.ListAgentRunEventsResponse{Ok: false, Error: err.Error()}, nil
	}

	out := make([]string, 0, len(events))
	for _, event := range events {
		body, err := json.Marshal(event)
		if err != nil {
			continue
		}
		out = append(out, string(body))
	}
	return &pb.ListAgentRunEventsResponse{Ok: true, EventsJson: out}, nil
}

func (s *AgentService) CancelAgentRun(ctx context.Context, req *pb.CancelAgentRunRequest) (*pb.CancelAgentRunResponse, error) {
	ws, err := s.resolveWorkspace(ctx, req.WorkspaceId)
	if err != nil {
		return &pb.CancelAgentRunResponse{Ok: false, Error: err.Error()}, nil
	}
	if err := auth.RequireWorkspaceAccess(ctx, ws.ExternalId); err != nil {
		return &pb.CancelAgentRunResponse{Ok: false, Error: err.Error()}, nil
	}
	if err := s.api.CancelRun(ctx, ws.Id, req.RunId); err != nil {
		return &pb.CancelAgentRunResponse{Ok: false, Error: err.Error()}, nil
	}
	return &pb.CancelAgentRunResponse{Ok: true, Status: "cancelled"}, nil
}

func agentRunToProto(run *types.AgentRun, workspaceExternalID string) *pb.AgentRun {
	if run == nil {
		return nil
	}
	return &pb.AgentRun{
		Id:              run.ID,
		WorkspaceId:     workspaceExternalID,
		AgentId:         stringOrEmpty(run.AgentID),
		OriginTaskId:    run.OriginTaskID,
		Status:          string(run.Status),
		SessionId:       run.SessionID,
		SessionKey:      stringOrEmpty(run.SessionKey),
		Provider:        stringOrEmpty(run.Provider),
		Model:           stringOrEmpty(run.Model),
		ExecHost:        run.ExecHost,
		ExecSecurity:    run.ExecSecurity,
		ExecAsk:         run.ExecAsk,
		RuntimeType:     run.RuntimeType,
		WorkspaceAccess: run.WorkspaceAccess,
		NetworkEnabled:  run.NetworkEnabled,
		Interactive:     run.Interactive,
		TimeoutMs:       int32(run.TimeoutMs),
		Error:           stringOrEmpty(run.Error),
		CreatedAt:       formatTime(run.CreatedAt),
		StartedAt:       formatTimePtr(run.StartedAt),
		EndedAt:         formatTimePtr(run.EndedAt),
	}
}

func agentRunSnapshotToProto(snapshot *types.AgentRunSnapshot) *pb.AgentRunSnapshot {
	if snapshot == nil {
		return nil
	}
	out := &pb.AgentRunSnapshot{
		Id:     strconv.FormatInt(snapshot.ID, 10),
		RunId:  snapshot.RunID,
		Seq:    snapshot.Seq,
		Status: string(snapshot.Status),
		Error:  stringOrEmpty(snapshot.Error),
		Ts:     snapshot.TS,
	}
	if snapshot.StartedAtMs != nil {
		out.StartedAtMs = *snapshot.StartedAtMs
		out.HasStartedAtMs = true
	}
	if snapshot.EndedAtMs != nil {
		out.EndedAtMs = *snapshot.EndedAtMs
		out.HasEndedAtMs = true
	}
	return out
}

func formatTimePtr(v *time.Time) string {
	if v == nil || v.IsZero() {
		return ""
	}
	return v.Format(time.RFC3339)
}
