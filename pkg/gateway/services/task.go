package services

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/beam-cloud/airstore/pkg/auth"
	"github.com/beam-cloud/airstore/pkg/orchestration"
	"github.com/beam-cloud/airstore/pkg/types"
	pb "github.com/beam-cloud/airstore/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

func (s *AgentService) CreateTask(ctx context.Context, req *pb.CreateTaskRequest) (*pb.AgentTaskAcceptedResponse, error) {
	workspaceID := auth.WorkspaceId(ctx)
	if workspaceID == 0 {
		return &pb.AgentTaskAcceptedResponse{Ok: false, Error: "authentication required"}, nil
	}

	if s.api == nil {
		return &pb.AgentTaskAcceptedResponse{Ok: false, Error: "task service unavailable"}, nil
	}

	params, err := agentCommandParamsFromProto(req)
	if err != nil {
		return &pb.AgentTaskAcceptedResponse{Ok: false, Error: err.Error()}, nil
	}

	task, idempotentHit, err := s.api.AcceptAgentCommand(ctx, workspaceID, params)
	if err != nil {
		return &pb.AgentTaskAcceptedResponse{Ok: false, Error: err.Error()}, nil
	}

	workspaceExtID := s.resolveWorkspaceExternalID(ctx, workspaceID)

	return &pb.AgentTaskAcceptedResponse{
		Ok:            true,
		Accepted:      true,
		IdempotentHit: idempotentHit,
		Task:          agentTaskToProto(task, workspaceExtID),
		RunId:         stringOrEmpty(task.TargetRunID),
	}, nil
}

func agentCommandParamsFromProto(req *pb.CreateTaskRequest) (orchestration.AgentCommandParams, error) {
	policy, err := runPolicyFromProtoStruct(req.Policy)
	if err != nil {
		return orchestration.AgentCommandParams{}, err
	}
	inputProvenance, err := inputProvenanceFromProtoStruct(req.InputProvenance)
	if err != nil {
		return orchestration.AgentCommandParams{}, err
	}
	routing, err := routingContextFromProtoStruct(req.Routing)
	if err != nil {
		return orchestration.AgentCommandParams{}, err
	}
	attachments := attachmentsFromProtoStructs(req.Attachments)

	var deliver *bool
	if req.Deliver != nil {
		value := req.GetDeliver()
		deliver = &value
	}
	var timeoutMs *int
	if req.TimeoutMs != nil {
		value := int(req.GetTimeoutMs())
		timeoutMs = &value
	}

	agentID := req.GetAgentId()

	return orchestration.AgentCommandParams{
		Message:           req.GetMessage(),
		AgentID:           &agentID,
		SessionID:         req.GetSessionId(),
		SessionKey:        optionalStringPointer(req.GetSessionKey()),
		Deliver:           deliver,
		TimeoutMs:         timeoutMs,
		Policy:            policy,
		IdempotencyKey:    req.GetIdempotencyKey(),
		Lane:              optionalStringPointer(req.GetLane()),
		ExtraSystemPrompt: optionalStringPointer(req.GetExtraSystemPrompt()),
		InputProvenance:   inputProvenance,
		Routing:           routing,
		Attachments:       attachments,
		Label:             optionalStringPointer(req.GetLabel()),
		SpawnedBy:         optionalStringPointer(req.GetSpawnedBy()),
		ParentTaskID:      optionalStringPointer(req.GetParentTaskId()),
	}, nil
}

func optionalStringPointer(value string) *string {
	if value == "" {
		return nil
	}
	return &value
}

func (s *AgentService) DeleteTask(ctx context.Context, req *pb.DeleteTaskRequest) (*pb.DeleteResponse, error) {
	workspaceID := auth.WorkspaceId(ctx)
	if workspaceID == 0 {
		return &pb.DeleteResponse{Ok: false, Error: "authentication required"}, nil
	}

	if s.api == nil {
		return &pb.DeleteResponse{Ok: false, Error: "task service unavailable"}, nil
	}

	if err := s.api.CancelTask(ctx, workspaceID, req.Id); err != nil {
		if _, ok := err.(*types.ErrAgentTaskNotFound); ok {
			return &pb.DeleteResponse{Ok: false, Error: "task not found"}, nil
		}
		return &pb.DeleteResponse{Ok: false, Error: err.Error()}, nil
	}

	return &pb.DeleteResponse{Ok: true}, nil
}

func (s *AgentService) ListTasks(ctx context.Context, _ *pb.ListTasksRequest) (*pb.ListTasksResponse, error) {
	workspaceID := auth.WorkspaceId(ctx)
	if workspaceID == 0 {
		return &pb.ListTasksResponse{Ok: false, Error: "authentication required"}, nil
	}

	if s.api == nil {
		return &pb.ListTasksResponse{Ok: false, Error: "task service unavailable"}, nil
	}

	tasks, err := s.api.ListTasks(ctx, workspaceID, 100)
	if err != nil {
		return &pb.ListTasksResponse{Ok: false, Error: err.Error()}, nil
	}

	workspaceExtID := s.resolveWorkspaceExternalID(ctx, workspaceID)

	out := make([]*pb.AgentTask, 0, len(tasks))
	for _, task := range tasks {
		out = append(out, agentTaskToProto(task, workspaceExtID))
	}

	return &pb.ListTasksResponse{Ok: true, Tasks: out}, nil
}

func (s *AgentService) GetTask(ctx context.Context, req *pb.GetTaskRequest) (*pb.AgentTaskResponse, error) {
	workspaceID := auth.WorkspaceId(ctx)
	if workspaceID == 0 {
		return &pb.AgentTaskResponse{Ok: false, Error: "authentication required"}, nil
	}

	if s.api == nil {
		return &pb.AgentTaskResponse{Ok: false, Error: "task service unavailable"}, nil
	}

	task, err := s.api.GetTask(ctx, workspaceID, req.Id)
	if err != nil {
		if _, ok := err.(*types.ErrAgentTaskNotFound); ok {
			return &pb.AgentTaskResponse{Ok: false, Error: "task not found"}, nil
		}
		return &pb.AgentTaskResponse{Ok: false, Error: err.Error()}, nil
	}

	workspaceExtID := s.resolveWorkspaceExternalID(ctx, workspaceID)

	return &pb.AgentTaskResponse{Ok: true, Task: agentTaskToProto(task, workspaceExtID)}, nil
}

func (s *AgentService) GetTaskLogs(ctx context.Context, req *pb.GetTaskLogsRequest) (*pb.GetTaskLogsResponse, error) {
	workspaceID := auth.WorkspaceId(ctx)
	if workspaceID == 0 {
		return &pb.GetTaskLogsResponse{Ok: false, Error: "authentication required"}, nil
	}

	if s.api == nil {
		return &pb.GetTaskLogsResponse{Ok: false, Error: "task service unavailable"}, nil
	}

	logs, err := s.api.GetTaskLogs(ctx, workspaceID, req.Id)
	if err != nil {
		if _, ok := err.(*types.ErrAgentTaskNotFound); ok {
			return &pb.GetTaskLogsResponse{Ok: false, Error: "task not found"}, nil
		}
		return &pb.GetTaskLogsResponse{Ok: false, Error: err.Error()}, nil
	}

	out := make([]*pb.TaskLogEntry, 0, len(logs))
	for _, log := range logs {
		out = append(out, &pb.TaskLogEntry{
			TaskId:    req.Id,
			Timestamp: log.Timestamp,
			Stream:    log.Stream,
			Data:      log.Data,
		})
	}
	return &pb.GetTaskLogsResponse{Ok: true, Logs: out}, nil
}

func (s *AgentService) EnqueueRunInput(ctx context.Context, req *pb.EnqueueRunInputRequest) (*pb.AgentTaskAcceptedResponse, error) {
	if s.api == nil {
		return &pb.AgentTaskAcceptedResponse{Ok: false, Error: "task service unavailable"}, nil
	}

	ws, err := s.resolveWorkspace(ctx, req.WorkspaceId)
	if err != nil {
		return &pb.AgentTaskAcceptedResponse{Ok: false, Error: err.Error()}, nil
	}
	if err := auth.RequireWorkspaceAccess(ctx, ws.ExternalId); err != nil {
		return &pb.AgentTaskAcceptedResponse{Ok: false, Error: err.Error()}, nil
	}

	queueMode := types.AgentQueueMode(req.QueueMode)
	task, deduped, _, err := s.api.EnqueueRunInput(ctx, ws.Id, req.RunId, queueMode, req.Message, req.IdempotencyKey)
	if err != nil {
		return &pb.AgentTaskAcceptedResponse{Ok: false, Error: err.Error()}, nil
	}

	return &pb.AgentTaskAcceptedResponse{
		Ok:            true,
		Accepted:      true,
		IdempotentHit: deduped,
		Task:          agentTaskToProto(task, ws.ExternalId),
		RunId:         stringOrEmpty(task.TargetRunID),
	}, nil
}

func agentTaskToProto(task *types.AgentTask, workspaceExternalID string) *pb.AgentTask {
	if task == nil {
		return nil
	}
	return &pb.AgentTask{
		Id:             task.ID,
		WorkspaceId:    workspaceExternalID,
		AgentId:        stringOrEmpty(task.AgentID),
		Kind:           types.AgentTaskKindAgentCommand,
		QueueMode:      string(task.QueueMode),
		State:          string(task.State),
		IdempotencyKey: task.IdempotencyKey,
		TargetRunId:    stringOrEmpty(task.TargetRunID),
		DroppedReason:  stringOrEmpty(task.DroppedReason),
		CreatedAt:      formatTime(task.CreatedAt),
		UpdatedAt:      formatTime(task.UpdatedAt),
	}
}

func stringOrEmpty(v *string) string {
	if v == nil {
		return ""
	}
	return *v
}

func (s *AgentService) resolveWorkspaceExternalID(ctx context.Context, workspaceID uint) string {
	workspaceExtID := auth.WorkspaceExtId(ctx)
	if workspaceExtID != "" {
		return workspaceExtID
	}

	workspace, err := s.backend.GetWorkspace(ctx, workspaceID)
	if err == nil && workspace != nil {
		return workspace.ExternalId
	}

	return ""
}

func runPolicyFromProtoStruct(value *structpb.Struct) (*orchestration.RunExecutionPolicy, error) {
	if value == nil {
		return nil, nil
	}
	out := orchestration.RunExecutionPolicy{}
	if err := decodeStructMap(value.AsMap(), &out); err != nil {
		return nil, fmt.Errorf("invalid policy: %w", err)
	}
	return &out, nil
}

func inputProvenanceFromProtoStruct(value *structpb.Struct) (*orchestration.InputProvenance, error) {
	if value == nil {
		return nil, nil
	}
	out := orchestration.InputProvenance{}
	if err := decodeStructMap(value.AsMap(), &out); err != nil {
		return nil, fmt.Errorf("invalid input_provenance: %w", err)
	}
	return &out, nil
}

func routingContextFromProtoStruct(value *structpb.Struct) (orchestration.RoutingContext, error) {
	if value == nil {
		return orchestration.RoutingContext{}, nil
	}
	out := orchestration.RoutingContext{}
	if err := decodeStructMap(value.AsMap(), &out); err != nil {
		return orchestration.RoutingContext{}, fmt.Errorf("invalid routing: %w", err)
	}
	return out, nil
}

func attachmentsFromProtoStructs(values []*structpb.Struct) []map[string]any {
	if len(values) == 0 {
		return nil
	}
	out := make([]map[string]any, 0, len(values))
	for _, value := range values {
		if value == nil {
			continue
		}
		out = append(out, value.AsMap())
	}
	return out
}

func decodeStructMap(data map[string]any, target any) error {
	body, err := json.Marshal(data)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(body, target); err != nil {
		return err
	}
	return nil
}
