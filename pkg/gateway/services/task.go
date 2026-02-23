package services

import (
	"context"
	"strings"

	"github.com/beam-cloud/airstore/pkg/auth"
	"github.com/beam-cloud/airstore/pkg/orchestration"
	"github.com/beam-cloud/airstore/pkg/types"
	pb "github.com/beam-cloud/airstore/proto"
)

func (s *AgentService) CreateTask(ctx context.Context, req *pb.CreateTaskRequest) (*pb.AgentTaskAcceptedResponse, error) {
	workspaceID := auth.WorkspaceId(ctx)
	if workspaceID == 0 {
		return &pb.AgentTaskAcceptedResponse{Ok: false, Error: "authentication required"}, nil
	}

	if s.api == nil {
		return &pb.AgentTaskAcceptedResponse{Ok: false, Error: "task service unavailable"}, nil
	}

	message := strings.TrimSpace(req.Message)
	if message == "" {
		return &pb.AgentTaskAcceptedResponse{Ok: false, Error: "message is required"}, nil
	}

	agentID := strings.TrimSpace(req.AgentId)
	if agentID == "" {
		return &pb.AgentTaskAcceptedResponse{Ok: false, Error: "agent_id is required"}, nil
	}

	var sessionKey *string
	if value := strings.TrimSpace(req.SessionKey); value != "" {
		sessionKey = &value
	}
	var lane *string
	if value := strings.TrimSpace(req.Lane); value != "" {
		lane = &value
	}
	var extraSystemPrompt *string
	if value := strings.TrimSpace(req.ExtraSystemPrompt); value != "" {
		extraSystemPrompt = &value
	}

	task, idempotentHit, err := s.api.AcceptAgentCommand(ctx, workspaceID, orchestration.AgentCommandParams{
		Message:           message,
		AgentID:           &agentID,
		SessionID:         strings.TrimSpace(req.SessionId),
		SessionKey:        sessionKey,
		IdempotencyKey:    strings.TrimSpace(req.IdempotencyKey),
		Lane:              lane,
		ExtraSystemPrompt: extraSystemPrompt,
		Routing:           orchestration.RoutingContext{},
	})
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

func newestExecutionID(attempts []*types.AgentRunAttempt) string {
	for i := len(attempts) - 1; i >= 0; i-- {
		attempt := attempts[i]
		if attempt == nil || attempt.ExecutionID == nil {
			continue
		}

		executionID := strings.TrimSpace(*attempt.ExecutionID)
		if executionID != "" {
			return executionID
		}
	}

	return ""
}

func (s *AgentService) DeleteTask(ctx context.Context, req *pb.DeleteTaskRequest) (*pb.DeleteResponse, error) {
	workspaceID := auth.WorkspaceId(ctx)
	if workspaceID == 0 {
		return &pb.DeleteResponse{Ok: false, Error: "authentication required"}, nil
	}

	if s.api == nil {
		return &pb.DeleteResponse{Ok: false, Error: "task service unavailable"}, nil
	}

	task, err := s.api.GetTask(ctx, workspaceID, req.Id)
	if err != nil {
		if _, ok := err.(*types.ErrAgentTaskNotFound); ok {
			return &pb.DeleteResponse{Ok: false, Error: "task not found"}, nil
		}
		return &pb.DeleteResponse{Ok: false, Error: err.Error()}, nil
	}

	if task.TargetRunID != nil {
		if err := s.api.CancelRun(ctx, workspaceID, *task.TargetRunID); err != nil {
			return &pb.DeleteResponse{Ok: false, Error: err.Error()}, nil
		}
	}

	if task.State == types.AgentTaskStateAccepted ||
		task.State == types.AgentTaskStateQueued ||
		task.State == types.AgentTaskStateDispatched {
		if err := s.backend.UpdateTaskState(ctx, task.ID, types.AgentTaskStateCancelled, nil, task.TargetRunID); err != nil {
			return &pb.DeleteResponse{Ok: false, Error: err.Error()}, nil
		}
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

	task, err := s.api.GetTask(ctx, workspaceID, req.Id)
	if err != nil {
		if _, ok := err.(*types.ErrAgentTaskNotFound); ok {
			return &pb.GetTaskLogsResponse{Ok: false, Error: "task not found"}, nil
		}
		return &pb.GetTaskLogsResponse{Ok: false, Error: err.Error()}, nil
	}
	if s.s2Client == nil || !s.s2Client.Enabled() {
		return &pb.GetTaskLogsResponse{Ok: true, Logs: []*pb.TaskLogEntry{}}, nil
	}
	if task.TargetRunID == nil {
		return &pb.GetTaskLogsResponse{Ok: true, Logs: []*pb.TaskLogEntry{}}, nil
	}

	attempts, err := s.backend.ListAgentRunAttempts(ctx, *task.TargetRunID)
	if err != nil {
		return &pb.GetTaskLogsResponse{Ok: false, Error: err.Error()}, nil
	}
	executionID := newestExecutionID(attempts)
	if executionID == "" {
		return &pb.GetTaskLogsResponse{Ok: true, Logs: []*pb.TaskLogEntry{}}, nil
	}

	logs, _, err := s.s2Client.ReadLogs(ctx, executionID, 0)
	if err != nil {
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
	task, deduped, err := s.api.EnqueueRunInput(ctx, ws.Id, req.RunId, queueMode, req.Message, req.IdempotencyKey)
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
		Kind:           string(task.Kind),
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

