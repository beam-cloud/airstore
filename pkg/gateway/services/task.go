package services

import (
	"context"
	"time"

	"github.com/beam-cloud/airstore/pkg/auth"
	"github.com/beam-cloud/airstore/pkg/common"
	"github.com/beam-cloud/airstore/pkg/repository"
	"github.com/beam-cloud/airstore/pkg/types"
	pb "github.com/beam-cloud/airstore/proto"
)

type TaskService struct {
	pb.UnimplementedTaskServiceServer
	backend      repository.BackendRepository
	taskQueue    repository.TaskQueue
	s2Client     *common.S2Client
	defaultImage string
}

func NewTaskService(
	backend repository.BackendRepository,
	taskQueue repository.TaskQueue,
	s2Client *common.S2Client,
	defaultImage string,
) *TaskService {
	return &TaskService{
		backend:      backend,
		taskQueue:    taskQueue,
		s2Client:     s2Client,
		defaultImage: defaultImage,
	}
}

func (s *TaskService) CreateTask(ctx context.Context, req *pb.CreateTaskRequest) (*pb.TaskResponse, error) {
	workspaceID := auth.WorkspaceId(ctx)
	if workspaceID == 0 {
		return &pb.TaskResponse{Ok: false, Error: "authentication required"}, nil
	}

	image := req.Image
	if req.Prompt != "" && image == "" {
		image = s.defaultImage
	}
	taskType := types.TaskType(req.Type)
	if taskType == "" {
		taskType = types.TaskTypeBackground
	}
	if taskType != types.TaskTypeBackground && taskType != types.TaskTypeInteractive {
		return &pb.TaskResponse{Ok: false, Error: "type must be 'background' or 'interactive'"}, nil
	}
	if image == "" && taskType == types.TaskTypeInteractive {
		image = s.defaultImage
	}
	if image == "" {
		return &pb.TaskResponse{Ok: false, Error: "image or prompt is required"}, nil
	}

	var createdByMemberID *uint
	memberID := auth.MemberId(ctx)
	if memberID > 0 {
		createdByMemberID = &memberID
	}

	memberToken, err := auth.EnsureTaskMountToken(ctx, workspaceID, extractRawToken(ctx), s.backend)
	if err != nil {
		return &pb.TaskResponse{Ok: false, Error: "failed to provision workspace token: " + err.Error()}, nil
	}

	env := req.Env
	if env == nil {
		env = map[string]string{}
	}
	entrypoint := req.Entrypoint
	if entrypoint == nil {
		entrypoint = []string{}
	}

	task := &types.Task{
		WorkspaceId:       workspaceID,
		CreatedByMemberId: createdByMemberID,
		MemberToken:       memberToken,
		Status:            types.TaskStatusPending,
		Type:              taskType,
		Prompt:            req.Prompt,
		Image:             image,
		Entrypoint:        entrypoint,
		Env:               env,
	}
	if err := s.backend.CreateTask(ctx, task); err != nil {
		return &pb.TaskResponse{Ok: false, Error: err.Error()}, nil
	}
	if s.taskQueue != nil {
		_ = s.taskQueue.Push(ctx, task)
	}
	return &pb.TaskResponse{Ok: true, Task: taskToPb(task)}, nil
}

func (s *TaskService) DeleteTask(ctx context.Context, req *pb.DeleteTaskRequest) (*pb.DeleteResponse, error) {
	workspaceID := auth.WorkspaceId(ctx)
	if workspaceID == 0 {
		return &pb.DeleteResponse{Ok: false, Error: "authentication required"}, nil
	}

	task, err := s.backend.GetTask(ctx, req.Id)
	if err != nil {
		if _, ok := err.(*types.ErrTaskNotFound); ok {
			return &pb.DeleteResponse{Ok: false, Error: "task not found"}, nil
		}
		return &pb.DeleteResponse{Ok: false, Error: err.Error()}, nil
	}
	if task.WorkspaceId != workspaceID {
		return &pb.DeleteResponse{Ok: false, Error: "task not found"}, nil
	}

	if err := s.backend.DeleteTask(ctx, req.Id); err != nil {
		return &pb.DeleteResponse{Ok: false, Error: err.Error()}, nil
	}
	return &pb.DeleteResponse{Ok: true}, nil
}

func (s *TaskService) ListTasks(ctx context.Context, _ *pb.ListTasksRequest) (*pb.ListTasksResponse, error) {
	workspaceID := auth.WorkspaceId(ctx)
	if workspaceID == 0 {
		return &pb.ListTasksResponse{Ok: false, Error: "authentication required"}, nil
	}

	tasks, err := s.backend.ListTasks(ctx, workspaceID)
	if err != nil {
		return &pb.ListTasksResponse{Ok: false, Error: err.Error()}, nil
	}

	out := make([]*pb.Task, 0, len(tasks))
	for _, task := range tasks {
		out = append(out, taskToPb(task))
	}
	return &pb.ListTasksResponse{Ok: true, Tasks: out}, nil
}

func (s *TaskService) GetTask(ctx context.Context, req *pb.GetTaskRequest) (*pb.TaskResponse, error) {
	workspaceID := auth.WorkspaceId(ctx)
	if workspaceID == 0 {
		return &pb.TaskResponse{Ok: false, Error: "authentication required"}, nil
	}

	task, err := s.backend.GetTask(ctx, req.Id)
	if err != nil {
		if _, ok := err.(*types.ErrTaskNotFound); ok {
			return &pb.TaskResponse{Ok: false, Error: "task not found"}, nil
		}
		return &pb.TaskResponse{Ok: false, Error: err.Error()}, nil
	}
	if task.WorkspaceId != workspaceID {
		return &pb.TaskResponse{Ok: false, Error: "task not found"}, nil
	}
	return &pb.TaskResponse{Ok: true, Task: taskToPb(task)}, nil
}

func (s *TaskService) GetTaskLogs(ctx context.Context, req *pb.GetTaskLogsRequest) (*pb.GetTaskLogsResponse, error) {
	workspaceID := auth.WorkspaceId(ctx)
	if workspaceID == 0 {
		return &pb.GetTaskLogsResponse{Ok: false, Error: "authentication required"}, nil
	}

	task, err := s.backend.GetTask(ctx, req.Id)
	if err != nil {
		if _, ok := err.(*types.ErrTaskNotFound); ok {
			return &pb.GetTaskLogsResponse{Ok: false, Error: "task not found"}, nil
		}
		return &pb.GetTaskLogsResponse{Ok: false, Error: err.Error()}, nil
	}
	if task.WorkspaceId != workspaceID {
		return &pb.GetTaskLogsResponse{Ok: false, Error: "task not found"}, nil
	}
	if s.s2Client == nil || !s.s2Client.Enabled() {
		return &pb.GetTaskLogsResponse{Ok: true, Logs: []*pb.TaskLogEntry{}}, nil
	}

	logs, _, err := s.s2Client.ReadLogs(ctx, req.Id, 0)
	if err != nil {
		return &pb.GetTaskLogsResponse{Ok: false, Error: err.Error()}, nil
	}

	out := make([]*pb.TaskLogEntry, 0, len(logs))
	for _, log := range logs {
		out = append(out, &pb.TaskLogEntry{
			TaskId:    log.TaskID,
			Timestamp: log.Timestamp,
			Stream:    log.Stream,
			Data:      log.Data,
		})
	}
	return &pb.GetTaskLogsResponse{Ok: true, Logs: out}, nil
}

func taskToPb(t *types.Task) *pb.Task {
	t.NormalizeType()
	task := &pb.Task{
		Id:        t.ExternalId,
		Status:    string(t.Status),
		Type:      string(t.Type),
		Prompt:    t.Prompt,
		Image:     t.Image,
		Error:     t.Error,
		CreatedAt: t.CreatedAt.Format(time.RFC3339),
	}
	if t.ExitCode != nil {
		task.ExitCode = int32(*t.ExitCode)
		task.HasExitCode = true
	}
	if t.StartedAt != nil {
		task.StartedAt = t.StartedAt.Format(time.RFC3339)
	}
	if t.FinishedAt != nil {
		task.FinishedAt = t.FinishedAt.Format(time.RFC3339)
	}
	return task
}
