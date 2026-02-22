package services

import (
	"context"
	"strings"
	"time"

	"github.com/beam-cloud/airstore/pkg/repository"
	"github.com/beam-cloud/airstore/pkg/scheduler"
	"github.com/beam-cloud/airstore/pkg/types"
	pb "github.com/beam-cloud/airstore/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type WorkerService struct {
	pb.UnimplementedWorkerServiceServer
	scheduler  *scheduler.Scheduler
	backend    *repository.PostgresBackend
	workerRepo repository.WorkerRepository
}

func NewWorkerService(sched *scheduler.Scheduler, backend *repository.PostgresBackend, workerRepo repository.WorkerRepository) *WorkerService {
	return &WorkerService{
		scheduler:  sched,
		backend:    backend,
		workerRepo: workerRepo,
	}
}

func (s *WorkerService) RegisterWorker(ctx context.Context, req *pb.RegisterWorkerRequest) (*pb.RegisterWorkerResponse, error) {
	worker := &types.Worker{
		Hostname: req.Hostname,
		PoolName: req.PoolName,
		Cpu:      req.Cpu,
		Memory:   req.Memory,
		Version:  req.Version,
	}

	if err := s.scheduler.RegisterWorker(ctx, worker); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to register worker: %v", err)
	}

	return &pb.RegisterWorkerResponse{WorkerId: worker.ID}, nil
}

func (s *WorkerService) Heartbeat(ctx context.Context, req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	if err := s.scheduler.WorkerHeartbeat(ctx, req.WorkerId); err != nil {
		if _, ok := err.(*types.ErrWorkerNotFound); ok {
			return nil, status.Errorf(codes.NotFound, "worker not found: %s", req.WorkerId)
		}
		return nil, status.Errorf(codes.Internal, "heartbeat failed: %v", err)
	}

	return &pb.HeartbeatResponse{}, nil
}

func (s *WorkerService) UpdateStatus(ctx context.Context, req *pb.UpdateStatusRequest) (*pb.UpdateStatusResponse, error) {
	workerStatus := types.WorkerStatus(req.Status)

	if err := s.scheduler.UpdateWorkerStatus(ctx, req.WorkerId, workerStatus); err != nil {
		if _, ok := err.(*types.ErrWorkerNotFound); ok {
			return nil, status.Errorf(codes.NotFound, "worker not found: %s", req.WorkerId)
		}
		return nil, status.Errorf(codes.Internal, "failed to update status: %v", err)
	}

	return &pb.UpdateStatusResponse{}, nil
}

func (s *WorkerService) Deregister(ctx context.Context, req *pb.DeregisterRequest) (*pb.DeregisterResponse, error) {
	if err := s.scheduler.DeregisterWorker(ctx, req.WorkerId); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to deregister worker: %v", err)
	}

	return &pb.DeregisterResponse{}, nil
}

func (s *WorkerService) GetWorker(ctx context.Context, req *pb.GetWorkerRequest) (*pb.GetWorkerResponse, error) {
	worker, err := s.scheduler.GetWorker(ctx, req.WorkerId)
	if err != nil {
		if _, ok := err.(*types.ErrWorkerNotFound); ok {
			return nil, status.Errorf(codes.NotFound, "worker not found: %s", req.WorkerId)
		}
		return nil, status.Errorf(codes.Internal, "failed to get worker: %v", err)
	}

	return &pb.GetWorkerResponse{
		Id:           worker.ID,
		Status:       string(worker.Status),
		PoolName:     worker.PoolName,
		Hostname:     worker.Hostname,
		Cpu:          worker.Cpu,
		Memory:       worker.Memory,
		LastSeenAt:   worker.LastSeenAt.Unix(),
		RegisteredAt: worker.RegisteredAt.Unix(),
		Version:      worker.Version,
	}, nil
}

func (s *WorkerService) ListWorkers(ctx context.Context, req *pb.ListWorkersRequest) (*pb.ListWorkersResponse, error) {
	workers, err := s.scheduler.GetWorkers(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to list workers: %v", err)
	}

	response := &pb.ListWorkersResponse{
		Workers: make([]*pb.GetWorkerResponse, 0, len(workers)),
	}

	for _, worker := range workers {
		response.Workers = append(response.Workers, &pb.GetWorkerResponse{
			Id:           worker.ID,
			Status:       string(worker.Status),
			PoolName:     worker.PoolName,
			Hostname:     worker.Hostname,
			Cpu:          worker.Cpu,
			Memory:       worker.Memory,
			LastSeenAt:   worker.LastSeenAt.Unix(),
			RegisteredAt: worker.RegisteredAt.Unix(),
			Version:      worker.Version,
		})
	}

	return response, nil
}

func (s *WorkerService) SetTaskStarted(ctx context.Context, req *pb.SetTaskStartedRequest) (*pb.SetTaskStartedResponse, error) {
	if s.backend == nil {
		return nil, status.Errorf(codes.Unavailable, "task persistence not available")
	}
	if err := s.backend.SetTaskStarted(ctx, req.TaskId); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to set task started: %v", err)
	}

	attempt, err := s.backend.GetRunAttemptByExecutionTaskExternalID(ctx, req.TaskId)
	if err == nil {
		now := time.Now()
		_ = s.backend.UpdateAgentRunAttemptStart(ctx, attempt.ID, now)
		_ = s.backend.UpdateAgentRunLifecycle(ctx, attempt.RunID, types.AgentRunStatusRunning, &now, nil, nil)
		_ = appendRunSnapshot(ctx, s.backend, attempt.RunID, types.AgentRunStatusRunning, &now, nil, nil, map[string]any{
			"attempt_id": attempt.ID,
			"task_id":    req.TaskId,
			"event":      "started",
		})
		_ = updateExecutionInstanceCounts(ctx, s.backend, attempt.RunID, 1)
	}

	return &pb.SetTaskStartedResponse{}, nil
}

func (s *WorkerService) SetTaskResult(ctx context.Context, req *pb.SetTaskResultRequest) (*pb.SetTaskResultResponse, error) {
	if s.backend == nil {
		return nil, status.Errorf(codes.Unavailable, "task persistence not available")
	}

	if err := s.backend.SetTaskResult(ctx, req.TaskId, int(req.ExitCode), req.Error); err != nil {
		if _, ok := err.(*types.ErrTaskNotFound); ok {
			return nil, status.Errorf(codes.NotFound, "task not found: %s", req.TaskId)
		}
		return nil, status.Errorf(codes.Internal, "failed to set task result: %v", err)
	}

	attempt, err := s.backend.GetRunAttemptByExecutionTaskExternalID(ctx, req.TaskId)
	if err == nil {
		now := time.Now()
		exitCode := int(req.ExitCode)

		attemptStatus := types.AgentAttemptStatusOK
		runStatus := types.AgentRunStatusOK
		var errMsg *string

		if req.Error != "" {
			msg := req.Error
			errMsg = &msg
		}
		lowerErr := strings.ToLower(req.Error)
		switch {
		case strings.Contains(lowerErr, "timeout"):
			attemptStatus = types.AgentAttemptStatusTimeout
			runStatus = types.AgentRunStatusTimeout
		case strings.Contains(lowerErr, "cancel"):
			attemptStatus = types.AgentAttemptStatusCancelled
			runStatus = types.AgentRunStatusCancelled
		case req.ExitCode != 0 || req.Error != "":
			attemptStatus = types.AgentAttemptStatusError
			runStatus = types.AgentRunStatusError
		}

		_ = s.backend.UpdateAgentRunAttemptResult(ctx, attempt.ID, attemptStatus, &exitCode, now, errMsg)
		_ = s.backend.UpdateAgentRunLifecycle(ctx, attempt.RunID, runStatus, nil, &now, errMsg)
		_ = appendRunSnapshot(ctx, s.backend, attempt.RunID, runStatus, nil, &now, errMsg, map[string]any{
			"attempt_id": attempt.ID,
			"task_id":    req.TaskId,
			"exit_code":  req.ExitCode,
			"error":      req.Error,
			"event":      "finished",
		})
		_ = updateExecutionInstanceCounts(ctx, s.backend, attempt.RunID, -1)
	}

	return &pb.SetTaskResultResponse{}, nil
}

func appendRunSnapshot(
	ctx context.Context,
	backend *repository.PostgresBackend,
	runID string,
	status types.AgentRunStatus,
	startedAt *time.Time,
	endedAt *time.Time,
	errorMsg *string,
	payload map[string]any,
) error {
	seq, err := backend.IncrementAgentRunSnapshotSeq(ctx, runID)
	if err != nil {
		return err
	}
	var startedAtMs *int64
	var endedAtMs *int64
	if startedAt != nil {
		v := startedAt.UnixMilli()
		startedAtMs = &v
	}
	if endedAt != nil {
		v := endedAt.UnixMilli()
		endedAtMs = &v
	}
	return backend.AppendAgentRunSnapshot(ctx, &types.AgentRunSnapshot{
		RunID:       runID,
		Seq:         seq,
		Status:      status,
		StartedAtMs: startedAtMs,
		EndedAtMs:   endedAtMs,
		Error:       errorMsg,
		TS:          time.Now().UnixMilli(),
		PayloadJSON: payload,
	})
}

func updateExecutionInstanceCounts(ctx context.Context, backend *repository.PostgresBackend, runID string, runningDelta int) error {
	run, err := backend.GetAgentRunByID(ctx, runID)
	if err != nil {
		return err
	}
	instanceKeyVal, ok := run.DeliveryJSON["instance_key"]
	if !ok {
		return nil
	}
	instanceKey, ok := instanceKeyVal.(string)
	if !ok || instanceKey == "" {
		return nil
	}
	instance, err := backend.GetExecutionInstanceByKey(ctx, instanceKey)
	if err != nil {
		return err
	}
	running := instance.RunningAttempts + runningDelta
	if running < 0 {
		running = 0
	}
	now := time.Now()
	return backend.UpdateExecutionInstanceState(
		ctx,
		instanceKey,
		running,
		instance.PendingAttempts,
		instance.StoppingAttempts,
		instance.DesiredDispatchConcurrency,
		instance.Status,
		&now,
	)
}

func (s *WorkerService) AllocateIP(ctx context.Context, req *pb.AllocateIPRequest) (*pb.AllocateIPResponse, error) {
	if s.workerRepo == nil {
		return nil, status.Errorf(codes.Unavailable, "IP allocation not available")
	}

	alloc, err := s.workerRepo.AllocateIP(ctx, req.SandboxId, req.WorkerId)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to allocate IP: %v", err)
	}

	return &pb.AllocateIPResponse{
		Ip:        alloc.IP,
		Gateway:   alloc.Gateway,
		PrefixLen: int32(alloc.PrefixLen),
	}, nil
}

func (s *WorkerService) ReleaseIP(ctx context.Context, req *pb.ReleaseIPRequest) (*pb.ReleaseIPResponse, error) {
	if s.workerRepo == nil {
		return nil, status.Errorf(codes.Unavailable, "IP allocation not available")
	}

	if err := s.workerRepo.ReleaseIP(ctx, req.SandboxId); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to release IP: %v", err)
	}

	return &pb.ReleaseIPResponse{}, nil
}
