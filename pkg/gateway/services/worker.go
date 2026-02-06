package services

import (
	"context"

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

	return &pb.SetTaskResultResponse{}, nil
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
