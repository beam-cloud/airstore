package services

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/beam-cloud/airstore/pkg/orchestration"
	"github.com/beam-cloud/airstore/pkg/repository"
	"github.com/beam-cloud/airstore/pkg/scheduler"
	"github.com/beam-cloud/airstore/pkg/types"
	pb "github.com/beam-cloud/airstore/proto"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type WorkerService struct {
	pb.UnimplementedWorkerServiceServer
	scheduler  *scheduler.Scheduler
	backend    repository.BackendRepository
	workerRepo repository.WorkerRepository
	taskQueue  repository.TaskQueue
}

type delayedTaskQueue interface {
	PushDelayed(ctx context.Context, task *types.Task, delay time.Duration) error
}

func NewWorkerService(
	sched *scheduler.Scheduler,
	backend repository.BackendRepository,
	workerRepo repository.WorkerRepository,
	taskQueue repository.TaskQueue,
) *WorkerService {
	return &WorkerService{
		scheduler:  sched,
		backend:    backend,
		workerRepo: workerRepo,
		taskQueue:  taskQueue,
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

	attempt, err := s.backend.GetRunAttemptByExecutionTaskExternalID(ctx, req.TaskId)
	if err == nil {
		run, runErr := s.backend.GetAgentRunByID(ctx, attempt.RunID)
		if runErr == nil && run.Status.IsTerminal() {
			now := time.Now()
			errMsg := "run is already terminal"
			_ = s.backend.UpdateAgentRunAttemptResult(ctx, attempt.ID, types.AgentAttemptStatusCancelled, nil, now, &errMsg)
			_ = s.backend.SetTaskResult(ctx, req.TaskId, -1, errMsg)
			_ = appendRunSnapshot(ctx, s.backend, attempt.RunID, run.Status, nil, &now, &errMsg, map[string]any{
				"attempt_id": attempt.ID,
				"task_id":    req.TaskId,
				"event":      "start_rejected_terminal_run",
			})
			return nil, status.Errorf(codes.FailedPrecondition, "run is already terminal")
		}
	}

	if err := s.backend.SetTaskStarted(ctx, req.TaskId); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to set task started: %v", err)
	}

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
		_ = updateExecutionInstanceCounts(ctx, s.backend, attempt.RunID, -1)

		if isSuperseded, supersededErr := hasNewerAttempt(ctx, s.backend, attempt.RunID, attempt.AttemptNo); supersededErr == nil && isSuperseded {
			_ = appendRunSnapshot(ctx, s.backend, attempt.RunID, types.AgentRunStatusRunning, nil, nil, nil, map[string]any{
				"attempt_id": attempt.ID,
				"task_id":    req.TaskId,
				"event":      "attempt_superseded",
			})
			return &pb.SetTaskResultResponse{}, nil
		}

		if shouldRetryAttempt(attemptStatus) {
			if retryInfo, retryErr := s.scheduleRetryAttempt(ctx, attempt, req.TaskId); retryErr == nil && retryInfo.scheduled {
				_ = s.backend.UpdateAgentRunLifecycle(ctx, attempt.RunID, types.AgentRunStatusRunning, nil, nil, nil)
				_ = appendRunSnapshot(ctx, s.backend, attempt.RunID, types.AgentRunStatusRunning, nil, nil, nil, map[string]any{
					"attempt_id":                            attempt.ID,
					"task_id":                               req.TaskId,
					"exit_code":                             req.ExitCode,
					"error":                                 req.Error,
					"event":                                 "retry_scheduled",
					"next_attempt_no":                       retryInfo.nextAttemptNo,
					types.AgentExecutionMetaKeyRetryDelayMs: retryInfo.delayMs,
				})
				return &pb.SetTaskResultResponse{}, nil
			}
		}

		_ = s.backend.UpdateAgentRunLifecycle(ctx, attempt.RunID, runStatus, nil, &now, errMsg)
		_ = appendRunSnapshot(ctx, s.backend, attempt.RunID, runStatus, nil, &now, errMsg, map[string]any{
			"attempt_id": attempt.ID,
			"task_id":    req.TaskId,
			"exit_code":  req.ExitCode,
			"error":      req.Error,
			"event":      "finished",
		})
	}

	return &pb.SetTaskResultResponse{}, nil
}

func appendRunSnapshot(
	ctx context.Context,
	backend repository.BackendRepository,
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

func updateExecutionInstanceCounts(ctx context.Context, backend repository.BackendRepository, runID string, runningDelta int) error {
	run, err := backend.GetAgentRunByID(ctx, runID)
	if err != nil {
		return err
	}
	instanceKeyVal, ok := run.DeliveryJSON[types.AgentExecutionMetaKeyInstanceKey]
	if !ok {
		return nil
	}
	instanceKey, ok := instanceKeyVal.(string)
	if !ok || instanceKey == "" {
		return nil
	}
	now := time.Now()
	return backend.AdjustExecutionInstanceRunningAttempts(ctx, instanceKey, runningDelta, &now)
}

func hasNewerAttempt(ctx context.Context, backend repository.BackendRepository, runID string, currentAttemptNo int) (bool, error) {
	attempts, err := backend.ListAgentRunAttempts(ctx, runID)
	if err != nil {
		return false, err
	}
	for _, attempt := range attempts {
		if attempt != nil && attempt.AttemptNo > currentAttemptNo {
			return true, nil
		}
	}
	return false, nil
}

type retryScheduleResult struct {
	scheduled     bool
	nextAttemptNo int
	delayMs       int
}

func shouldRetryAttempt(status types.AgentAttemptStatus) bool {
	switch status {
	case types.AgentAttemptStatusError, types.AgentAttemptStatusTimeout:
		return true
	default:
		return false
	}
}

func (s *WorkerService) scheduleRetryAttempt(ctx context.Context, attempt *types.AgentRunAttempt, taskID string) (retryScheduleResult, error) {
	if s.backend == nil || s.taskQueue == nil || attempt == nil {
		return retryScheduleResult{}, fmt.Errorf("retry dependencies are not available")
	}

	run, err := s.backend.GetAgentRunByID(ctx, attempt.RunID)
	if err != nil {
		return retryScheduleResult{}, err
	}
	retryPolicy := retryPolicyFromRun(run)
	if attempt.AttemptNo >= retryPolicy.maxAttempts {
		return retryScheduleResult{scheduled: false}, nil
	}

	sourceTask, err := s.backend.GetTask(ctx, taskID)
	if err != nil {
		return retryScheduleResult{}, err
	}

	nextAttemptNo := attempt.AttemptNo + 1
	attempts, err := s.backend.ListAgentRunAttempts(ctx, run.ID)
	if err == nil {
		for _, existing := range attempts {
			if existing != nil && existing.AttemptNo >= nextAttemptNo {
				nextAttemptNo = existing.AttemptNo + 1
			}
		}
	}

	retryAttempt := &types.AgentRunAttempt{
		RunID:           run.ID,
		AttemptNo:       nextAttemptNo,
		Status:          types.AgentAttemptStatusPending,
		Strategy:        types.AgentAttemptStrategyRetry,
		Provider:        run.Provider,
		Model:           run.Model,
		ExecHost:        run.ExecHost,
		ExecSecurity:    run.ExecSecurity,
		ExecAsk:         run.ExecAsk,
		RuntimeType:     run.RuntimeType,
		WorkspaceAccess: run.WorkspaceAccess,
		NetworkEnabled:  run.NetworkEnabled,
		Interactive:     run.Interactive,
	}
	if run.ExecAsk != string(orchestration.ExecAskOff) {
		retryAttempt.Status = types.AgentAttemptStatusBlocked
	}
	if err := s.backend.CreateAgentRunAttempt(ctx, retryAttempt); err != nil {
		return retryScheduleResult{}, err
	}
	if retryAttempt.Status == types.AgentAttemptStatusBlocked {
		return retryScheduleResult{scheduled: false}, nil
	}

	_, memberToken, err := s.backend.EnsureWorkspaceServiceToken(ctx, run.WorkspaceID)
	if err != nil {
		return retryScheduleResult{}, err
	}
	taskEnv := cloneStringMap(sourceTask.Env)
	executionPolicy := cloneAnyMap(sourceTask.ExecutionPolicy)
	if executionPolicy == nil {
		executionPolicy = map[string]any{}
	}
	executionPolicy[types.AgentExecutionMetaKeyRetry] = map[string]any{
		"max_attempts": retryPolicy.maxAttempts,
		"delay_ms":     retryPolicy.delayMs,
	}

	retryTask := &types.Task{
		WorkspaceId:       run.WorkspaceID,
		MemberToken:       memberToken,
		Status:            types.TaskStatusPending,
		Type:              sourceTask.Type,
		Prompt:            sourceTask.Prompt,
		Image:             sourceTask.Image,
		Entrypoint:        cloneStringSlice(sourceTask.Entrypoint),
		Env:               taskEnv,
		Resources:         resolveTaskResources(sourceTask),
		RunAttemptID:      &retryAttempt.ID,
		TimeoutMs:         sourceTask.TimeoutMs,
		ExecHost:          strPtrOrNil(run.ExecHost),
		ExecSecurity:      strPtrOrNil(run.ExecSecurity),
		ExecAsk:           strPtrOrNil(run.ExecAsk),
		RuntimeType:       strPtrOrNil(run.RuntimeType),
		WorkspaceAccess:   strPtrOrNil(run.WorkspaceAccess),
		NetworkEnabled:    boolPtr(run.NetworkEnabled),
		ExecutionPolicy:   executionPolicy,
		CreatedByMemberId: nil,
	}
	if retryTask.TimeoutMs == nil {
		timeout := run.TimeoutMs
		retryTask.TimeoutMs = &timeout
	}
	if err := s.backend.CreateTask(ctx, retryTask); err != nil {
		return retryScheduleResult{}, err
	}
	if err := s.backend.BindAttemptExecutionTask(ctx, retryAttempt.ID, retryTask.ExternalId); err != nil {
		return retryScheduleResult{}, err
	}
	if retryPolicy.delayMs <= 0 {
		if err := s.taskQueue.Push(ctx, retryTask); err != nil {
			return retryScheduleResult{}, err
		}
	} else {
		delay := time.Duration(retryPolicy.delayMs) * time.Millisecond
		if delayedQueue, ok := s.taskQueue.(delayedTaskQueue); ok {
			if err := delayedQueue.PushDelayed(ctx, retryTask, delay); err != nil {
				return retryScheduleResult{}, err
			}
		} else {
			taskCopy := *retryTask
			runID := run.ID
			go func(delay time.Duration, queuedTask types.Task) {
				timer := time.NewTimer(delay)
				defer timer.Stop()
				<-timer.C

				latestRun, err := s.backend.GetAgentRunByID(context.Background(), runID)
				if err != nil {
					log.Warn().
						Err(err).
						Str("task_id", queuedTask.ExternalId).
						Str("run_id", runID).
						Msg("failed to recheck run status before delayed retry enqueue")
					return
				}
				if latestRun.Status.IsTerminal() {
					log.Info().
						Str("task_id", queuedTask.ExternalId).
						Str("run_id", runID).
						Str("run_status", string(latestRun.Status)).
						Msg("skipping delayed retry enqueue for terminal run")
					return
				}
				if err := s.taskQueue.Push(context.Background(), &queuedTask); err != nil {
					log.Warn().
						Err(err).
						Str("task_id", queuedTask.ExternalId).
						Str("run_id", runID).
						Int("delay_ms", retryPolicy.delayMs).
						Msg("failed to enqueue delayed retry task")
				}
			}(delay, taskCopy)
		}
	}

	return retryScheduleResult{
		scheduled:     true,
		nextAttemptNo: nextAttemptNo,
		delayMs:       retryPolicy.delayMs,
	}, nil
}

type runRetryPolicy struct {
	maxAttempts int
	delayMs     int
}

func retryPolicyFromRun(run *types.AgentRun) runRetryPolicy {
	policy := runRetryPolicy{
		maxAttempts: orchestration.DefaultRetryMaxAttempts,
		delayMs:     orchestration.DefaultRetryDelayMs,
	}
	if run == nil || len(run.DeliveryJSON) == 0 {
		return policy
	}

	if value, ok := run.DeliveryJSON[types.AgentExecutionMetaKeyRetryMaxAttempts]; ok {
		if parsed := intFromAny(value); parsed > 0 {
			policy.maxAttempts = parsed
		}
	}
	if value, ok := run.DeliveryJSON[types.AgentExecutionMetaKeyRetryDelayMs]; ok {
		if parsed := intFromAny(value); parsed >= 0 {
			policy.delayMs = parsed
		}
	}
	if nested, ok := run.DeliveryJSON[types.AgentExecutionMetaKeyRetry].(map[string]any); ok {
		if parsed := intFromAny(nested["max_attempts"]); parsed > 0 {
			policy.maxAttempts = parsed
		}
		if parsed := intFromAny(nested["delay_ms"]); parsed >= 0 {
			policy.delayMs = parsed
		}
	}
	return policy
}

func intFromAny(value any) int {
	switch typed := value.(type) {
	case int:
		return typed
	case int32:
		return int(typed)
	case int64:
		return int(typed)
	case float32:
		return int(typed)
	case float64:
		return int(typed)
	case string:
		var parsed int
		if _, err := fmt.Sscanf(strings.TrimSpace(typed), "%d", &parsed); err == nil {
			return parsed
		}
		return 0
	default:
		return 0
	}
}

func cloneStringMap(src map[string]string) map[string]string {
	if len(src) == 0 {
		return map[string]string{}
	}
	dst := make(map[string]string, len(src))
	for key, value := range src {
		dst[key] = value
	}
	return dst
}

func cloneAnyMap(src map[string]any) map[string]any {
	if len(src) == 0 {
		return map[string]any{}
	}
	dst := make(map[string]any, len(src))
	for key, value := range src {
		dst[key] = value
	}
	return dst
}

func cloneStringSlice(src []string) []string {
	if len(src) == 0 {
		return []string{}
	}
	dst := make([]string, len(src))
	copy(dst, src)
	return dst
}

func resolveTaskResources(task *types.Task) *types.TaskResources {
	if task == nil {
		return nil
	}
	if task.Resources != nil {
		return task.Resources
	}
	if task.ExecutionPolicy == nil {
		return nil
	}
	raw, ok := task.ExecutionPolicy[types.AgentExecutionMetaKeyResources]
	if !ok || raw == nil {
		return nil
	}
	resourcesMap, ok := raw.(map[string]any)
	if !ok {
		return nil
	}
	resources := &types.TaskResources{
		CPU:    int64(intFromAny(resourcesMap["cpu"])),
		Memory: int64(intFromAny(resourcesMap["memory"])),
		GPU:    intFromAny(resourcesMap["gpu"]),
	}
	if resources.CPU == 0 && resources.Memory == 0 && resources.GPU == 0 {
		return nil
	}
	return resources
}

func strPtrOrNil(value string) *string {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	return &value
}

func boolPtr(value bool) *bool {
	return &value
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
