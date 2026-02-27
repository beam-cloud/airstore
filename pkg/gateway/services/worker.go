package services

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/beam-cloud/airstore/pkg/common"
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
	scheduler           *scheduler.Scheduler
	backend             repository.BackendRepository
	workerRepo          repository.WorkerRepository
	taskQueue           repository.TaskQueue
	redisClient         *common.RedisClient
	claimLeaseTTL       time.Duration
	recoveryLoopEnabled bool
	recoveryInterval    time.Duration
	recoveryBatchSize   int
	unclaimedStaleAfter time.Duration
}

const (
	defaultRunClaimLeaseTTL       = 45 * time.Second
	defaultRecoveryLoopInterval   = 10 * time.Second
	defaultRecoveryLoopBatchSize  = 50
	defaultUnclaimedRunStaleAfter = 2 * time.Minute
)

type delayedTaskQueue interface {
	PushDelayed(ctx context.Context, task *types.RunExecution, delay time.Duration) error
}

func NewWorkerService(
	sched *scheduler.Scheduler,
	backend repository.BackendRepository,
	workerRepo repository.WorkerRepository,
	taskQueue repository.TaskQueue,
	redisClient *common.RedisClient,
	schedulerConfig types.SchedulerConfig,
) *WorkerService {
	claimLeaseTTL := schedulerConfig.RunClaimLeaseTTL
	if claimLeaseTTL <= 0 {
		claimLeaseTTL = defaultRunClaimLeaseTTL
	}
	recoveryInterval := schedulerConfig.RecoveryLoopInterval
	if recoveryInterval <= 0 {
		recoveryInterval = defaultRecoveryLoopInterval
	}
	recoveryBatchSize := schedulerConfig.RecoveryLoopBatchSize
	if recoveryBatchSize <= 0 {
		recoveryBatchSize = defaultRecoveryLoopBatchSize
	}
	unclaimedStaleAfter := schedulerConfig.UnclaimedRunStaleAfter
	if unclaimedStaleAfter <= 0 {
		unclaimedStaleAfter = defaultUnclaimedRunStaleAfter
	}

	return &WorkerService{
		scheduler:           sched,
		backend:             backend,
		workerRepo:          workerRepo,
		taskQueue:           taskQueue,
		redisClient:         redisClient,
		claimLeaseTTL:       claimLeaseTTL,
		recoveryLoopEnabled: schedulerConfig.RecoveryLoopEnabled,
		recoveryInterval:    recoveryInterval,
		recoveryBatchSize:   recoveryBatchSize,
		unclaimedStaleAfter: unclaimedStaleAfter,
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

	if s.backend != nil {
		now := time.Now()
		expiresAt := now.Add(s.claimLeaseDuration())
		refreshed, err := s.backend.RefreshAgentRunClaims(ctx, req.WorkerId, now, expiresAt)
		if err != nil {
			log.Warn().
				Err(err).
				Str("worker_id", req.WorkerId).
				Msg("failed to refresh run claim leases on heartbeat")
		} else if refreshed > 0 {
			log.Debug().
				Str("worker_id", req.WorkerId).
				Int64("claims_refreshed", refreshed).
				Msg("refreshed active run claim leases")
		}
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

	attempt, err := s.lookupRunAttemptByExecutionID(ctx, req.TaskId)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to lookup run attempt: %v", err)
	}
	if attempt != nil {
		run, runErr := s.backend.GetAgentRunByID(ctx, attempt.RunID)
		if runErr == nil && run.Status.IsTerminal() {
			now := time.Now()
			errMsg := "run is already terminal"
			_ = s.backend.UpdateAgentRunAttemptResult(ctx, attempt.ID, types.AgentAttemptStatusCancelled, nil, now, &errMsg)
			_ = s.backend.SetRunExecutionResult(ctx, req.TaskId, -1, errMsg)
			_ = appendRunSnapshot(ctx, s.backend, attempt.RunID, run.Status, nil, &now, &errMsg, map[string]any{
				"attempt_id": attempt.ID,
				"task_id":    req.TaskId,
				"event":      string(types.AgentRunEventStartRejectedTerminalRun),
			})
			_ = s.markOriginTaskTerminalIfCurrentRun(ctx, attempt.RunID)
			return nil, status.Errorf(codes.FailedPrecondition, "run is already terminal")
		}
	}

	if err := s.backend.SetRunExecutionStarted(ctx, req.TaskId); err != nil {
		if _, ok := err.(*types.ErrRunExecutionNotFound); ok {
			return nil, status.Errorf(codes.NotFound, "task not found: %s", req.TaskId)
		}
		if isRunExecutionTerminalTransitionError(err) {
			return nil, status.Errorf(codes.FailedPrecondition, "run is already terminal")
		}
		return nil, status.Errorf(codes.Internal, "failed to set task started: %v", err)
	}

	if attempt != nil {
		now := time.Now()
		_ = s.backend.UpdateAgentRunAttemptStart(ctx, attempt.ID, now)
		workerID := s.resolveTaskWorkerID(ctx, req.TaskId)
		if workerID != "" {
			expiresAt := now.Add(s.claimLeaseDuration())
			if claimErr := s.backend.SetAgentRunClaim(ctx, attempt.RunID, workerID, now, expiresAt); claimErr != nil {
				log.Warn().
					Err(claimErr).
					Str("run_id", attempt.RunID).
					Str("task_id", req.TaskId).
					Str("worker_id", workerID).
					Msg("failed to set run claim lease on start")
			}
		}
		_ = s.backend.UpdateAgentRunLifecycle(ctx, attempt.RunID, types.AgentRunStatusRunning, &now, nil, nil)
		_ = appendRunSnapshot(ctx, s.backend, attempt.RunID, types.AgentRunStatusRunning, &now, nil, nil, map[string]any{
			"attempt_id": attempt.ID,
			"task_id":    req.TaskId,
			"event":      string(types.AgentRunEventStarted),
		})
		_ = updateExecutionInstanceCounts(ctx, s.backend, attempt.RunID, 1)
	} else {
		log.Debug().
			Str("task_id", req.TaskId).
			Msg("started run execution without run attempt mapping")
	}

	return &pb.SetTaskStartedResponse{}, nil
}

func (s *WorkerService) SetTaskResult(ctx context.Context, req *pb.SetTaskResultRequest) (*pb.SetTaskResultResponse, error) {
	if s.backend == nil {
		return nil, status.Errorf(codes.Unavailable, "task persistence not available")
	}

	attempt, attemptErr := s.lookupRunAttemptByExecutionID(ctx, req.TaskId)
	if attemptErr != nil {
		return nil, status.Errorf(codes.Internal, "failed to lookup run attempt: %v", attemptErr)
	}
	if attempt != nil && !isRunAttemptActive(attempt) {
		log.Info().
			Str("task_id", req.TaskId).
			Str("run_id", attempt.RunID).
			Str("attempt_id", attempt.ID).
			Msg("ignoring stale task result callback for non-active attempt")
		return &pb.SetTaskResultResponse{}, nil
	}

	if err := s.backend.SetRunExecutionResult(ctx, req.TaskId, int(req.ExitCode), req.Error); err != nil {
		if _, ok := err.(*types.ErrRunExecutionNotFound); ok {
			return nil, status.Errorf(codes.NotFound, "task not found: %s", req.TaskId)
		}
		return nil, status.Errorf(codes.Internal, "failed to set task result: %v", err)
	}

	if attempt != nil {
		if _, finalizeErr := s.finalizeRunAttempt(
			ctx,
			attempt,
			req.TaskId,
			int(req.ExitCode),
			req.Error,
			types.AgentRunEventFinished,
			nil,
		); finalizeErr != nil {
			log.Error().
				Err(finalizeErr).
				Str("task_id", req.TaskId).
				Str("run_id", attempt.RunID).
				Str("attempt_id", attempt.ID).
				Msg("failed to finalize run attempt after task result")
		}
	}

	return &pb.SetTaskResultResponse{}, nil
}

func isRunAttemptActive(attempt *types.AgentRunAttempt) bool {
	if attempt == nil {
		return false
	}
	if attempt.EndedAt != nil {
		return false
	}
	return attempt.Status.IsInFlight()
}

func isRunAttemptNotFound(err error) bool {
	if err == nil {
		return false
	}
	_, ok := err.(*types.ErrAgentRunAttemptNotFound)
	return ok
}

func isRunExecutionTerminalTransitionError(err error) bool {
	if err == nil {
		return false
	}
	lower := strings.ToLower(err.Error())
	return strings.Contains(lower, "cannot be started (already") ||
		strings.Contains(lower, "already finished")
}

func (s *WorkerService) lookupRunAttemptByExecutionID(ctx context.Context, taskID string) (*types.AgentRunAttempt, error) {
	if s.backend == nil {
		return nil, nil
	}
	attempt, err := s.backend.GetRunAttemptByExecutionID(ctx, taskID)
	if err != nil {
		if isRunAttemptNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	return attempt, nil
}

func classifyRunResult(exitCode int, errText string) (types.AgentAttemptStatus, types.AgentRunStatus, *string) {
	attemptStatus := types.AgentAttemptStatusOK
	runStatus := types.AgentRunStatusOK
	var errMsg *string
	if strings.TrimSpace(errText) != "" {
		msg := errText
		errMsg = &msg
	}

	lowerErr := strings.ToLower(errText)
	switch {
	case strings.Contains(lowerErr, "timeout"):
		attemptStatus = types.AgentAttemptStatusTimeout
		runStatus = types.AgentRunStatusTimeout
	case strings.Contains(lowerErr, "cancel"):
		attemptStatus = types.AgentAttemptStatusCancelled
		runStatus = types.AgentRunStatusCancelled
	case exitCode != 0 || strings.TrimSpace(errText) != "":
		attemptStatus = types.AgentAttemptStatusError
		runStatus = types.AgentRunStatusError
	}
	return attemptStatus, runStatus, errMsg
}

func mergePayload(dst map[string]any, extra map[string]any) map[string]any {
	if dst == nil {
		dst = map[string]any{}
	}
	for key, value := range extra {
		dst[key] = value
	}
	return dst
}

func (s *WorkerService) finalizeRunAttempt(
	ctx context.Context,
	attempt *types.AgentRunAttempt,
	taskID string,
	exitCode int,
	errText string,
	finishedEvent types.AgentRunEventType,
	extraPayload map[string]any,
) (bool, error) {
	if s.backend == nil || attempt == nil {
		return false, nil
	}
	if !isRunAttemptActive(attempt) {
		return false, nil
	}

	now := time.Now()
	attemptStatus, runStatus, errMsg := classifyRunResult(exitCode, errText)

	if err := s.backend.UpdateAgentRunAttemptResult(ctx, attempt.ID, attemptStatus, &exitCode, now, errMsg); err != nil {
		return false, fmt.Errorf("update run attempt result: %w", err)
	}
	if err := s.backend.ClearAgentRunClaim(ctx, attempt.RunID); err != nil {
		log.Warn().
			Err(err).
			Str("run_id", attempt.RunID).
			Msg("failed to clear run claim lease during finalization")
	}
	if err := updateExecutionInstanceCounts(ctx, s.backend, attempt.RunID, -1); err != nil {
		log.Warn().
			Err(err).
			Str("run_id", attempt.RunID).
			Msg("failed to decrement execution instance counters during finalization")
	}

	if shouldRetryAttempt(attemptStatus) {
		retryInfo, retryErr := s.scheduleRetryRun(ctx, attempt, taskID)
		if retryErr != nil {
			log.Warn().
				Err(retryErr).
				Str("run_id", attempt.RunID).
				Str("attempt_id", attempt.ID).
				Msg("failed to schedule retry run")
		} else if retryInfo.scheduled {
			payload := attemptSnapshotPayload(attempt.ID, taskID, exitCode, errText, types.AgentRunEventRetryScheduled)
			payload["next_run_id"] = retryInfo.nextRunID
			payload["next_attempt_no"] = retryInfo.nextAttemptNo
			payload[types.AgentExecutionMetaKeyRetryDelayMs] = retryInfo.delayMs
			payload = mergePayload(payload, extraPayload)
			if err := s.backend.UpdateAgentRunLifecycle(ctx, attempt.RunID, runStatus, nil, &now, errMsg); err != nil {
				return false, fmt.Errorf("update run lifecycle for retry: %w", err)
			}
			if err := appendRunSnapshot(ctx, s.backend, attempt.RunID, runStatus, nil, &now, errMsg, payload); err != nil {
				return false, fmt.Errorf("append retry snapshot: %w", err)
			}
			return true, nil
		}
	}

	payload := attemptSnapshotPayload(attempt.ID, taskID, exitCode, errText, finishedEvent)
	payload = mergePayload(payload, extraPayload)
	if err := s.backend.UpdateAgentRunLifecycle(ctx, attempt.RunID, runStatus, nil, &now, errMsg); err != nil {
		return false, fmt.Errorf("update run lifecycle: %w", err)
	}
	if err := appendRunSnapshot(ctx, s.backend, attempt.RunID, runStatus, nil, &now, errMsg, payload); err != nil {
		return false, fmt.Errorf("append completion snapshot: %w", err)
	}
	if err := s.markOriginTaskTerminalIfCurrentRun(ctx, attempt.RunID); err != nil {
		return false, fmt.Errorf("mark origin task terminal: %w", err)
	}
	return false, nil
}

func attemptSnapshotPayload(
	attemptID string,
	taskID string,
	exitCode int,
	errText string,
	event types.AgentRunEventType,
) map[string]any {
	return map[string]any{
		"attempt_id": attemptID,
		"task_id":    taskID,
		"exit_code":  exitCode,
		"error":      errText,
		"event":      string(event),
	}
}

func (s *WorkerService) resolveTaskWorkerID(ctx context.Context, taskID string) string {
	if s.taskQueue == nil || strings.TrimSpace(taskID) == "" {
		return ""
	}
	state, err := s.taskQueue.GetState(ctx, taskID)
	if err != nil || state == nil {
		return ""
	}
	return strings.TrimSpace(state.WorkerID)
}

func (s *WorkerService) claimLeaseDuration() time.Duration {
	if s.claimLeaseTTL > 0 {
		return s.claimLeaseTTL
	}
	return defaultRunClaimLeaseTTL
}

func (s *WorkerService) markOriginTaskTerminalIfCurrentRun(ctx context.Context, runID string) error {
	if s.backend == nil || strings.TrimSpace(runID) == "" {
		return nil
	}

	run, err := s.backend.GetAgentRunByID(ctx, runID)
	if err != nil {
		return err
	}
	task, err := s.backend.GetTaskByID(ctx, run.OriginTaskID)
	if err != nil {
		return err
	}

	// Ignore stale completions from superseded runs.
	if task.TargetRunID != nil && *task.TargetRunID != run.ID {
		return nil
	}
	if run.EndedAt != nil && task.UpdatedAt.After(*run.EndedAt) && task.State.IsDispatchable() {
		// Task state was reopened after this run had already ended.
		return nil
	}

	if task.State.IsTerminal() {
		return nil
	}
	targetRunID := run.ID
	nextState := types.AgentTaskStateDone
	if run.Status == types.AgentRunStatusCancelled {
		nextState = types.AgentTaskStateCancelled
	} else if run.Interactive && run.Status == types.AgentRunStatusOK {
		nextState = types.AgentTaskStateIdle
	}
	return s.backend.UpdateTaskState(ctx, task.ID, nextState, nil, &targetRunID)
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

type retryScheduleResult struct {
	scheduled     bool
	nextAttemptNo int
	delayMs       int
	nextRunID     string
}

func shouldRetryAttempt(status types.AgentAttemptStatus) bool {
	switch status {
	case types.AgentAttemptStatusError, types.AgentAttemptStatusTimeout:
		return true
	default:
		return false
	}
}

func (s *WorkerService) scheduleRetryRun(ctx context.Context, attempt *types.AgentRunAttempt, taskID string) (retryScheduleResult, error) {
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

	originTask, err := s.backend.GetTaskByID(ctx, run.OriginTaskID)
	if err != nil {
		return retryScheduleResult{}, err
	}
	if originTask.TargetRunID != nil && *originTask.TargetRunID != run.ID {
		// Another completion handler already advanced retries to a newer run.
		return retryScheduleResult{scheduled: false}, nil
	}
	if err := s.ensureSessionAvailableForRetry(ctx, run.WorkspaceID, run.SessionID, run.ID); err != nil {
		return retryScheduleResult{}, err
	}

	sourceTask, err := s.backend.GetRunExecution(ctx, taskID)
	if err != nil {
		return retryScheduleResult{}, err
	}

	nextAttemptNo := attempt.AttemptNo + 1
	if run.ExecAsk != string(orchestration.ExecAskOff) {
		return retryScheduleResult{scheduled: false}, nil
	}

	retryDelivery := cloneAnyMap(run.DeliveryJSON)
	if retryDelivery == nil {
		retryDelivery = map[string]any{}
	}
	retryDelivery[types.AgentExecutionMetaKeyRetryMaxAttempts] = retryPolicy.maxAttempts
	retryDelivery[types.AgentExecutionMetaKeyRetryDelayMs] = retryPolicy.delayMs
	retryDelivery["retry_from_run_id"] = run.ID
	retryDelivery["retry_attempt_no"] = nextAttemptNo

	retryRun := &types.AgentRun{
		WorkspaceID:     run.WorkspaceID,
		AgentID:         run.AgentID,
		OriginTaskID:    run.OriginTaskID,
		Status:          types.AgentRunStatusAccepted,
		SessionID:       run.SessionID,
		SessionKey:      run.SessionKey,
		Provider:        run.Provider,
		Model:           run.Model,
		ExecHost:        run.ExecHost,
		ExecSecurity:    run.ExecSecurity,
		ExecAsk:         run.ExecAsk,
		RuntimeType:     run.RuntimeType,
		WorkspaceAccess: run.WorkspaceAccess,
		NetworkEnabled:  run.NetworkEnabled,
		Interactive:     run.Interactive,
		TimeoutMs:       run.TimeoutMs,
		UsageJSON:       map[string]any{},
		DeliveryJSON:    retryDelivery,
	}
	if err := s.backend.CreateAgentRun(ctx, retryRun); err != nil {
		return retryScheduleResult{}, err
	}

	targetRunID := retryRun.ID
	if err := s.backend.UpdateTaskState(ctx, run.OriginTaskID, types.AgentTaskStateRunning, nil, &targetRunID); err != nil {
		return retryScheduleResult{}, err
	}

	retryAttempt := &types.AgentRunAttempt{
		RunID:           retryRun.ID,
		AttemptNo:       nextAttemptNo,
		Status:          types.AgentAttemptStatusPending,
		Strategy:        types.AgentAttemptStrategyRetry,
		Provider:        retryRun.Provider,
		Model:           retryRun.Model,
		ExecHost:        retryRun.ExecHost,
		ExecSecurity:    retryRun.ExecSecurity,
		ExecAsk:         retryRun.ExecAsk,
		RuntimeType:     retryRun.RuntimeType,
		WorkspaceAccess: retryRun.WorkspaceAccess,
		NetworkEnabled:  retryRun.NetworkEnabled,
		Interactive:     retryRun.Interactive,
	}
	if err := s.backend.CreateAgentRunAttempt(ctx, retryAttempt); err != nil {
		return retryScheduleResult{}, err
	}

	_ = appendRunSnapshot(ctx, s.backend, retryRun.ID, types.AgentRunStatusAccepted, nil, nil, nil, map[string]any{
		"event":             string(types.AgentRunEventAccepted),
		"task_id":           retryRun.OriginTaskID,
		"attempt_id":        retryAttempt.ID,
		"retry_from_run_id": run.ID,
		"retry_attempt_no":  nextAttemptNo,
	})

	_, memberToken, err := s.backend.EnsureWorkspaceServiceToken(ctx, run.WorkspaceID)
	if err != nil {
		return retryScheduleResult{}, err
	}
	taskEnv := cloneStringMap(sourceTask.Env)
	taskEnv["AIRSTORE_RUN_ID"] = retryRun.ID
	taskEnv["AIRSTORE_RUN_ATTEMPT_ID"] = retryAttempt.ID
	taskEnv["AIRSTORE_ORIGIN_TASK_ID"] = retryRun.OriginTaskID
	executionPolicy := cloneAnyMap(sourceTask.ExecutionPolicy)
	if executionPolicy == nil {
		executionPolicy = map[string]any{}
	}
	executionPolicy[types.AgentExecutionMetaKeyRunID] = retryRun.ID
	executionPolicy[types.AgentExecutionMetaKeyRunAttemptID] = retryAttempt.ID
	executionPolicy[types.AgentExecutionMetaKeyOriginTaskID] = retryRun.OriginTaskID
	executionPolicy[types.AgentExecutionMetaKeyRetry] = map[string]any{
		"max_attempts": retryPolicy.maxAttempts,
		"delay_ms":     retryPolicy.delayMs,
	}

	retryTask := &types.RunExecution{
		WorkspaceId:       retryRun.WorkspaceID,
		MemberToken:       memberToken,
		Status:            types.RunExecutionStatusPending,
		Type:              sourceTask.Type,
		Prompt:            sourceTask.Prompt,
		Image:             sourceTask.Image,
		Entrypoint:        cloneStringSlice(sourceTask.Entrypoint),
		Env:               taskEnv,
		Resources:         resolveRunExecutionResources(sourceTask),
		RunAttemptID:      &retryAttempt.ID,
		Attempt:           nextAttemptNo,
		MaxAttempts:       retryPolicy.maxAttempts,
		TimeoutMs:         sourceTask.TimeoutMs,
		ExecHost:          strPtrOrNil(retryRun.ExecHost),
		ExecSecurity:      strPtrOrNil(retryRun.ExecSecurity),
		ExecAsk:           strPtrOrNil(retryRun.ExecAsk),
		RuntimeType:       strPtrOrNil(retryRun.RuntimeType),
		WorkspaceAccess:   strPtrOrNil(retryRun.WorkspaceAccess),
		NetworkEnabled:    boolPtr(retryRun.NetworkEnabled),
		ExecutionPolicy:   executionPolicy,
		CreatedByMemberId: nil,
	}
	if retryTask.TimeoutMs == nil {
		timeout := retryRun.TimeoutMs
		retryTask.TimeoutMs = &timeout
	}
	if err := s.backend.CreateRunExecution(ctx, retryTask); err != nil {
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
			retryRunID := retryRun.ID
			go func(delay time.Duration, queuedTask types.RunExecution) {
				timer := time.NewTimer(delay)
				defer timer.Stop()
				<-timer.C

				latestRun, err := s.backend.GetAgentRunByID(context.Background(), retryRunID)
				if err != nil {
					log.Warn().
						Err(err).
						Str("task_id", queuedTask.ExternalId).
						Str("run_id", retryRunID).
						Msg("failed to recheck run status before delayed retry enqueue")
					return
				}
				if latestRun.Status.IsTerminal() {
					log.Info().
						Str("task_id", queuedTask.ExternalId).
						Str("run_id", retryRunID).
						Str("run_status", string(latestRun.Status)).
						Msg("skipping delayed retry enqueue for terminal run")
					return
				}
				if err := s.taskQueue.Push(context.Background(), &queuedTask); err != nil {
					log.Warn().
						Err(err).
						Str("task_id", queuedTask.ExternalId).
						Str("run_id", retryRunID).
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
		nextRunID:     retryRun.ID,
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

func resolveRunExecutionResources(exec *types.RunExecution) *types.RunExecutionResources {
	if exec == nil {
		return nil
	}
	if exec.Resources != nil {
		return exec.Resources
	}
	if exec.ExecutionPolicy == nil {
		return nil
	}
	raw, ok := exec.ExecutionPolicy[types.AgentExecutionMetaKeyResources]
	if !ok || raw == nil {
		return nil
	}
	resourcesMap, ok := raw.(map[string]any)
	if !ok {
		return nil
	}
	resources := &types.RunExecutionResources{
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

func (s *WorkerService) ensureSessionAvailableForRetry(
	ctx context.Context,
	workspaceID uint,
	sessionID string,
	excludeRunIDs ...string,
) error {
	if s == nil || s.backend == nil {
		return nil
	}
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" {
		return nil
	}

	conflicts, err := s.backend.ListActiveRunsBySession(ctx, workspaceID, sessionID, excludeRunIDs, 5)
	if err != nil {
		return err
	}
	if len(conflicts) == 0 {
		return nil
	}

	conflictRunID := strings.TrimSpace(conflicts[0].ID)
	if conflictRunID == "" {
		return fmt.Errorf("session ID %s is already in use", sessionID)
	}
	return fmt.Errorf("session ID %s is already in use by active run %s", sessionID, conflictRunID)
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
		Ip:            alloc.IP,
		Gateway:       alloc.Gateway,
		PrefixLen:     int32(alloc.PrefixLen),
		Ipv6:          alloc.IPv6,
		GatewayIpv6:   alloc.GatewayIPv6,
		PrefixLenIpv6: int32(alloc.PrefixLenIPv6),
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
