package services

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/beam-cloud/airstore/pkg/common"
	"github.com/beam-cloud/airstore/pkg/orchestration"
	"github.com/beam-cloud/airstore/pkg/repository"
	"github.com/beam-cloud/airstore/pkg/scheduler"
	"github.com/beam-cloud/airstore/pkg/types"
	"github.com/beam-cloud/airstore/pkg/views"
	viewbaml "github.com/beam-cloud/airstore/pkg/views/baml_client"
	viewbamltypes "github.com/beam-cloud/airstore/pkg/views/baml_client/types"
	pb "github.com/beam-cloud/airstore/proto"
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type WorkerService struct {
	pb.UnimplementedWorkerServiceServer
	scheduler              *scheduler.Scheduler
	backend                repository.BackendRepository
	workerRepo             repository.WorkerRepository
	taskQueue              repository.TaskQueue
	redisClient            *common.RedisClient
	terminalIO             repository.TerminalIORepository
	lifecycle              *orchestration.TaskLifecycle
	viewStore              *views.ViewStore
	claimLeaseTTL          time.Duration
	unclaimedRunStaleAfter time.Duration
	recoveryLoopEnabled    bool
	recoveryInterval       time.Duration
	recoveryBatchSize      int
}

const (
	defaultRunClaimLeaseTTL       = 45 * time.Second
	defaultUnclaimedRunStaleAfter = 2 * time.Minute
	defaultRecoveryLoopInterval   = 10 * time.Second
	defaultRecoveryLoopBatchSize  = 50
)

func NewWorkerService(
	sched *scheduler.Scheduler,
	backend repository.BackendRepository,
	workerRepo repository.WorkerRepository,
	taskQueue repository.TaskQueue,
	redisClient *common.RedisClient,
	schedulerConfig types.SchedulerConfig,
	viewStore *views.ViewStore,
) *WorkerService {
	claimLeaseTTL := schedulerConfig.RunClaimLeaseTTL
	if claimLeaseTTL <= 0 {
		claimLeaseTTL = defaultRunClaimLeaseTTL
	}
	unclaimedRunStaleAfter := schedulerConfig.UnclaimedRunStaleAfter
	if unclaimedRunStaleAfter <= 0 {
		unclaimedRunStaleAfter = defaultUnclaimedRunStaleAfter
	}
	recoveryInterval := schedulerConfig.RecoveryLoopInterval
	if recoveryInterval <= 0 {
		recoveryInterval = defaultRecoveryLoopInterval
	}
	recoveryBatchSize := schedulerConfig.RecoveryLoopBatchSize
	if recoveryBatchSize <= 0 {
		recoveryBatchSize = defaultRecoveryLoopBatchSize
	}

	var terminalIO repository.TerminalIORepository
	if redisClient != nil {
		terminalIO = repository.NewRedisTerminalIORepository(redisClient)
	}

	var orchStore *repository.OrchestrationStore
	if redisClient != nil {
		orchStore = repository.NewOrchestrationStore(backend, redisClient)
	}

	return &WorkerService{
		scheduler:              sched,
		backend:                backend,
		workerRepo:             workerRepo,
		taskQueue:              taskQueue,
		redisClient:            redisClient,
		terminalIO:             terminalIO,
		lifecycle:              orchestration.NewTaskLifecycle(backend, orchStore, terminalIO),
		viewStore:              viewStore,
		claimLeaseTTL:          claimLeaseTTL,
		unclaimedRunStaleAfter: unclaimedRunStaleAfter,
		recoveryLoopEnabled:    schedulerConfig.RecoveryLoopEnabled,
		recoveryInterval:       recoveryInterval,
		recoveryBatchSize:      recoveryBatchSize,
	}
}

func (s *WorkerService) publishTaskUpdate(ctx context.Context, workspaceID uint, taskID string) {
	if s == nil || s.redisClient == nil || strings.TrimSpace(taskID) == "" {
		return
	}
	store := repository.NewOrchestrationStore(s.backend, s.redisClient)
	if err := store.PublishTaskLive(ctx, taskID); err != nil {
		log.Debug().Err(err).Str("task_id", taskID).Msg("failed to publish task live update")
	}
	if err := store.PublishWorkspaceLive(ctx, workspaceID); err != nil {
		log.Debug().Err(err).Uint("workspace_id", workspaceID).Msg("failed to publish workspace live update")
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
	attemptID := strings.TrimSpace(req.AttemptId)
	if attemptID == "" {
		return nil, status.Error(codes.InvalidArgument, "attempt_id is required")
	}

	attempt, err := s.lookupRunAttemptByExecutionID(ctx, req.TaskId)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to lookup run attempt: %v", err)
	}
	if attempt == nil {
		return nil, status.Errorf(codes.FailedPrecondition, "run attempt mapping not found")
	}
	if attempt.ID != attemptID {
		log.Info().
			Str("task_id", req.TaskId).
			Str("expected_attempt", attemptID).
			Str("current_attempt", attempt.ID).
			Msg("ignoring stale task start callback for superseded attempt")
		return &pb.SetTaskStartedResponse{}, nil
	}
	run, runErr := s.backend.GetAgentRunByID(ctx, attempt.RunID)
	if runErr == nil && run.Status.IsTerminal() {
		now := time.Now()
		errMsg := "run is already terminal"
		_ = s.backend.UpdateAgentRunAttemptResult(ctx, attempt.ID, types.AgentAttemptStatusCancelled, nil, now, &errMsg)
		_, _ = s.backend.SetRunExecutionResultForAttempt(ctx, req.TaskId, attemptID, -1, errMsg)
		_ = appendRunSnapshot(ctx, s.backend, attempt.RunID, run.Status, nil, &now, &errMsg, map[string]any{
			types.AgentRunEventPayloadKeyAttemptID: attempt.ID,
			types.AgentRunEventPayloadKeyTaskID:    req.TaskId,
			types.AgentRunEventPayloadKeyEvent:     string(types.AgentRunEventStartRejectedTerminalRun),
		})
		_ = s.settleOriginTask(ctx, attempt.RunID)
		return nil, status.Errorf(codes.FailedPrecondition, "run is already terminal")
	}

	applied, err := s.backend.SetRunExecutionStartedForAttempt(ctx, req.TaskId, attemptID)
	if err != nil {
		if _, ok := err.(*types.ErrRunExecutionNotFound); ok {
			return nil, status.Errorf(codes.NotFound, "task not found: %s", req.TaskId)
		}
		if isRunExecutionTerminalTransitionError(err) {
			return nil, status.Errorf(codes.FailedPrecondition, "run is already terminal")
		}
		return nil, status.Errorf(codes.Internal, "failed to set task started: %v", err)
	}
	if !applied {
		return &pb.SetTaskStartedResponse{}, nil
	}

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
		types.AgentRunEventPayloadKeyAttemptID: attempt.ID,
		types.AgentRunEventPayloadKeyTaskID:    req.TaskId,
		types.AgentRunEventPayloadKeyEvent:     string(types.AgentRunEventStarted),
	})
	_ = updateExecutionInstanceCounts(ctx, s.backend, attempt.RunID, 1)

	return &pb.SetTaskStartedResponse{}, nil
}

func (s *WorkerService) SetTaskResult(ctx context.Context, req *pb.SetTaskResultRequest) (*pb.SetTaskResultResponse, error) {
	if s.backend == nil {
		return nil, status.Errorf(codes.Unavailable, "task persistence not available")
	}
	attemptID := strings.TrimSpace(req.AttemptId)
	if attemptID == "" {
		return nil, status.Error(codes.InvalidArgument, "attempt_id is required")
	}

	attempt, attemptErr := s.lookupRunAttemptByExecutionID(ctx, req.TaskId)
	if attemptErr != nil {
		return nil, status.Errorf(codes.Internal, "failed to lookup run attempt: %v", attemptErr)
	}
	if attempt == nil {
		return nil, status.Errorf(codes.FailedPrecondition, "run attempt mapping not found")
	}
	if !attempt.IsActive() {
		log.Debug().
			Str("task_id", req.TaskId).
			Str("run_id", attempt.RunID).
			Str("attempt_id", attempt.ID).
			Str("reported_attempt_id", attemptID).
			Str("attempt_status", string(attempt.Status)).
			Bool("attempt_ended", attempt.EndedAt != nil).
			Msg("dropping duplicate/stale task result callback after attempt finalized")
		return &pb.SetTaskResultResponse{}, nil
	}

	// Defense-in-depth: if the worker supplies an attempt_id and it no longer
	// matches the current attempt on the run, this result belongs to a
	// superseded execution. Skip finalization to avoid marking a newer
	// attempt terminal while its worker is still running.
	if attempt.ID != attemptID {
		log.Debug().
			Str("task_id", req.TaskId).
			Str("expected_attempt", attemptID).
			Str("current_attempt", attempt.ID).
			Msg("dropping stale task result callback for superseded attempt")
		return &pb.SetTaskResultResponse{}, nil
	}

	resultKey := fmt.Sprintf("run_result:%s:%s", strings.TrimSpace(req.TaskId), attemptID)
	payload := orchestration.RunResultEnvelope{
		TaskID:    strings.TrimSpace(req.TaskId),
		AttemptID: attemptID,
		ExitCode:  int(req.ExitCode),
		ErrorText: req.Error,
		ResultKey: resultKey,
		PostRun:   postRunFromProto(req),
	}.ToMap()
	if err := s.backend.EnqueueOrchestrationOutboxEvent(ctx, &types.OrchestrationOutboxEvent{
		EventType:   types.OrchestrationOutboxEventTypeRunResult,
		DedupeKey:   resultKey,
		PayloadJSON: payload,
		AvailableAt: time.Now(),
	}); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to enqueue task result: %v", err)
	}

	return &pb.SetTaskResultResponse{}, nil
}

func postRunFromProto(req *pb.SetTaskResultRequest) *types.RunExecutionPostRun {
	if req == nil {
		return nil
	}
	return types.NormalizeRunExecutionPostRun(&types.RunExecutionPostRun{
		WaitingForInput:     req.WaitingForInput,
		WakeSignal:          wakeSignalFromProto(req.WakeSignal),
		SubtaskRequests:     subtaskRequestsFromProto(req.SubtaskRequests),
		SourceWatchRequests: sourceWatchRequestsFromProto(req.SourceWatchRequests),
	})
}

func wakeSignalFromProto(signal *pb.WakeSignal) *types.RunExecutionWakeSignal {
	if signal == nil {
		return nil
	}

	agenda := make([]*types.TaskWakeAgendaItem, 0, len(signal.WakeAgenda))
	for idx, item := range signal.WakeAgenda {
		if item == nil {
			continue
		}
		agenda = append(agenda, &types.TaskWakeAgendaItem{
			Seq:    idx + 1,
			Type:   item.Type,
			Title:  item.Title,
			Reason: item.Reason,
		})
	}

	return types.NormalizeRunExecutionWakeSignal(&types.RunExecutionWakeSignal{
		DelayMinutes:   int(signal.DelayMinutes),
		Reason:         signal.Reason,
		FollowUpPrompt: signal.FollowUpPrompt,
		WakeAgenda:     agenda,
	})
}

func subtaskRequestsFromProto(requests []*pb.SubtaskRequest) []*types.SubtaskRequest {
	if len(requests) == 0 {
		return nil
	}
	out := make([]*types.SubtaskRequest, 0, len(requests))
	for _, req := range requests {
		if req == nil {
			continue
		}
		out = append(out, &types.SubtaskRequest{
			SourceOutputID:   req.SourceOutputId,
			EntityLabel:      req.EntityLabel,
			Prompt:           req.Prompt,
			WakeDelayMinutes: int(req.WakeDelayMinutes),
		})
	}
	return out
}

func sourceWatchRequestsFromProto(requests []*pb.SourceWatchRequest) []*types.SourceWatchRequest {
	if len(requests) == 0 {
		return nil
	}
	out := make([]*types.SourceWatchRequest, 0, len(requests))
	for _, req := range requests {
		if req == nil {
			continue
		}
		out = append(out, &types.SourceWatchRequest{
			Integration:        req.Integration,
			Reason:             req.Reason,
			Query:              req.Query,
			FilenameFormat:     req.FilenameFormat,
			EventTypes:         append([]string{}, req.EventTypes...),
			EntityKey:          req.EntityKey,
			EntityLabel:        req.EntityLabel,
			SourceOutputID:     req.SourceOutputId,
			ThreadID:           req.ThreadId,
			MessageID:          req.MessageId,
			IncludeAttachments: req.IncludeAttachments,
			IncludeInline:      req.IncludeInline,
			IncludeMessageBody: req.IncludeMessageBody,
		})
	}
	return out
}

func (s *WorkerService) UpdateTaskState(ctx context.Context, req *pb.UpdateTaskStateRequest) (*pb.UpdateTaskStateResponse, error) {
	if s.backend == nil {
		return nil, status.Errorf(codes.Unavailable, "task persistence not available")
	}
	taskID := strings.TrimSpace(req.TaskId)
	runID := strings.TrimSpace(req.RunId)
	state := types.AgentTaskState(strings.TrimSpace(req.State))
	if taskID == "" || runID == "" || state == "" {
		return nil, status.Error(codes.InvalidArgument, "task_id, state, and run_id are required")
	}
	if state != types.AgentTaskStateWaiting && state != types.AgentTaskStateRunning {
		return nil, status.Errorf(codes.InvalidArgument, "only waiting/running transitions are allowed, got %q", state)
	}

	update, err := buildTaskLiveUpdate(taskID, runID, state, req)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "update task state: %v", err)
	}
	if _, err := s.lifecycle.TransitionLive(ctx, update); err != nil {
		return nil, status.Errorf(codes.Internal, "update task state: %v", err)
	}
	if run, err := s.backend.GetAgentRunByID(ctx, runID); err == nil && run != nil {
		s.publishTaskUpdate(ctx, run.WorkspaceID, taskID)
	}
	return &pb.UpdateTaskStateResponse{}, nil
}

func buildTaskLiveUpdate(
	taskID string,
	runID string,
	state types.AgentTaskState,
	req *pb.UpdateTaskStateRequest,
) (types.TaskLiveUpdate, error) {
	update := types.TaskLiveUpdate{
		TaskID: taskID,
		RunID:  runID,
		State:  state,
	}
	if state != types.AgentTaskStateWaiting {
		return update, nil
	}
	blocker, err := buildTaskBlockerSpec(req)
	if err != nil {
		return update, err
	}
	update.Blocker = blocker
	return update, nil
}

func buildTaskBlockerSpec(req *pb.UpdateTaskStateRequest) (*types.TaskBlockerSpec, error) {
	if req == nil {
		return nil, fmt.Errorf("request is required")
	}
	inputKind := types.InputKind(strings.TrimSpace(req.InputKind))
	spec := &types.TaskBlockerSpec{
		Kind:      types.TaskBlockerKind(strings.TrimSpace(req.BlockerKind)),
		InputKind: inputKind,
		OutputIDs: trimNonEmptyStrings(req.BlockerOutputIds),
	}
	if spec.Kind == "" {
		spec.Kind = types.TaskBlockerKindForInputKind(inputKind)
	}
	if waitGroupID := strings.TrimSpace(req.BlockerWaitGroupId); waitGroupID != "" {
		spec.WaitGroupID = &waitGroupID
	}
	if payload := strings.TrimSpace(req.BlockerPayloadJson); payload != "" {
		if err := json.Unmarshal([]byte(payload), &spec.PayloadJSON); err != nil {
			return nil, fmt.Errorf("invalid blocker payload: %w", err)
		}
	} else {
		return nil, fmt.Errorf("waiting update requires blocker payload")
	}
	if spec.Kind == "" && spec.InputKind == "" {
		return nil, fmt.Errorf("waiting update requires blocker kind or input kind")
	}
	if spec.InputKind == "" {
		switch spec.Kind {
		case types.TaskBlockerKindApproval:
			spec.InputKind = types.InputKindApproveReject
		default:
			spec.InputKind = types.InputKindFreeText
		}
	}
	if spec.Kind == "" {
		spec.Kind = types.TaskBlockerKindForInputKind(spec.InputKind)
	}
	return spec, nil
}

func trimNonEmptyStrings(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	out := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		trimmed := strings.TrimSpace(value)
		if trimmed == "" {
			continue
		}
		if _, ok := seen[trimmed]; ok {
			continue
		}
		seen[trimmed] = struct{}{}
		out = append(out, trimmed)
	}
	return out
}

func (s *WorkerService) ClaimTaskInput(ctx context.Context, req *pb.ClaimTaskInputRequest) (*pb.ClaimTaskInputResponse, error) {
	if s.backend == nil {
		return nil, status.Errorf(codes.Unavailable, "task persistence not available")
	}
	taskID := strings.TrimSpace(req.TaskId)
	runID := strings.TrimSpace(req.RunId)
	executionID := strings.TrimSpace(req.ExecutionId)
	if taskID == "" || runID == "" || executionID == "" {
		return nil, status.Error(codes.InvalidArgument, "task_id, run_id, and execution_id are required")
	}
	input, err := s.backend.ClaimNextTaskInput(ctx, taskID, runID, executionID)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "claim task input: %v", err)
	}
	if input == nil {
		return &pb.ClaimTaskInputResponse{Found: false}, nil
	}
	resp := &pb.ClaimTaskInputResponse{
		Found:   true,
		InputId: input.ID,
		Message: input.Message,
		Kind:    string(input.Kind),
		Seq:     int32(input.Seq),
	}
	if input.Action != nil {
		resp.Action = string(*input.Action)
	}
	return resp, nil
}

func (s *WorkerService) AckTaskInput(ctx context.Context, req *pb.AckTaskInputRequest) (*pb.AckTaskInputResponse, error) {
	if s.backend == nil {
		return nil, status.Errorf(codes.Unavailable, "task persistence not available")
	}
	inputID := strings.TrimSpace(req.InputId)
	if inputID == "" {
		return nil, status.Error(codes.InvalidArgument, "input_id is required")
	}
	if err := s.backend.AckTaskInputConsumed(ctx, inputID); err != nil {
		return nil, status.Errorf(codes.Internal, "ack task input: %v", err)
	}
	return &pb.AckTaskInputResponse{}, nil
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

func (s *WorkerService) settleOriginTask(ctx context.Context, runID string) error {
	return s.lifecycle.Settle(ctx, runID, nil)
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

// ── Task Outputs ────────────────────────────────────────────────────────────

func scopedIdempotentTaskOutputID(workspaceID uint32, taskID, rawID string) string {
	taskID = strings.TrimSpace(taskID)
	rawID = strings.TrimSpace(rawID)
	if rawID == "" {
		return ""
	}
	return uuid.NewSHA1(
		uuid.NameSpaceOID,
		[]byte(fmt.Sprintf("task-output:%d:%s:%s", workspaceID, taskID, rawID)),
	).String()
}

func (s *WorkerService) CreateTaskOutput(ctx context.Context, req *pb.CreateTaskOutputRequest) (*pb.CreateTaskOutputResponse, error) {
	if s.backend == nil {
		return nil, status.Errorf(codes.Unavailable, "task persistence not available")
	}

	output := &types.TaskOutput{
		WorkspaceID: uint(req.WorkspaceId),
		TaskID:      req.TaskId,
		OutputType:  req.OutputType,
		Title:       req.Title,
	}
	if req.RunId != "" {
		output.RunID = &req.RunId
	}
	if req.AgentId != "" {
		output.AgentID = &req.AgentId
	}
	if req.DataJson != "" {
		if err := json.Unmarshal([]byte(req.DataJson), &output.Data); err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "invalid data_json: %v", err)
		}
	} else {
		output.Data = map[string]any{}
	}
	if req.MetadataJson != "" {
		if err := json.Unmarshal([]byte(req.MetadataJson), &output.Metadata); err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "invalid metadata_json: %v", err)
		}
	}
	if output.Metadata != nil {
		if idempID, ok := output.Metadata["_idempotent_output_id"].(string); ok && idempID != "" {
			output.ID = scopedIdempotentTaskOutputID(req.WorkspaceId, req.TaskId, idempID)
			delete(output.Metadata, "_idempotent_output_id")
		}
	}
	if req.Uri != "" {
		output.URI = &req.Uri
	}
	if req.Status != "" {
		output.Status = req.Status
	}

	if err := s.backend.CreateTaskOutput(ctx, output); err != nil {
		if _, ok := err.(*types.ErrTaskOutputConflict); ok {
			return nil, status.Errorf(codes.AlreadyExists, "create output: %v", err)
		}
		return nil, status.Errorf(codes.Internal, "create output: %v", err)
	}

	go s.enrichViewRows(context.Background(), output)
	s.publishTaskUpdate(ctx, output.WorkspaceID, output.TaskID)
	return &pb.CreateTaskOutputResponse{Id: output.ID}, nil
}

const (
	enrichViewRowsTimeout     = 60 * time.Second
	enrichViewRowsMaxExisting = 200
	enrichCellValueMaxLen     = 200
)

// enrichViewRows runs async BAML mapping on the gateway side after an output
// is persisted. It classifies which rows are affected, then populates cells
// for each matched row (or inserts new ones for unmatched entities).
func (s *WorkerService) enrichViewRows(ctx context.Context, output *types.TaskOutput) {
	if s.viewStore == nil || !s.viewStore.Available() || s.backend == nil || output == nil {
		return
	}
	if strings.TrimSpace(output.OutputType) == "" {
		return
	}

	agentID := ""
	if output.AgentID != nil {
		agentID = strings.TrimSpace(*output.AgentID)
	}
	if agentID == "" {
		return
	}

	ctx, cancel := context.WithTimeout(ctx, enrichViewRowsTimeout)
	defer cancel()

	contexts, err := types.LoadViewOutputSchemaContexts(ctx, s.backend, output.WorkspaceID, agentID)
	if err != nil {
		log.Warn().Err(err).Str("task_id", output.TaskID).Msg("enrichViewRows: failed to load schema contexts")
		return
	}
	if len(contexts) == 0 {
		return
	}

	for _, schemaCtx := range contexts {
		if err := s.enrichViewRowsForContext(ctx, output, schemaCtx); err != nil {
			log.Warn().Err(err).
				Str("task_id", output.TaskID).
				Str("view_id", schemaCtx.ViewID).
				Str("sheet_id", schemaCtx.SheetID).
				Msg("enrichViewRows: failed for context")
		}
	}
}

func (s *WorkerService) enrichViewRowsForContext(
	ctx context.Context,
	output *types.TaskOutput,
	schemaCtx types.ViewOutputSchemaContext,
) error {
	if schemaCtx.ViewID == "" || schemaCtx.SheetID == "" || len(schemaCtx.Columns) == 0 {
		return nil
	}

	if !outputMatchesSchemaContext(output, schemaCtx) {
		return nil
	}

	existingRows, err := s.viewStore.GetRows(ctx, schemaCtx.ViewID, schemaCtx.SheetID, schemaCtx.ComponentID)
	if err != nil {
		return fmt.Errorf("load existing rows: %w", err)
	}

	sortRowsByUpdatedDesc(existingRows)
	if len(existingRows) > enrichViewRowsMaxExisting {
		existingRows = existingRows[:enrichViewRowsMaxExisting]
	}

	schemaColumnKeys := schemaCtx.ColumnKeys()
	columns := make([]viewbamltypes.ViewColumn, len(schemaCtx.Columns))
	for i, c := range schemaCtx.Columns {
		columns[i] = viewbamltypes.ViewColumn{Key: c.Key, Label: c.Label, Type: c.Type}
	}

	outputData := serializeOutputForBAML(output)
	summary := ""
	if output.Summary != nil {
		summary = *output.Summary
	}

	// Step 1: Classify which rows are affected
	classifyRowsJSON := serializeRowsForClassification(existingRows, schemaColumnKeys)

	classification, err := viewbaml.ClassifyAffectedRows(
		ctx,
		columns,
		output.OutputType,
		output.Title,
		summary,
		outputData,
		classifyRowsJSON,
	)
	if err != nil {
		return fmt.Errorf("BAML ClassifyAffectedRows: %w", err)
	}

	log.Info().
		Str("task_id", output.TaskID).
		Str("view_id", schemaCtx.ViewID).
		Int("matched_rows", len(classification.Affected_row_ids)).
		Int("unmatched_entities", len(classification.Unmatched_entities)).
		Msg("enrichViewRows: classification complete")

	rowIndex := make(map[string]*views.ViewRow, len(existingRows))
	for i := range existingRows {
		rowIndex[existingRows[i].ID] = &existingRows[i]
	}

	schemaHash := s.computeSchemaHash(ctx, output.WorkspaceID, schemaCtx)
	updated := false

	// Step 2: Populate cells for each matched row
	for _, rowID := range classification.Affected_row_ids {
		row, ok := rowIndex[rowID]
		if !ok {
			log.Warn().Str("row_id", rowID).Msg("enrichViewRows: classified row not found")
			continue
		}
		rowCellsJSON := serializeRowCellsFiltered(row, schemaColumnKeys)

		populateResult, err := viewbaml.PopulateRowCells(
			ctx,
			columns,
			output.OutputType,
			output.Title,
			summary,
			outputData,
			rowID,
			rowCellsJSON,
			"",
		)
		if err != nil {
			log.Warn().Err(err).Str("row_id", rowID).Msg("enrichViewRows: PopulateRowCells failed")
			continue
		}

		cells := extractNonEmptyCells(populateResult.Cells)
		if len(cells) == 0 {
			continue
		}

		if err := s.viewStore.MergeCells(ctx, schemaCtx.ViewID, rowID, cells, output.ID); err != nil {
			log.Warn().Err(err).Str("row_id", rowID).Msg("enrichViewRows: merge cells failed")
			continue
		}

		log.Info().
			Str("task_id", output.TaskID).
			Str("view_id", schemaCtx.ViewID).
			Str("row_id", rowID).
			Int("cells", len(cells)).
			Msg("enrichViewRows: enriched existing row")
		updated = true
	}

	// Step 3: Insert new rows for unmatched entities
	for _, entityHint := range classification.Unmatched_entities {
		if strings.TrimSpace(entityHint) == "" {
			continue
		}

		populateResult, err := viewbaml.PopulateRowCells(
			ctx,
			columns,
			output.OutputType,
			output.Title,
			summary,
			outputData,
			"",
			"",
			entityHint,
		)
		if err != nil {
			log.Warn().Err(err).Str("entity", entityHint).Msg("enrichViewRows: PopulateRowCells for insert failed")
			continue
		}

		cells := extractNonEmptyCells(populateResult.Cells)
		if len(cells) == 0 {
			continue
		}

		rowKey := populateResult.Row_key
		if rowKey == "" {
			rowKey = "task"
		}
		normalizedKey := views.NormalizeRowKey(rowKey)

		existingRow, _ := s.viewStore.FindRowByKey(ctx, schemaCtx.ViewID, schemaCtx.SheetID, schemaCtx.ComponentID, normalizedKey)
		if existingRow != nil {
			if err := s.viewStore.MergeCells(ctx, schemaCtx.ViewID, existingRow.ID, cells, output.ID); err != nil {
				log.Warn().Err(err).Str("row_id", existingRow.ID).Msg("enrichViewRows: merge into existing row by key failed")
				continue
			}
			log.Info().
				Str("task_id", output.TaskID).
				Str("view_id", schemaCtx.ViewID).
				Str("row_id", existingRow.ID).
				Str("row_key", normalizedKey).
				Int("cells", len(cells)).
				Msg("enrichViewRows: merged into existing row found by key")
			updated = true
			continue
		}

		rowID := fmt.Sprintf("%s:%s:%s", schemaCtx.SheetID, schemaCtx.ComponentID, normalizedKey)
		row := views.ViewRow{
			ID:              rowID,
			SheetID:         schemaCtx.SheetID,
			ComponentID:     schemaCtx.ComponentID,
			GroupID:         output.TaskID,
			TaskID:          output.TaskID,
			RowKey:          normalizedKey,
			SchemaHash:      schemaHash,
			OutputIDs:       []string{output.ID},
			OutputSignature: output.ID,
			SourceOutputIDs: []string{output.ID},
			Cells:           cells,
			UpdatedAt:       time.Now(),
		}
		if err := s.viewStore.UpsertRows(ctx, schemaCtx.ViewID, []views.ViewRow{row}); err != nil {
			log.Warn().Err(err).Str("entity", entityHint).Msg("enrichViewRows: insert row failed")
			continue
		}

		log.Info().
			Str("task_id", output.TaskID).
			Str("view_id", schemaCtx.ViewID).
			Str("row_id", rowID).
			Int("cells", len(cells)).
			Msg("enrichViewRows: inserted new row")
		updated = true
	}

	if updated {
		s.publishTaskUpdate(ctx, output.WorkspaceID, output.TaskID)
	}
	return nil
}

func extractNonEmptyCells(bamlCells []viewbamltypes.ViewCell) map[string]string {
	cells := make(map[string]string, len(bamlCells))
	for _, cell := range bamlCells {
		if cell.Column != "" && cell.Value != "" {
			cells[cell.Column] = cell.Value
		}
	}
	return cells
}

func sortRowsByUpdatedDesc(rows []views.ViewRow) {
	for i := 1; i < len(rows); i++ {
		for j := i; j > 0 && rows[j].UpdatedAt.After(rows[j-1].UpdatedAt); j-- {
			rows[j], rows[j-1] = rows[j-1], rows[j]
		}
	}
}

func outputMatchesSchemaContext(output *types.TaskOutput, ctx types.ViewOutputSchemaContext) bool {
	ot := strings.TrimSpace(strings.ToLower(output.OutputType))
	if ot == "" {
		return false
	}
	if ctx.OutputType != "" && strings.TrimSpace(strings.ToLower(ctx.OutputType)) == ot {
		return true
	}
	ak := ""
	if output.Metadata != nil {
		if v, ok := output.Metadata["artifact_key"].(string); ok {
			ak = strings.TrimSpace(strings.ToLower(v))
		}
	}
	if ctx.ArtifactKey != "" && ak != "" && strings.TrimSpace(strings.ToLower(ctx.ArtifactKey)) == ak {
		return true
	}
	return ctx.OutputType == "" && ctx.ArtifactKey == ""
}

func serializeOutputForBAML(output *types.TaskOutput) string {
	compact := map[string]any{
		"id":          output.ID,
		"output_type": output.OutputType,
		"title":       output.Title,
	}
	if output.Summary != nil && *output.Summary != "" {
		compact["summary"] = *output.Summary
	}
	if output.URI != nil && *output.URI != "" {
		compact["uri"] = *output.URI
	}
	if len(output.Data) > 0 {
		compact["data"] = output.Data
	}
	if output.Metadata != nil {
		filtered := make(map[string]any)
		for k, v := range output.Metadata {
			if !strings.HasPrefix(k, "_") {
				filtered[k] = v
			}
		}
		if len(filtered) > 0 {
			compact["metadata"] = filtered
		}
	}
	b, err := json.Marshal(compact)
	if err != nil {
		return "{}"
	}
	return string(b)
}

// serializeRowsForClassification produces a compact JSON array with only
// identity-relevant columns (name, email, address, phone, etc.) plus the row
// ID. This keeps the BAML prompt small so the classifier can handle 200+ rows.
func serializeRowsForClassification(rows []views.ViewRow, schemaKeys []string) string {
	if len(rows) == 0 {
		return ""
	}
	schemaSet := make(map[string]struct{}, len(schemaKeys))
	for _, k := range schemaKeys {
		schemaSet[k] = struct{}{}
	}

	type compactRow struct {
		ID    string            `json:"_id"`
		Cells map[string]string `json:"cells"`
	}
	compact := make([]compactRow, 0, len(rows))
	for _, r := range rows {
		merged := r.MergedCells()
		filtered := make(map[string]string)
		for k, v := range merged {
			if v == "" {
				continue
			}
			if _, inSchema := schemaSet[k]; !inSchema && len(schemaSet) > 0 {
				continue
			}
			if len(v) > enrichCellValueMaxLen {
				v = v[:enrichCellValueMaxLen] + "..."
			}
			filtered[k] = v
		}
		if len(filtered) == 0 {
			continue
		}
		compact = append(compact, compactRow{ID: r.ID, Cells: filtered})
	}
	if len(compact) == 0 {
		return ""
	}
	b, err := json.Marshal(compact)
	if err != nil {
		return ""
	}
	return string(b)
}

// serializeRowCellsFiltered returns a JSON object of a single row's cells,
// filtered to only schema-defined columns.
func serializeRowCellsFiltered(row *views.ViewRow, schemaKeys []string) string {
	if row == nil {
		return "{}"
	}
	schemaSet := make(map[string]struct{}, len(schemaKeys))
	for _, k := range schemaKeys {
		schemaSet[k] = struct{}{}
	}
	merged := row.MergedCells()
	filtered := make(map[string]string)
	for k, v := range merged {
		if v == "" {
			continue
		}
		if _, inSchema := schemaSet[k]; !inSchema && len(schemaSet) > 0 {
			continue
		}
		if len(v) > enrichCellValueMaxLen {
			v = v[:enrichCellValueMaxLen] + "..."
		}
		filtered[k] = v
	}
	b, err := json.Marshal(filtered)
	if err != nil {
		return "{}"
	}
	return string(b)
}

func (s *WorkerService) computeSchemaHash(
	ctx context.Context,
	workspaceID uint,
	schemaCtx types.ViewOutputSchemaContext,
) string {
	if s.backend == nil {
		return ""
	}
	view, err := s.backend.GetView(ctx, workspaceID, schemaCtx.ViewID)
	if err != nil || view == nil {
		return ""
	}
	for _, sheet := range view.Definition.Sheets {
		if sheet.ID != schemaCtx.SheetID {
			continue
		}
		for _, comp := range sheet.Components {
			if comp.ID == schemaCtx.ComponentID && comp.IsTable() {
				return views.MappingSchemaHash(sheet, comp)
			}
		}
		for _, comp := range sheet.Components {
			if comp.IsTable() {
				return views.MappingSchemaHash(sheet, comp)
			}
		}
	}
	return ""
}

func (s *WorkerService) AppendTaskOutputRows(ctx context.Context, req *pb.AppendTaskOutputRowsRequest) (*pb.AppendTaskOutputRowsResponse, error) {
	if s.backend == nil {
		return nil, status.Errorf(codes.Unavailable, "task persistence not available")
	}
	if err := s.backend.AppendTaskOutputRows(ctx, uint(req.WorkspaceId), req.OutputId, []byte(req.RowsJson)); err != nil {
		if _, ok := err.(*types.ErrTaskOutputNotFound); ok {
			return nil, status.Errorf(codes.NotFound, "output not found: %s", req.OutputId)
		}
		return nil, status.Errorf(codes.Internal, "append rows: %v", err)
	}
	s.publishOutputTaskUpdate(ctx, uint(req.WorkspaceId), req.OutputId)
	return &pb.AppendTaskOutputRowsResponse{}, nil
}

func (s *WorkerService) FinalizeTaskOutput(ctx context.Context, req *pb.FinalizeTaskOutputRequest) (*pb.FinalizeTaskOutputResponse, error) {
	if s.backend == nil {
		return nil, status.Errorf(codes.Unavailable, "task persistence not available")
	}
	if req.Summary != "" {
		if err := s.backend.UpdateTaskOutputSummary(ctx, uint(req.WorkspaceId), req.OutputId, req.Summary); err != nil {
			return nil, status.Errorf(codes.Internal, "finalize output: %v", err)
		}
	}
	s.publishOutputTaskUpdate(ctx, uint(req.WorkspaceId), req.OutputId)
	return &pb.FinalizeTaskOutputResponse{}, nil
}

func (s *WorkerService) UpdateTaskOutputStatus(ctx context.Context, req *pb.UpdateTaskOutputStatusRequest) (*pb.UpdateTaskOutputStatusResponse, error) {
	if s.backend == nil {
		return nil, status.Errorf(codes.Unavailable, "task persistence not available")
	}
	if err := s.backend.UpdateTaskOutputStatus(ctx, uint(req.WorkspaceId), req.OutputId, req.Status); err != nil {
		if _, ok := err.(*types.ErrTaskOutputNotFound); ok {
			return nil, status.Errorf(codes.NotFound, "output not found: %s", req.OutputId)
		}
		return nil, status.Errorf(codes.Internal, "update output status: %v", err)
	}
	s.publishOutputTaskUpdate(ctx, uint(req.WorkspaceId), req.OutputId)
	return &pb.UpdateTaskOutputStatusResponse{}, nil
}

func (s *WorkerService) publishOutputTaskUpdate(ctx context.Context, workspaceID uint, outputID string) {
	if s.backend == nil {
		return
	}
	output, err := s.backend.GetTaskOutput(ctx, workspaceID, outputID)
	if err != nil {
		log.Warn().Err(err).Str("output_id", outputID).Msg("lookup task output for publish failed")
		return
	}
	s.publishTaskUpdate(ctx, output.WorkspaceID, output.TaskID)
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
