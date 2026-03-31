package services

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/beam-cloud/airstore/pkg/common"
	"github.com/beam-cloud/airstore/pkg/types"
	pb "github.com/beam-cloud/airstore/proto"
	"github.com/bsm/redislock"
	"github.com/rs/zerolog/log"
)

const (
	defaultRecoveryLockTTL             = 15 * time.Second
	terminalQueueReconcileGracePeriod  = 5 * time.Second
)

func (s *WorkerService) StartRecoveryLoop(ctx context.Context) {
	if s == nil || !s.recoveryLoopEnabled || s.backend == nil || s.redisClient == nil {
		return
	}
	go s.recoveryLoop(ctx)
	log.Info().Dur("interval", s.recoveryInterval).Msg("worker orphan recovery loop started")
}

func (s *WorkerService) recoveryLoop(ctx context.Context) {
	if s.recoveryInterval <= 0 {
		s.recoveryInterval = 10 * time.Second
	}
	s.runRecoveryCycle(ctx)
	ticker := time.NewTicker(s.recoveryInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.runRecoveryCycle(ctx)
		}
	}
}

func (s *WorkerService) runRecoveryCycle(ctx context.Context) {
	lock := common.NewRedisLock(s.redisClient)
	lockKey := common.Keys.AgentRunRecoveryLock()
	ttl := max(defaultRecoveryLockTTL, s.recoveryInterval*2)
	if err := lock.Acquire(ctx, lockKey, common.RedisLockOptions{TtlS: int(ttl.Seconds()), Retries: 0}); err != nil {
		if !errors.Is(err, redislock.ErrNotObtained) {
			log.Debug().Err(err).Msg("recovery: lock failed")
		}
		return
	}
	defer func() { _ = lock.Release(lockKey) }()

	var detected, recovered int

	// Each sweep is independent and idempotent. Handlers use CAS-style DB
	// operations (WHERE status IN ..., etc.) so concurrent/duplicate
	// processing of the same run is harmless — the second attempt no-ops.

	d, r := s.sweepTerminalQueueRuns(ctx)
	detected += d
	recovered += r

	now := time.Now()
	d, r = s.sweepExpiredClaims(ctx, now)
	detected += d
	recovered += r

	d, r = s.sweepStaleUnclaimedRuns(ctx, now)
	detected += d
	recovered += r

	staleCutoff := now.Add(-(s.claimLeaseTTL + 30*time.Second))
	d, r = s.sweepOrphanedRunningTasks(ctx, staleCutoff)
	detected += d
	recovered += r

	d, r = s.sweepRunningTasksWithNoRun(ctx, staleCutoff)
	detected += d
	recovered += r

	if detected > 0 {
		log.Info().Int("detected", detected).Int("recovered", recovered).Msg("recovery cycle complete")
	}
}

// sweepTerminalQueueRuns reconciles runs whose task queue state went terminal
// but the run DB record is still active (worker finished but result never
// persisted to Postgres).
func (s *WorkerService) sweepTerminalQueueRuns(ctx context.Context) (detected, recovered int) {
	runs, err := s.backend.ListClaimedAgentRuns(ctx, s.recoveryBatchSize)
	if err != nil {
		log.Warn().Err(err).Msg("recovery: failed to list claimed runs")
		return
	}
	for _, run := range runs {
		if run == nil || run.ID == "" {
			continue
		}
		if d, r := s.processClaimedRun(ctx, run); d {
			detected++
			if r {
				recovered++
			}
		}
	}
	return
}

// sweepExpiredClaims recovers runs whose worker died (claim lease expired
// without heartbeat renewal).
func (s *WorkerService) sweepExpiredClaims(ctx context.Context, now time.Time) (detected, recovered int) {
	runs, err := s.backend.ListExpiredClaimedAgentRuns(ctx, now, s.recoveryBatchSize)
	if err != nil {
		log.Warn().Err(err).Msg("recovery: failed to list expired claimed runs")
		return
	}
	for _, run := range runs {
		if run == nil || run.ID == "" {
			continue
		}
		if d, r := s.processExpiredClaimRun(ctx, now, run); d {
			detected++
			if r {
				recovered++
			}
		}
	}
	return
}

// sweepStaleUnclaimedRuns requeues runs that were never claimed by any worker.
func (s *WorkerService) sweepStaleUnclaimedRuns(ctx context.Context, now time.Time) (detected, recovered int) {
	if s.unclaimedRunStaleAfter <= 0 {
		return
	}
	runs, err := s.backend.ListStaleUnclaimedAgentRuns(ctx, now.Add(-s.unclaimedRunStaleAfter), s.recoveryBatchSize)
	if err != nil {
		log.Warn().Err(err).Msg("recovery: failed to list stale unclaimed runs")
		return
	}
	for _, run := range runs {
		if run == nil || run.ID == "" {
			continue
		}
		if d, r := s.processStaleUnclaimedRun(ctx, run); d {
			detected++
			if r {
				recovered++
			}
		}
	}
	return
}

// sweepOrphanedRunningTasks catches tasks stuck in "running" whose run is
// already terminal or unclaimed — e.g. because an earlier run-level recovery
// succeeded but the task-level Settle failed.
func (s *WorkerService) sweepOrphanedRunningTasks(ctx context.Context, staleCutoff time.Time) (detected, recovered int) {
	runIDs, err := s.backend.ListOrphanedRunningTaskRunIDs(ctx, staleCutoff, s.recoveryBatchSize)
	if err != nil {
		log.Warn().Err(err).Msg("recovery: failed to list orphaned running tasks")
		return
	}
	for _, runID := range runIDs {
		if s.settleOrphanedTask(ctx, runID) {
			detected++
			recovered++
		}
	}
	return
}

// sweepRunningTasksWithNoRun moves tasks stuck in "running" with no target run
// to error. These are orphans that were never assigned a run.
func (s *WorkerService) sweepRunningTasksWithNoRun(ctx context.Context, staleCutoff time.Time) (detected, recovered int) {
	taskIDs, err := s.backend.ListRunningTasksWithNoRun(ctx, staleCutoff, s.recoveryBatchSize)
	if err != nil {
		log.Warn().Err(err).Msg("recovery: failed to list running tasks with no run")
		return
	}
	for _, taskID := range taskIDs {
		if s.errorOrphanedTask(ctx, taskID) {
			detected++
			recovered++
		}
	}
	return
}

// settleOrphanedTask attempts to settle a task whose run is terminal but whose
// task state is still "running". Returns true if the task was transitioned.
func (s *WorkerService) settleOrphanedTask(ctx context.Context, runID string) bool {
	if err := s.lifecycle.Settle(ctx, runID, nil); err == nil {
		log.Info().Str("run_id", runID).Msg("recovery: settled orphaned running task")
		return true
	}
	run, err := s.backend.GetAgentRunByID(ctx, runID)
	if err != nil || run == nil {
		return false
	}
	s.settleOrphanFallback(ctx, run, "stale running task recovery")
	return true
}

// processExpiredClaimRun handles runs whose worker claim lease expired.
func (s *WorkerService) processExpiredClaimRun(ctx context.Context, now time.Time, run *types.AgentRun) (detected, recovered bool) {
	if run == nil || !run.Status.IsActive() || run.ClaimedByWorker == nil {
		return false, false
	}
	workerID := strings.TrimSpace(*run.ClaimedByWorker)
	if workerID == "" {
		return false, false
	}
	cleared, err := s.backend.ClearExpiredAgentRunClaim(ctx, run.ID, workerID, now)
	if err != nil || !cleared {
		return false, false
	}
	r, _ := s.recoverOrphanedRun(ctx, run, "claim_lease_expired")
	return true, r
}

// processClaimedRun reconciles runs whose task queue state is terminal but the run is still active.
func (s *WorkerService) processClaimedRun(ctx context.Context, run *types.AgentRun) (detected, recovered bool) {
	if run == nil || !run.Status.IsActive() || run.ClaimedByWorker == nil || s.taskQueue == nil {
		return false, false
	}
	state, err := s.taskQueue.GetState(ctx, run.ID)
	if err != nil || state == nil || !isTerminalQueueState(state.Status) {
		return false, false
	}
	if !state.FinishedAt.IsZero() && time.Since(state.FinishedAt) < terminalQueueReconcileGracePeriod {
		return false, false
	}

	exitCode := state.ExitCode
	errText := strings.TrimSpace(state.Error)
	var usage *types.LLMUsage
	if result, err := s.taskQueue.GetResult(ctx, run.ID); err == nil && result != nil {
		if e := strings.TrimSpace(result.Error); e != "" || errText == "" {
			errText = e
		}
		exitCode = result.ExitCode
		usage = result.Usage
	}

	attempt, err := s.lookupRunAttemptByExecutionID(ctx, run.ID)
	if err != nil || attempt == nil || attempt.ID == "" {
		return false, false
	}
	pf := usage.ProtoFields()
	req := &pb.SetTaskResultRequest{
		TaskId: run.ID, AttemptId: attempt.ID,
		ExitCode: int32(exitCode), Error: errText,
		LlmInputTokens:              pf.InputTokens,
		LlmOutputTokens:             pf.OutputTokens,
		LlmCacheCreationInputTokens: pf.CacheCreationInputTokens,
		LlmCacheReadInputTokens:     pf.CacheReadInputTokens,
		LlmTotalTokens:              pf.TotalTokens,
		TotalCostUsd:                pf.TotalCostUSD,
		LlmModelUsageJson:           pf.ModelUsageJSON,
	}
	if _, err := s.SetTaskResult(ctx, req); err != nil {
		return false, false
	}
	_ = s.taskQueue.Complete(ctx, run.ID, &types.RunExecutionResult{
		ID: run.ID, ExitCode: exitCode, Error: errText, Usage: usage,
	})
	return true, true
}

// processStaleUnclaimedRun requeues runs that were never claimed by any worker.
func (s *WorkerService) processStaleUnclaimedRun(ctx context.Context, run *types.AgentRun) (detected, recovered bool) {
	if run == nil || !run.Status.IsActive() || run.ClaimedByWorker != nil || s.taskQueue == nil {
		return false, false
	}
	state, err := s.taskQueue.GetState(ctx, run.ID)
	if err == nil && state != nil && state.Status == types.RunExecutionStatusPending {
		return false, false
	}
	execTask, err := s.backend.GetRunExecution(ctx, run.ID)
	if err != nil || execTask == nil || execTask.IsTerminal() {
		return false, false
	}
	if err := s.taskQueue.Requeue(ctx, execTask); err != nil {
		return false, false
	}
	log.Info().Str("run_id", run.ID).Msg("recovery: requeued stale unclaimed run")
	return true, true
}

// recoverOrphanedRun cleans up an orphaned run and settles the origin task.
func (s *WorkerService) recoverOrphanedRun(ctx context.Context, run *types.AgentRun, reason string) (bool, bool) {
	if run == nil || !run.Status.IsActive() {
		return false, false
	}
	errorMsg := fmt.Sprintf("orphaned run recovered: %s", reason)

	_ = s.backend.ReleaseStaleTaskInputClaims(ctx, run.ID)
	if s.terminalIO != nil {
		_ = s.terminalIO.SetRunInteraction(ctx, run.WorkspaceID, run.ID,
			types.RunInteraction{State: types.RunInteractionStateClosed}, 5*time.Minute)
	}
	if s.taskQueue != nil {
		_ = s.taskQueue.Fail(ctx, run.ID, fmt.Errorf("%s", errorMsg))
	}

	if err := s.backend.SetRunExecutionResult(ctx, run.ID, -1, errorMsg); err != nil {
		var notFound *types.ErrRunExecutionNotFound
		if !errors.As(err, &notFound) {
			return false, false
		}
	}

	if attempt, _ := s.lookupRunAttemptByExecutionID(ctx, run.ID); attempt != nil && attempt.IsActive() {
		now := time.Now()
		st, rs, errStr := types.ClassifyExecutionOutcome(-1, errorMsg)
		_ = s.backend.UpdateAgentRunAttemptResult(ctx, attempt.ID, st, intPtr(-1), now, errStr)
		_ = s.backend.ClearAgentRunClaim(ctx, attempt.RunID)
		_ = updateExecutionInstanceCounts(ctx, s.backend, attempt.RunID, -1)
		_ = s.backend.UpdateAgentRunLifecycle(ctx, attempt.RunID, rs, nil, &now, errStr)
	}

	if err := s.lifecycle.Settle(ctx, run.ID, nil); err != nil {
		log.Warn().Err(err).Str("run_id", run.ID).Msg("recovery: settle failed, applying fallback")
		s.settleOrphanFallback(ctx, run, errorMsg)
		return true, false
	}
	return true, true
}

// settleOrphanFallback is a last-resort path when Settle fails after the run
// has already been marked terminal. Without this, the task stays stuck in
// "running" forever because the run is no longer visible to the claim-based
// recovery sweeps.
func (s *WorkerService) settleOrphanFallback(ctx context.Context, run *types.AgentRun, errorMsg string) {
	task, err := s.backend.GetTaskByID(ctx, run.OriginTaskID)
	if err != nil || task == nil || task.State.IsTerminal() {
		return
	}
	if task.State != types.AgentTaskStateRunning && task.State != types.AgentTaskStateWaiting {
		return
	}
	if err := s.backend.UpdateTaskState(ctx, types.TaskStateUpdate{
		TaskID:      task.ID,
		State:       types.AgentTaskStateError,
		TargetRunID: task.TargetRunID,
	}); err != nil {
		log.Warn().Err(err).Str("task_id", task.ID).Msg("recovery: fallback task state update failed")
		return
	}
	log.Info().Str("task_id", task.ID).Str("run_id", run.ID).Msg("recovery: fallback moved task to error")
	s.publishTaskUpdate(ctx, task.WorkspaceID, task.ID)
}

// errorOrphanedTask moves a task with no run from "running" to "error".
func (s *WorkerService) errorOrphanedTask(ctx context.Context, taskID string) bool {
	task, err := s.backend.GetTaskByID(ctx, taskID)
	if err != nil || task == nil || task.State.IsTerminal() || task.State != types.AgentTaskStateRunning {
		return false
	}
	if err := s.backend.UpdateTaskState(ctx, types.TaskStateUpdate{
		TaskID: task.ID,
		State:  types.AgentTaskStateError,
	}); err != nil {
		log.Warn().Err(err).Str("task_id", taskID).Msg("recovery: failed to error orphaned task with no run")
		return false
	}
	log.Info().Str("task_id", taskID).Msg("recovery: moved orphaned task with no run to error")
	s.publishTaskUpdate(ctx, task.WorkspaceID, task.ID)
	return true
}

func intPtr(v int) *int { return &v }

func isTerminalQueueState(status types.RunExecutionStatus) bool {
	switch status {
	case types.RunExecutionStatusComplete, types.RunExecutionStatusFailed, types.RunExecutionStatusCancelled:
		return true
	}
	return false
}
