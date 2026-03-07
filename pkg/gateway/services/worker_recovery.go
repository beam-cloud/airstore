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

const defaultRecoveryLockTTL = 15 * time.Second
const terminalQueueReconcileGracePeriod = 5 * time.Second

type orphanRecoveryStats struct {
	detected    int
	recovered   int
	cleanupOnly int
}

type orphanRecoveryOutcome struct {
	detected    bool
	recovered   bool
	cleanupOnly bool
}

func (s *WorkerService) StartRecoveryLoop(ctx context.Context) {
	if s == nil || !s.recoveryLoopEnabled {
		return
	}
	if s.backend == nil || s.redisClient == nil {
		log.Warn().Msg("worker recovery loop disabled: backend or redis unavailable")
		return
	}

	go s.recoveryLoop(ctx)
	log.Info().
		Dur("interval", s.recoveryInterval).
		Int("batch_size", s.recoveryBatchSize).
		Msg("worker orphan recovery loop started")
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
	lockTTL := defaultRecoveryLockTTL
	if s.recoveryInterval > 0 && s.recoveryInterval*2 > lockTTL {
		lockTTL = s.recoveryInterval * 2
	}
	lockTTLSec := int(lockTTL.Seconds())
	if lockTTLSec <= 0 {
		lockTTLSec = int(defaultRecoveryLockTTL.Seconds())
	}

	if err := lock.Acquire(ctx, lockKey, common.RedisLockOptions{TtlS: lockTTLSec, Retries: 0}); err != nil {
		if !errors.Is(err, redislock.ErrNotObtained) {
			log.Debug().Err(err).Msg("worker recovery loop: lock acquisition failed")
		}
		return
	}
	defer func() {
		if err := lock.Release(lockKey); err != nil && !errors.Is(err, redislock.ErrLockNotHeld) {
			log.Debug().Err(err).Msg("worker recovery loop: failed to release lock")
		}
	}()

	now := time.Now()
	stats := orphanRecoveryStats{}
	seenRuns := map[string]struct{}{}

	claimedRuns, err := s.backend.ListClaimedAgentRuns(ctx, s.recoveryBatchSize)
	if err != nil {
		log.Warn().Err(err).Msg("worker recovery loop: failed to list claimed runs")
		return
	}
	for _, run := range claimedRuns {
		if run == nil || strings.TrimSpace(run.ID) == "" {
			continue
		}
		if _, dup := seenRuns[run.ID]; dup {
			continue
		}
		outcome, recErr := s.processClaimedRun(ctx, run)
		if recErr != nil {
			log.Warn().Err(recErr).Str("run_id", run.ID).Msg("worker recovery loop: failed to reconcile claimed run")
			continue
		}
		if !outcome.detected {
			continue
		}
		seenRuns[run.ID] = struct{}{}
		s.applyRecoveryOutcome(&stats, outcome)
	}

	// Guardrail: recovery is index-driven SQL + direct-key Redis operations only.
	// Never use Redis SCAN/KEYS in this path.
	expiredRuns, err := s.backend.ListExpiredClaimedAgentRuns(ctx, now, s.recoveryBatchSize)
	if err != nil {
		log.Warn().Err(err).Msg("worker recovery loop: failed to list expired claimed runs")
		return
	}
	for _, run := range expiredRuns {
		if run == nil || strings.TrimSpace(run.ID) == "" {
			continue
		}
		if _, dup := seenRuns[run.ID]; dup {
			continue
		}
		seenRuns[run.ID] = struct{}{}
		outcome, recErr := s.processExpiredClaimRun(ctx, now, run)
		if recErr != nil {
			log.Warn().Err(recErr).Str("run_id", run.ID).Msg("worker recovery loop: failed to recover expired claim run")
			continue
		}
		s.applyRecoveryOutcome(&stats, outcome)
	}

	if s.unclaimedRunStaleAfter > 0 {
		staleUnclaimedRuns, err := s.backend.ListStaleUnclaimedAgentRuns(ctx, now.Add(-s.unclaimedRunStaleAfter), s.recoveryBatchSize)
		if err != nil {
			log.Warn().Err(err).Msg("worker recovery loop: failed to list stale unclaimed runs")
			return
		}
		for _, run := range staleUnclaimedRuns {
			if run == nil || strings.TrimSpace(run.ID) == "" {
				continue
			}
			if _, dup := seenRuns[run.ID]; dup {
				continue
			}
			seenRuns[run.ID] = struct{}{}
			outcome, recErr := s.processStaleUnclaimedRun(ctx, run)
			if recErr != nil {
				log.Warn().Err(recErr).Str("run_id", run.ID).Msg("worker recovery loop: failed to recover stale unclaimed run")
				continue
			}
			s.applyRecoveryOutcome(&stats, outcome)
		}
	}

	if stats.detected > 0 || stats.recovered > 0 {
		if s.taskQueue != nil && stats.detected > 0 && stats.recovered == 0 {
			inFlight, inFlightErr := s.taskQueue.InFlightCount(ctx)
			if inFlightErr != nil {
				log.Warn().Err(inFlightErr).Msg("worker recovery loop: failed to inspect in-flight queue depth")
			} else if inFlight > 0 {
				log.Warn().
					Int("orphan_detected", stats.detected).
					Int64("in_flight", inFlight).
					Msg("worker recovery loop: orphans detected but none recovered while in-flight tasks remain")
			}
		}

		log.Info().
			Int("orphan_detected", stats.detected).
			Int("orphan_recovered", stats.recovered).
			Int("orphan_cleanup_only", stats.cleanupOnly).
			Msg("worker recovery loop cycle complete")
	}
}

func (s *WorkerService) applyRecoveryOutcome(stats *orphanRecoveryStats, outcome orphanRecoveryOutcome) {
	if !outcome.detected {
		return
	}
	stats.detected++
	if !outcome.recovered {
		return
	}
	stats.recovered++
	if outcome.cleanupOnly {
		stats.cleanupOnly++
	}
}

func (s *WorkerService) processExpiredClaimRun(ctx context.Context, now time.Time, run *types.AgentRun) (orphanRecoveryOutcome, error) {
	if run == nil || !run.Status.IsActive() || run.ClaimedByWorker == nil {
		return orphanRecoveryOutcome{}, nil
	}
	workerID := strings.TrimSpace(*run.ClaimedByWorker)
	if workerID == "" {
		return orphanRecoveryOutcome{}, nil
	}

	cleared, err := s.backend.ClearExpiredAgentRunClaim(ctx, run.ID, workerID, now)
	if err != nil {
		return orphanRecoveryOutcome{}, err
	}
	if !cleared {
		// Lease was refreshed after query; not stale anymore.
		return orphanRecoveryOutcome{}, nil
	}

	recovered, cleanupOnly, err := s.recoverOrphanedRun(ctx, run, "claim_lease_expired")
	return orphanRecoveryOutcome{
		detected:    true,
		recovered:   recovered,
		cleanupOnly: cleanupOnly,
	}, err
}

func (s *WorkerService) processClaimedRun(ctx context.Context, run *types.AgentRun) (orphanRecoveryOutcome, error) {
	if run == nil || !run.Status.IsActive() || run.ClaimedByWorker == nil {
		return orphanRecoveryOutcome{}, nil
	}
	if s.taskQueue == nil {
		return orphanRecoveryOutcome{}, nil
	}

	state, err := s.taskQueue.GetState(ctx, run.ID)
	if err != nil || state == nil {
		return orphanRecoveryOutcome{}, nil
	}
	if !runExecutionStateIsTerminal(state.Status) {
		return orphanRecoveryOutcome{}, nil
	}
	if !isTerminalQueueStateReady(state, terminalQueueReconcileGracePeriod) {
		return orphanRecoveryOutcome{}, nil
	}

	exitCode := state.ExitCode
	errText := strings.TrimSpace(state.Error)
	if result, resultErr := s.taskQueue.GetResult(ctx, run.ID); resultErr == nil && result != nil {
		if strings.TrimSpace(result.Error) != "" || errText == "" {
			errText = strings.TrimSpace(result.Error)
		}
		exitCode = result.ExitCode
	}
	attempt, attemptErr := s.lookupRunAttemptByExecutionID(ctx, run.ID)
	if attemptErr != nil {
		return orphanRecoveryOutcome{}, attemptErr
	}
	if attempt == nil || strings.TrimSpace(attempt.ID) == "" {
		return orphanRecoveryOutcome{}, fmt.Errorf("run attempt mapping not found for run %s", run.ID)
	}
	_, setErr := s.SetTaskResult(ctx, &pb.SetTaskResultRequest{
		TaskId:    run.ID,
		AttemptId: strings.TrimSpace(attempt.ID),
		ExitCode:  int32(exitCode),
		Error:     errText,
	})
	if setErr != nil {
		return orphanRecoveryOutcome{}, setErr
	}
	if completeErr := s.taskQueue.Complete(ctx, run.ID, &types.RunExecutionResult{
		ID:       run.ID,
		ExitCode: exitCode,
		Error:    errText,
	}); completeErr != nil {
		log.Warn().
			Err(completeErr).
			Str("run_id", run.ID).
			Msg("worker recovery loop: failed to reconcile terminal queue state after run finalization")
	}
	return orphanRecoveryOutcome{
		detected:  true,
		recovered: true,
	}, nil
}

func (s *WorkerService) processStaleUnclaimedRun(ctx context.Context, run *types.AgentRun) (orphanRecoveryOutcome, error) {
	if run == nil || !run.Status.IsActive() || run.ClaimedByWorker != nil {
		return orphanRecoveryOutcome{}, nil
	}
	if s.taskQueue == nil || s.backend == nil {
		return orphanRecoveryOutcome{}, nil
	}

	state, err := s.taskQueue.GetState(ctx, run.ID)
	if err == nil && state != nil && state.Status == types.RunExecutionStatusPending {
		return orphanRecoveryOutcome{}, nil
	}

	execTask, err := s.backend.GetRunExecution(ctx, run.ID)
	if err != nil {
		return orphanRecoveryOutcome{}, err
	}
	if execTask == nil || execTask.IsTerminal() {
		return orphanRecoveryOutcome{}, nil
	}
	if err := s.taskQueue.Requeue(ctx, execTask); err != nil {
		return orphanRecoveryOutcome{}, err
	}
	log.Info().
		Str("run_id", run.ID).
		Msg("worker recovery loop: requeued stale unclaimed run")
	return orphanRecoveryOutcome{
		detected:  true,
		recovered: true,
	}, nil
}

func (s *WorkerService) recoverOrphanedRun(ctx context.Context, run *types.AgentRun, reason string) (bool, bool, error) {
	if run == nil || !run.Status.IsActive() {
		return false, false, nil
	}

	attempt, attemptErr := s.lookupRunAttemptByExecutionID(ctx, run.ID)
	if attemptErr != nil {
		return false, false, attemptErr
	}
	attemptActive := attempt.IsActive()
	if attempt != nil && !attemptActive {
		log.Info().
			Str("run_id", run.ID).
			Str("attempt_id", attempt.ID).
			Str("attempt_status", string(attempt.Status)).
			Msg("worker recovery loop: run attempt is stale, reconciling queue/result without scheduling retry")
	}

	errorMsg := fmt.Sprintf("orphaned run recovered automatically: %s", reason)

	// Close the interaction state so the UI doesn't show a stale
	// "waiting for input" / "working" indicator for a dead session.
	if s.terminalIO != nil {
		if err := s.terminalIO.SetRunInteraction(
			ctx, run.WorkspaceID, run.ID,
			types.RunInteractionStateClosed, "",
			5*time.Minute,
		); err != nil {
			log.Warn().Err(err).Str("run_id", run.ID).
				Msg("worker recovery loop: failed to close interaction state for orphaned run")
		}
	}

	if s.taskQueue != nil {
		if err := s.taskQueue.Fail(ctx, run.ID, fmt.Errorf("%s", errorMsg)); err != nil {
			log.Warn().
				Err(err).
				Str("run_id", run.ID).
				Msg("worker recovery loop: failed to clean queue state for orphaned run")
		}
	}

	if err := s.backend.SetRunExecutionResult(ctx, run.ID, -1, errorMsg); err != nil {
		var notFoundErr *types.ErrRunExecutionNotFound
		if !errors.As(err, &notFoundErr) {
			return false, false, err
		}
	}

	if !attemptActive {
		_ = s.markOriginTaskTerminalIfCurrentRun(ctx, run.ID)
		return true, true, nil
	}
	_, setErr := s.SetTaskResult(ctx, &pb.SetTaskResultRequest{
		TaskId:    run.ID,
		AttemptId: strings.TrimSpace(attempt.ID),
		ExitCode:  -1,
		Error:     errorMsg,
	})
	if setErr != nil {
		fallbackErr := s.finalizeOrphanedRunDirect(ctx, attempt, run.ID, -1, errorMsg)
		if fallbackErr != nil {
			return false, false, fmt.Errorf("set orphaned run result: %w; direct fallback failed: %v", setErr, fallbackErr)
		}
		log.Warn().
			Err(setErr).
			Str("run_id", run.ID).
			Str("attempt_id", attempt.ID).
			Msg("failed to enqueue orphaned run result; applied direct finalization fallback")
		return true, false, nil
	}
	return true, false, nil
}

func (s *WorkerService) finalizeOrphanedRunDirect(
	ctx context.Context,
	attempt *types.AgentRunAttempt,
	taskID string,
	exitCode int,
	errText string,
) error {
	if s == nil || s.backend == nil || attempt == nil {
		return nil
	}
	now := time.Now()
	attemptStatus, runStatus, errMsg := types.ClassifyExecutionOutcome(exitCode, errText)
	if err := s.backend.UpdateAgentRunAttemptResult(ctx, attempt.ID, attemptStatus, &exitCode, now, errMsg); err != nil {
		return err
	}
	if err := s.backend.UpdateAgentRunLifecycle(ctx, attempt.RunID, runStatus, nil, &now, errMsg); err != nil {
		return err
	}
	_ = appendRunSnapshot(ctx, s.backend, attempt.RunID, runStatus, nil, &now, errMsg, map[string]any{
		types.AgentRunEventPayloadKeyAttemptID: attempt.ID,
		types.AgentRunEventPayloadKeyTaskID:    taskID,
		types.AgentRunEventPayloadKeyExitCode:  exitCode,
		types.AgentRunEventPayloadKeyError:     errText,
		types.AgentRunEventPayloadKeyEvent:     string(types.AgentRunEventOrphanRecovered),
		"recovery_mode":                        "direct_fallback",
	})
	_ = updateExecutionInstanceCounts(ctx, s.backend, attempt.RunID, -1)
	return s.markOriginTaskTerminalIfCurrentRun(ctx, attempt.RunID)
}

func runExecutionStateIsTerminal(status types.RunExecutionStatus) bool {
	switch status {
	case types.RunExecutionStatusComplete, types.RunExecutionStatusFailed, types.RunExecutionStatusCancelled:
		return true
	default:
		return false
	}
}

func isTerminalQueueStateReady(state *types.RunExecutionState, grace time.Duration) bool {
	if state == nil {
		return false
	}
	if grace <= 0 {
		return true
	}
	if state.FinishedAt.IsZero() {
		return true
	}
	return time.Since(state.FinishedAt) >= grace
}
