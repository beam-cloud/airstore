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
	detected       int
	recovered      int
	retryScheduled int
	exhausted      int
	cleanupOnly    int
}

type orphanRecoveryOutcome struct {
	detected       bool
	recovered      bool
	retryScheduled bool
	cleanupOnly    bool
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
		Dur("unclaimed_stale_after", s.unclaimedStaleDuration()).
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

	cutoff := now.Add(-s.unclaimedStaleDuration())
	staleUnclaimedRuns, err := s.backend.ListStaleUnclaimedAgentRuns(ctx, cutoff, s.recoveryBatchSize)
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
			Int("orphan_retry_scheduled", stats.retryScheduled).
			Int("orphan_exhausted", stats.exhausted).
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
		return
	}
	if outcome.retryScheduled {
		stats.retryScheduled++
		return
	}
	stats.exhausted++
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

	recovered, retryScheduled, cleanupOnly, err := s.recoverOrphanedRun(ctx, run, "claim_lease_expired")
	return orphanRecoveryOutcome{
		detected:       true,
		recovered:      recovered,
		retryScheduled: retryScheduled,
		cleanupOnly:    cleanupOnly,
	}, err
}

func (s *WorkerService) processStaleUnclaimedRun(ctx context.Context, run *types.AgentRun) (orphanRecoveryOutcome, error) {
	if run == nil || !run.Status.IsActive() || run.ClaimedByWorker != nil {
		return orphanRecoveryOutcome{}, nil
	}
	if !s.shouldRecoverUnclaimedRun(ctx, run) {
		return orphanRecoveryOutcome{}, nil
	}

	recovered, retryScheduled, cleanupOnly, err := s.recoverOrphanedRun(ctx, run, "unclaimed_run_stale")
	return orphanRecoveryOutcome{
		detected:       true,
		recovered:      recovered,
		retryScheduled: retryScheduled,
		cleanupOnly:    cleanupOnly,
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
	_, setErr := s.SetTaskResult(ctx, &pb.SetTaskResultRequest{
		TaskId:   run.ID,
		ExitCode: int32(exitCode),
		Error:    errText,
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

func (s *WorkerService) shouldRecoverUnclaimedRun(ctx context.Context, run *types.AgentRun) bool {
	if run == nil {
		return false
	}
	if s.taskQueue == nil {
		return true
	}

	state, err := s.taskQueue.GetState(ctx, run.ID)
	if err != nil || state == nil {
		// Missing state for an old active run is suspicious enough to recover.
		return true
	}

	switch state.Status {
	case types.RunExecutionStatusPending, types.RunExecutionStatusScheduled:
		// Still queued, not orphaned.
		return false
	case types.RunExecutionStatusRunning:
		workerID := strings.TrimSpace(state.WorkerID)
		if workerID == "" || s.workerRepo == nil {
			return true
		}
		worker, err := s.workerRepo.GetWorker(ctx, workerID)
		if err != nil {
			return true
		}
		return time.Since(worker.LastSeenAt) > s.claimLeaseDuration()
	default:
		return true
	}
}

func (s *WorkerService) recoverOrphanedRun(ctx context.Context, run *types.AgentRun, reason string) (bool, bool, bool, error) {
	if run == nil || !run.Status.IsActive() {
		return false, false, false, nil
	}

	attempt, attemptErr := s.lookupRunAttemptByExecutionID(ctx, run.ID)
	if attemptErr != nil {
		return false, false, false, attemptErr
	}
	attemptActive := attempt != nil && isRunAttemptActive(attempt)
	if attempt != nil && !attemptActive {
		log.Info().
			Str("run_id", run.ID).
			Str("attempt_id", attempt.ID).
			Str("attempt_status", string(attempt.Status)).
			Msg("worker recovery loop: run attempt is stale, reconciling queue/result without scheduling retry")
	}

	errorMsg := fmt.Sprintf("orphaned run recovered automatically: %s", reason)
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
			return false, false, false, err
		}
	}

	if !attemptActive {
		_ = s.markOriginTaskTerminalIfCurrentRun(ctx, run.ID)
		return true, false, true, nil
	}

	retryScheduled, err := s.finalizeRunAttempt(
		ctx,
		attempt,
		run.ID,
		-1,
		errorMsg,
		types.AgentRunEventOrphanRecovered,
		map[string]any{
			"recovery_reason": reason,
			"recovery_mode":   "automatic",
		},
	)
	return true, retryScheduled, false, err
}

func (s *WorkerService) unclaimedStaleDuration() time.Duration {
	if s.unclaimedStaleAfter > 0 {
		return s.unclaimedStaleAfter
	}
	return defaultUnclaimedRunStaleAfter
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
