package services

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/beam-cloud/airstore/pkg/common"
	"github.com/beam-cloud/airstore/pkg/types"
	"github.com/bsm/redislock"
	"github.com/rs/zerolog/log"
)

const defaultRecoveryLockTTL = 15 * time.Second

type orphanRecoveryStats struct {
	detected       int
	recovered      int
	retryScheduled int
	exhausted      int
}

type orphanRecoveryOutcome struct {
	detected       bool
	recovered      bool
	retryScheduled bool
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
		log.Info().
			Int("orphan_detected", stats.detected).
			Int("orphan_recovered", stats.recovered).
			Int("orphan_retry_scheduled", stats.retryScheduled).
			Int("orphan_exhausted", stats.exhausted).
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

	recovered, retryScheduled, err := s.recoverOrphanedRun(ctx, run, "claim_lease_expired")
	return orphanRecoveryOutcome{
		detected:       true,
		recovered:      recovered,
		retryScheduled: retryScheduled,
	}, err
}

func (s *WorkerService) processStaleUnclaimedRun(ctx context.Context, run *types.AgentRun) (orphanRecoveryOutcome, error) {
	if run == nil || !run.Status.IsActive() || run.ClaimedByWorker != nil {
		return orphanRecoveryOutcome{}, nil
	}
	if !s.shouldRecoverUnclaimedRun(ctx, run) {
		return orphanRecoveryOutcome{}, nil
	}

	recovered, retryScheduled, err := s.recoverOrphanedRun(ctx, run, "unclaimed_run_stale")
	return orphanRecoveryOutcome{
		detected:       true,
		recovered:      recovered,
		retryScheduled: retryScheduled,
	}, err
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

func (s *WorkerService) recoverOrphanedRun(ctx context.Context, run *types.AgentRun, reason string) (bool, bool, error) {
	if run == nil || !run.Status.IsActive() {
		return false, false, nil
	}

	attempt, attemptErr := s.lookupRunAttemptByExecutionID(ctx, run.ID)
	if attemptErr != nil {
		return false, false, attemptErr
	}
	if attempt != nil && !isRunAttemptActive(attempt) {
		return false, false, nil
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
			return false, false, err
		}
	}

	if attempt == nil {
		_ = s.markOriginTaskTerminalIfCurrentRun(ctx, run.ID)
		return true, false, nil
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
	return true, retryScheduled, err
}

func (s *WorkerService) unclaimedStaleDuration() time.Duration {
	if s.unclaimedStaleAfter > 0 {
		return s.unclaimedStaleAfter
	}
	return defaultUnclaimedRunStaleAfter
}
