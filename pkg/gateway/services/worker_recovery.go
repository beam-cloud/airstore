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

	seen := map[string]struct{}{}
	var detected, recovered int

	// 1. Claimed runs whose queue state went terminal
	if runs, err := s.backend.ListClaimedAgentRuns(ctx, s.recoveryBatchSize); err == nil {
		for _, run := range runs {
			if run == nil || run.ID == "" {
				continue
			}
			if _, dup := seen[run.ID]; dup {
				continue
			}
			seen[run.ID] = struct{}{}
			if d, r := s.processClaimedRun(ctx, run); d {
				detected++
				if r { recovered++ }
			}
		}
	}

	// 2. Expired claim leases
	now := time.Now()
	if runs, err := s.backend.ListExpiredClaimedAgentRuns(ctx, now, s.recoveryBatchSize); err == nil {
		for _, run := range runs {
			if run == nil || run.ID == "" {
				continue
			}
			if _, dup := seen[run.ID]; dup {
				continue
			}
			seen[run.ID] = struct{}{}
			if d, r := s.processExpiredClaimRun(ctx, now, run); d {
				detected++
				if r { recovered++ }
			}
		}
	}

	// 3. Stale unclaimed runs
	if s.unclaimedRunStaleAfter > 0 {
		if runs, err := s.backend.ListStaleUnclaimedAgentRuns(ctx, now.Add(-s.unclaimedRunStaleAfter), s.recoveryBatchSize); err == nil {
			for _, run := range runs {
				if run == nil || run.ID == "" {
					continue
				}
				if _, dup := seen[run.ID]; dup {
					continue
				}
				seen[run.ID] = struct{}{}
				if d, r := s.processStaleUnclaimedRun(ctx, run); d {
					detected++
					if r { recovered++ }
				}
			}
		}
	}

	if detected > 0 {
		log.Info().Int("detected", detected).Int("recovered", recovered).Msg("recovery cycle complete")
	}
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
	if result, err := s.taskQueue.GetResult(ctx, run.ID); err == nil && result != nil {
		if e := strings.TrimSpace(result.Error); e != "" || errText == "" {
			errText = e
		}
		exitCode = result.ExitCode
	}

	attempt, err := s.lookupRunAttemptByExecutionID(ctx, run.ID)
	if err != nil || attempt == nil || attempt.ID == "" {
		return false, false
	}
	if _, err := s.SetTaskResult(ctx, &pb.SetTaskResultRequest{
		TaskId: run.ID, AttemptId: attempt.ID,
		ExitCode: int32(exitCode), Error: errText,
	}); err != nil {
		return false, false
	}
	_ = s.taskQueue.Complete(ctx, run.ID, &types.RunExecutionResult{
		ID: run.ID, ExitCode: exitCode, Error: errText,
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

	// Clean up side effects
	_ = s.backend.ReleaseStaleTaskInputClaims(ctx, run.ID)
	if s.terminalIO != nil {
		_ = s.terminalIO.SetRunInteraction(ctx, run.WorkspaceID, run.ID,
			types.RunInteraction{State: types.RunInteractionStateClosed}, 5*time.Minute)
	}
	if s.taskQueue != nil {
		_ = s.taskQueue.Fail(ctx, run.ID, fmt.Errorf("%s", errorMsg))
	}

	// Mark run terminal
	if err := s.backend.SetRunExecutionResult(ctx, run.ID, -1, errorMsg); err != nil {
		var notFound *types.ErrRunExecutionNotFound
		if !errors.As(err, &notFound) {
			return false, false
		}
	}

	// Finalize attempt
	if attempt, _ := s.lookupRunAttemptByExecutionID(ctx, run.ID); attempt != nil && attempt.IsActive() {
		now := time.Now()
		st, rs, errStr := types.ClassifyExecutionOutcome(-1, errorMsg)
		_ = s.backend.UpdateAgentRunAttemptResult(ctx, attempt.ID, st, intPtr(-1), now, errStr)
		_ = s.backend.ClearAgentRunClaim(ctx, attempt.RunID)
		_ = updateExecutionInstanceCounts(ctx, s.backend, attempt.RunID, -1)
		_ = s.backend.UpdateAgentRunLifecycle(ctx, attempt.RunID, rs, nil, &now, errStr)
	}

	// Settle task via central lifecycle
	if err := s.lifecycle.Settle(ctx, run.ID, nil); err != nil {
		log.Warn().Err(err).Str("run_id", run.ID).Msg("recovery: settle failed")
		return true, false
	}
	return true, false
}

func intPtr(v int) *int { return &v }

func isTerminalQueueState(status types.RunExecutionStatus) bool {
	switch status {
	case types.RunExecutionStatusComplete, types.RunExecutionStatusFailed, types.RunExecutionStatusCancelled:
		return true
	}
	return false
}
