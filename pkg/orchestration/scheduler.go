package orchestration

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/beam-cloud/airstore/pkg/common"
	"github.com/beam-cloud/airstore/pkg/repository"
	"github.com/beam-cloud/airstore/pkg/types"
	"github.com/rs/zerolog/log"
)

const (
	schedulerPollInterval = 10 * time.Second
	schedulerBatch        = 50
	schedulerWorkers      = 5
	schedulerMinLockTTL   = 30 * time.Second
	schedulerSubmitTimeout = 30 * time.Second
)

// CronScheduler polls for due scheduled tasks and fires them as agent tasks
// via AcceptAgentCommand, reusing the full orchestration pipeline.
type CronScheduler struct {
	backend repository.BackendRepository
	agents  *AgentAPI
	rdb     *common.RedisClient
}

func NewCronScheduler(
	backend repository.BackendRepository,
	agents *AgentAPI,
	rdb *common.RedisClient,
) *CronScheduler {
	return &CronScheduler{backend: backend, agents: agents, rdb: rdb}
}

// Start runs the poll loop. Call as a goroutine.
func (s *CronScheduler) Start(ctx context.Context) {
	log.Info().Msg("cron scheduler started")

	t := time.NewTicker(schedulerPollInterval)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			s.poll(ctx)
		}
	}
}

func (s *CronScheduler) poll(ctx context.Context) {
	schedules, err := s.backend.ClaimDueScheduledTasks(ctx, time.Now(), schedulerBatch)
	if err != nil {
		log.Warn().Err(err).Msg("cron scheduler: failed to fetch due schedules")
		return
	}
	if len(schedules) == 0 {
		return
	}

	log.Info().Int("due_schedules", len(schedules)).Msg("cron scheduler: poll cycle starting")

	sem := make(chan struct{}, schedulerWorkers)
	var wg sync.WaitGroup
	var fired, locked, failed atomic.Int32

	for _, sched := range schedules {
		lockKey := common.Keys.CronScheduleLock(sched.ExternalID)
		lockTTL := computeScheduleLockTTL(sched.CronExpr)
		acquired, err := s.rdb.SetNX(ctx, lockKey, "1", lockTTL).Result()
		if err != nil || !acquired {
			locked.Add(1)
			continue
		}

		wg.Add(1)
		sem <- struct{}{}

		go func(schedule *types.ScheduledTask) {
			defer wg.Done()
			defer func() { <-sem }()

			if err := s.fireSchedule(ctx, schedule); err != nil {
				failed.Add(1)
				log.Warn().Err(err).
					Str("schedule", schedule.ExternalID).
					Str("cron", schedule.CronExpr).
					Msg("cron scheduler: fire failed")
			} else {
				fired.Add(1)
			}
		}(sched)
	}

	wg.Wait()

	log.Info().
		Int32("fired", fired.Load()).
		Int32("already_locked", locked.Load()).
		Int32("failed", failed.Load()).
		Msg("cron scheduler: poll cycle complete")
}

func (s *CronScheduler) fireSchedule(ctx context.Context, schedule *types.ScheduledTask) error {
	nextRun, err := NextCronTime(schedule.CronExpr, time.Now())
	if err != nil {
		return fmt.Errorf("compute next run: %w", err)
	}

	advanced, err := s.backend.AdvanceScheduledTask(ctx, schedule.ID, schedule.NextRunAt, nextRun)
	if err != nil {
		return fmt.Errorf("advance schedule: %w", err)
	}
	if !advanced {
		return nil
	}

	submitCtx, cancel := context.WithTimeout(ctx, schedulerSubmitTimeout)
	defer cancel()

	tickTS := schedule.NextRunAt.Unix()
	idempotencyKey := fmt.Sprintf("schedule:%s:%d", schedule.ExternalID, tickTS)
	sessionID := fmt.Sprintf("schedule-session:%s:%d", schedule.ExternalID, tickTS)
	lane := fmt.Sprintf("schedule-lane:%s", schedule.ExternalID)
	source := "scheduled_task"
	label := fmt.Sprintf("Schedule: %s", truncateSchedulePrompt(schedule.Prompt, 60))
	spawnedBy := fmt.Sprintf("schedule:%s", schedule.ExternalID)

	_, _, err = s.agents.AcceptAgentCommand(submitCtx, schedule.WorkspaceID, AgentCommandParams{
		Message:        schedule.Prompt,
		AgentID:        &schedule.AgentID,
		SessionID:      sessionID,
		Lane:           &lane,
		IdempotencyKey: idempotencyKey,
		InputProvenance: &InputProvenance{
			Source: &source,
		},
		Label:     &label,
		SpawnedBy: &spawnedBy,
	})
	if err != nil {
		return fmt.Errorf("accept scheduled task: %w", err)
	}

	return nil
}

func computeScheduleLockTTL(cronExpr string) time.Duration {
	next1, err := NextCronTime(cronExpr, time.Now())
	if err != nil {
		return schedulerMinLockTTL
	}
	next2, err := NextCronTime(cronExpr, next1)
	if err != nil {
		return schedulerMinLockTTL
	}
	interval := next2.Sub(next1)
	ttl := time.Duration(float64(interval) * 0.9)
	if ttl < schedulerMinLockTTL {
		ttl = schedulerMinLockTTL
	}
	return ttl
}

func truncateSchedulePrompt(prompt string, maxLen int) string {
	s := strings.TrimSpace(prompt)
	s = strings.ReplaceAll(s, "\n", " ")
	if len(s) > maxLen {
		return s[:maxLen] + "..."
	}
	return s
}
