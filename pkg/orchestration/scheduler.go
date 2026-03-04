package orchestration

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/beam-cloud/airstore/pkg/repository"
	"github.com/beam-cloud/airstore/pkg/types"
	"github.com/rs/zerolog/log"
)

const (
	schedulerPollInterval  = 10 * time.Second
	schedulerBatch         = 50
	schedulerWorkers       = 5
	schedulerSubmitTimeout = 30 * time.Second
)

// CronScheduler polls for due scheduled tasks and fires them as agent tasks.
//
// Multi-replica safety: AdvanceScheduledTask is a compare-and-swap on
// next_run_at, so only one replica can win a given tick. If the submit
// fails after the CAS, we revert next_run_at to allow retry on the
// next poll cycle rather than silently skipping the occurrence.
type CronScheduler struct {
	backend repository.BackendRepository
	agents  *AgentAPI
}

func NewCronScheduler(
	backend repository.BackendRepository,
	agents *AgentAPI,
) *CronScheduler {
	return &CronScheduler{backend: backend, agents: agents}
}

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
	schedules, err := s.backend.ListDueScheduledTasks(ctx, time.Now(), schedulerBatch)
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
	var fired, skipped, failed atomic.Int32

	for _, sched := range schedules {
		wg.Add(1)
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			wg.Done()
			return
		}

		go func(schedule *types.ScheduledTask) {
			defer wg.Done()
			defer func() { <-sem }()

			if err := s.fireSchedule(ctx, schedule); err != nil {
				if err == errCASLost {
					skipped.Add(1)
				} else {
					failed.Add(1)
					log.Warn().Err(err).
						Str("schedule", schedule.ExternalID).
						Str("cron", schedule.CronExpr).
						Msg("cron scheduler: fire failed")
				}
			} else {
				fired.Add(1)
			}
		}(sched)
	}

	wg.Wait()

	log.Info().
		Int32("fired", fired.Load()).
		Int32("skipped", skipped.Load()).
		Int32("failed", failed.Load()).
		Msg("cron scheduler: poll cycle complete")
}

var errCASLost = fmt.Errorf("another replica already advanced this schedule")

func (s *CronScheduler) fireSchedule(ctx context.Context, schedule *types.ScheduledTask) error {
	nextRun, err := NextCronTime(schedule.CronExpr, time.Now(), schedule.Timezone)
	if err != nil {
		return fmt.Errorf("compute next run: %w", err)
	}

	advanced, err := s.backend.AdvanceScheduledTask(ctx, schedule.ID, schedule.NextRunAt, nextRun)
	if err != nil {
		return fmt.Errorf("advance schedule: %w", err)
	}
	if !advanced {
		return errCASLost
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
		if revertErr := s.revertAdvance(ctx, schedule, nextRun); revertErr != nil {
			log.Warn().Err(revertErr).
				Str("schedule", schedule.ExternalID).
				Msg("cron scheduler: failed to revert advance after submit error")
		}
		return fmt.Errorf("accept scheduled task: %w", err)
	}

	return nil
}

// revertAdvance undoes a successful AdvanceScheduledTask so the occurrence
// can be retried on the next poll cycle.
func (s *CronScheduler) revertAdvance(ctx context.Context, schedule *types.ScheduledTask, advancedTo time.Time) error {
	reverted, err := s.backend.RevertScheduledTaskAdvance(ctx, schedule.ID, advancedTo, schedule.NextRunAt)
	if err != nil {
		return err
	}
	if !reverted {
		return fmt.Errorf("CAS revert lost (schedule %s)", schedule.ExternalID)
	}
	return nil
}

func truncateSchedulePrompt(prompt string, maxLen int) string {
	s := strings.TrimSpace(prompt)
	s = strings.ReplaceAll(s, "\n", " ")
	if len(s) > maxLen {
		return s[:maxLen] + "..."
	}
	return s
}
