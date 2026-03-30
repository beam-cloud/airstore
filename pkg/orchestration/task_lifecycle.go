package orchestration

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/beam-cloud/airstore/pkg/repository"
	"github.com/beam-cloud/airstore/pkg/types"
	"github.com/rs/zerolog/log"
)

// TaskLifecycle is the single authority for task state transitions.
// Every state change on agent_task must go through this struct.
type TaskLifecycle struct {
	backend    repository.BackendRepository
	store      *repository.OrchestrationStore
	terminalIO repository.TerminalIORepository
	outcomes   *OutcomeProjector
}

func NewTaskLifecycle(
	backend repository.BackendRepository,
	store *repository.OrchestrationStore,
	terminalIO repository.TerminalIORepository,
) *TaskLifecycle {
	return &TaskLifecycle{
		backend:    backend,
		store:      store,
		terminalIO: terminalIO,
		outcomes:   NewOutcomeProjector(backend),
	}
}

// validTransitions defines the legal state machine edges.
var validTransitions = map[types.AgentTaskState][]types.AgentTaskState{
	types.AgentTaskStateQueued:   {types.AgentTaskStateRunning, types.AgentTaskStateDropped, types.AgentTaskStateCancelled},
	types.AgentTaskStateRunning:  {types.AgentTaskStateWaiting, types.AgentTaskStateDone, types.AgentTaskStateError, types.AgentTaskStateSleeping, types.AgentTaskStateDropped, types.AgentTaskStateQueued, types.AgentTaskStateCancelled},
	types.AgentTaskStateWaiting:  {types.AgentTaskStateRunning, types.AgentTaskStateDone, types.AgentTaskStateError, types.AgentTaskStateQueued, types.AgentTaskStateCancelled},
	types.AgentTaskStateSleeping: {types.AgentTaskStateQueued, types.AgentTaskStateCancelled},
	types.AgentTaskStateError:     {types.AgentTaskStateQueued, types.AgentTaskStateCancelled},
	types.AgentTaskStateDropped:   {types.AgentTaskStateQueued, types.AgentTaskStateCancelled},
	types.AgentTaskStateCancelled: {types.AgentTaskStateQueued},
}

func isValidTransition(from, to types.AgentTaskState) bool {
	for _, s := range validTransitions[from] {
		if s == to {
			return true
		}
	}
	return false
}

// SettleOpts carries optional signals that only the result projector provides.
type SettleOpts struct {
	WaitingForInput bool
	WakeSignal      *types.RunExecutionWakeSignal
	Blocker         *types.TaskBlockerSpec
}

// Settle derives the correct task state from a completed run and applies it.
// This is THE single path for run-completion settlement. It is idempotent.
func (lc *TaskLifecycle) Settle(ctx context.Context, runID string, opts *SettleOpts) error {
	if lc == nil || lc.backend == nil || strings.TrimSpace(runID) == "" {
		return nil
	}
	if opts == nil {
		opts = &SettleOpts{}
	}

	run, err := lc.backend.GetAgentRunByID(ctx, runID)
	if err != nil {
		return err
	}
	if run == nil {
		return nil
	}

	task, err := lc.backend.GetTaskByID(ctx, run.OriginTaskID)
	if err != nil {
		return err
	}
	if task == nil {
		return nil
	}
	if task.State.IsTerminal() {
		return nil
	}
	// If the task was updated after this run ended, a newer run or
	// requeue has taken over; this settlement is stale.
	if run.EndedAt != nil && task.UpdatedAt.After(*run.EndedAt) {
		return nil
	}

	// Retryable errors get a dispatch retry instead of a terminal state.
	if run.Status == types.AgentRunStatusError && run.Error != nil && isRetryableError(*run.Error) {
		retried, retryErr := lc.scheduleRetry(ctx, task, run)
		if retryErr != nil {
			return retryErr
		}
		if retried {
			return nil
		}
	}

	targetRunID := run.ID
	nextState := types.TaskTerminalStateForRun(run.Status)

	if opts.WaitingForInput && nextState == types.AgentTaskStateDone {
		nextState = types.AgentTaskStateWaiting
	}

	// Follow-up wake: transition to sleeping with deferred dispatch.
	if opts.WakeSignal != nil && nextState == types.AgentTaskStateDone {
		return lc.sleepWithWake(ctx, task, run, opts.WakeSignal)
	}

	if !isValidTransition(task.State, nextState) {
		log.Warn().
			Str("task_id", task.ID).
			Str("run_id", run.ID).
			Str("from", string(task.State)).
			Str("to", string(nextState)).
			Msg("task lifecycle: invalid transition, skipping")
		return nil
	}

	var updated bool
	if nextState == types.AgentTaskStateWaiting && opts.Blocker != nil {
		updated, _, err = lc.backend.OpenTaskBlockerIfCurrentRun(ctx, types.TaskBlockerOpenRequest{
			WorkspaceID:   task.WorkspaceID,
			TaskID:        run.OriginTaskID,
			ExpectedRunID: run.ID,
			Blocker:       opts.Blocker,
		})
	} else {
		updated, err = lc.backend.UpdateTaskStateIfCurrentRun(ctx, types.CurrentRunTaskStateUpdate{
			TaskID:        run.OriginTaskID,
			ExpectedRunID: run.ID,
			State:         nextState,
			TargetRunID:   &targetRunID,
		})
	}
	if err != nil {
		return err
	}
	if !updated {
		// target_run_id may be NULL (cleared by requeueTaskForResume) which
		// causes the WHERE target_run_id = $8 clause to miss. Re-fetch and
		// fall back to an unconditional update if the task is still non-terminal
		// and no other run has claimed it.
		fresh, refetchErr := lc.backend.GetTaskByID(ctx, run.OriginTaskID)
		if refetchErr != nil || fresh == nil || fresh.State.IsTerminal() {
			return refetchErr
		}
		if fresh.TargetRunID != nil {
			return nil // another run took over
		}
		if !isValidTransition(fresh.State, nextState) {
			return nil
		}
		if err := lc.backend.UpdateTaskState(ctx, types.TaskStateUpdate{
			TaskID:      fresh.ID,
			State:       nextState,
			TargetRunID: &targetRunID,
		}); err != nil {
			return err
		}
		task = fresh
	}

	task.State = nextState
	task.TargetRunID = &targetRunID
	if lc.outcomes != nil {
		if err := lc.outcomes.Sync(ctx, task, run); err != nil {
			return err
		}
	}
	lc.publishUpdate(ctx, task.WorkspaceID, task.ID)
	return nil
}

func (lc *TaskLifecycle) Resume(ctx context.Context, taskID, runID string) (bool, error) {
	if lc == nil || lc.backend == nil || strings.TrimSpace(taskID) == "" || strings.TrimSpace(runID) == "" {
		return false, nil
	}
	return lc.backend.UpdateTaskStateIfCurrentRun(ctx, types.CurrentRunTaskStateUpdate{
		TaskID:        taskID,
		ExpectedRunID: runID,
		State:         types.AgentTaskStateRunning,
	})
}

func (lc *TaskLifecycle) Done(ctx context.Context, taskID string, targetRunID *string) error {
	if lc == nil || lc.backend == nil || strings.TrimSpace(taskID) == "" {
		return nil
	}
	return lc.backend.UpdateTaskState(ctx, types.TaskStateUpdate{
		TaskID:      taskID,
		State:       types.AgentTaskStateDone,
		TargetRunID: targetRunID,
	})
}

func (lc *TaskLifecycle) Queue(ctx context.Context, taskID string, targetRunID *string) error {
	if lc == nil || lc.backend == nil || strings.TrimSpace(taskID) == "" {
		return nil
	}
	return lc.backend.UpdateTaskState(ctx, types.TaskStateUpdate{
		TaskID:      taskID,
		State:       types.AgentTaskStateQueued,
		TargetRunID: targetRunID,
	})
}

func (lc *TaskLifecycle) updateWithRetryDrop(ctx context.Context, task *types.AgentTask, dropReason string) error {
	if lc == nil || lc.backend == nil || task == nil {
		return nil
	}
	return lc.backend.UpdateTaskState(ctx, types.TaskStateUpdate{
		TaskID:        task.ID,
		State:         types.AgentTaskStateDropped,
		DroppedReason: &dropReason,
		TargetRunID:   task.TargetRunID,
	})
}

func (lc *TaskLifecycle) queueForRetry(ctx context.Context, task *types.AgentTask) error {
	if lc == nil || lc.backend == nil || task == nil {
		return nil
	}
	return lc.backend.UpdateTaskState(ctx, types.TaskStateUpdate{
		TaskID:      task.ID,
		State:       types.AgentTaskStateQueued,
		TargetRunID: task.TargetRunID,
	})
}

func (lc *TaskLifecycle) SetOutcomeProjector(projector *OutcomeProjector) {
	if lc == nil {
		return
	}
	lc.outcomes = projector
}

func (lc *TaskLifecycle) outcomeProjector() *OutcomeProjector {
	if lc == nil {
		return nil
	}
	return lc.outcomes
}

func (lc *TaskLifecycle) syncOutcome(ctx context.Context, task *types.AgentTask, run *types.AgentRun) error {
	if lc.outcomes == nil {
		return nil
	}
	return lc.outcomes.Sync(ctx, task, run)
}

type OutcomeProjector struct {
	backend repository.BackendRepository
}

func NewOutcomeProjector(backend repository.BackendRepository) *OutcomeProjector {
	return &OutcomeProjector{backend: backend}
}

func (p *OutcomeProjector) Sync(ctx context.Context, task *types.AgentTask, run *types.AgentRun) error {
	if p == nil || p.backend == nil || task == nil || run == nil {
		return nil
	}
	taskID := strings.TrimSpace(task.ID)
	if taskID == "" {
		return nil
	}

	if run.Status == types.AgentRunStatusOK {
		if err := activateApprovedOutputs(ctx, p.backend, task); err != nil {
			log.Warn().Err(err).Str("task_id", taskID).Msg("failed to activate approved outputs")
		}
	}

	return syncTaskCost(ctx, p.backend, task)
}

func activateApprovedOutputs(ctx context.Context, backend repository.BackendRepository, task *types.AgentTask) error {
	outputs, err := backend.ListTaskOutputs(ctx, task.WorkspaceID, task.ID)
	if err != nil {
		return fmt.Errorf("list outputs for activation: %w", err)
	}
	for _, out := range outputs {
		if out == nil || out.Status != types.TaskOutputStatusApproved {
			continue
		}
		if err := backend.UpdateTaskOutputStatus(ctx, task.WorkspaceID, out.ID, types.TaskOutputStatusActive); err != nil {
			return fmt.Errorf("activate approved output %s: %w", out.ID, err)
		}
	}
	return nil
}

func syncTaskCost(ctx context.Context, backend repository.BackendRepository, task *types.AgentTask) error {
	taskID := strings.TrimSpace(task.ID)
	runs, err := backend.ListAgentRunsFiltered(ctx, task.WorkspaceID, types.AgentRunListFilter{
		TaskID: &taskID,
		Limit:  500,
	})
	if err != nil {
		return err
	}
	totalCost := 0.0
	for _, run := range runs {
		if run == nil {
			continue
		}
		totalCost += run.CostUSD
	}
	if err := backend.UpdateTaskCost(ctx, task.ID, totalCost); err != nil {
		return err
	}
	task.CostUSD = totalCost
	return nil
}

// TransitionLive applies a non-terminal state change during execution
// (running <-> waiting). Only the active worker should call this.
func (lc *TaskLifecycle) TransitionLive(ctx context.Context, update types.TaskLiveUpdate) (bool, error) {
	if lc == nil || lc.backend == nil {
		return false, nil
	}
	if update.State != types.AgentTaskStateWaiting && update.State != types.AgentTaskStateRunning {
		return false, fmt.Errorf("task lifecycle: TransitionLive only supports waiting/running, got %s", update.State)
	}
	if update.State == types.AgentTaskStateWaiting {
		if update.Blocker == nil {
			return false, fmt.Errorf("task lifecycle: waiting update requires blocker")
		}
		run, err := lc.backend.GetAgentRunByID(ctx, update.RunID)
		if err != nil {
			return false, err
		}
		if run == nil {
			return false, nil
		}
		updated, _, err := lc.backend.OpenTaskBlockerIfCurrentRun(ctx, types.TaskBlockerOpenRequest{
			WorkspaceID:   run.WorkspaceID,
			TaskID:        update.TaskID,
			ExpectedRunID: update.RunID,
			Blocker:       update.Blocker,
		})
		return updated, err
	}
	return lc.backend.UpdateTaskStateIfCurrentRun(ctx, types.CurrentRunTaskStateUpdate{
		TaskID:        update.TaskID,
		ExpectedRunID: update.RunID,
		State:         update.State,
	})
}

// Cancel transitions a task to cancelled. Allowed from any non-terminal state.
func (lc *TaskLifecycle) Cancel(ctx context.Context, taskID string) error {
	if lc == nil || lc.backend == nil {
		return nil
	}
	return lc.backend.UpdateTaskState(ctx, types.TaskStateUpdate{
		TaskID: taskID,
		State:  types.AgentTaskStateCancelled,
	})
}

// Retry transitions a stopped task (error, dropped, or cancelled) back to
// queued and enqueues a fresh dispatch so the orchestration loop picks it up.
func (lc *TaskLifecycle) Retry(ctx context.Context, taskID string) error {
	if lc == nil || lc.backend == nil {
		return nil
	}
	task, err := lc.backend.GetTaskByID(ctx, taskID)
	if err != nil {
		return err
	}
	if task == nil {
		return &types.ErrAgentTaskNotFound{ID: taskID}
	}
	if !task.State.IsRetryable() {
		return &types.ErrTaskNotRetryable{ID: taskID, State: task.State}
	}

	if err := lc.backend.UpdateTaskState(ctx, types.TaskStateUpdate{
		TaskID: taskID,
		State:  types.AgentTaskStateQueued,
	}); err != nil {
		return fmt.Errorf("transition task to queued: %w", err)
	}

	dedupeKey := fmt.Sprintf("retry:%s:%d", taskID, time.Now().UnixNano())
	outboxEvent := &types.OrchestrationOutboxEvent{
		EventType: types.OrchestrationOutboxEventTypeTaskDispatch,
		DedupeKey: dedupeKey,
		PayloadJSON: map[string]any{
			types.OrchestrationOutboxPayloadTaskID: taskID,
		},
	}
	if err := lc.backend.EnqueueOrchestrationOutboxEvent(ctx, outboxEvent); err != nil {
		return fmt.Errorf("enqueue retry dispatch: %w", err)
	}

	log.Info().Str("task_id", taskID).Str("from_state", string(task.State)).Msg("task retried")
	lc.publishUpdate(ctx, task.WorkspaceID, task.ID)
	return nil
}

// Dispatch transitions queued -> running when a run is materialized.
func (lc *TaskLifecycle) Dispatch(ctx context.Context, taskID, runID string) error {
	if lc == nil || lc.backend == nil {
		return nil
	}
	return lc.backend.UpdateTaskState(ctx, types.TaskStateUpdate{
		TaskID:      taskID,
		State:       types.AgentTaskStateRunning,
		TargetRunID: &runID,
	})
}

// Drop transitions a task to dropped with a reason.
func (lc *TaskLifecycle) Drop(ctx context.Context, taskID string, reason string) error {
	if lc == nil || lc.backend == nil {
		return nil
	}
	return lc.backend.UpdateTaskState(ctx, types.TaskStateUpdate{
		TaskID:        taskID,
		State:         types.AgentTaskStateDropped,
		DroppedReason: &reason,
	})
}

func (lc *TaskLifecycle) sleepWithWake(ctx context.Context, task *types.AgentTask, run *types.AgentRun, ws *types.RunExecutionWakeSignal) error {
	delayMin := wakeBackoffDelay(task.WakeCount, ws.DelayMinutes)
	wakeAt := time.Now().Add(time.Duration(delayMin) * time.Minute)
	wakeReason := strings.TrimSpace(ws.Reason)
	if wakeReason == "" {
		for _, item := range ws.WakeAgenda {
			if item == nil {
				continue
			}
			if title := strings.TrimSpace(item.Title); title != "" {
				wakeReason = title
				break
			}
			if reason := strings.TrimSpace(item.Reason); reason != "" {
				wakeReason = reason
				break
			}
		}
	}
	dedupeKey := fmt.Sprintf("wake_dispatch:%s:%s", task.ID, run.ID)
	outboxEvent := &types.OrchestrationOutboxEvent{
		EventType: types.OrchestrationOutboxEventTypeTaskDispatch,
		DedupeKey: dedupeKey,
		PayloadJSON: map[string]any{
			types.OrchestrationOutboxPayloadTaskID:                task.ID,
			types.OrchestrationOutboxPayloadDispatchPrompt:        ws.FollowUpPrompt,
			types.OrchestrationOutboxPayloadWakeFollowUpPrompt:    ws.FollowUpPrompt,
			types.OrchestrationOutboxPayloadResumeSession:         true,
			types.OrchestrationOutboxPayloadResumeExcludeRunID:    run.ID,
			types.OrchestrationOutboxPayloadResumeCheckpointRunID: run.ID,
			types.OrchestrationOutboxPayloadWakeReason:            wakeReason,
		},
		AvailableAt: wakeAt,
	}
	ok, err := lc.backend.SleepTaskWithOutbox(ctx, task.ID, run.ID, wakeAt, wakeReason, ws.WakeAgenda, outboxEvent)
	if err != nil {
		return fmt.Errorf("sleep task with outbox: %w", err)
	}
	if ok {
		log.Info().
			Str("task_id", task.ID).
			Str("run_id", run.ID).
			Int("llm_delay", ws.DelayMinutes).
			Int("actual_delay", delayMin).
			Int("wake_count", task.WakeCount).
			Time("wake_at", wakeAt).
			Msg("task transitioned to sleeping")
		lc.publishUpdate(ctx, task.WorkspaceID, task.ID)
	}
	return nil
}

func (lc *TaskLifecycle) scheduleRetry(ctx context.Context, task *types.AgentTask, run *types.AgentRun) (bool, error) {
	if task == nil || run == nil || run.Status != types.AgentRunStatusError {
		return false, nil
	}
	if run.Error == nil || !isRetryableError(*run.Error) {
		return false, nil
	}
	retryAttempt := intFromAny(run.DeliveryJSON[types.OrchestrationOutboxPayloadDispatchAttempt])
	payload := buildRetryPayload(ctx, lc.backend, lc.terminalIO, task, run)
	delay := computeDispatchRetryDelay(retryAttempt)

	nextAttempt := retryAttempt + 1
	if nextAttempt > dispatchRetryMaxAttempts {
		dropReason := types.AgentTaskDropReasonDispatchRetryExhausted
		if err := lc.backend.UpdateTaskState(ctx, types.TaskStateUpdate{
			TaskID:        task.ID,
			State:         types.AgentTaskStateDropped,
			DroppedReason: &dropReason,
			TargetRunID:   task.TargetRunID,
		}); err != nil {
			return true, err
		}
		lc.publishUpdate(ctx, task.WorkspaceID, task.ID)
		return true, nil
	}

	guardKey := dispatchRetryGuardKey(task, run, nextAttempt)
	acquired, err := lc.backend.AcquireOrchestrationRetryGuard(ctx, guardKey)
	if err != nil {
		return true, err
	}
	if !acquired {
		return true, nil
	}

	if err := lc.backend.UpdateTaskState(ctx, types.TaskStateUpdate{
		TaskID:      task.ID,
		State:       types.AgentTaskStateQueued,
		TargetRunID: task.TargetRunID,
	}); err != nil {
		return true, err
	}
	lc.publishUpdate(ctx, task.WorkspaceID, task.ID)

	retryPayload := cloneAnyMap(payload)
	if retryPayload == nil {
		retryPayload = map[string]any{}
	}
	retryPayload[types.OrchestrationOutboxPayloadTaskID] = task.ID
	retryPayload[types.OrchestrationOutboxPayloadDispatchAttempt] = nextAttempt

	outboxEvent := &types.OrchestrationOutboxEvent{
		EventType:   types.OrchestrationOutboxEventTypeTaskDispatch,
		DedupeKey:   guardKey,
		PayloadJSON: retryPayload,
		AvailableAt: time.Now().Add(delay),
	}
	if err := lc.backend.EnqueueOrchestrationOutboxEvent(ctx, outboxEvent); err != nil {
		return true, err
	}

	log.Info().
		Str("task_id", task.ID).
		Str("run_id", run.ID).
		Str("error", *run.Error).
		Int("retry_attempt", nextAttempt).
		Dur("delay", delay).
		Msg("requeued task after transient failure")
	return true, nil
}

// buildRetryPayload builds the dispatch payload for a retry attempt.
// Standalone version that takes explicit dependencies.
func buildRetryPayload(
	ctx context.Context,
	backend repository.BackendRepository,
	terminalIO repository.TerminalIORepository,
	task *types.AgentTask,
	run *types.AgentRun,
) map[string]any {
	payload := map[string]any{}
	prompt := ""
	if exec, err := backend.GetRunExecution(ctx, run.ID); err == nil && exec != nil {
		prompt = strings.TrimSpace(exec.Prompt)
	}
	if prompt == "" {
		prompt = runInputPrompt(task.PayloadJSON)
	}
	if prompt != "" {
		payload[types.OrchestrationOutboxPayloadDispatchPrompt] = prompt
	}
	if run.Interactive {
		payload[types.OrchestrationOutboxPayloadResumeSession] = true
		payload[types.OrchestrationOutboxPayloadResumeExcludeRunID] = run.ID
		if cpID := strings.TrimSpace(stringFromPayload(run.DeliveryJSON, types.OrchestrationOutboxPayloadResumeCheckpointRunID)); cpID != "" {
			payload[types.OrchestrationOutboxPayloadResumeCheckpointRunID] = cpID
		} else if terminalIO != nil {
			if cp, err := terminalIO.GetSessionCheckpoint(ctx, run.WorkspaceID, run.SessionID); err == nil && cp != nil && cp.RunID != "" {
				payload[types.OrchestrationOutboxPayloadResumeCheckpointRunID] = cp.RunID
			}
		}
	}
	return payload
}

func (lc *TaskLifecycle) publishUpdate(ctx context.Context, workspaceID uint, taskID string) {
	if lc.store == nil || strings.TrimSpace(taskID) == "" {
		return
	}
	if err := lc.store.PublishTaskLive(ctx, taskID); err != nil {
		log.Debug().Err(err).Str("task_id", taskID).Msg("failed to publish task live update")
	}
	if err := lc.store.PublishWorkspaceLive(ctx, workspaceID); err != nil {
		log.Debug().Err(err).Uint("workspace_id", workspaceID).Msg("failed to publish workspace live update")
	}
}
