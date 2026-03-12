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
}

func NewTaskLifecycle(
	backend repository.BackendRepository,
	store *repository.OrchestrationStore,
	terminalIO repository.TerminalIORepository,
) *TaskLifecycle {
	return &TaskLifecycle{backend: backend, store: store, terminalIO: terminalIO}
}

// validTransitions defines the legal state machine edges.
var validTransitions = map[types.AgentTaskState][]types.AgentTaskState{
	types.AgentTaskStateQueued:   {types.AgentTaskStateRunning, types.AgentTaskStateDropped, types.AgentTaskStateCancelled},
	types.AgentTaskStateRunning:  {types.AgentTaskStateWaiting, types.AgentTaskStateDone, types.AgentTaskStateSleeping, types.AgentTaskStateDropped, types.AgentTaskStateQueued, types.AgentTaskStateCancelled},
	types.AgentTaskStateWaiting:  {types.AgentTaskStateRunning, types.AgentTaskStateDone, types.AgentTaskStateQueued, types.AgentTaskStateCancelled},
	types.AgentTaskStateSleeping: {types.AgentTaskStateQueued, types.AgentTaskStateCancelled},
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

	updated, err := lc.backend.UpdateTaskStateIfCurrentRun(
		ctx, run.OriginTaskID, run.ID, nextState, nil, &targetRunID, "", nil,
	)
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
		if err := lc.backend.UpdateTaskState(ctx, fresh.ID, nextState, nil, &targetRunID); err != nil {
			return err
		}
		task = fresh
	}

	task.State = nextState
	task.TargetRunID = &targetRunID
	if err := SyncTaskOutcome(ctx, lc.backend, task, run); err != nil {
		return err
	}
	lc.publishUpdate(ctx, task.WorkspaceID, task.ID)
	return nil
}

// TransitionLive applies a non-terminal state change during execution
// (running <-> waiting). Only the active worker should call this.
func (lc *TaskLifecycle) TransitionLive(
	ctx context.Context,
	taskID, runID string,
	state types.AgentTaskState,
	inputKind types.InputKind,
	waitingSummary *string,
) (bool, error) {
	if lc == nil || lc.backend == nil {
		return false, nil
	}
	if state != types.AgentTaskStateWaiting && state != types.AgentTaskStateRunning {
		return false, fmt.Errorf("task lifecycle: TransitionLive only supports waiting/running, got %s", state)
	}
	return lc.backend.UpdateTaskStateIfCurrentRun(
		ctx, taskID, runID, state, nil, nil, inputKind, waitingSummary,
	)
}

// Cancel transitions a task to cancelled. Allowed from any non-terminal state.
func (lc *TaskLifecycle) Cancel(ctx context.Context, taskID string) error {
	if lc == nil || lc.backend == nil {
		return nil
	}
	return lc.backend.UpdateTaskState(ctx, taskID, types.AgentTaskStateCancelled, nil, nil)
}

// Dispatch transitions queued -> running when a run is materialized.
func (lc *TaskLifecycle) Dispatch(ctx context.Context, taskID, runID string) error {
	if lc == nil || lc.backend == nil {
		return nil
	}
	return lc.backend.UpdateTaskState(ctx, taskID, types.AgentTaskStateRunning, nil, &runID)
}

// Drop transitions a task to dropped with a reason.
func (lc *TaskLifecycle) Drop(ctx context.Context, taskID string, reason string) error {
	if lc == nil || lc.backend == nil {
		return nil
	}
	return lc.backend.UpdateTaskState(ctx, taskID, types.AgentTaskStateDropped, &reason, nil)
}

func (lc *TaskLifecycle) sleepWithWake(ctx context.Context, task *types.AgentTask, run *types.AgentRun, ws *types.RunExecutionWakeSignal) error {
	delayMin := wakeBackoffDelay(task.WakeCount, ws.DelayMinutes)
	wakeAt := time.Now().Add(time.Duration(delayMin) * time.Minute)
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
		},
		AvailableAt: wakeAt,
	}
	ok, err := lc.backend.SleepTaskWithOutbox(ctx, task.ID, run.ID, wakeAt, ws.Reason, outboxEvent)
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
		if err := lc.backend.UpdateTaskState(ctx, task.ID, types.AgentTaskStateDropped, &dropReason, task.TargetRunID); err != nil {
			return true, err
		}
		lc.publishUpdate(ctx, task.WorkspaceID, task.ID)
		return true, nil
	}

	guardKey := fmt.Sprintf("dispatch_retry:%s:%d", task.ID, nextAttempt)
	acquired, err := lc.backend.AcquireOrchestrationRetryGuard(ctx, guardKey)
	if err != nil {
		return true, err
	}
	if !acquired {
		return true, nil
	}

	if err := lc.backend.UpdateTaskState(ctx, task.ID, types.AgentTaskStateQueued, nil, task.TargetRunID); err != nil {
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
		DedupeKey:   fmt.Sprintf("dispatch_retry:%s:%d", task.ID, nextAttempt),
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
