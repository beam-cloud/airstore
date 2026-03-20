package orchestration

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/beam-cloud/airstore/pkg/common"
	"github.com/beam-cloud/airstore/pkg/repository"
	"github.com/beam-cloud/airstore/pkg/types"
	"github.com/google/uuid"
	redislib "github.com/redis/go-redis/v9"
	"github.com/rs/zerolog/log"
)

type RuntimeLoops struct {
	backend            repository.BackendRepository
	store              *repository.OrchestrationStore
	terminalIO         repository.TerminalIORepository
	lifecycle          *TaskLifecycle
	instanceController *ExecutionInstanceController
	runFactory         *RunFactory
	taskFlows          *TaskFlows
	dispatchConsumerID string
	resultConsumerID   string
	publishTaskUpdate  func(context.Context, uint, string)
}

func NewRuntimeLoops(
	backend repository.BackendRepository,
	store *repository.OrchestrationStore,
	terminalIO repository.TerminalIORepository,
	lifecycle *TaskLifecycle,
	instanceController *ExecutionInstanceController,
	runFactory *RunFactory,
	taskFlows *TaskFlows,
	dispatchConsumerID string,
	resultConsumerID string,
	publishTaskUpdate func(context.Context, uint, string),
) *RuntimeLoops {
	return &RuntimeLoops{
		backend:            backend,
		store:              store,
		terminalIO:         terminalIO,
		lifecycle:          lifecycle,
		instanceController: instanceController,
		runFactory:         runFactory,
		taskFlows:          taskFlows,
		dispatchConsumerID: dispatchConsumerID,
		resultConsumerID:   resultConsumerID,
		publishTaskUpdate:  publishTaskUpdate,
	}
}

func (r *RuntimeLoops) RunOutboxLoop(ctx context.Context) {
	ticker := time.NewTicker(outboxPublisherInterval)
	defer ticker.Stop()

	r.publishOutboxBatch(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.publishOutboxBatch(ctx)
		}
	}
}

func (r *RuntimeLoops) RunDispatchLoop(ctx context.Context) {
	if r == nil || r.store == nil {
		log.Warn().Msg("orchestration dispatch loop disabled: store is unavailable")
		return
	}

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		reclaimed, err := r.store.ClaimPendingTaskDispatch(
			ctx,
			r.dispatchConsumerID,
			dispatchPendingMinIdle,
			dispatchReadBatch,
		)
		if err != nil {
			log.Warn().Err(err).Msg("failed to claim pending task-dispatch events")
		} else if err := r.processDispatchMessages(ctx, reclaimed); err != nil {
			log.Warn().Err(err).Msg("failed to process reclaimed task-dispatch events")
			continue
		}

		messages, err := r.store.ReadTaskDispatch(
			ctx,
			r.dispatchConsumerID,
			dispatchReadBlock,
			dispatchReadBatch,
		)
		if err != nil {
			log.Warn().Err(err).Msg("failed to read task-dispatch stream")
			continue
		}
		if err := r.processDispatchMessages(ctx, messages); err != nil {
			log.Warn().Err(err).Msg("failed to process task-dispatch events")
		}
	}
}

func (r *RuntimeLoops) RunResultLoop(ctx context.Context) {
	if r == nil || r.store == nil || r.backend == nil {
		log.Warn().Msg("orchestration result projector disabled: store is unavailable")
		return
	}

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		reclaimed, err := r.store.ClaimPendingRunResults(
			ctx,
			r.resultConsumerID,
			resultPendingMinIdle,
			resultReadBatch,
		)
		if err != nil {
			log.Warn().Err(err).Msg("failed to claim pending run-result events")
		} else if err := r.processRunResultMessages(ctx, reclaimed); err != nil {
			log.Warn().Err(err).Msg("failed to process reclaimed run-result events")
			continue
		}

		messages, err := r.store.ReadRunResults(
			ctx,
			r.resultConsumerID,
			resultReadBlock,
			resultReadBatch,
		)
		if err != nil {
			log.Warn().Err(err).Msg("failed to read run-result stream")
			continue
		}
		if err := r.processRunResultMessages(ctx, messages); err != nil {
			log.Warn().Err(err).Msg("failed to process run-result events")
		}
	}
}

func (r *RuntimeLoops) publishOutboxBatch(ctx context.Context) {
	if r == nil || r.backend == nil || r.store == nil {
		return
	}
	events, err := r.backend.ClaimPendingOrchestrationOutboxEvents(ctx, outboxPublisherBatchSize)
	if err != nil {
		log.Warn().Err(err).Msg("failed to claim orchestration outbox events")
		return
	}
	for _, event := range events {
		if event == nil {
			continue
		}
		if err := r.publishOutboxEvent(ctx, event); err != nil {
			_ = r.backend.MarkOrchestrationOutboxEventError(ctx, event.ID, err.Error())
			log.Warn().
				Err(err).
				Int64("outbox_id", event.ID).
				Str("event_type", string(event.EventType)).
				Msg("failed to publish orchestration outbox event")
			continue
		}
		if err := r.backend.MarkOrchestrationOutboxEventPublished(ctx, event.ID); err != nil {
			log.Warn().
				Err(err).
				Int64("outbox_id", event.ID).
				Msg("failed to mark orchestration outbox event as published")
		}
	}
}

func (r *RuntimeLoops) publishOutboxEvent(ctx context.Context, event *types.OrchestrationOutboxEvent) error {
	if r == nil || r.store == nil || event == nil {
		return fmt.Errorf("orchestration outbox publisher is unavailable")
	}

	switch event.EventType {
	case types.OrchestrationOutboxEventTypeTaskDispatch:
		_, err := r.store.PublishTaskDispatch(ctx, event.PayloadJSON)
		return err
	case types.OrchestrationOutboxEventTypeRunResult:
		_, err := r.store.PublishRunResult(ctx, event.PayloadJSON)
		return err
	default:
		return fmt.Errorf("unsupported orchestration outbox event type %q", event.EventType)
	}
}

func (r *RuntimeLoops) processDispatchMessages(ctx context.Context, messages []redislib.XMessage) error {
	for _, message := range messages {
		if err := r.processDispatchMessage(ctx, message); err != nil {
			return err
		}
	}
	return nil
}

func (r *RuntimeLoops) processDispatchMessage(ctx context.Context, message redislib.XMessage) error {
	dispatch := parseDispatchEnvelope(message.Values)
	if dispatch.TaskID == "" {
		_ = r.store.AckTaskDispatch(ctx, message.ID)
		return nil
	}

	task, claimed, err := r.backend.ClaimQueuedTaskForDispatch(ctx, dispatch.TaskID, dispatchClaimStaleAfter)
	if err != nil {
		return err
	}
	if !claimed || task == nil {
		_ = r.store.AckTaskDispatch(ctx, message.ID)
		return nil
	}

	applyDispatchPayload(task, message.Values, dispatch.RetryAttempt)

	if err := r.dispatchTask(ctx, task); err != nil {
		reason := "dispatch_error"
		delay := computeDispatchRetryDelay(dispatch.RetryAttempt)
		var retryRequest *dispatchRetryRequest
		if errors.As(err, &retryRequest) {
			if retryRequest.reason != "" {
				reason = retryRequest.reason
			}
			if retryRequest.delay > 0 {
				delay = retryRequest.delay
			}
		}
		if scheduleErr := r.scheduleRetry(
			ctx,
			task,
			dispatch.RetryAttempt,
			reason,
			delay,
			dispatchRetryPayloadFromValues(message.Values),
		); scheduleErr != nil {
			return scheduleErr
		}
	}
	_ = r.store.AckTaskDispatch(ctx, message.ID)
	return nil
}

func (r *RuntimeLoops) dispatchTask(ctx context.Context, task *types.AgentTask) error {
	if task == nil {
		return nil
	}
	switch task.QueueMode {
	case types.AgentQueueModeInterrupt:
		return r.handleInterruptTask(ctx, task)
	default:
		return r.handleExecutionTask(ctx, task)
	}
}

func (r *RuntimeLoops) handleInterruptTask(ctx context.Context, task *types.AgentTask) error {
	if task.TargetRunID == nil {
		reason := types.AgentTaskDropReasonInterruptMissingTarget
		if r.lifecycle != nil {
			if err := r.lifecycle.Drop(ctx, task.ID, reason); err != nil {
				return err
			}
		}
		r.notifyTaskUpdate(ctx, task.WorkspaceID, task.ID)
		return nil
	}

	run, err := r.backend.GetAgentRunByID(ctx, *task.TargetRunID)
	if err != nil {
		return err
	}

	_, _ = r.cancelInFlightRunExecutions(ctx, run.ID)
	if r.runFactory != nil && r.runFactory.resumeBarrier != nil {
		if err := r.runFactory.resumeBarrier.waitForSessionLeaseDrain(ctx, run.WorkspaceID, run.SessionID); err != nil {
			return err
		}
	}

	now := time.Now()
	errMsg := types.AgentRunErrorInterruptedByQueuedInput
	if err := r.backend.UpdateAgentRunLifecycle(ctx, run.ID, types.AgentRunStatusCancelled, nil, &now, &errMsg); err != nil {
		return err
	}
	if r.lifecycle != nil {
		if err := r.lifecycle.Done(ctx, task.ID, task.TargetRunID); err != nil {
			return err
		}
	}
	r.notifyTaskUpdate(ctx, task.WorkspaceID, task.ID)
	return nil
}

func (r *RuntimeLoops) cancelInFlightRunExecutions(ctx context.Context, runID string) (bool, error) {
	attempts, err := r.backend.ListAgentRunAttempts(ctx, runID)
	if err != nil {
		return false, err
	}

	cancelled := false
	var firstErr error
	for _, attempt := range attempts {
		if attempt == nil || attempt.ExecutionID == nil {
			continue
		}

		executionID := strings.TrimSpace(*attempt.ExecutionID)
		if executionID == "" {
			continue
		}

		cancelled = true
		if err := r.backend.CancelRunExecution(ctx, executionID); err != nil && !isRunExecutionCancelNoopError(err) {
			if firstErr == nil {
				firstErr = err
			}
			log.Warn().
				Err(err).
				Str("run_id", runID).
				Str("execution_id", executionID).
				Msg("failed to mark run execution cancelled")
		}

		if r.terminalIO != nil {
			if err := r.terminalIO.PublishCancel(ctx, executionID); err != nil {
				log.Warn().
					Err(err).
					Str("run_id", runID).
					Str("execution_id", executionID).
					Msg("failed to publish run cancellation signal")
			}
		}
	}
	return cancelled, firstErr
}

func (r *RuntimeLoops) scheduleRetry(
	ctx context.Context,
	task *types.AgentTask,
	retryAttempt int,
	reason string,
	delay time.Duration,
	retryPayload map[string]any,
) error {
	if r == nil || r.backend == nil || task == nil {
		return fmt.Errorf("dispatch retry dependencies are unavailable")
	}

	nextAttempt := retryAttempt + 1
	if nextAttempt > dispatchRetryMaxAttempts {
		dropReason := types.AgentTaskDropReasonDispatchRetryExhausted
		if r.lifecycle != nil {
			if err := r.lifecycle.Drop(ctx, task.ID, dropReason); err != nil {
				return err
			}
		}
		r.notifyTaskUpdate(ctx, task.WorkspaceID, task.ID)
		if r.store != nil {
			_, _ = r.store.PublishTaskDispatchDLQ(ctx, map[string]any{
				types.OrchestrationOutboxPayloadTaskID:          task.ID,
				types.OrchestrationOutboxPayloadReason:          reason,
				types.OrchestrationOutboxPayloadRetryDelay:      int(delay.Milliseconds()),
				types.OrchestrationOutboxPayloadDispatchAttempt: retryAttempt,
			})
		}
		return nil
	}

	guardKey := fmt.Sprintf("dispatch_retry:%s:%d", task.ID, nextAttempt)
	acquired, err := r.backend.AcquireOrchestrationRetryGuard(ctx, guardKey)
	if err != nil {
		return err
	}
	if !acquired {
		return nil
	}

	if r.lifecycle != nil {
		if err := r.lifecycle.Queue(ctx, task.ID, task.TargetRunID); err != nil {
			return err
		}
	}
	r.notifyTaskUpdate(ctx, task.WorkspaceID, task.ID)

	if delay <= 0 {
		delay = computeDispatchRetryDelay(retryAttempt)
	}

	payload := cloneAnyMap(retryPayload)
	if payload == nil {
		payload = map[string]any{}
	}
	payload[types.OrchestrationOutboxPayloadTaskID] = task.ID
	payload[types.OrchestrationOutboxPayloadReason] = reason
	payload[types.OrchestrationOutboxPayloadRetryDelay] = int(delay.Milliseconds())
	payload[types.OrchestrationOutboxPayloadDispatchAttempt] = nextAttempt

	return r.backend.EnqueueOrchestrationOutboxEvent(ctx, &types.OrchestrationOutboxEvent{
		EventType:   types.OrchestrationOutboxEventTypeTaskDispatch,
		DedupeKey:   guardKey,
		PayloadJSON: payload,
		AvailableAt: time.Now().Add(delay),
	})
}

func (r *RuntimeLoops) handleExecutionTask(ctx context.Context, task *types.AgentTask) error {
	runPolicy := runPolicyFromPayload(task.PayloadJSON)
	instanceKey := instanceKeyFromPayload(task.WorkspaceID, task.AgentID, task.PayloadJSON, runPolicy)

	if _, err := r.instanceController.EnsureInstance(ctx, ExecutionInstanceConfig{
		InstanceKey:            instanceKey,
		WorkspaceID:            task.WorkspaceID,
		AgentID:                task.AgentID,
		Lane:                   nil,
		ExecutionClassKey:      strings.TrimPrefix(instanceKey, "execclass_"),
		FailedAttemptThreshold: 5,
		InstanceLockKey:        common.Keys.AgentInstanceLock(instanceKey),
	}); err != nil {
		return err
	}

	desiredDispatch := 1
	var runningAttempts int
	hasInstanceState := false
	if instance, err := r.backend.GetExecutionInstanceByKey(ctx, instanceKey); err == nil {
		hasInstanceState = true
		runningAttempts = instance.RunningAttempts
		if instance.DesiredDispatchConcurrency > 0 {
			desiredDispatch = instance.DesiredDispatchConcurrency
		}
	}

	if err := r.instanceController.RouteDispatchTarget(ctx, instanceKey, desiredDispatch); err != nil {
		log.Warn().Err(err).Str("instance_key", instanceKey).Int("dispatch_target", desiredDispatch).Msg("failed to route dispatch target")
	}
	if hasInstanceState && runningAttempts >= desiredDispatch {
		return &dispatchRetryRequest{
			reason: "dispatch_capacity",
			delay:  dispatchCapacityRequeueDelay,
		}
	}

	run, runPolicy, prompt, err := r.runFactory.MaterializeRun(ctx, task)
	if err != nil {
		if isSessionBusyError(err) {
			return &dispatchRetryRequest{
				reason: "session_busy",
				delay:  sessionBusyRequeueDelay,
			}
		}
		reason := types.AgentTaskDropReasonRunMaterializationFail
		if r.lifecycle != nil {
			_ = r.lifecycle.Drop(ctx, task.ID, reason)
		}
		r.notifyTaskUpdate(ctx, task.WorkspaceID, task.ID)
		return nil
	}

	_, err = r.runFactory.CreateAttemptExecutionTask(
		ctx,
		run,
		runPolicy,
		prompt,
		r.runFactory.ResolveRunAgentConfig(ctx, run, task.PayloadJSON),
		task.PayloadJSON,
	)
	return err
}

func (r *RuntimeLoops) processRunResultMessages(ctx context.Context, messages []redislib.XMessage) error {
	for _, message := range messages {
		if err := r.processRunResultMessage(ctx, message); err != nil {
			return err
		}
	}
	return nil
}

func (r *RuntimeLoops) processRunResultMessage(ctx context.Context, message redislib.XMessage) error {
	result := parseRunResultEnvelope(message.Values)
	if result.TaskID == "" || result.AttemptID == "" {
		_ = r.store.AckRunResults(ctx, message.ID)
		return nil
	}
	if result.ResultKey == "" {
		result.ResultKey = fmt.Sprintf("run_result:%s:%s", result.TaskID, result.AttemptID)
	}

	if err := r.applyRunResult(ctx, result); err != nil {
		if r.store != nil {
			_, _ = r.store.PublishRunResultDLQ(ctx, map[string]any{
				types.OrchestrationOutboxPayloadTaskID:          result.TaskID,
				types.OrchestrationOutboxPayloadAttemptID:       result.AttemptID,
				types.OrchestrationOutboxPayloadExitCode:        result.ExitCode,
				types.OrchestrationOutboxPayloadError:           result.ErrorText,
				types.OrchestrationOutboxPayloadReason:          err.Error(),
				types.OrchestrationOutboxPayloadDispatchAttempt: result.RetryAttempt,
				types.OrchestrationOutboxPayloadIdempotency:     result.ResultKey,
			})
		}
		_ = r.store.AckRunResults(ctx, message.ID)
		return nil
	}

	_, _ = r.backend.AcquireOrchestrationResultInbox(ctx, result.ResultKey, message.ID)
	_ = r.store.AckRunResults(ctx, message.ID)
	return nil
}

func (r *RuntimeLoops) applyRunResult(ctx context.Context, result RunResultEnvelope) error {
	attempt, err := r.backend.GetRunAttemptByExecutionID(ctx, result.TaskID)
	if err != nil {
		if isRunAttemptNotFound(err) {
			return nil
		}
		return err
	}
	if attempt == nil || strings.TrimSpace(attempt.ID) != result.AttemptID {
		return nil
	}
	applied, err := r.backend.SetRunExecutionResultForAttempt(
		ctx,
		result.TaskID,
		result.AttemptID,
		result.ExitCode,
		result.ErrorText,
	)
	if err != nil {
		return err
	}
	if !applied || !attempt.IsActive() {
		return nil
	}
	return r.finalizeRunAttempt(
		ctx,
		attempt,
		result.TaskID,
		result.ExitCode,
		result.ErrorText,
		result.WaitingForInput,
		result.Wake.signal(),
		result.SubtaskRequests,
	)
}

func (r *RuntimeLoops) finalizeRunAttempt(
	ctx context.Context,
	attempt *types.AgentRunAttempt,
	taskID string,
	exitCode int,
	errText string,
	waitingForInput bool,
	wakeSignal *types.RunExecutionWakeSignal,
	subtaskReqs []*types.SubtaskRequest,
) error {
	if r.backend == nil || attempt == nil {
		return nil
	}
	if !attempt.IsActive() {
		return nil
	}

	now := time.Now()
	attemptStatus, runStatus, errMsg := types.ClassifyExecutionOutcome(exitCode, errText)

	if err := r.backend.UpdateAgentRunAttemptResult(ctx, attempt.ID, attemptStatus, &exitCode, now, errMsg); err != nil {
		return fmt.Errorf("update run attempt result: %w", err)
	}
	if err := r.backend.ClearAgentRunClaim(ctx, attempt.RunID); err != nil {
		log.Warn().Err(err).Str("run_id", attempt.RunID).Msg("failed to clear run claim lease during finalization")
	}
	if err := r.updateExecutionInstanceCounts(ctx, attempt.RunID, -1); err != nil {
		log.Warn().Err(err).Str("run_id", attempt.RunID).Msg("failed to decrement execution instance counters during finalization")
	}
	payload := map[string]any{
		types.AgentRunEventPayloadKeyAttemptID: attempt.ID,
		types.AgentRunEventPayloadKeyTaskID:    taskID,
		types.AgentRunEventPayloadKeyExitCode:  exitCode,
		types.AgentRunEventPayloadKeyError:     errText,
		types.AgentRunEventPayloadKeyEvent:     string(types.AgentRunEventFinished),
	}
	if err := r.backend.UpdateAgentRunLifecycle(ctx, attempt.RunID, runStatus, nil, &now, errMsg); err != nil {
		return fmt.Errorf("update run lifecycle: %w", err)
	}
	if err := appendRunSnapshotWithBackend(ctx, r.backend, attempt.RunID, runStatus, nil, &now, errMsg, payload); err != nil {
		return fmt.Errorf("append completion snapshot: %w", err)
	}
	if err := r.settleOriginTask(ctx, attempt.RunID, waitingForInput, wakeSignal, subtaskReqs); err != nil {
		return fmt.Errorf("settle origin task: %w", err)
	}
	return nil
}

func (r *RuntimeLoops) settleOriginTask(ctx context.Context, runID string, waitingForInput bool, wakeSignal *types.RunExecutionWakeSignal, subtaskReqs []*types.SubtaskRequest) error {
	settleWake := wakeSignal
	if len(subtaskReqs) > 0 {
		settleWake = nil
	}
	if r.lifecycle != nil {
		if err := r.lifecycle.Settle(ctx, runID, &SettleOpts{
			WaitingForInput: waitingForInput,
			WakeSignal:      settleWake,
		}); err != nil {
			return err
		}
	}
	if len(subtaskReqs) == 0 || r.taskFlows == nil {
		return nil
	}

	run, err := r.backend.GetAgentRunByID(ctx, runID)
	if err != nil || run == nil {
		return fmt.Errorf("lookup run for subtask creation: %w", err)
	}
	task, err := r.backend.GetTaskByID(ctx, run.OriginTaskID)
	if err != nil || task == nil {
		return fmt.Errorf("lookup task for subtask creation: %w", err)
	}

	for _, req := range subtaskReqs {
		parentID := task.ID
		label := req.EntityLabel
		spawnedBy := types.AgentTaskSpawnedByFanOut
		child, _, err := r.taskFlows.AcceptAgentCommand(ctx, task.WorkspaceID, AgentCommandParams{
			Message:        req.Prompt,
			AgentID:        task.AgentID,
			SessionID:      uuid.NewString(),
			IdempotencyKey: uuid.NewString(),
			ParentTaskID:   &parentID,
			Label:          &label,
			SpawnedBy:      &spawnedBy,
			DispatchDelay:  time.Duration(req.WakeDelayMinutes) * time.Minute,
		})
		if err != nil {
			log.Warn().Err(err).Str("entity", label).Msg("failed to create subtask")
			continue
		}
		if err := r.backend.CreateSpawnBinding(ctx, child.ID, req.SourceOutputID, label); err != nil {
			log.Warn().Err(err).Str("child_id", child.ID).Msg("spawn binding failed")
		}
		log.Info().Str("parent", task.ID).Str("child", child.ID).Str("entity", label).Msg("subtask created")
	}
	return nil
}

func (r *RuntimeLoops) updateExecutionInstanceCounts(ctx context.Context, runID string, runningDelta int) error {
	run, err := r.backend.GetAgentRunByID(ctx, runID)
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
	return r.backend.AdjustExecutionInstanceRunningAttempts(ctx, instanceKey, runningDelta, &now)
}

func (r *RuntimeLoops) notifyTaskUpdate(ctx context.Context, workspaceID uint, taskID string) {
	if r.publishTaskUpdate != nil {
		r.publishTaskUpdate(ctx, workspaceID, taskID)
	}
}
