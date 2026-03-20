package orchestration

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/beam-cloud/airstore/pkg/common"
	"github.com/beam-cloud/airstore/pkg/repository"
	"github.com/beam-cloud/airstore/pkg/types"
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
)

type TaskFlows struct {
	backend           repository.BackendRepository
	terminalIO        repository.TerminalIORepository
	s2                *common.S2Client
	lifecycle         *TaskLifecycle
	publishTaskUpdate func(context.Context, uint, string)
	resolveRunState   func(context.Context, *types.AgentRun) (*types.RunInteraction, error)
	activeExecutionID func(context.Context, string) (string, bool, error)
}

func NewTaskFlows(
	backend repository.BackendRepository,
	terminalIO repository.TerminalIORepository,
	s2 *common.S2Client,
	lifecycle *TaskLifecycle,
	publishTaskUpdate func(context.Context, uint, string),
	resolveRunState func(context.Context, *types.AgentRun) (*types.RunInteraction, error),
	activeExecutionID func(context.Context, string) (string, bool, error),
) *TaskFlows {
	return &TaskFlows{
		backend:           backend,
		terminalIO:        terminalIO,
		s2:                s2,
		lifecycle:         lifecycle,
		publishTaskUpdate: publishTaskUpdate,
		resolveRunState:   resolveRunState,
		activeExecutionID: activeExecutionID,
	}
}

func (f *TaskFlows) AcceptAgentCommand(
	ctx context.Context,
	workspaceID uint,
	params AgentCommandParams,
) (*types.AgentTask, bool, error) {
	normalizeAgentCommandDefaults(&params)
	if err := ValidateAgentCommandParams(&params); err != nil {
		return nil, false, err
	}
	agentConfig := map[string]any{}
	agentProvider := ""
	agentModel := ""
	if params.AgentID != nil {
		agentID := strings.TrimSpace(*params.AgentID)
		if agentID == "" {
			return nil, false, fmt.Errorf("agent_id must not be empty")
		}
		profile, err := f.backend.GetAgentProfile(ctx, workspaceID, agentID)
		if err != nil {
			return nil, false, err
		}
		resolved := profile.ID
		params.AgentID = &resolved
		agentConfig, err = normalizeAgentProfileConfig(profile.ConfigJSON, profile.AgentKey)
		if err != nil {
			return nil, false, err
		}
		agentProvider = providerFromAgentConfig(agentConfig)
		if !isSupportedProvider(agentProvider) {
			return nil, false, fmt.Errorf("agent provider %q is not supported", agentProvider)
		}
		agentModel = agentConfigString(agentConfig, agentConfigKeyModel)
	}

	existing, err := f.backend.GetTaskByIdempotency(ctx, workspaceID, params.AgentID, params.IdempotencyKey)
	if err == nil {
		return existing, true, nil
	}

	runPolicy := DefaultRunExecutionPolicy()
	if params.Policy != nil {
		runPolicy = NormalizeRunExecutionPolicy(*params.Policy)
	}
	instanceKey := ExecutionClassKey(workspaceID, params.AgentID, params.Lane, runPolicy)

	if params.HookID == nil {
		latestRun, err := f.latestRunForSessionAgent(ctx, workspaceID, params.AgentID, params.SessionID)
		if err != nil {
			return nil, false, err
		}
		if latestRun != nil {
			task, deduped, _, err := f.AcceptRunInput(
				ctx,
				workspaceID,
				latestRun.ID,
				types.AgentQueueModeFollowup,
				params.Message,
				params.IdempotencyKey,
			)
			if err != nil {
				return nil, false, err
			}
			return task, deduped, nil
		}
	}

	priority := params.Priority
	if strings.TrimSpace(priority) == "" {
		priority = "normal"
	}
	payload := newTaskCommandPayload(params, runPolicy, instanceKey, agentConfig, agentProvider, agentModel)
	payload.Priority = priority

	taskState := types.AgentTaskStateQueued
	var wakeAt *time.Time
	var wakeReason *string
	var dispatch *types.OrchestrationOutboxEvent
	if params.DispatchDelay > 0 {
		scheduledAt := time.Now().Add(params.DispatchDelay)
		taskState = types.AgentTaskStateSleeping
		wakeAt = &scheduledAt
		if reason := initialDelayedTaskWakeReason(params); reason != "" {
			wakeReason = &reason
		}
		dispatch = &types.OrchestrationOutboxEvent{
			AvailableAt: scheduledAt,
		}
	}

	task := &types.AgentTask{
		WorkspaceID:    workspaceID,
		AgentID:        params.AgentID,
		QueueMode:      types.AgentQueueModeQueue,
		State:          taskState,
		IdempotencyKey: params.IdempotencyKey,
		PayloadJSON:    payload.ToMap(),
		RoutingJSON:    routingToMap(params.Routing),
		ParentTaskID:   params.ParentTaskID,
		Priority:       priority,
		BudgetUSD:      params.BudgetUSD,
		WakeAt:         wakeAt,
		WakeReason:     wakeReason,
	}
	if err := f.backend.CreateTaskWithOutbox(ctx, task, dispatch); err != nil {
		if existing, lookupErr := f.backend.GetTaskByIdempotency(ctx, workspaceID, params.AgentID, params.IdempotencyKey); lookupErr == nil {
			return existing, true, nil
		}
		return nil, false, err
	}
	f.notifyTaskUpdate(ctx, task.WorkspaceID, task.ID)
	return task, false, nil
}

func initialDelayedTaskWakeReason(params AgentCommandParams) string {
	if params.Label != nil && strings.TrimSpace(*params.Label) != "" {
		return fmt.Sprintf("Follow up with %s", strings.TrimSpace(*params.Label))
	}
	return "Scheduled follow-up"
}

func (f *TaskFlows) AcceptTaskInput(
	ctx context.Context,
	workspaceID uint,
	taskID string,
	kind types.InputKind,
	action *types.TaskInputAction,
	message string,
	idempotencyKey string,
	items []types.ItemDecision,
) (*types.AgentTask, error) {
	if strings.TrimSpace(taskID) == "" {
		return nil, fmt.Errorf("task_id is required")
	}

	task, err := f.backend.GetTask(ctx, workspaceID, taskID)
	if err != nil {
		return nil, err
	}

	if len(items) == 0 && action != nil && (kind == "" || kind == types.InputKindApproveReject) {
		autoItems, err := f.pendingItemDecisions(ctx, workspaceID, task, *action)
		if err != nil {
			return nil, err
		}
		items = autoItems
	}
	if len(items) > 0 {
		processedMessage, err := f.processItemDecisions(ctx, workspaceID, taskID, items, message)
		if err != nil {
			return nil, err
		}
		message = processedMessage
		if action == nil {
			approve := types.TaskInputActionApprove
			action = &approve
		}
	}

	if strings.TrimSpace(message) == "" && action != nil {
		switch *action {
		case types.TaskInputActionApprove:
			message = "Approved. Please proceed."
		case types.TaskInputActionReject:
			message = "Rejected. Please revise."
		}
	}
	if strings.TrimSpace(message) == "" {
		return nil, fmt.Errorf("message is required")
	}
	if idempotencyKey == "" {
		idempotencyKey = uuid.NewString()
	}
	if kind == "" {
		if action != nil {
			kind = types.InputKindApproveReject
		} else {
			kind = types.InputKindFreeText
		}
	}

	if shouldSupersedePendingApprovalOutputs(task, kind, action) {
		if err := f.supersedePendingApprovalOutputs(ctx, workspaceID, task); err != nil {
			return nil, err
		}
	}
	resolution := taskBlockerResolutionForInput(task, kind, action, message, items)

	sessionID := ""
	if task.TargetRunID != nil {
		run, rerr := f.backend.GetAgentRun(ctx, workspaceID, *task.TargetRunID)
		if rerr == nil {
			sessionID = run.SessionID
		}
	}

	input := &types.TaskInput{
		WorkspaceID:    workspaceID,
		TaskID:         taskID,
		SessionID:      sessionID,
		Kind:           kind,
		Action:         action,
		Message:        message,
		IdempotencyKey: idempotencyKey,
	}
	if err := f.backend.AppendTaskInput(ctx, input); err != nil {
		return nil, fmt.Errorf("append task input: %w", err)
	}
	if resolution != nil {
		if _, err := f.backend.ResolveCurrentTaskBlocker(ctx, workspaceID, taskID, resolution); err != nil {
			log.Warn().Err(err).Str("task_id", taskID).Msg("failed to resolve task blocker after storing input")
		} else {
			task.CurrentBlocker = nil
			task.CurrentBlockerID = nil
			task.InputKind = ""
			task.WaitingSummary = nil
		}
	}

	f.persistUserInputLog(ctx, task, message)

	if err := f.deliverTaskInput(ctx, task); err != nil {
		log.Warn().Err(err).Str("task_id", taskID).Msg("task input delivery failed (input is durable, will be claimed on next wake)")
	}

	f.notifyTaskUpdate(ctx, task.WorkspaceID, task.ID)

	updated, uerr := f.backend.GetTask(ctx, workspaceID, taskID)
	if uerr == nil {
		return updated, nil
	}
	return task, nil
}

func (f *TaskFlows) AcceptRunInput(
	ctx context.Context,
	workspaceID uint,
	targetRunID string,
	queueMode types.AgentQueueMode,
	message string,
	idempotencyKey string,
) (*types.AgentTask, bool, types.RunInputDeliveryOutcome, error) {
	if strings.TrimSpace(message) == "" {
		return nil, false, "", fmt.Errorf("message is required")
	}
	idempotencyKey = normalizeGeneratedID(idempotencyKey)

	run, err := f.backend.GetAgentRun(ctx, workspaceID, targetRunID)
	if err != nil {
		return nil, false, "", err
	}
	if strings.TrimSpace(run.OriginTaskID) == "" {
		return nil, false, "", fmt.Errorf("run %s has no origin task", run.ID)
	}

	task, err := f.AcceptTaskInput(ctx, workspaceID, run.OriginTaskID, types.InputKindFreeText, nil, message, idempotencyKey, nil)
	if err != nil {
		return nil, false, "", err
	}
	return task, false, types.RunInputDeliveryQueued, nil
}

func (f *TaskFlows) RunPendingSweep(ctx context.Context) {
	ticker := time.NewTicker(pendingInputSweepInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := f.retryOrphanedPendingInputs(ctx); err != nil {
				log.Warn().Err(err).Msg("failed to recover orphaned pending task inputs")
			}
		}
	}
}

func (f *TaskFlows) latestRunForSessionAgent(
	ctx context.Context,
	workspaceID uint,
	agentID *string,
	sessionID string,
) (*types.AgentRun, error) {
	sessionID = strings.TrimSpace(sessionID)
	if f == nil || f.backend == nil || sessionID == "" {
		return nil, nil
	}
	agentID = trimOptionalString(agentID)

	matchesAgent := func(run *types.AgentRun) bool {
		if run == nil {
			return false
		}
		if agentID == nil || strings.TrimSpace(*agentID) == "" {
			return true
		}
		if run.AgentID == nil {
			return false
		}
		return strings.TrimSpace(*run.AgentID) == strings.TrimSpace(*agentID)
	}

	if f.terminalIO != nil {
		if owner, _ := f.terminalIO.GetSessionLeaseOwner(ctx, workspaceID, sessionID); owner != "" {
			if leaseRunID := ExtractLeaseExecutionID(owner); leaseRunID != "" {
				if leaseRun, err := f.backend.GetAgentRun(ctx, workspaceID, leaseRunID); err == nil && matchesAgent(leaseRun) {
					return leaseRun, nil
				}
			}
		}
	}

	baseFilter := types.AgentRunListFilter{
		AgentID:   agentID,
		SessionID: strPtr(sessionID),
		Limit:     1,
	}
	for _, statuses := range [][]types.AgentRunStatus{
		{types.AgentRunStatusRunning},
		{types.AgentRunStatusAccepted},
		nil,
	} {
		filter := baseFilter
		filter.Statuses = statuses
		runs, err := f.backend.ListAgentRunsFiltered(ctx, workspaceID, filter)
		if err != nil {
			return nil, err
		}
		for _, run := range runs {
			if run != nil {
				return run, nil
			}
		}
	}

	return nil, nil
}

func (f *TaskFlows) pendingItemDecisions(
	ctx context.Context,
	workspaceID uint,
	task *types.AgentTask,
	action types.TaskInputAction,
) ([]types.ItemDecision, error) {
	if task == nil {
		return nil, fmt.Errorf("task is required")
	}
	outputs, err := f.backend.ListTaskOutputs(ctx, workspaceID, task.ID)
	if err != nil {
		return nil, fmt.Errorf("list task outputs for %s: %w", task.ID, err)
	}
	selected := pendingOutputsForCurrentBlocker(task, outputs)
	sort.SliceStable(selected, func(i, j int) bool {
		if !selected[i].CreatedAt.Equal(selected[j].CreatedAt) {
			return selected[i].CreatedAt.Before(selected[j].CreatedAt)
		}
		return selected[i].ID < selected[j].ID
	})
	items := make([]types.ItemDecision, 0, len(outputs))
	for _, output := range selected {
		items = append(items, types.ItemDecision{
			OutputID: output.ID,
			Action:   action,
		})
	}
	return items, nil
}

func (f *TaskFlows) supersedePendingApprovalOutputs(ctx context.Context, workspaceID uint, task *types.AgentTask) error {
	if task == nil {
		return nil
	}
	outputs, err := f.backend.ListTaskOutputs(ctx, workspaceID, task.ID)
	if err != nil {
		return fmt.Errorf("list task outputs for %s: %w", task.ID, err)
	}
	selected := pendingApprovalOutputs(outputs)
	if task.CurrentBlocker != nil && task.CurrentBlocker.Status == types.TaskBlockerStatusOpen {
		selected = pendingOutputsForCurrentBlocker(task, outputs)
	}
	if len(selected) == 0 {
		return nil
	}
	updates := make([]taskOutputStatusUpdate, 0, len(selected))
	for _, output := range selected {
		updates = append(updates, taskOutputStatusUpdate{
			output:         output,
			originalStatus: output.Status,
			targetStatus:   types.TaskOutputStatusCancelled,
		})
	}
	return applyOutputStatusUpdates(ctx, workspaceID, f.backend, updates)
}

func (f *TaskFlows) processItemDecisions(ctx context.Context, workspaceID uint, taskID string, items []types.ItemDecision, userMessage string) (string, error) {
	buckets := map[string][]string{"Approved": nil, "Rejected": nil}
	updates := make([]itemDecisionUpdate, 0, len(items))
	for _, item := range items {
		outputID := strings.TrimSpace(item.OutputID)
		if outputID == "" {
			return "", &types.ErrInvalidTaskInput{Message: "item output_id is required"}
		}

		targetStatus, err := taskOutputStatusForDecision(item)
		if err != nil {
			return "", err
		}

		output, err := f.backend.GetTaskOutput(ctx, workspaceID, outputID)
		if err != nil {
			var notFound *types.ErrTaskOutputNotFound
			if errors.As(err, &notFound) {
				return "", &types.ErrInvalidTaskInput{Message: fmt.Sprintf("item output %s not found", outputID)}
			}
			return "", fmt.Errorf("get task output %s: %w", outputID, err)
		}
		if output.TaskID != taskID {
			return "", &types.ErrInvalidTaskInput{
				Message: fmt.Sprintf("item output %s does not belong to task %s", outputID, taskID),
			}
		}

		updates = append(updates, itemDecisionUpdate{
			item: item,
			taskOutputStatusUpdate: taskOutputStatusUpdate{
				output:         output,
				originalStatus: output.Status,
				targetStatus:   targetStatus,
			},
		})
	}
	statusUpdates := make([]taskOutputStatusUpdate, 0, len(updates))
	for _, update := range updates {
		statusUpdates = append(statusUpdates, update.taskOutputStatusUpdate)
	}
	if err := applyOutputStatusUpdates(ctx, workspaceID, f.backend, statusUpdates); err != nil {
		return "", err
	}

	for _, update := range updates {
		label := update.output.Title
		if update.item.Action == types.TaskInputActionReject && update.item.Reason != "" {
			label += " — " + update.item.Reason
		}
		bucket := "Approved"
		if update.item.Action == types.TaskInputActionReject {
			bucket = "Rejected"
		}
		buckets[bucket] = append(buckets[bucket], label)
	}

	var parts []string
	for _, header := range []string{"Approved", "Rejected"} {
		if list := buckets[header]; len(list) > 0 {
			lines := header + ":\n"
			for i, l := range list {
				lines += fmt.Sprintf("%d. %s\n", i+1, l)
			}
			parts = append(parts, lines)
		}
	}
	if userMessage != "" {
		parts = append(parts, userMessage)
	}
	if len(parts) == 0 {
		return "Approved. Please proceed.", nil
	}
	return strings.Join(parts, "\n"), nil
}

func (f *TaskFlows) deliverTaskInput(ctx context.Context, task *types.AgentTask) error {
	if task.TargetRunID == nil || strings.TrimSpace(*task.TargetRunID) == "" {
		return nil
	}

	run, err := f.backend.GetAgentRun(ctx, task.WorkspaceID, *task.TargetRunID)
	if err != nil {
		return err
	}

	if run.Status.IsTerminal() {
		return f.requeueTaskForResume(ctx, task, run)
	}

	if f.resolveRunState != nil {
		interaction, _ := f.resolveRunState(ctx, run)
		if interaction != nil && interaction.State == types.RunInteractionStateClosed {
			return f.requeueTaskForResume(ctx, task, run)
		}
	}

	if f.activeExecutionID == nil {
		return nil
	}
	execID, hasActiveAttempt, err := f.activeExecutionID(ctx, run.ID)
	if err != nil {
		return err
	}
	if !hasActiveAttempt {
		return f.requeueTaskForResume(ctx, task, run)
	}
	if execID != "" && f.terminalIO != nil {
		if err := f.terminalIO.PublishInputWake(ctx, execID); err != nil {
			return err
		}
		if freshRun, err := f.backend.GetAgentRun(ctx, task.WorkspaceID, *task.TargetRunID); err == nil && freshRun.Status.IsTerminal() {
			return f.requeueTaskForResume(ctx, task, freshRun)
		}
		if f.lifecycle != nil {
			updated, err := f.lifecycle.Resume(ctx, task.ID, *task.TargetRunID)
			if err != nil {
				return err
			}
			if updated {
				task.State = types.AgentTaskStateRunning
				task.InputKind = ""
				task.WaitingSummary = nil
				task.CurrentBlocker = nil
				task.CurrentBlockerID = nil
			}
		}
		f.notifyTaskUpdate(ctx, task.WorkspaceID, task.ID)
		return nil
	}
	return nil
}

func (f *TaskFlows) requeueTaskForResume(ctx context.Context, task *types.AgentTask, lastRun *types.AgentRun) error {
	inputMessage, _ := f.backend.ConsumeOldestPendingInput(ctx, task.ID)
	task.PayloadJSON = restartTaskPayload(task.PayloadJSON, lastRun, inputMessage)
	requeued, err := f.backend.RequeueTaskWithOutboxIfCurrentRun(
		ctx,
		task,
		lastRun.ID,
		&types.OrchestrationOutboxEvent{
			EventType: types.OrchestrationOutboxEventTypeTaskDispatch,
			DedupeKey: fmt.Sprintf("task_dispatch:%s:input_resume:%s", task.ID, uuid.NewString()),
			PayloadJSON: map[string]any{
				types.OrchestrationOutboxPayloadTaskID: task.ID,
			},
		},
	)
	if err != nil {
		return err
	}
	if !requeued {
		return fmt.Errorf("task %s is no longer attached to run %s", task.ID, lastRun.ID)
	}
	f.notifyTaskUpdate(ctx, task.WorkspaceID, task.ID)
	return nil
}

func (f *TaskFlows) retryOrphanedPendingInputs(ctx context.Context) error {
	if f == nil || f.backend == nil {
		return nil
	}
	inputs, err := f.backend.ListOrphanedPendingInputs(ctx, pendingInputSweepMaxAge, pendingInputSweepBatch)
	if err != nil || len(inputs) == 0 {
		return err
	}

	seenTasks := make(map[string]struct{}, len(inputs))
	for _, input := range inputs {
		if input == nil {
			continue
		}
		taskID := strings.TrimSpace(input.TaskID)
		if taskID == "" {
			continue
		}
		if _, exists := seenTasks[taskID]; exists {
			continue
		}
		seenTasks[taskID] = struct{}{}

		task, err := f.backend.GetTask(ctx, input.WorkspaceID, taskID)
		if err != nil {
			log.Warn().Err(err).Str("task_id", taskID).Msg("failed to load task for orphaned input recovery")
			continue
		}
		if err := f.deliverTaskInput(ctx, task); err != nil {
			log.Warn().Err(err).Str("task_id", taskID).Msg("failed to redeliver orphaned task input")
		}
	}
	return nil
}

func (f *TaskFlows) persistUserInputLog(ctx context.Context, task *types.AgentTask, message string) {
	if f.s2 == nil || !f.s2.Enabled() {
		return
	}

	runID := ""
	if task.TargetRunID != nil {
		runID = strings.TrimSpace(*task.TargetRunID)
	}
	if runID == "" {
		runs, err := f.backend.ListAgentRunsFiltered(ctx, task.WorkspaceID, types.AgentRunListFilter{
			TaskID: &task.ID,
			Limit:  1,
		})
		if err != nil || len(runs) == 0 || runs[0] == nil {
			log.Warn().Str("task_id", task.ID).Msg("persistUserInputLog: no run found")
			return
		}
		runID = runs[0].ID
	}

	attempts, err := f.backend.ListAgentRunAttempts(ctx, runID)
	if err != nil {
		log.Warn().Err(err).Str("run_id", runID).Msg("persistUserInputLog: list attempts failed")
		return
	}
	execID := newestExecutionID(attempts)
	if execID == "" {
		log.Warn().Str("run_id", runID).Msg("persistUserInputLog: no execution found")
		return
	}
	entry := common.TaskLogEntry{
		TaskID:    execID,
		Timestamp: time.Now().UnixMilli(),
		Stream:    "user",
		Data:      message,
		ChunkType: "user_input",
	}
	if err := f.s2.Append(ctx, common.Streams.TaskLogs(execID), entry); err != nil {
		log.Warn().Err(err).Str("exec_id", execID).Msg("persistUserInputLog: S2 append failed")
	} else {
		log.Info().Str("task_id", task.ID).Str("exec_id", execID).Str("stream", common.Streams.TaskLogs(execID)).Int("msg_len", len(message)).Msg("persistUserInputLog: wrote user_input to S2")
	}
}

func (f *TaskFlows) notifyTaskUpdate(ctx context.Context, workspaceID uint, taskID string) {
	if f.publishTaskUpdate != nil {
		f.publishTaskUpdate(ctx, workspaceID, taskID)
	}
}
