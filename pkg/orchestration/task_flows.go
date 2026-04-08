package orchestration

import (
	"context"
	"encoding/json"
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

// ToolExecutor runs a deferred tool call server-side (after user approval)
// and tracks rejections so the write gate can auto-fail retries.
type ToolExecutor interface {
	ExecuteDeferred(ctx context.Context, req types.DeferredToolExecutionRequest) (stdout, stderr string, exitCode int, err error)
	RecordToolRejection(ctx context.Context, taskID, tool, command string) error
	GrantWritePreapproval(ctx context.Context, taskID string) error
}

type TaskFlows struct {
	backend           repository.BackendRepository
	terminalIO        repository.TerminalIORepository
	s2                *common.S2Client
	lifecycle         *TaskLifecycle
	toolExecutor      ToolExecutor
	publishTaskUpdate func(context.Context, uint, string)
	resolveRunState   func(context.Context, *types.AgentRun) (*types.RunInteraction, error)
}

func NewTaskFlows(
	backend repository.BackendRepository,
	terminalIO repository.TerminalIORepository,
	s2 *common.S2Client,
	lifecycle *TaskLifecycle,
	publishTaskUpdate func(context.Context, uint, string),
	resolveRunState func(context.Context, *types.AgentRun) (*types.RunInteraction, error),
) *TaskFlows {
	return &TaskFlows{
		backend:           backend,
		terminalIO:        terminalIO,
		s2:                s2,
		lifecycle:         lifecycle,
		publishTaskUpdate: publishTaskUpdate,
		resolveRunState:   resolveRunState,
	}
}

// SetToolExecutor enables server-side execution of deferred tool calls on approval.
func (f *TaskFlows) SetToolExecutor(executor ToolExecutor) {
	if f != nil {
		f.toolExecutor = executor
	}
}

// maybeExecuteDeferredToolCall checks whether the current blocker holds a
// deferred tool call (created by the gateway write gate) and handles
// approval / rejection. For approvals it executes the tool server-side and
// returns the result. For rejections it returns a clear message telling the
// agent not to retry. Returns "" for non-tool-call blockers so the normal
// blocker flow is unaffected.
//
// userMessage must be the raw user input BEFORE any defaults are applied.
// Empty string means the user provided no feedback (plain approve/reject).
func (f *TaskFlows) maybeExecuteDeferredToolCall(
	ctx context.Context,
	task *types.AgentTask,
	action *types.TaskInputAction,
	userMessage string,
) string {
	blocker := task.CurrentBlocker
	if blocker == nil || blocker.PayloadJSON == nil {
		return ""
	}
	tcRaw, ok := blocker.PayloadJSON["tool_call"]
	if !ok {
		return ""
	}

	tcBytes, err := json.Marshal(tcRaw)
	if err != nil {
		return ""
	}
	var tc struct {
		Tool        string   `json:"tool"`
		Args        []string `json:"args"`
		WorkspaceID uint     `json:"workspace_id"`
		MemberID    uint     `json:"member_id"`
		Summary     string   `json:"summary"`
	}
	if json.Unmarshal(tcBytes, &tc) != nil || tc.Tool == "" {
		return ""
	}

	summary := tc.Summary
	if summary == "" {
		summary = tc.Tool
	}

	if action == nil {
		return ""
	}

	if *action == types.TaskInputActionReject {
		hasFeedback := strings.TrimSpace(userMessage) != ""
		if !hasFeedback {
			command := ""
			if len(tc.Args) > 0 {
				command = tc.Args[0]
			}
			if f.toolExecutor != nil && command != "" {
				_ = f.toolExecutor.RecordToolRejection(ctx, task.ID, tc.Tool, command)
			}
			return fmt.Sprintf(
				"The user rejected your request to %s.\nDo not retry this exact action. Acknowledge the rejection and decide how to proceed.",
				summary,
			)
		}
		return fmt.Sprintf(
			"The user rejected your request to %s and provided feedback below. Revise the action based on their feedback and try again.",
			summary,
		)
	}
	if *action != types.TaskInputActionApprove {
		return ""
	}

	if f.toolExecutor == nil {
		return fmt.Sprintf(
			"The user approved your request to %s, but the server could not execute it. Please retry the tool call directly.",
			summary,
		)
	}

	log.Info().
		Str("task_id", task.ID).
		Str("tool", tc.Tool).
		Msg("executing deferred tool call after approval")

	stdout, stderr, exitCode, execErr := f.toolExecutor.ExecuteDeferred(ctx, types.DeferredToolExecutionRequest{
		Task:        task,
		WorkspaceID: tc.WorkspaceID,
		MemberID:    tc.MemberID,
		ToolName:    tc.Tool,
		Args:        tc.Args,
	})

	var b strings.Builder
	fmt.Fprintf(&b, "The user approved your request to %s. The action has ALREADY been executed on the server — do NOT call the same command again.\n\n", summary)
	fmt.Fprintf(&b, "Tool: %s\nExit code: %d\n", tc.Tool, exitCode)
	if out := strings.TrimSpace(stdout); out != "" {
		fmt.Fprintf(&b, "Output:\n%s\n", out)
	}
	if se := strings.TrimSpace(stderr); se != "" {
		fmt.Fprintf(&b, "Stderr:\n%s\n", se)
	}
	if execErr != nil {
		fmt.Fprintf(&b, "Error: %s\n", execErr.Error())
	}
	b.WriteString("\nProceed with your next steps (e.g. updating rows). Do not retry the above tool call.")
	return b.String()
}

type taskInputDelivery struct {
	run          *types.AgentRun
	executionID  string
	shouldResume bool
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

	// Handle deferred tool calls BEFORE applying default messages, so we
	// can distinguish "user typed nothing" from "user provided feedback"
	// by checking whether message is empty — no string-matching hacks.
	toolCallResult := f.maybeExecuteDeferredToolCall(ctx, task, action, message)
	grantPreapproval := false
	if toolCallResult != "" {
		if action != nil && *action == types.TaskInputActionReject && strings.TrimSpace(message) != "" {
			message = toolCallResult + "\n\n" + message
		} else {
			message = toolCallResult
		}
		// Grant pre-approval after executing a deferred tool call so that
		// if the agent retries the same write (it saw exit code 1 originally),
		// the retry passes through without creating another blocker.
		if action != nil && *action == types.TaskInputActionApprove && f.toolExecutor != nil {
			grantPreapproval = true
		}
	} else if action != nil && *action == types.TaskInputActionApprove && f.toolExecutor != nil {
		grantPreapproval = true
	}

	if strings.TrimSpace(message) == "" && action != nil {
		switch *action {
		case types.TaskInputActionApprove:
			message = "Approved. Proceed immediately — execute the pending action now."
		case types.TaskInputActionReject:
			message = "Rejected. Please revise."
		}
	}
	if strings.TrimSpace(message) == "" {
		return nil, fmt.Errorf("message is required")
	}
	if action != nil && *action == types.TaskInputActionApprove {
		message = f.rewriteApprovalApproveMessage(ctx, workspaceID, task, message)
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
	if shouldTreatInputAsApprovalRevision(task, kind, action, idempotencyKey) {
		message = f.rewriteApprovalRevisionMessage(ctx, workspaceID, task, message)
	}

	if shouldSupersedePendingApprovalOutputs(task, kind, action, idempotencyKey) {
		if err := f.supersedePendingApprovalOutputs(ctx, workspaceID, task); err != nil {
			return nil, err
		}
	}
	resolution := taskBlockerResolutionForInput(task, kind, action, message, items, idempotencyKey)

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

	if grantPreapproval {
		_ = f.toolExecutor.GrantWritePreapproval(ctx, task.ID)
	}

	f.persistUserInputLog(ctx, task, message)

	if err := f.deliverTaskInput(ctx, task); err != nil {
		log.Warn().Err(err).Str("task_id", taskID).Msg("task input delivery failed (input is durable, will be retried)")
	}

	f.notifyTaskUpdate(ctx, task.WorkspaceID, task.ID)

	updated, uerr := f.backend.GetTask(ctx, workspaceID, taskID)
	if uerr == nil {
		return updated, nil
	}
	return task, nil
}

func (f *TaskFlows) rewriteApprovalRevisionMessage(
	ctx context.Context,
	workspaceID uint,
	task *types.AgentTask,
	userMessage string,
) string {
	userMessage = strings.TrimSpace(userMessage)
	if f == nil || f.backend == nil || task == nil {
		return userMessage
	}

	var titles []string
	outputs, err := f.backend.ListTaskOutputs(ctx, workspaceID, task.ID)
	if err == nil {
		selected := pendingOutputsForCurrentBlocker(task, outputs)
		for _, output := range selected {
			if output == nil {
				continue
			}
			title := strings.TrimSpace(output.Title)
			if title != "" {
				titles = append(titles, title)
			}
		}
	}

	var parts []string
	parts = append(parts,
		"Revision requested for the pending approval item. Update the proposed work to reflect the feedback below. Do not execute, deliver, publish, apply, or finalize it yet. Return an updated version for approval unless the user explicitly approves proceeding.",
	)
	if len(titles) > 0 {
		var lines strings.Builder
		lines.WriteString("Current approval item")
		if len(titles) > 1 {
			lines.WriteString("s")
		}
		lines.WriteString(":\n")
		for i, title := range titles {
			lines.WriteString(fmt.Sprintf("%d. %s\n", i+1, title))
		}
		parts = append(parts, strings.TrimSpace(lines.String()))
	}
	if userMessage != "" {
		parts = append(parts, "User feedback:\n"+userMessage)
	}
	return strings.Join(parts, "\n\n")
}

func (f *TaskFlows) rewriteApprovalApproveMessage(
	ctx context.Context,
	workspaceID uint,
	task *types.AgentTask,
	userMessage string,
) string {
	userMessage = strings.TrimSpace(userMessage)
	if f == nil || f.backend == nil || task == nil {
		return userMessage
	}

	outputs, err := f.backend.ListTaskOutputs(ctx, workspaceID, task.ID)
	if err != nil {
		return userMessage
	}
	selected := pendingOutputsForCurrentBlocker(task, outputs)
	if len(selected) == 0 {
		return userMessage
	}

	instructions := make([]string, 0, len(selected))
	for _, output := range selected {
		if output == nil || !output.IsDraftEmail() {
			continue
		}
		draftID := strings.TrimSpace(output.DataString("draft_id", "draftId"))
		threadID := strings.TrimSpace(output.DataString("thread_id", "threadId"))
		recipient := strings.TrimSpace(output.DataString("to", "recipient", "recipient_email"))
		subject := strings.TrimSpace(output.DataString("subject"))
		if draftID == "" {
			continue
		}
		var parts []string
		parts = append(parts, "Send the existing Gmail draft that was already created")
		if recipient != "" {
			parts = append(parts, fmt.Sprintf("to %s", recipient))
		}
		if subject != "" {
			parts = append(parts, fmt.Sprintf("subject %q", subject))
		}
		if draftID != "" {
			parts = append(parts, fmt.Sprintf("(draft_id=%s)", draftID))
		}
		if threadID != "" {
			parts = append(parts, fmt.Sprintf("on thread_id=%s", threadID))
		}
		parts = append(parts, "Do not compose a new email or create a fresh thread; send the approved draft instead.")
		instructions = append(instructions, strings.Join(parts, " "))
	}
	if len(instructions) == 0 {
		return userMessage
	}

	extra := strings.Join(instructions, "\n")
	if userMessage == "" {
		return extra
	}
	lower := strings.ToLower(userMessage)
	if strings.Contains(lower, "draft_id=") || strings.Contains(lower, "existing gmail draft") {
		return userMessage
	}
	return userMessage + "\n\n" + extra
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
			targetStatus:   types.TaskOutputStatusRejected,
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
	if len(buckets["Approved"]) > 0 && len(buckets["Rejected"]) == 0 {
		parts = append(parts, "Proceed immediately — execute the approved action(s) now. Do not re-create or re-describe them.")
	}
	if userMessage != "" {
		parts = append(parts, userMessage)
	}
	if len(parts) == 0 {
		return "Approved. Proceed immediately — execute the pending action now.", nil
	}
	return strings.Join(parts, "\n"), nil
}

func (f *TaskFlows) deliverTaskInput(ctx context.Context, task *types.AgentTask) error {
	delivery, err := f.resolveTaskInputDelivery(ctx, task)
	if err != nil {
		return err
	}
	if delivery == nil || delivery.run == nil {
		if task.State.IsRetryable() {
			return f.wakeStoppedTask(ctx, task)
		}
		return nil
	}
	if delivery.shouldResume {
		return f.requeueTaskForResume(ctx, task, delivery.run)
	}
	if delivery.executionID == "" || f.terminalIO == nil {
		return nil
	}
	if freshRun, err := f.backend.GetAgentRun(ctx, task.WorkspaceID, *task.TargetRunID); err == nil && freshRun.Status.IsTerminal() {
		return f.requeueTaskForResume(ctx, task, freshRun)
	}
	// Commit state to running BEFORE waking the worker so Settle never
	// races against a stale waiting state.
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
	if err := f.terminalIO.PublishInputWake(ctx, delivery.executionID); err != nil {
		return err
	}
	return nil
}

func (f *TaskFlows) requeueTaskForResume(ctx context.Context, task *types.AgentTask, lastRun *types.AgentRun) error {
	inputMessage := ""
	consumePendingInput := false
	if pendingInputs, err := f.backend.ListPendingTaskInputs(ctx, task.ID, 1); err != nil {
		return err
	} else if len(pendingInputs) > 0 && pendingInputs[0] != nil {
		inputMessage = strings.TrimSpace(pendingInputs[0].Message)
		consumePendingInput = true
	}
	nextPayload := restartTaskPayload(task.PayloadJSON, lastRun, inputMessage)
	requeueTask := *task
	requeueTask.PayloadJSON = nextPayload
	requeued, err := f.backend.RequeueTaskWithOutboxIfCurrentRun(
		ctx,
		&requeueTask,
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
		log.Debug().Str("task_id", task.ID).Str("run_id", lastRun.ID).Msg("task requeue CAS miss — already requeued or retried by another path")
		return nil
	}
	if consumePendingInput {
		if _, err := f.backend.ConsumeOldestPendingInput(ctx, task.ID); err != nil {
			return err
		}
	}
	task.PayloadJSON = nextPayload
	f.notifyTaskUpdate(ctx, task.WorkspaceID, task.ID)
	return nil
}

// wakeStoppedTask transitions a stopped task (error, dropped, or cancelled) back
// to queued and enqueues a dispatch that carries the pending user input as the
// prompt. This handles the case where target_run_id is nil (dispatch failed
// before a run was materialized, or the task was cancelled) so
// deliverTaskInput/requeueTaskForResume can't route to an existing run.
func (f *TaskFlows) wakeStoppedTask(ctx context.Context, task *types.AgentTask) error {
	if f.lifecycle == nil {
		return nil
	}

	inputMessage := ""
	consumePendingInput := false
	if pendingInputs, err := f.backend.ListPendingTaskInputs(ctx, task.ID, 1); err != nil {
		return err
	} else if len(pendingInputs) > 0 && pendingInputs[0] != nil {
		inputMessage = strings.TrimSpace(pendingInputs[0].Message)
		consumePendingInput = true
	}

	dispatchPayload := map[string]any{
		types.OrchestrationOutboxPayloadTaskID: task.ID,
	}
	if inputMessage != "" {
		dispatchPayload[types.OrchestrationOutboxPayloadDispatchPrompt] = inputMessage
	}

	// If we can find the last run, set up session resume so the agent
	// continues from where it left off rather than starting fresh.
	lastRun, _ := f.lastRunForTask(ctx, task)
	if lastRun != nil {
		dispatchPayload[types.OrchestrationOutboxPayloadResumeSession] = true
		dispatchPayload[types.OrchestrationOutboxPayloadResumeExcludeRunID] = lastRun.ID
		dispatchPayload[types.OrchestrationOutboxPayloadResumeCheckpointRunID] = lastRun.ID
	}

	if err := f.lifecycle.Queue(ctx, task.ID, nil); err != nil {
		return fmt.Errorf("transition stopped task to queued: %w", err)
	}

	if consumePendingInput {
		if _, err := f.backend.ConsumeOldestPendingInput(ctx, task.ID); err != nil {
			log.Warn().Err(err).Str("task_id", task.ID).Msg("failed to consume pending input after waking stopped task")
		}
	}

	dedupeKey := fmt.Sprintf("wake_stopped:%s:%d", task.ID, time.Now().UnixNano())
	if err := f.backend.EnqueueOrchestrationOutboxEvent(ctx, &types.OrchestrationOutboxEvent{
		EventType:   types.OrchestrationOutboxEventTypeTaskDispatch,
		DedupeKey:   dedupeKey,
		PayloadJSON: dispatchPayload,
	}); err != nil {
		return fmt.Errorf("enqueue wake dispatch: %w", err)
	}

	log.Info().Str("task_id", task.ID).Str("from_state", string(task.State)).Msg("woke stopped task after user input")
	f.notifyTaskUpdate(ctx, task.WorkspaceID, task.ID)
	return nil
}

func (f *TaskFlows) lastRunForTask(ctx context.Context, task *types.AgentTask) (*types.AgentRun, error) {
	runs, err := f.backend.ListAgentRunsFiltered(ctx, task.WorkspaceID, types.AgentRunListFilter{
		TaskID: &task.ID,
		Limit:  1,
	})
	if err != nil || len(runs) == 0 || runs[0] == nil {
		return nil, err
	}
	return runs[0], nil
}

func (f *TaskFlows) resolveTaskInputDelivery(ctx context.Context, task *types.AgentTask) (*taskInputDelivery, error) {
	if task == nil || task.TargetRunID == nil || strings.TrimSpace(*task.TargetRunID) == "" {
		return nil, nil
	}
	run, err := f.backend.GetAgentRun(ctx, task.WorkspaceID, *task.TargetRunID)
	if err != nil {
		return nil, err
	}
	delivery := &taskInputDelivery{run: run}
	if run.Status.IsTerminal() {
		delivery.shouldResume = true
		return delivery, nil
	}
	if f.resolveRunState != nil {
		interaction, err := f.resolveRunState(ctx, run)
		if err != nil {
			return nil, err
		}
		if interaction != nil {
			if interaction.State == types.RunInteractionStateClosed {
				delivery.shouldResume = true
				return delivery, nil
			}
			delivery.executionID = strings.TrimSpace(interaction.ActiveExecutionID)
		}
	}
	if delivery.executionID == "" {
		delivery.shouldResume = true
		return delivery, nil
	}
	if f.terminalIO == nil || strings.TrimSpace(run.SessionID) == "" {
		return delivery, nil
	}
	owner, err := f.terminalIO.GetSessionLeaseOwner(ctx, run.WorkspaceID, run.SessionID)
	if err != nil {
		return nil, err
	}
	if owner != "" && ReconcileStaleSessionLease(ctx, f.backend, f.terminalIO, run.WorkspaceID, run.SessionID, owner) {
		owner = ""
	}
	leaseExecutionID := strings.TrimSpace(ExtractLeaseExecutionID(owner))
	if owner == "" || leaseExecutionID == "" {
		delivery.shouldResume = true
		return delivery, nil
	}
	if leaseExecutionID != delivery.executionID {
		log.Warn().
			Str("task_id", task.ID).
			Str("run_id", run.ID).
			Str("lease_execution_id", leaseExecutionID).
			Str("active_execution_id", delivery.executionID).
			Msg("requeueing follow-up input for stale run execution")
		delivery.shouldResume = true
	}
	return delivery, nil
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
	}
}

func (f *TaskFlows) notifyTaskUpdate(ctx context.Context, workspaceID uint, taskID string) {
	if f.publishTaskUpdate != nil {
		f.publishTaskUpdate(ctx, workspaceID, taskID)
	}
}
