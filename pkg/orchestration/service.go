package orchestration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/beam-cloud/airstore/pkg/common"
	"github.com/beam-cloud/airstore/pkg/repository"
	"github.com/beam-cloud/airstore/pkg/types"
	"github.com/google/uuid"
	redislib "github.com/redis/go-redis/v9"
	"github.com/rs/zerolog/log"
)

var (
	activeSkillsHeaderRE   = regexp.MustCompile(`(?m)^## (?:MANDATORY - )?Active Skills\s*$`)
	activeSkillReferenceRE = regexp.MustCompile(`(?m)(?:^\s*-\s+(/workspace/skills/[^\s/]+)(?:\s+--\s+(.*))?\s*$)|(?:^\s*\d+\.\s+cat\s+(/workspace/skills/[^\s/]+)(?:/SKILL\.md)?(?:\s+--\s+(.*))?\s*$)`)
)

const (
	pendingInputSweepInterval = 5 * time.Second
	pendingInputSweepMaxAge   = 10 * time.Second
	pendingInputSweepBatch    = 100
)

type AgentService struct {
	backend            repository.BackendRepository
	taskQueue          repository.TaskQueue
	orchestrationStore *repository.OrchestrationStore
	terminalIO         repository.TerminalIORepository
	s2                 *common.S2Client
	defaultImage       string
	dispatchConsumerID string
	resultConsumerID   string
	instanceController *ExecutionInstanceController
	lifecycle          *TaskLifecycle
}

func NewAgentService(
	ctx context.Context,
	backend repository.BackendRepository,
	taskQueue repository.TaskQueue,
	redis *common.RedisClient,
	s2 *common.S2Client,
	defaultImage string,
) *AgentService {
	orchestrationStore := repository.NewOrchestrationStore(backend, redis)
	var terminalIO repository.TerminalIORepository
	if redis != nil {
		terminalIO = repository.NewRedisTerminalIORepository(redis)
	}
	return &AgentService{
		backend:            backend,
		taskQueue:          taskQueue,
		orchestrationStore: orchestrationStore,
		terminalIO:         terminalIO,
		s2:                 s2,
		defaultImage:       defaultImage,
		dispatchConsumerID: "dispatch-" + uuid.NewString(),
		resultConsumerID:   "result-" + uuid.NewString(),
		instanceController: NewExecutionInstanceController(ctx, backend, orchestrationStore, common.Keys.AgentInstanceLock),
		lifecycle:          NewTaskLifecycle(backend, orchestrationStore, terminalIO),
	}
}

func (s *AgentService) Start(ctx context.Context) {
	if s.orchestrationStore != nil {
		if err := s.orchestrationStore.EnsureTaskDispatchGroup(ctx); err != nil {
			log.Warn().Err(err).Msg("failed to ensure orchestration task-dispatch stream group")
		}
		if err := s.orchestrationStore.EnsureRunResultGroup(ctx); err != nil {
			log.Warn().Err(err).Msg("failed to ensure orchestration run-result stream group")
		}
		go s.outboxPublisherLoop(ctx)
		go s.resultProjectorLoop(ctx)
	}
	if s.backend != nil {
		go s.pendingInputSweeperLoop(ctx)
	}
	go s.dispatchLoop(ctx)
}

func defaultRunInteractionState(run *types.AgentRun) types.RunInteractionState {
	if run == nil || run.Status.IsTerminal() {
		return types.RunInteractionStateClosed
	}
	return types.RunInteractionStateWorking
}

func normalizeRunInteractionState(state types.RunInteractionState, fallback types.RunInteractionState) types.RunInteractionState {
	switch state {
	case types.RunInteractionStateWorking, types.RunInteractionStateWaitingForInput, types.RunInteractionStateClosed:
		return state
	default:
		return fallback
	}
}

func (s *AgentService) GetRunInteraction(ctx context.Context, workspaceID uint, runID string) (*types.RunInteraction, error) {
	if strings.TrimSpace(runID) == "" {
		return nil, nil
	}
	run, err := s.backend.GetAgentRun(ctx, workspaceID, runID)
	if err != nil {
		return nil, err
	}
	return s.resolveRunInteraction(ctx, run)
}

func (s *AgentService) resolveRunInteraction(ctx context.Context, run *types.AgentRun) (*types.RunInteraction, error) {
	if run == nil {
		return nil, nil
	}
	interaction := &types.RunInteraction{
		State: defaultRunInteractionState(run),
	}
	if run.Status.IsTerminal() {
		return interaction, nil
	}

	if s.terminalIO != nil {
		stored, err := s.terminalIO.GetRunInteraction(ctx, run.WorkspaceID, run.ID)
		if err == nil && stored != nil {
			interaction.State = normalizeRunInteractionState(stored.State, interaction.State)
			interaction.ActiveExecutionID = strings.TrimSpace(stored.ActiveExecutionID)
			interaction.UpdatedAt = stored.UpdatedAt
		}
	}

	if interaction.ActiveExecutionID == "" {
		execID, err := s.activeExecutionExternalID(ctx, run.ID)
		if err == nil {
			interaction.ActiveExecutionID = execID
		}
	}
	if interaction.ActiveExecutionID == "" && interaction.State == types.RunInteractionStateWaitingForInput {
		interaction.State = types.RunInteractionStateWorking
	}
	return interaction, nil
}

func newestInFlightAttempt(
	attempts []*types.AgentRunAttempt,
) (*types.AgentRunAttempt, string, bool) {
	for i := len(attempts) - 1; i >= 0; i-- {
		attempt := attempts[i]
		if attempt == nil || attempt.EndedAt != nil || !attempt.Status.IsInFlight() {
			continue
		}
		executionID := ""
		if attempt.ExecutionID != nil {
			executionID = strings.TrimSpace(*attempt.ExecutionID)
		}
		return attempt, executionID, true
	}
	return nil, "", false
}

func (s *AgentService) activeAttemptExecutionID(
	ctx context.Context,
	runID string,
) (string, bool, error) {
	attempts, err := s.backend.ListAgentRunAttempts(ctx, runID)
	if err != nil {
		return "", false, err
	}
	_, executionID, hasActiveAttempt := newestInFlightAttempt(attempts)
	return executionID, hasActiveAttempt, nil
}

// activeExecutionExternalID resolves a run ID to the external ID of
// its currently active execution (the key used for terminal I/O).
func (s *AgentService) activeExecutionExternalID(ctx context.Context, runID string) (string, error) {
	executionID, hasActiveAttempt, err := s.activeAttemptExecutionID(ctx, runID)
	if err != nil {
		return "", err
	}
	if !hasActiveAttempt || executionID == "" {
		return "", nil
	}
	exec, err := s.backend.GetRunExecution(ctx, executionID)
	if err != nil || exec == nil {
		return "", nil
	}
	return exec.ExternalId, nil
}

func (s *AgentService) publishTaskUpdate(ctx context.Context, workspaceID uint, taskID string) {
	if s == nil || s.orchestrationStore == nil || strings.TrimSpace(taskID) == "" {
		return
	}
	if err := s.orchestrationStore.PublishTaskLive(ctx, taskID); err != nil {
		log.Debug().Err(err).Str("task_id", taskID).Msg("failed to publish task live update")
	}
	if err := s.orchestrationStore.PublishWorkspaceLive(ctx, workspaceID); err != nil {
		log.Debug().Err(err).Uint("workspace_id", workspaceID).Msg("failed to publish workspace live update")
	}
}

func (s *AgentService) AcceptAgentCommand(
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
		profile, err := s.backend.GetAgentProfile(ctx, workspaceID, agentID)
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
		if !isClaudeCompatibleProvider(agentProvider) {
			return nil, false, fmt.Errorf("agent provider %q is not supported", agentProvider)
		}
		agentModel = agentConfigString(
			agentConfig,
			agentConfigKeyModel,
		)
	}

	existing, err := s.backend.GetTaskByIdempotency(ctx, workspaceID, params.AgentID, params.IdempotencyKey)
	if err == nil {
		return existing, true, nil
	}

	runPolicy := DefaultRunExecutionPolicy()
	if params.Policy != nil {
		runPolicy = NormalizeRunExecutionPolicy(*params.Policy)
	}
	instanceKey := ExecutionClassKey(workspaceID, params.AgentID, params.Lane, runPolicy)

	if params.HookID == nil {
		latestRun, err := s.latestRunForSessionAgent(ctx, workspaceID, params.AgentID, params.SessionID)
		if err != nil {
			return nil, false, err
		}
		if latestRun != nil {
			task, deduped, _, err := s.AcceptRunInput(
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
	payload := map[string]any{
		"message":                              params.Message,
		"session_id":                           params.SessionID,
		"session_key":                          params.SessionKey,
		types.AgentExecutionMetaKeyAgentID:     params.AgentID,
		"hook_id":                              params.HookID,
		"timeout_ms":                           timeoutOrDefault(params.TimeoutMs, 600000),
		"policy":                               runPolicy,
		"lane":                                 params.Lane,
		"extra_system_prompt":                  params.ExtraSystemPrompt,
		"input_provenance":                     params.InputProvenance,
		"deliver":                              params.Deliver,
		"attachments":                          params.Attachments,
		types.AgentExecutionMetaKeyInstanceKey: instanceKey,
		"label":                                params.Label,
		"spawned_by":                           params.SpawnedBy,
		"priority":                             priority,
	}
	if len(agentConfig) > 0 {
		payload[agentPayloadKeyAgentConfig] = agentConfig
	}
	if agentProvider != "" {
		payload[agentConfigKeyProvider] = agentProvider
	}
	if agentModel != "" {
		payload[agentConfigKeyModel] = agentModel
	}

	task := &types.AgentTask{
		WorkspaceID:    workspaceID,
		AgentID:        params.AgentID,
		QueueMode:      types.AgentQueueModeQueue,
		State:          types.AgentTaskStateQueued,
		IdempotencyKey: params.IdempotencyKey,
		PayloadJSON:    payload,
		RoutingJSON:    routingToMap(params.Routing),
		Priority:       priority,
		BudgetUSD:      params.BudgetUSD,
	}
	if err := s.backend.CreateTaskWithOutbox(ctx, task, nil); err != nil {
		if existing, lookupErr := s.backend.GetTaskByIdempotency(ctx, workspaceID, params.AgentID, params.IdempotencyKey); lookupErr == nil {
			return existing, true, nil
		}
		return nil, false, err
	}
	s.publishTaskUpdate(ctx, task.WorkspaceID, task.ID)
	return task, false, nil
}

func (s *AgentService) latestRunForSessionAgent(
	ctx context.Context,
	workspaceID uint,
	agentID *string,
	sessionID string,
) (*types.AgentRun, error) {
	sessionID = strings.TrimSpace(sessionID)
	if s == nil || s.backend == nil || sessionID == "" {
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

	// Prefer the run currently holding the session lease. This is the run
	// that can actually accept follow-up input right now.
	if s.terminalIO != nil {
		if owner, _ := s.terminalIO.GetSessionLeaseOwner(ctx, workspaceID, sessionID); owner != "" {
			if leaseRunID := ExtractLeaseExecutionID(owner); leaseRunID != "" {
				if leaseRun, err := s.backend.GetAgentRun(ctx, workspaceID, leaseRunID); err == nil && matchesAgent(leaseRun) {
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
		nil, // fallback to latest run of any status
	} {
		filter := baseFilter
		filter.Statuses = statuses
		runs, err := s.backend.ListAgentRunsFiltered(ctx, workspaceID, filter)
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

// AcceptTaskInput appends a durable follow-up input to the task's inbox and
// attempts to deliver it to whichever run is currently active. If no run is
// live (terminal or interaction closed), the task is requeued for a new dispatch.
func (s *AgentService) AcceptTaskInput(
	ctx context.Context,
	workspaceID uint,
	taskID string,
	kind types.InputKind,
	action *types.TaskInputAction,
	message string,
	idempotencyKey string,
) (*types.AgentTask, error) {
	if strings.TrimSpace(taskID) == "" {
		return nil, fmt.Errorf("task_id is required")
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

	task, err := s.backend.GetTask(ctx, workspaceID, taskID)
	if err != nil {
		return nil, err
	}

	sessionID := ""
	if task.TargetRunID != nil {
		run, rerr := s.backend.GetAgentRun(ctx, workspaceID, *task.TargetRunID)
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
	if err := s.backend.AppendTaskInput(ctx, input); err != nil {
		return nil, fmt.Errorf("append task input: %w", err)
	}

	s.persistUserInputLog(ctx, task, message)

	if err := s.deliverTaskInput(ctx, task); err != nil {
		log.Warn().Err(err).Str("task_id", taskID).Msg("task input delivery failed (input is durable, will be claimed on next wake)")
	}

	s.publishTaskUpdate(ctx, task.WorkspaceID, task.ID)

	updated, uerr := s.backend.GetTask(ctx, workspaceID, taskID)
	if uerr == nil {
		return updated, nil
	}
	return task, nil
}

// deliverTaskInput attempts to wake the active run or requeue the task.
func (s *AgentService) deliverTaskInput(ctx context.Context, task *types.AgentTask) error {
	if task.TargetRunID == nil || strings.TrimSpace(*task.TargetRunID) == "" {
		return nil
	}

	run, err := s.backend.GetAgentRun(ctx, task.WorkspaceID, *task.TargetRunID)
	if err != nil {
		return err
	}

	if run.Status.IsTerminal() {
		return s.requeueTaskForResume(ctx, task, run)
	}

	interaction, _ := s.resolveRunInteraction(ctx, run)
	if interaction != nil && interaction.State == types.RunInteractionStateClosed {
		return s.requeueTaskForResume(ctx, task, run)
	}

	execID, hasActiveAttempt, err := s.activeAttemptExecutionID(ctx, run.ID)
	if err != nil {
		return err
	}
	if !hasActiveAttempt {
		return s.requeueTaskForResume(ctx, task, run)
	}
	if execID != "" && s.terminalIO != nil {
		if err := s.terminalIO.PublishInputWake(ctx, execID); err != nil {
			return err
		}
		// Re-check: the run may have become terminal between our first check
		// and the wake publish. If so, the wake went to a dead worker.
		if freshRun, err := s.backend.GetAgentRun(ctx, task.WorkspaceID, *task.TargetRunID); err == nil && freshRun.Status.IsTerminal() {
			return s.requeueTaskForResume(ctx, task, freshRun)
		}
		s.publishTaskUpdate(ctx, task.WorkspaceID, task.ID)
		return nil
	}
	return nil
}

// requeueTaskForResume requeues a task via the outbox when no active run can
// consume the pending input.
func (s *AgentService) requeueTaskForResume(ctx context.Context, task *types.AgentTask, lastRun *types.AgentRun) error {
	inputMessage, _ := s.backend.ConsumeOldestPendingInput(ctx, task.ID)
	task.PayloadJSON = restartTaskPayload(task.PayloadJSON, lastRun, inputMessage)
	requeued, err := s.backend.RequeueTaskWithOutboxIfCurrentRun(
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
	s.publishTaskUpdate(ctx, task.WorkspaceID, task.ID)
	return nil
}

func (s *AgentService) pendingInputSweeperLoop(ctx context.Context) {
	ticker := time.NewTicker(pendingInputSweepInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := s.retryOrphanedPendingInputs(ctx); err != nil {
				log.Warn().Err(err).Msg("failed to recover orphaned pending task inputs")
			}
		}
	}
}

func (s *AgentService) retryOrphanedPendingInputs(ctx context.Context) error {
	if s == nil || s.backend == nil {
		return nil
	}
	inputs, err := s.backend.ListOrphanedPendingInputs(ctx, pendingInputSweepMaxAge, pendingInputSweepBatch)
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

		task, err := s.backend.GetTask(ctx, input.WorkspaceID, taskID)
		if err != nil {
			log.Warn().Err(err).Str("task_id", taskID).Msg("failed to load task for orphaned input recovery")
			continue
		}
		if err := s.deliverTaskInput(ctx, task); err != nil {
			log.Warn().Err(err).Str("task_id", taskID).Msg("failed to redeliver orphaned task input")
		}
	}
	return nil
}

// AcceptRunInput is the legacy run-scoped entry point. It resolves the origin
// task from the run and delegates to AcceptTaskInput for durable delivery.
func (s *AgentService) AcceptRunInput(
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

	run, err := s.backend.GetAgentRun(ctx, workspaceID, targetRunID)
	if err != nil {
		return nil, false, "", err
	}
	if strings.TrimSpace(run.OriginTaskID) == "" {
		return nil, false, "", fmt.Errorf("run %s has no origin task", run.ID)
	}

	task, err := s.AcceptTaskInput(ctx, workspaceID, run.OriginTaskID, types.InputKindFreeText, nil, message, idempotencyKey)
	if err != nil {
		return nil, false, "", err
	}
	return task, false, types.RunInputDeliveryQueued, nil
}

func (s *AgentService) dispatchLoop(ctx context.Context) {
	if s.orchestrationStore == nil {
		log.Warn().Msg("orchestration dispatch loop disabled: store is unavailable")
		return
	}

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		reclaimed, err := s.orchestrationStore.ClaimPendingTaskDispatch(
			ctx,
			s.dispatchConsumerID,
			dispatchPendingMinIdle,
			dispatchReadBatch,
		)
		if err != nil {
			log.Warn().Err(err).Msg("failed to claim pending task-dispatch events")
		} else if err := s.processDispatchMessages(ctx, reclaimed); err != nil {
			log.Warn().Err(err).Msg("failed to process reclaimed task-dispatch events")
			continue
		}

		messages, err := s.orchestrationStore.ReadTaskDispatch(
			ctx,
			s.dispatchConsumerID,
			dispatchReadBlock,
			dispatchReadBatch,
		)
		if err != nil {
			log.Warn().Err(err).Msg("failed to read task-dispatch stream")
			continue
		}
		if err := s.processDispatchMessages(ctx, messages); err != nil {
			log.Warn().Err(err).Msg("failed to process task-dispatch events")
		}
	}
}

func (s *AgentService) processDispatchMessages(ctx context.Context, messages []redislib.XMessage) error {
	for _, message := range messages {
		if err := s.processDispatchMessage(ctx, message); err != nil {
			return err
		}
	}
	return nil
}

func (s *AgentService) processDispatchMessage(ctx context.Context, message redislib.XMessage) error {
	taskID := strings.TrimSpace(streamValueAsString(message.Values, types.OrchestrationOutboxPayloadTaskID))
	if taskID == "" {
		_ = s.orchestrationStore.AckTaskDispatch(ctx, message.ID)
		return nil
	}

	retryAttempt := intFromAny(message.Values[types.OrchestrationOutboxPayloadDispatchAttempt])
	task, claimed, err := s.backend.ClaimQueuedTaskForDispatch(ctx, taskID, dispatchClaimStaleAfter)
	if err != nil {
		return err
	}
	if !claimed || task == nil {
		_ = s.orchestrationStore.AckTaskDispatch(ctx, message.ID)
		return nil
	}

	applyDispatchPayload(task, message.Values, retryAttempt)

	if err := s.dispatchTask(ctx, task); err != nil {
		reason := "dispatch_error"
		delay := computeDispatchRetryDelay(retryAttempt)
		var retryRequest *dispatchRetryRequest
		if errors.As(err, &retryRequest) {
			if retryRequest.reason != "" {
				reason = retryRequest.reason
			}
			if retryRequest.delay > 0 {
				delay = retryRequest.delay
			}
		}
		if scheduleErr := s.scheduleDispatchRetry(
			ctx,
			task,
			retryAttempt,
			reason,
			delay,
			dispatchRetryPayloadFromValues(message.Values),
		); scheduleErr != nil {
			return scheduleErr
		}
	}
	_ = s.orchestrationStore.AckTaskDispatch(ctx, message.ID)
	return nil
}

func applyDispatchPayload(task *types.AgentTask, values map[string]any, retryAttempt int) {
	if task == nil {
		return
	}
	if task.PayloadJSON == nil {
		task.PayloadJSON = map[string]any{}
	}
	if prompt := dispatchPromptFromValues(values); prompt != "" {
		task.PayloadJSON["message"] = prompt
		task.PayloadJSON["prompt"] = prompt
	}
	if boolFromAny(values[types.OrchestrationOutboxPayloadResumeSession]) {
		task.PayloadJSON[types.OrchestrationOutboxPayloadResumeSession] = true
	}
	if excludeRunID := strings.TrimSpace(streamValueAsString(values, types.OrchestrationOutboxPayloadResumeExcludeRunID)); excludeRunID != "" {
		task.PayloadJSON[types.OrchestrationOutboxPayloadResumeExcludeRunID] = excludeRunID
	}
	if checkpointRunID := strings.TrimSpace(streamValueAsString(values, types.OrchestrationOutboxPayloadResumeCheckpointRunID)); checkpointRunID != "" {
		task.PayloadJSON[types.OrchestrationOutboxPayloadResumeCheckpointRunID] = checkpointRunID
	}
	if retryAttempt > 0 {
		task.PayloadJSON[types.OrchestrationOutboxPayloadDispatchAttempt] = retryAttempt
	}
}

func dispatchPromptFromValues(values map[string]any) string {
	if prompt := strings.TrimSpace(streamValueAsString(values, types.OrchestrationOutboxPayloadDispatchPrompt)); prompt != "" {
		return prompt
	}
	return strings.TrimSpace(streamValueAsString(values, types.OrchestrationOutboxPayloadWakeFollowUpPrompt))
}

func dispatchRetryPayloadFromValues(values map[string]any) map[string]any {
	payload := map[string]any{}
	if prompt := dispatchPromptFromValues(values); prompt != "" {
		payload[types.OrchestrationOutboxPayloadDispatchPrompt] = prompt
	}
	if boolFromAny(values[types.OrchestrationOutboxPayloadResumeSession]) {
		payload[types.OrchestrationOutboxPayloadResumeSession] = true
	}
	if excludeRunID := strings.TrimSpace(streamValueAsString(values, types.OrchestrationOutboxPayloadResumeExcludeRunID)); excludeRunID != "" {
		payload[types.OrchestrationOutboxPayloadResumeExcludeRunID] = excludeRunID
	}
	if checkpointRunID := strings.TrimSpace(streamValueAsString(values, types.OrchestrationOutboxPayloadResumeCheckpointRunID)); checkpointRunID != "" {
		payload[types.OrchestrationOutboxPayloadResumeCheckpointRunID] = checkpointRunID
	}
	return payload
}

type runResultProjectorMessage struct {
	taskID             string
	attemptID          string
	exitCode           int
	errorText          string
	resultKey          string
	retryAttempt       int
	waitingForInput    bool
	wakeDelayMinutes   int
	wakeReason         string
	wakeFollowUpPrompt string
	wakeAgenda         []*types.TaskWakeAgendaItem
}

func parseWakeAgendaPayload(raw string) []*types.TaskWakeAgendaItem {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}

	type wakeAgendaPayloadItem struct {
		Type   string `json:"type"`
		Title  string `json:"title"`
		Reason string `json:"reason"`
	}

	var payload []wakeAgendaPayloadItem
	if err := json.Unmarshal([]byte(raw), &payload); err != nil {
		return nil
	}

	items := make([]*types.TaskWakeAgendaItem, 0, len(payload))
	for idx, item := range payload {
		title := strings.TrimSpace(item.Title)
		reason := strings.TrimSpace(item.Reason)
		if title == "" && reason == "" {
			continue
		}
		items = append(items, &types.TaskWakeAgendaItem{
			Seq:    idx + 1,
			Type:   strings.TrimSpace(item.Type),
			Title:  title,
			Reason: reason,
		})
	}
	return items
}

func (s *AgentService) resultProjectorLoop(ctx context.Context) {
	if s.orchestrationStore == nil || s.backend == nil {
		log.Warn().Msg("orchestration result projector disabled: store is unavailable")
		return
	}
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		reclaimed, err := s.orchestrationStore.ClaimPendingRunResults(
			ctx,
			s.resultConsumerID,
			resultPendingMinIdle,
			resultReadBatch,
		)
		if err != nil {
			log.Warn().Err(err).Msg("failed to claim pending run-result events")
		} else if err := s.processRunResultMessages(ctx, reclaimed); err != nil {
			log.Warn().Err(err).Msg("failed to process reclaimed run-result events")
			continue
		}

		messages, err := s.orchestrationStore.ReadRunResults(
			ctx,
			s.resultConsumerID,
			resultReadBlock,
			resultReadBatch,
		)
		if err != nil {
			log.Warn().Err(err).Msg("failed to read run-result stream")
			continue
		}
		if err := s.processRunResultMessages(ctx, messages); err != nil {
			log.Warn().Err(err).Msg("failed to process run-result events")
		}
	}
}

func (s *AgentService) processRunResultMessages(ctx context.Context, messages []redislib.XMessage) error {
	for _, message := range messages {
		if err := s.processRunResultMessage(ctx, message); err != nil {
			return err
		}
	}
	return nil
}

func (s *AgentService) processRunResultMessage(ctx context.Context, message redislib.XMessage) error {
	result := runResultProjectorMessage{
		taskID:             strings.TrimSpace(streamValueAsString(message.Values, types.OrchestrationOutboxPayloadTaskID)),
		attemptID:          strings.TrimSpace(streamValueAsString(message.Values, types.OrchestrationOutboxPayloadAttemptID)),
		exitCode:           intFromAny(message.Values[types.OrchestrationOutboxPayloadExitCode]),
		errorText:          streamValueAsString(message.Values, types.OrchestrationOutboxPayloadError),
		resultKey:          strings.TrimSpace(streamValueAsString(message.Values, types.OrchestrationOutboxPayloadIdempotency)),
		retryAttempt:       intFromAny(message.Values[types.OrchestrationOutboxPayloadDispatchAttempt]),
		waitingForInput:    boolFromAny(message.Values[types.OrchestrationOutboxPayloadWaitingForInput]),
		wakeDelayMinutes:   intFromAny(message.Values[types.OrchestrationOutboxPayloadWakeDelayMinutes]),
		wakeReason:         streamValueAsString(message.Values, types.OrchestrationOutboxPayloadWakeReason),
		wakeFollowUpPrompt: streamValueAsString(message.Values, types.OrchestrationOutboxPayloadWakeFollowUpPrompt),
		wakeAgenda:         parseWakeAgendaPayload(streamValueAsString(message.Values, types.OrchestrationOutboxPayloadWakeAgenda)),
	}
	if result.taskID == "" || result.attemptID == "" {
		_ = s.orchestrationStore.AckRunResults(ctx, message.ID)
		return nil
	}
	if result.resultKey == "" {
		result.resultKey = fmt.Sprintf("run_result:%s:%s", result.taskID, result.attemptID)
	}

	if err := s.applyRunResultProjectorMessage(ctx, result); err != nil {
		if s.orchestrationStore != nil {
			_, _ = s.orchestrationStore.PublishRunResultDLQ(ctx, map[string]any{
				types.OrchestrationOutboxPayloadTaskID:          result.taskID,
				types.OrchestrationOutboxPayloadAttemptID:       result.attemptID,
				types.OrchestrationOutboxPayloadExitCode:        result.exitCode,
				types.OrchestrationOutboxPayloadError:           result.errorText,
				types.OrchestrationOutboxPayloadReason:          err.Error(),
				types.OrchestrationOutboxPayloadDispatchAttempt: result.retryAttempt,
				types.OrchestrationOutboxPayloadIdempotency:     result.resultKey,
			})
		}
		_ = s.orchestrationStore.AckRunResults(ctx, message.ID)
		return nil
	}

	// Record successful projection for dedupe visibility.
	_, _ = s.backend.AcquireOrchestrationResultInbox(ctx, result.resultKey, message.ID)
	_ = s.orchestrationStore.AckRunResults(ctx, message.ID)
	return nil
}

func (s *AgentService) applyRunResultProjectorMessage(ctx context.Context, result runResultProjectorMessage) error {
	if s == nil || s.backend == nil {
		return fmt.Errorf("run result projector dependencies are unavailable")
	}
	attempt, err := s.backend.GetRunAttemptByExecutionID(ctx, result.taskID)
	if err != nil {
		if isRunAttemptNotFound(err) {
			return nil
		}
		return err
	}
	if attempt == nil || strings.TrimSpace(attempt.ID) != result.attemptID {
		return nil
	}
	applied, err := s.backend.SetRunExecutionResultForAttempt(
		ctx,
		result.taskID,
		result.attemptID,
		result.exitCode,
		result.errorText,
	)
	if err != nil {
		return err
	}
	if !applied || !attempt.IsActive() {
		return nil
	}
	var wakeSignal *types.RunExecutionWakeSignal
	if result.wakeDelayMinutes > 0 || result.wakeReason != "" || result.wakeFollowUpPrompt != "" || len(result.wakeAgenda) > 0 {
		wakeSignal = &types.RunExecutionWakeSignal{
			DelayMinutes:   result.wakeDelayMinutes,
			Reason:         result.wakeReason,
			FollowUpPrompt: result.wakeFollowUpPrompt,
			WakeAgenda:     result.wakeAgenda,
		}
	}

	return s.finalizeRunAttempt(
		ctx,
		attempt,
		result.taskID,
		result.exitCode,
		result.errorText,
		result.waitingForInput,
		wakeSignal,
	)
}

func isRunAttemptNotFound(err error) bool {
	if err == nil {
		return false
	}
	_, ok := err.(*types.ErrAgentRunAttemptNotFound)
	return ok
}

func (s *AgentService) finalizeRunAttempt(
	ctx context.Context,
	attempt *types.AgentRunAttempt,
	taskID string,
	exitCode int,
	errText string,
	waitingForInput bool,
	wakeSignal *types.RunExecutionWakeSignal,
) error {
	if s.backend == nil || attempt == nil {
		return nil
	}
	if !attempt.IsActive() {
		return nil
	}

	now := time.Now()
	attemptStatus, runStatus, errMsg := types.ClassifyExecutionOutcome(exitCode, errText)

	if err := s.backend.UpdateAgentRunAttemptResult(ctx, attempt.ID, attemptStatus, &exitCode, now, errMsg); err != nil {
		return fmt.Errorf("update run attempt result: %w", err)
	}
	if err := s.backend.ClearAgentRunClaim(ctx, attempt.RunID); err != nil {
		log.Warn().
			Err(err).
			Str("run_id", attempt.RunID).
			Msg("failed to clear run claim lease during finalization")
	}
	if err := s.updateExecutionInstanceCounts(ctx, attempt.RunID, -1); err != nil {
		log.Warn().
			Err(err).
			Str("run_id", attempt.RunID).
			Msg("failed to decrement execution instance counters during finalization")
	}
	payload := map[string]any{
		types.AgentRunEventPayloadKeyAttemptID: attempt.ID,
		types.AgentRunEventPayloadKeyTaskID:    taskID,
		types.AgentRunEventPayloadKeyExitCode:  exitCode,
		types.AgentRunEventPayloadKeyError:     errText,
		types.AgentRunEventPayloadKeyEvent:     string(types.AgentRunEventFinished),
	}
	if err := s.backend.UpdateAgentRunLifecycle(ctx, attempt.RunID, runStatus, nil, &now, errMsg); err != nil {
		return fmt.Errorf("update run lifecycle: %w", err)
	}
	if err := s.appendRunSnapshot(ctx, attempt.RunID, runStatus, nil, &now, errMsg, payload); err != nil {
		return fmt.Errorf("append completion snapshot: %w", err)
	}
	if err := s.settleOriginTask(ctx, attempt.RunID, waitingForInput, wakeSignal); err != nil {
		return fmt.Errorf("settle origin task: %w", err)
	}
	return nil
}

// wakeBackoffDelay normalizes the requested wake delay.
// Self-scheduled wakes must respect the planner's chosen delay rather than
// using a shorter exponential backoff placeholder.
func wakeBackoffDelay(wakeCount, ceilingMinutes int) int {
	_ = wakeCount
	if ceilingMinutes <= 0 {
		return 5
	}
	return ceilingMinutes
}

func (s *AgentService) settleOriginTask(ctx context.Context, runID string, waitingForInput bool, wakeSignal *types.RunExecutionWakeSignal) error {
	return s.lifecycle.Settle(ctx, runID, &SettleOpts{
		WaitingForInput: waitingForInput,
		WakeSignal:      wakeSignal,
	})
}

func (s *AgentService) updateExecutionInstanceCounts(ctx context.Context, runID string, runningDelta int) error {
	run, err := s.backend.GetAgentRunByID(ctx, runID)
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
	return s.backend.AdjustExecutionInstanceRunningAttempts(ctx, instanceKey, runningDelta, &now)
}

func streamValueAsString(values map[string]any, key string) string {
	if len(values) == 0 || strings.TrimSpace(key) == "" {
		return ""
	}
	raw, ok := values[key]
	if !ok || raw == nil {
		return ""
	}
	switch typed := raw.(type) {
	case string:
		return typed
	case []byte:
		return string(typed)
	default:
		return fmt.Sprintf("%v", typed)
	}
}

func (s *AgentService) dispatchTask(ctx context.Context, task *types.AgentTask) error {
	if task == nil {
		return nil
	}
	switch task.QueueMode {
	case types.AgentQueueModeInterrupt:
		return s.handleInterruptTask(ctx, task)
	default:
		return s.handleExecutionTask(ctx, task)
	}
}

func (s *AgentService) handleInterruptTask(ctx context.Context, task *types.AgentTask) error {
	if task.TargetRunID == nil {
		reason := types.AgentTaskDropReasonInterruptMissingTarget
		if err := s.lifecycle.Drop(ctx, task.ID, reason); err != nil {
			return err
		}
		s.publishTaskUpdate(ctx, task.WorkspaceID, task.ID)
		return nil
	}

	run, err := s.backend.GetAgentRunByID(ctx, *task.TargetRunID)
	if err != nil {
		return err
	}

	_, _ = s.cancelInFlightRunExecutions(ctx, run.ID)

	if err := s.waitForSessionLeaseDrain(ctx, run.WorkspaceID, run.SessionID); err != nil {
		return err
	}

	now := time.Now()
	errMsg := types.AgentRunErrorInterruptedByQueuedInput
	if err := s.backend.UpdateAgentRunLifecycle(ctx, run.ID, types.AgentRunStatusCancelled, nil, &now, &errMsg); err != nil {
		return err
	}
	_ = s.appendRunSnapshot(ctx, run.ID, types.AgentRunStatusCancelled, nil, &now, &errMsg, map[string]any{
		types.AgentRunEventPayloadKeyCause: types.AgentRunEventCauseInterrupt,
	})
	_ = s.publishRunEvent(ctx, run.ID, types.AgentRunEventInterrupted, map[string]any{
		types.AgentRunEventPayloadKeyTaskID: task.ID,
	})
	if err := s.backend.UpdateTaskState(ctx, task.ID, types.AgentTaskStateDone, nil, task.TargetRunID); err != nil {
		return err
	}
	s.publishTaskUpdate(ctx, task.WorkspaceID, task.ID)
	return nil
}

func (s *AgentService) cancelInFlightRunExecutions(ctx context.Context, runID string) (bool, error) {
	attempts, err := s.backend.ListAgentRunAttempts(ctx, runID)
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
		if err := s.backend.CancelRunExecution(ctx, executionID); err != nil && !isRunExecutionCancelNoopError(err) {
			if firstErr == nil {
				firstErr = err
			}
			log.Warn().
				Err(err).
				Str("run_id", runID).
				Str("execution_id", executionID).
				Msg("failed to mark run execution cancelled")
		}

		if s.terminalIO != nil {
			if err := s.terminalIO.PublishCancel(ctx, executionID); err != nil {
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

func isRunExecutionCancelNoopError(err error) bool {
	if err == nil {
		return false
	}
	lower := strings.ToLower(err.Error())
	return strings.Contains(lower, "already finished") ||
		strings.Contains(lower, "cannot be cancelled")
}

const (
	sessionDrainMaxWait      = 10 * time.Second
	sessionDrainPollStep     = 500 * time.Millisecond
	dispatchReadBlock        = 2 * time.Second
	dispatchReadBatch        = int64(64)
	dispatchPendingMinIdle   = 15 * time.Second
	dispatchClaimStaleAfter  = 45 * time.Second
	dispatchRetryMaxAttempts = 8
	dispatchRetryBaseDelay   = 500 * time.Millisecond
	dispatchRetryMaxDelay    = 30 * time.Second
	resultReadBlock          = 2 * time.Second
	resultReadBatch          = int64(64)
	resultPendingMinIdle     = 15 * time.Second
	outboxPublisherInterval  = 250 * time.Millisecond
	outboxPublisherBatchSize = 100
)

var (
	dispatchCapacityRequeueDelay = 500 * time.Millisecond
	sessionBusyRequeueDelay      = 2 * time.Second
)

type dispatchRetryRequest struct {
	reason string
	delay  time.Duration
}

func (e *dispatchRetryRequest) Error() string {
	if e == nil {
		return "dispatch retry requested"
	}
	return fmt.Sprintf("dispatch retry requested (%s)", e.reason)
}

func computeDispatchRetryDelay(retryAttempt int) time.Duration {
	if retryAttempt < 0 {
		retryAttempt = 0
	}
	delay := dispatchRetryBaseDelay * time.Duration(1<<retryAttempt)
	if delay > dispatchRetryMaxDelay {
		return dispatchRetryMaxDelay
	}
	return delay
}

func (s *AgentService) outboxPublisherLoop(ctx context.Context) {
	ticker := time.NewTicker(outboxPublisherInterval)
	defer ticker.Stop()

	// Prime a pass before waiting for the first tick.
	s.publishOutboxBatch(ctx)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.publishOutboxBatch(ctx)
		}
	}
}

func (s *AgentService) publishOutboxBatch(ctx context.Context) {
	if s == nil || s.backend == nil || s.orchestrationStore == nil {
		return
	}
	events, err := s.backend.ClaimPendingOrchestrationOutboxEvents(ctx, outboxPublisherBatchSize)
	if err != nil {
		log.Warn().Err(err).Msg("failed to claim orchestration outbox events")
		return
	}
	for _, event := range events {
		if event == nil {
			continue
		}
		if err := s.publishOutboxEvent(ctx, event); err != nil {
			_ = s.backend.MarkOrchestrationOutboxEventError(ctx, event.ID, err.Error())
			log.Warn().
				Err(err).
				Int64("outbox_id", event.ID).
				Str("event_type", string(event.EventType)).
				Msg("failed to publish orchestration outbox event")
			continue
		}
		if err := s.backend.MarkOrchestrationOutboxEventPublished(ctx, event.ID); err != nil {
			log.Warn().
				Err(err).
				Int64("outbox_id", event.ID).
				Msg("failed to mark orchestration outbox event as published")
		}
	}
}

func (s *AgentService) publishOutboxEvent(ctx context.Context, event *types.OrchestrationOutboxEvent) error {
	if s == nil || s.orchestrationStore == nil || event == nil {
		return fmt.Errorf("orchestration outbox publisher is unavailable")
	}

	switch event.EventType {
	case types.OrchestrationOutboxEventTypeTaskDispatch:
		_, err := s.orchestrationStore.PublishTaskDispatch(ctx, event.PayloadJSON)
		return err
	case types.OrchestrationOutboxEventTypeRunResult:
		_, err := s.orchestrationStore.PublishRunResult(ctx, event.PayloadJSON)
		return err
	default:
		return fmt.Errorf("unsupported orchestration outbox event type %q", event.EventType)
	}
}

func (s *AgentService) scheduleDispatchRetry(
	ctx context.Context,
	task *types.AgentTask,
	retryAttempt int,
	reason string,
	delay time.Duration,
	retryPayload map[string]any,
) error {
	if s == nil || s.backend == nil || task == nil {
		return fmt.Errorf("dispatch retry dependencies are unavailable")
	}

	nextAttempt := retryAttempt + 1
	if nextAttempt > dispatchRetryMaxAttempts {
		dropReason := types.AgentTaskDropReasonDispatchRetryExhausted
		if err := s.lifecycle.Drop(ctx, task.ID, dropReason); err != nil {
			return err
		}
		s.publishTaskUpdate(ctx, task.WorkspaceID, task.ID)
		if s.orchestrationStore != nil {
			_, _ = s.orchestrationStore.PublishTaskDispatchDLQ(ctx, map[string]any{
				types.OrchestrationOutboxPayloadTaskID:          task.ID,
				types.OrchestrationOutboxPayloadReason:          reason,
				types.OrchestrationOutboxPayloadRetryDelay:      int(delay.Milliseconds()),
				types.OrchestrationOutboxPayloadDispatchAttempt: retryAttempt,
			})
		}
		return nil
	}

	guardKey := fmt.Sprintf("dispatch_retry:%s:%d", task.ID, nextAttempt)
	acquired, err := s.backend.AcquireOrchestrationRetryGuard(ctx, guardKey)
	if err != nil {
		return err
	}
	if !acquired {
		return nil
	}

	if err := s.backend.UpdateTaskState(ctx, task.ID, types.AgentTaskStateQueued, nil, task.TargetRunID); err != nil {
		return err
	}
	s.publishTaskUpdate(ctx, task.WorkspaceID, task.ID)

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

	return s.backend.EnqueueOrchestrationOutboxEvent(ctx, &types.OrchestrationOutboxEvent{
		EventType:   types.OrchestrationOutboxEventTypeTaskDispatch,
		DedupeKey:   guardKey,
		PayloadJSON: payload,
		AvailableAt: time.Now().Add(delay),
	})
}

func (s *AgentService) waitForResumeBarrier(
	ctx context.Context,
	workspaceID uint,
	sessionID string,
	expectedCheckpointRunID string,
	excludeRunIDs ...string,
) error {
	if err := s.waitForSessionLeaseDrain(ctx, workspaceID, sessionID); err != nil {
		return err
	}
	if err := s.waitForSessionCheckpoint(ctx, workspaceID, sessionID, expectedCheckpointRunID); err != nil {
		return err
	}
	return s.ensureSessionAvailableForNewRun(ctx, workspaceID, sessionID, excludeRunIDs...)
}

func (s *AgentService) waitForSessionCheckpoint(
	ctx context.Context,
	workspaceID uint,
	sessionID string,
	expectedRunID string,
) error {
	if s == nil || s.terminalIO == nil {
		return nil
	}
	sessionID = strings.TrimSpace(sessionID)
	expectedRunID = strings.TrimSpace(expectedRunID)
	if sessionID == "" || expectedRunID == "" {
		return nil
	}

	deadline := time.After(sessionDrainMaxWait)
	tick := time.NewTicker(sessionDrainPollStep)
	defer tick.Stop()

	for {
		checkpoint, err := s.terminalIO.GetSessionCheckpoint(ctx, workspaceID, sessionID)
		if err != nil {
			return fmt.Errorf("check session checkpoint: %w", err)
		}
		if checkpoint != nil && strings.TrimSpace(checkpoint.RunID) == expectedRunID {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline:
			if checkpoint != nil && strings.TrimSpace(checkpoint.RunID) != "" {
				return fmt.Errorf(
					"session %s checkpoint for run %s not durable yet (latest checkpoint %s)",
					sessionID,
					expectedRunID,
					strings.TrimSpace(checkpoint.RunID),
				)
			}
			return fmt.Errorf("session %s checkpoint for run %s not durable yet", sessionID, expectedRunID)
		case <-tick.C:
		}
	}
}

func (s *AgentService) waitForSessionLeaseDrain(ctx context.Context, workspaceID uint, sessionID string) error {
	if s.terminalIO == nil || sessionID == "" {
		return nil
	}
	deadline := time.After(sessionDrainMaxWait)
	tick := time.NewTicker(sessionDrainPollStep)
	defer tick.Stop()

	reconciled := false
	for {
		owner, err := s.terminalIO.GetSessionLeaseOwner(ctx, workspaceID, sessionID)
		if err != nil {
			return fmt.Errorf("check session lease: %w", err)
		}
		if owner == "" {
			return nil
		}
		if !reconciled {
			if s.tryReconcileStaleSessionLease(ctx, workspaceID, sessionID, owner) {
				reconciled = true
				continue
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline:
			return fmt.Errorf("session %s still held by %s after drain timeout", sessionID, owner)
		case <-tick.C:
		}
	}
}

func (s *AgentService) tryReconcileStaleSessionLease(ctx context.Context, workspaceID uint, sessionID, owner string) bool {
	return ReconcileStaleSessionLease(ctx, s.backend, s.terminalIO, workspaceID, sessionID, owner)
}

// ReconcileStaleSessionLease checks whether the current lease owner
// references a terminal/missing execution. If so it force-releases the
// lease and returns true. Owner format is "workerID:executionID".
// Exported so gateway/services can reuse the same logic.
func ReconcileStaleSessionLease(
	ctx context.Context,
	backend repository.BackendRepository,
	terminalIO repository.TerminalIORepository,
	workspaceID uint,
	sessionID, owner string,
) bool {
	if backend == nil || terminalIO == nil || owner == "" {
		return false
	}
	executionID := ExtractLeaseExecutionID(owner)
	if executionID == "" {
		return false
	}
	exec, err := backend.GetRunExecution(ctx, executionID)
	if err != nil {
		log.Warn().
			Err(err).
			Str("session_id", sessionID).
			Str("lease_owner", owner).
			Str("execution_id", executionID).
			Msg("skip stale lease reconciliation: execution proof unavailable")
		return false
	}
	if exec == nil || exec.IsTerminal() {
		log.Info().
			Str("session_id", sessionID).
			Str("lease_owner", owner).
			Str("execution_id", executionID).
			Msg("force-releasing stale session lease")
		if releaseErr := terminalIO.ReleaseSessionLease(ctx, workspaceID, sessionID, owner); releaseErr != nil {
			log.Warn().Err(releaseErr).
				Str("session_id", sessionID).
				Str("lease_owner", owner).
				Msg("failed to release stale session lease")
			return false
		}
		return true
	}
	return false
}

// ExtractLeaseExecutionID parses the execution ID from a lease owner
// string formatted as "workerID:executionID".
func ExtractLeaseExecutionID(owner string) string {
	parts := strings.SplitN(owner, ":", 2)
	if len(parts) != 2 {
		return ""
	}
	return strings.TrimSpace(parts[1])
}

var retryablePatterns = []string{
	"already in use", "still held", "not durable", // session lease contention
	"toomanyrequests", "rate exceeded", "rate limit", "throttl", "429", // rate limits
	"connection refused", "connection reset", "i/o timeout", "no such host", // network
	"tls handshake timeout", "service unavailable", "503", "temporary failure", // infra
	"failed to fetch image", "failed to pull", "failed to prepare rootfs", // image pull
	"sandbox exited before startup", "failed to start sandbox", // sandbox startup
	"no conversation found", // claude session not found (stale resume)
}

func isRetryableError(errText string) bool {
	lower := strings.ToLower(errText)
	for _, p := range retryablePatterns {
		if strings.Contains(lower, p) {
			return true
		}
	}
	return false
}

// isSessionBusyError is used during dispatch to detect session lease contention.
func isSessionBusyError(err error) bool {
	if err == nil {
		return false
	}
	lower := strings.ToLower(err.Error())
	return strings.Contains(lower, "already in use") ||
		strings.Contains(lower, "still held") ||
		strings.Contains(lower, "not durable")
}

func (s *AgentService) handleExecutionTask(ctx context.Context, task *types.AgentTask) error {
	runPolicy := runPolicyFromPayload(task.PayloadJSON)
	instanceKey := instanceKeyFromPayload(task.WorkspaceID, task.AgentID, task.PayloadJSON, runPolicy)

	if _, err := s.instanceController.EnsureInstance(ctx, ExecutionInstanceConfig{
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
	if instance, err := s.backend.GetExecutionInstanceByKey(ctx, instanceKey); err == nil {
		hasInstanceState = true
		runningAttempts = instance.RunningAttempts
		if instance.DesiredDispatchConcurrency > 0 {
			desiredDispatch = instance.DesiredDispatchConcurrency
		}
	}

	if err := s.instanceController.RouteDispatchTarget(ctx, instanceKey, desiredDispatch); err != nil {
		log.Warn().Err(err).Str("instance_key", instanceKey).Int("dispatch_target", desiredDispatch).Msg("failed to route dispatch target")
	}
	if hasInstanceState && runningAttempts >= desiredDispatch {
		return &dispatchRetryRequest{
			reason: "dispatch_capacity",
			delay:  dispatchCapacityRequeueDelay,
		}
	}

	run, runPolicy, prompt, err := s.materializeRun(ctx, task)
	if err != nil {
		if isSessionBusyError(err) {
			return &dispatchRetryRequest{
				reason: "session_busy",
				delay:  sessionBusyRequeueDelay,
			}
		}
		reason := types.AgentTaskDropReasonRunMaterializationFail
		_ = s.lifecycle.Drop(ctx, task.ID, reason)
		s.publishTaskUpdate(ctx, task.WorkspaceID, task.ID)
		return nil
	}

	_, err = s.createAttemptExecutionTask(
		ctx,
		run,
		runPolicy,
		prompt,
		s.resolveRunAgentConfig(ctx, run, task.PayloadJSON),
		task.PayloadJSON,
	)
	return err
}

// persistUserInputLog writes the user's follow-up message to the S2 log
// stream so it appears in the event timeline. It tries the task's current
// TargetRunID first, then falls back to the most recent run for the task.
func (s *AgentService) persistUserInputLog(ctx context.Context, task *types.AgentTask, message string) {
	if s.s2 == nil || !s.s2.Enabled() {
		return
	}

	runID := ""
	if task.TargetRunID != nil {
		runID = strings.TrimSpace(*task.TargetRunID)
	}
	if runID == "" {
		runs, err := s.backend.ListAgentRunsFiltered(ctx, task.WorkspaceID, types.AgentRunListFilter{
			TaskID: &task.ID,
			Limit:  1,
		})
		if err != nil || len(runs) == 0 || runs[0] == nil {
			log.Warn().Str("task_id", task.ID).Msg("persistUserInputLog: no run found")
			return
		}
		runID = runs[0].ID
	}

	attempts, err := s.backend.ListAgentRunAttempts(ctx, runID)
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
	if err := s.s2.Append(ctx, common.Streams.TaskLogs(execID), entry); err != nil {
		log.Warn().Err(err).Str("exec_id", execID).Msg("persistUserInputLog: S2 append failed")
	} else {
		log.Info().Str("task_id", task.ID).Str("exec_id", execID).Str("stream", common.Streams.TaskLogs(execID)).Int("msg_len", len(message)).Msg("persistUserInputLog: wrote user_input to S2")
	}
}

func restartTaskPayload(basePayload map[string]any, terminalRun *types.AgentRun, message string) map[string]any {
	payload := cloneAnyMap(basePayload)
	if _, exists := payload["original_message"]; !exists {
		if orig, ok := payload["message"].(string); ok && orig != "" {
			payload["original_message"] = orig
		}
	}
	if message != "" {
		payload["message"] = message
		payload["prompt"] = message
	}

	payload["session_id"] = terminalRun.SessionID
	payload[types.OrchestrationOutboxPayloadResumeSession] = true
	payload[types.OrchestrationOutboxPayloadResumeExcludeRunID] = terminalRun.ID
	payload[types.OrchestrationOutboxPayloadResumeCheckpointRunID] = terminalRun.ID
	payload["timeout_ms"] = terminalRun.TimeoutMs
	payload[types.AgentExecutionMetaKeyInstanceKey] = executionInstanceKeyFromRun(terminalRun)

	if terminalRun.AgentID != nil {
		payload[types.AgentExecutionMetaKeyAgentID] = *terminalRun.AgentID
	}
	if terminalRun.SessionKey != nil {
		payload["session_key"] = *terminalRun.SessionKey
	} else {
		delete(payload, "session_key")
	}
	if terminalRun.Provider != nil {
		payload[agentConfigKeyProvider] = *terminalRun.Provider
	}
	if terminalRun.Model != nil {
		payload[agentConfigKeyModel] = *terminalRun.Model
	}

	return payload
}

func (s *AgentService) ensureSessionAvailableForNewRun(
	ctx context.Context,
	workspaceID uint,
	sessionID string,
	excludeRunIDs ...string,
) error {
	if s == nil || s.backend == nil {
		return nil
	}
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" {
		return nil
	}

	if s.terminalIO != nil {
		if owner, _ := s.terminalIO.GetSessionLeaseOwner(ctx, workspaceID, sessionID); owner != "" {
			if !s.tryReconcileStaleSessionLease(ctx, workspaceID, sessionID, owner) {
				return fmt.Errorf("session ID %s is already in use (lease: %s)", sessionID, owner)
			}
		}
	}

	conflicts, err := s.backend.ListActiveRunsBySession(ctx, workspaceID, sessionID, excludeRunIDs, 5)
	if err != nil {
		return err
	}
	if len(conflicts) > 0 {
		return fmt.Errorf("session ID %s is already in use (run: %s)", sessionID, conflicts[0].ID)
	}
	return nil
}

func (s *AgentService) agentConfigForRun(ctx context.Context, run *types.AgentRun) map[string]any {
	if run == nil || run.AgentID == nil || strings.TrimSpace(*run.AgentID) == "" {
		return map[string]any{}
	}
	profile, err := s.backend.GetAgentProfile(ctx, run.WorkspaceID, *run.AgentID)
	if err != nil {
		return map[string]any{}
	}
	return cloneAnyMap(profile.ConfigJSON)
}

func strengthenSkillDirectives(agentConfig map[string]any) {
	if len(agentConfig) == 0 {
		return
	}

	systemPrompt := strings.TrimSpace(stringFromPayload(agentConfig, agentConfigKeySystemPrompt))
	if systemPrompt == "" {
		return
	}

	start, end, section, ok := activeSkillsSection(systemPrompt)
	if !ok {
		return
	}

	lines := []string{
		"## MANDATORY - Active Skills",
		"",
		"These are the skills explicitly associated with this agent. You MUST read them before starting any work.",
		"These assigned skills take priority over broader workspace-wide skills under /workspace/skills and override your defaults. Only consult the wider directory if no assigned skill covers the task or you need additional context.",
		"",
	}
	seen := map[string]int{}
	for _, match := range activeSkillReferenceRE.FindAllStringSubmatch(section, -1) {
		path, description := strings.TrimSpace(match[1]), strings.TrimSpace(match[2])
		if path == "" {
			path, description = strings.TrimSpace(match[3]), strings.TrimSpace(match[4])
		}
		path = strings.TrimSuffix(path, "/SKILL.md")
		if !strings.HasPrefix(path, "/workspace/skills/") {
			continue
		}
		if idx, exists := seen[path]; exists {
			if description != "" && !strings.Contains(lines[idx], " -- ") {
				lines[idx] += " -- " + description
			}
			continue
		}
		line := fmt.Sprintf("%d. cat %s/SKILL.md", len(seen)+1, path)
		if description != "" {
			line += " -- " + description
		}
		seen[path] = len(lines)
		lines = append(lines, line)
	}
	if len(seen) == 0 {
		return
	}
	lines = append(lines,
		"",
		"DO NOT begin the user's task until you have read every skill above. Follow them before consulting broader workspace-wide skills.",
	)
	replacement := strings.Join(lines, "\n")
	rest := strings.TrimSpace(strings.Join([]string{
		strings.TrimSpace(systemPrompt[:start]),
		strings.TrimSpace(systemPrompt[end:]),
	}, "\n\n"))

	if rest == "" {
		agentConfig[agentConfigKeySystemPrompt] = replacement
		return
	}
	agentConfig[agentConfigKeySystemPrompt] = replacement + "\n\n" + rest
}

func activeSkillsSection(systemPrompt string) (start, end int, section string, ok bool) {
	header := activeSkillsHeaderRE.FindStringIndex(systemPrompt)
	if header == nil {
		return 0, 0, "", false
	}
	start = header[0]
	searchFrom := header[1]
	end = len(systemPrompt)
	if nextSection := strings.Index(systemPrompt[searchFrom:], "\n## "); nextSection != -1 {
		end = searchFrom + nextSection
	}
	return start, end, systemPrompt[start:end], true
}

func (s *AgentService) resolveRunAgentConfig(
	ctx context.Context,
	run *types.AgentRun,
	payload map[string]any,
) map[string]any {
	agentConfig := mapFromPayload(payload, agentPayloadKeyAgentConfig)
	if len(agentConfig) == 0 {
		agentConfig = s.agentConfigForRun(ctx, run)
	}
	agentConfig = cloneAnyMap(agentConfig)
	if skills := skillNamesFromConfig(agentConfig); len(skills) > 0 {
		injectSkillsSection(agentConfig, skills)
	} else {
		strengthenSkillDirectives(agentConfig)
	}
	return agentConfig
}

func skillNamesFromConfig(config map[string]any) []string {
	raw, ok := config[agentConfigKeySkills]
	if !ok || raw == nil {
		return nil
	}
	out := make([]string, 0)
	seen := map[string]struct{}{}
	appendSkill := func(value string) {
		value = strings.TrimSpace(value)
		if value == "" {
			return
		}
		if _, ok := seen[value]; ok {
			return
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	switch typed := raw.(type) {
	case []string:
		for _, value := range typed {
			appendSkill(value)
		}
	case []any:
		for _, value := range typed {
			if text, ok := value.(string); ok {
				appendSkill(text)
			}
		}
	case string:
		appendSkill(typed)
	}
	return out
}

func injectSkillsSection(agentConfig map[string]any, skills []string) {
	if len(skills) == 0 {
		return
	}
	lines := []string{
		"## MANDATORY - Active Skills",
		"",
		"These are the skills explicitly associated with this agent. You MUST read them before starting any work.",
		"These assigned skills take priority over broader workspace-wide skills under /workspace/skills and override your defaults. Only consult the wider directory if no assigned skill covers the task or you need additional context.",
		"",
	}
	for i, name := range skills {
		lines = append(lines, fmt.Sprintf("%d. cat /workspace/skills/%s/SKILL.md", i+1, name))
	}
	lines = append(lines,
		"",
		"DO NOT begin the user's task until you have read every skill above. Follow them before consulting broader workspace-wide skills.",
	)
	skillsSection := strings.Join(lines, "\n")

	systemPrompt := strings.TrimSpace(stringFromPayload(agentConfig, agentConfigKeySystemPrompt))
	if systemPrompt != "" {
		agentConfig[agentConfigKeySystemPrompt] = skillsSection + "\n\n" + systemPrompt
	} else {
		agentConfig[agentConfigKeySystemPrompt] = skillsSection
	}
}

func runInputPrompt(payload map[string]any) string {
	prompt := strings.TrimSpace(stringFromPayload(payload, "prompt"))
	if prompt == "" {
		prompt = strings.TrimSpace(stringFromPayload(payload, "message"))
	}
	return prompt
}

func (s *AgentService) materializeRun(
	ctx context.Context,
	task *types.AgentTask,
) (*types.AgentRun, RunExecutionPolicy, string, error) {
	payload := task.PayloadJSON
	prompt := runInputPrompt(payload)
	if prompt == "" {
		return nil, RunExecutionPolicy{}, "", fmt.Errorf("missing prompt/message in task payload")
	}
	if extraSystemPrompt := strings.TrimSpace(stringFromPayload(payload, "extra_system_prompt")); extraSystemPrompt != "" {
		payload["_extra_system_prompt_resolved"] = extraSystemPrompt
	}

	sessionID := stringFromPayload(payload, "session_id")
	if sessionID == "" {
		return nil, RunExecutionPolicy{}, "", fmt.Errorf("missing session_id in payload")
	}
	timeoutMs := intFromPayload(payload, "timeout_ms", 600000)
	if timeoutMs < 0 {
		return nil, RunExecutionPolicy{}, "", fmt.Errorf("timeout_ms must be >= 0")
	}

	runPolicy := runPolicyFromPayload(payload)
	provider, model := providerModelFromPayload(payload)
	if provider == nil {
		return nil, RunExecutionPolicy{}, "", fmt.Errorf("agent provider is required in task payload")
	}
	if !isClaudeCompatibleProvider(*provider) {
		return nil, RunExecutionPolicy{}, "", fmt.Errorf("agent provider %q is not supported", *provider)
	}
	runPolicy.Interactive = true
	if err := ValidateRunExecutionPolicy(runPolicy); err != nil {
		return nil, RunExecutionPolicy{}, "", err
	}

	if boolFromAny(payload[types.OrchestrationOutboxPayloadResumeSession]) {
		excludeRunIDs := []string{}
		if excludeRunID := strings.TrimSpace(stringFromPayload(payload, types.OrchestrationOutboxPayloadResumeExcludeRunID)); excludeRunID != "" {
			excludeRunIDs = append(excludeRunIDs, excludeRunID)
		}
		if err := s.waitForResumeBarrier(
			ctx,
			task.WorkspaceID,
			sessionID,
			stringFromPayload(payload, types.OrchestrationOutboxPayloadResumeCheckpointRunID),
			excludeRunIDs...,
		); err != nil {
			return nil, RunExecutionPolicy{}, "", err
		}
	} else {
		if err := s.ensureSessionAvailableForNewRun(ctx, task.WorkspaceID, sessionID); err != nil {
			return nil, RunExecutionPolicy{}, "", err
		}
	}

	sessionKey := strPtrMaybe(stringFromPayload(payload, "session_key"))
	agentID := task.AgentID
	instanceKey := instanceKeyFromPayload(task.WorkspaceID, agentID, payload, runPolicy)

	run := &types.AgentRun{
		WorkspaceID:     task.WorkspaceID,
		AgentID:         agentID,
		OriginTaskID:    task.ID,
		HookID:          uintPtrFromPayload(payload, "hook_id"),
		Status:          types.AgentRunStatusAccepted,
		SessionID:       sessionID,
		SessionKey:      sessionKey,
		Provider:        provider,
		Model:           model,
		ExecHost:        string(runPolicy.Host),
		ExecSecurity:    string(runPolicy.Security),
		ExecAsk:         string(runPolicy.Ask),
		RuntimeType:     runPolicy.RuntimeType,
		WorkspaceAccess: runPolicy.WorkspaceAccess,
		NetworkEnabled:  runPolicy.NetworkEnabled,
		Interactive:     runPolicy.Interactive,
		TimeoutMs:       timeoutMs,
		UsageJSON:       map[string]any{},
		DeliveryJSON:    buildRunDelivery(instanceKey, runPolicy, nil),
	}
	applyDeliveryMetadata(run.DeliveryJSON, payload, task.RoutingJSON)
	applyResumeDeliveryMetadata(run.DeliveryJSON, payload)

	if err := s.backend.CreateAgentRun(ctx, run); err != nil {
		return nil, RunExecutionPolicy{}, "", err
	}
	if err := s.lifecycle.Dispatch(ctx, task.ID, run.ID); err != nil {
		return nil, RunExecutionPolicy{}, "", err
	}
	s.publishTaskUpdate(ctx, task.WorkspaceID, task.ID)
	if err := s.appendRunSnapshot(ctx, run.ID, types.AgentRunStatusAccepted, nil, nil, nil, map[string]any{
		types.AgentRunEventPayloadKeyTaskID: task.ID,
	}); err != nil {
		log.Warn().Err(err).Str("run_id", run.ID).Msg("failed to append accepted snapshot")
	}
	_ = s.publishRunEvent(ctx, run.ID, types.AgentRunEventAccepted, map[string]any{
		types.AgentRunEventPayloadKeyTaskID: task.ID,
	})
	return run, runPolicy, prompt, nil
}

func (s *AgentService) createAttemptExecutionTask(
	ctx context.Context,
	run *types.AgentRun,
	runPolicy RunExecutionPolicy,
	prompt string,
	agentConfig map[string]any,
	payload map[string]any,
) (*types.AgentRunAttempt, error) {
	nextAttemptNo, err := s.nextRunAttemptNo(ctx, run.ID)
	if err != nil {
		return nil, err
	}

	attempt := &types.AgentRunAttempt{
		RunID:           run.ID,
		AttemptNo:       nextAttemptNo,
		Status:          types.AgentAttemptStatusPending,
		Strategy:        types.AgentAttemptStrategyPrimary,
		Provider:        run.Provider,
		Model:           run.Model,
		ExecHost:        run.ExecHost,
		ExecSecurity:    run.ExecSecurity,
		ExecAsk:         run.ExecAsk,
		RuntimeType:     run.RuntimeType,
		WorkspaceAccess: run.WorkspaceAccess,
		NetworkEnabled:  run.NetworkEnabled,
		Interactive:     run.Interactive,
	}
	if run.ExecAsk != string(ExecAskOff) {
		attempt.Status = types.AgentAttemptStatusBlocked
	}
	if err := s.backend.CreateAgentRunAttempt(ctx, attempt); err != nil {
		return nil, err
	}
	if attempt.Status == types.AgentAttemptStatusBlocked {
		return attempt, nil
	}

	_, memberToken, err := s.backend.EnsureWorkspaceServiceToken(ctx, run.WorkspaceID)
	if err != nil {
		return nil, err
	}
	taskEnv := map[string]string{}
	applyRunExecutionContextEnv(taskEnv, run, attempt.ID)
	applyAgentConfigEnv(taskEnv, agentConfig)
	applyPayloadRuntimeEnv(taskEnv, payload)
	retryPolicy := RetryPolicyOrDefault(runPolicy.Retry)
	executionPolicy := map[string]any{
		"host":                               run.ExecHost,
		"security":                           run.ExecSecurity,
		"ask":                                run.ExecAsk,
		"runtime_type":                       run.RuntimeType,
		"workspace_access":                   run.WorkspaceAccess,
		"network_enabled":                    run.NetworkEnabled,
		"interactive":                        run.Interactive,
		types.AgentExecutionMetaKeyResources: cloneAnyMap(runPolicy.Resources),
		types.AgentExecutionMetaKeyRetry: map[string]any{
			"max_attempts": retryPolicy.MaxAttempts,
			"delay_ms":     retryPolicy.DelayMs,
		},
	}
	applyRunExecutionContextMetadata(executionPolicy, run, attempt.ID)
	if run.Provider != nil {
		executionPolicy[agentConfigKeyProvider] = *run.Provider
	}
	if run.Model != nil {
		executionPolicy[agentConfigKeyModel] = *run.Model
	}
	applyPayloadExecutionMetadata(executionPolicy, payload)

	execTask := &types.RunExecution{
		WorkspaceId:       run.WorkspaceID,
		MemberToken:       memberToken,
		Status:            types.RunExecutionStatusPending,
		Type:              ToRunExecutionType(runPolicy),
		Prompt:            prompt,
		Image:             s.defaultImage,
		Entrypoint:        []string{},
		Env:               taskEnv,
		Resources:         ToRunExecutionResources(runPolicy),
		RunAttemptID:      &attempt.ID,
		Attempt:           attempt.AttemptNo,
		MaxAttempts:       retryPolicy.MaxAttempts,
		TimeoutMs:         &run.TimeoutMs,
		ExecHost:          strPtr(run.ExecHost),
		ExecSecurity:      strPtr(run.ExecSecurity),
		ExecAsk:           strPtr(run.ExecAsk),
		RuntimeType:       strPtr(run.RuntimeType),
		WorkspaceAccess:   strPtr(run.WorkspaceAccess),
		NetworkEnabled:    boolPtr(run.NetworkEnabled),
		ExecutionPolicy:   executionPolicy,
		CreatedByMemberId: nil,
		HookId:            run.HookID,
	}
	if err := s.backend.CreateRunExecution(ctx, execTask); err != nil {
		return nil, err
	}
	if err := s.backend.BindAttemptExecutionTask(ctx, attempt.ID, execTask.ExternalId); err != nil {
		return nil, err
	}
	if s.taskQueue == nil {
		return nil, fmt.Errorf("task queue is unavailable")
	}
	if err := s.taskQueue.Push(ctx, execTask); err != nil {
		return nil, err
	}
	s.publishTaskUpdate(ctx, run.WorkspaceID, run.OriginTaskID)
	return attempt, nil
}

func (s *AgentService) nextRunAttemptNo(ctx context.Context, runID string) (int, error) {
	attempts, err := s.backend.ListAgentRunAttempts(ctx, runID)
	if err != nil {
		return 0, err
	}
	maxAttempt := 0
	for _, attempt := range attempts {
		if attempt == nil {
			continue
		}
		if attempt.AttemptNo > maxAttempt {
			maxAttempt = attempt.AttemptNo
		}
	}
	return maxAttempt + 1, nil
}

func (s *AgentService) appendRunSnapshot(
	ctx context.Context,
	runID string,
	status types.AgentRunStatus,
	startedAt *time.Time,
	endedAt *time.Time,
	errorMsg *string,
	payload map[string]any,
) error {
	seq, err := s.backend.IncrementAgentRunSnapshotSeq(ctx, runID)
	if err != nil {
		return err
	}

	var startedMs *int64
	var endedMs *int64
	if startedAt != nil {
		v := startedAt.UnixMilli()
		startedMs = &v
	}
	if endedAt != nil {
		v := endedAt.UnixMilli()
		endedMs = &v
	}
	return s.backend.AppendAgentRunSnapshot(ctx, &types.AgentRunSnapshot{
		RunID:       runID,
		Seq:         seq,
		Status:      status,
		StartedAtMs: startedMs,
		EndedAtMs:   endedMs,
		Error:       errorMsg,
		TS:          time.Now().UnixMilli(),
		PayloadJSON: payload,
	})
}

func (s *AgentService) publishRunEvent(ctx context.Context, runID string, eventType types.AgentRunEventType, payload map[string]any) error {
	payload = normalizeRunEventPayload(runID, eventType, payload)
	event := map[string]any{
		"run_id":     runID,
		"event_type": string(eventType),
		"ts":         time.Now().UnixMilli(),
		"payload":    payload,
	}
	if s.s2 != nil && s.s2.Enabled() {
		if err := s.s2.AppendRunEvent(ctx, runID, string(eventType), payload); err != nil {
			log.Warn().Err(err).Str("run_id", runID).Msg("failed to append run event to s2")
		}
	}
	if s.orchestrationStore != nil {
		body, _ := json.Marshal(event)
		return s.orchestrationStore.PublishRunEvent(ctx, runID, body)
	}
	return nil
}

func normalizeRunEventPayload(runID string, eventType types.AgentRunEventType, payload map[string]any) map[string]any {
	out := cloneAnyMap(payload)
	out["run_id"] = runID
	out["event_type"] = string(eventType)

	if _, ok := out["kind"]; !ok {
		switch eventType {
		case types.AgentRunEventInputSteered,
			types.AgentRunEventInputDispatched,
			types.AgentRunEventSteerFallbackDispatched:
			out["kind"] = "input"
		case types.AgentRunEventInterrupted:
			out["kind"] = "control"
		default:
			out["kind"] = "lifecycle"
		}
	}
	return out
}

func routingToMap(r RoutingContext) map[string]any {
	out := map[string]any{}
	if r.To != nil {
		out["to"] = *r.To
	}
	if r.ReplyTo != nil {
		out["reply_to"] = *r.ReplyTo
	}
	if r.Channel != nil {
		out["channel"] = *r.Channel
	}
	if r.ReplyChannel != nil {
		out["reply_channel"] = *r.ReplyChannel
	}
	if r.AccountID != nil {
		out["account_id"] = *r.AccountID
	}
	if r.ReplyAccountID != nil {
		out["reply_account_id"] = *r.ReplyAccountID
	}
	if r.ThreadID != nil {
		out["thread_id"] = *r.ThreadID
	}
	if r.GroupID != nil {
		out["group_id"] = *r.GroupID
	}
	if r.GroupChannel != nil {
		out["group_channel"] = *r.GroupChannel
	}
	if r.GroupSpace != nil {
		out["group_space"] = *r.GroupSpace
	}
	return out
}

func timeoutOrDefault(timeout *int, fallback int) int {
	if timeout == nil {
		return fallback
	}
	return *timeout
}

func normalizeAgentCommandDefaults(params *AgentCommandParams) {
	if params == nil {
		return
	}
	params.Message = strings.TrimSpace(params.Message)
	params.AgentID = trimOptionalString(params.AgentID)
	params.SessionID = normalizeGeneratedID(params.SessionID)
	params.SessionKey = trimOptionalString(params.SessionKey)
	params.Lane = trimOptionalString(params.Lane)
	params.ExtraSystemPrompt = trimOptionalString(params.ExtraSystemPrompt)
	params.Label = trimOptionalString(params.Label)
	params.SpawnedBy = trimOptionalString(params.SpawnedBy)
	params.Priority = strings.ToLower(strings.TrimSpace(params.Priority))
	params.IdempotencyKey = normalizeGeneratedID(params.IdempotencyKey)

	if params.InputProvenance != nil {
		params.InputProvenance.Source = trimOptionalString(params.InputProvenance.Source)
		params.InputProvenance.MessageID = trimOptionalString(params.InputProvenance.MessageID)
		params.InputProvenance.Channel = trimOptionalString(params.InputProvenance.Channel)
		params.InputProvenance.ToolCallID = trimOptionalString(params.InputProvenance.ToolCallID)
		params.InputProvenance.CorrelationID = trimOptionalString(params.InputProvenance.CorrelationID)
	}

	params.Routing.To = trimOptionalString(params.Routing.To)
	params.Routing.ReplyTo = trimOptionalString(params.Routing.ReplyTo)
	params.Routing.Channel = trimOptionalString(params.Routing.Channel)
	params.Routing.ReplyChannel = trimOptionalString(params.Routing.ReplyChannel)
	params.Routing.AccountID = trimOptionalString(params.Routing.AccountID)
	params.Routing.ReplyAccountID = trimOptionalString(params.Routing.ReplyAccountID)
	params.Routing.ThreadID = trimOptionalString(params.Routing.ThreadID)
	params.Routing.GroupID = trimOptionalString(params.Routing.GroupID)
	params.Routing.GroupChannel = trimOptionalString(params.Routing.GroupChannel)
	params.Routing.GroupSpace = trimOptionalString(params.Routing.GroupSpace)
}

func trimOptionalString(value *string) *string {
	if value == nil {
		return nil
	}
	trimmed := strings.TrimSpace(*value)
	if trimmed == "" {
		return nil
	}
	return &trimmed
}

func normalizeGeneratedID(value string) string {
	if strings.TrimSpace(value) != "" {
		return strings.TrimSpace(value)
	}
	return uuid.NewString()
}

func runPolicyFromPayload(payload map[string]any) RunExecutionPolicy {
	policy := DefaultRunExecutionPolicy()
	rawPolicy, ok := payload["policy"]
	if !ok {
		return policy
	}
	body, _ := json.Marshal(rawPolicy)
	_ = json.Unmarshal(body, &policy)
	return NormalizeRunExecutionPolicy(policy)
}

func mapFromPayload(payload map[string]any, key string) map[string]any {
	raw, ok := payload[key]
	if !ok || raw == nil {
		return map[string]any{}
	}
	if typed, ok := raw.(map[string]any); ok {
		return typed
	}
	b, err := json.Marshal(raw)
	if err != nil {
		return map[string]any{}
	}
	out := map[string]any{}
	if err := json.Unmarshal(b, &out); err != nil {
		return map[string]any{}
	}
	return out
}

func cloneAnyMap(src map[string]any) map[string]any {
	if len(src) == 0 {
		return map[string]any{}
	}
	out := make(map[string]any, len(src))
	for k, v := range src {
		out[k] = v
	}
	return out
}

func agentConfigString(config map[string]any, keys ...string) string {
	if len(config) == 0 {
		return ""
	}
	for _, key := range keys {
		if key == "" {
			continue
		}
		if value := stringFromPayload(config, key); strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func providerFromAgentConfig(config map[string]any) string {
	provider := strings.ToLower(
		agentConfigString(config, agentConfigKeyProvider),
	)
	if provider != "" {
		return provider
	}
	return providerForRunner(agentConfigString(config, agentConfigKeyRunner))
}

func providerModelFromPayload(payload map[string]any) (*string, *string) {
	provider := strPtrMaybe(stringFromPayload(payload, agentConfigKeyProvider))
	model := strPtrMaybe(stringFromPayload(payload, agentConfigKeyModel))
	if provider != nil && model != nil {
		return provider, model
	}
	agentConfig := mapFromPayload(payload, agentPayloadKeyAgentConfig)
	if provider == nil {
		provider = strPtrMaybe(providerFromAgentConfig(agentConfig))
	}
	if model == nil {
		model = strPtrMaybe(
			agentConfigString(
				agentConfig,
				agentConfigKeyModel,
			),
		)
	}
	return provider, model
}

func applyRunRuntimeEnv(env map[string]string, run *types.AgentRun) {
	if env == nil || run == nil {
		return
	}
	env["AIRSTORE_RUN_ID"] = strings.TrimSpace(run.ID)
	env["AIRSTORE_ORIGIN_TASK_ID"] = strings.TrimSpace(run.OriginTaskID)
	if sessionID := strings.TrimSpace(run.SessionID); sessionID != "" {
		env["AIRSTORE_AGENT_SESSION_ID"] = sessionID
	}
	if run.Provider != nil && strings.TrimSpace(*run.Provider) != "" {
		env["AIRSTORE_AGENT_PROVIDER"] = strings.TrimSpace(*run.Provider)
	}
	if run.Model != nil && strings.TrimSpace(*run.Model) != "" {
		env["AIRSTORE_AGENT_MODEL"] = strings.TrimSpace(*run.Model)
	}
}

func applyRunExecutionContextEnv(env map[string]string, run *types.AgentRun, attemptID string) {
	if env == nil {
		return
	}
	applyRunRuntimeEnv(env, run)
	if strings.TrimSpace(attemptID) != "" {
		env["AIRSTORE_RUN_ATTEMPT_ID"] = strings.TrimSpace(attemptID)
	}
}

func applyRunExecutionContextMetadata(executionPolicy map[string]any, run *types.AgentRun, attemptID string) {
	if executionPolicy == nil || run == nil {
		return
	}
	executionPolicy[types.AgentExecutionMetaKeyRunID] = strings.TrimSpace(run.ID)
	if strings.TrimSpace(attemptID) != "" {
		executionPolicy[types.AgentExecutionMetaKeyRunAttemptID] = strings.TrimSpace(attemptID)
	}
	executionPolicy[types.AgentExecutionMetaKeyOriginTaskID] = strings.TrimSpace(run.OriginTaskID)
	if run.AgentID != nil && strings.TrimSpace(*run.AgentID) != "" {
		executionPolicy[types.AgentExecutionMetaKeyAgentID] = strings.TrimSpace(*run.AgentID)
	}
}

func applyAgentConfigEnv(env map[string]string, config map[string]any) {
	if env == nil || len(config) == 0 {
		return
	}
	for key, value := range config {
		sanitized := sanitizeEnvSegment(key)
		if sanitized == "" || value == nil {
			continue
		}
		env["AIRSTORE_AGENT_CONFIG_"+sanitized] = stringifyEnvValue(value)
	}
	wd := strings.TrimSpace(stringFromPayload(config, "workspace_dir"))
	if wd != "" {
		env["AIRSTORE_AGENT_WORKSPACE_DIR"] = wd
	}
	if sp := strings.TrimSpace(stringFromPayload(config, "system_prompt")); sp != "" {
		if wd != "" {
			sp = strings.ReplaceAll(sp, "{{workspace_dir}}", wd)
		}
		env["AIRSTORE_AGENT_SYSTEM_PROMPT"] = sp
	}
	if mode := strings.TrimSpace(stringFromPayload(config, "system_prompt_mode")); mode != "" {
		env["AIRSTORE_AGENT_SYSTEM_PROMPT_MODE"] = mode
	}
}

func applyPayloadRuntimeEnv(env map[string]string, payload map[string]any) {
	if env == nil || len(payload) == 0 {
		return
	}
	// Route extra_system_prompt from payload through env (fallback when agent config has none).
	if esp := strings.TrimSpace(stringFromPayload(payload, "_extra_system_prompt_resolved")); esp != "" {
		if strings.TrimSpace(env["AIRSTORE_AGENT_SYSTEM_PROMPT"]) == "" {
			env["AIRSTORE_AGENT_SYSTEM_PROMPT"] = esp
		}
	}
	if raw, ok := payload["deliver"]; ok && raw != nil {
		env["AIRSTORE_AGENT_DELIVER"] = stringifyEnvValue(raw)
	}
	if label := strings.TrimSpace(stringFromPayload(payload, "label")); label != "" {
		env["AIRSTORE_AGENT_LABEL"] = label
	}
	if spawnedBy := strings.TrimSpace(stringFromPayload(payload, "spawned_by")); spawnedBy != "" {
		env["AIRSTORE_AGENT_SPAWNED_BY"] = spawnedBy
	}
	if attachments, ok := payload["attachments"]; ok && attachments != nil {
		if encoded, err := json.Marshal(attachments); err == nil && len(encoded) > 0 {
			env["AIRSTORE_AGENT_ATTACHMENTS_JSON"] = string(encoded)
		}
	}
	if inputProvenance, ok := payload["input_provenance"]; ok && inputProvenance != nil {
		if encoded, err := json.Marshal(inputProvenance); err == nil && len(encoded) > 0 {
			env["AIRSTORE_AGENT_INPUT_PROVENANCE_JSON"] = string(encoded)
		}
	}
	if boolFromAny(payload[types.OrchestrationOutboxPayloadResumeSession]) {
		env["AIRSTORE_AGENT_RESUME_SESSION"] = "true"
	}
}

func applyPayloadExecutionMetadata(executionPolicy map[string]any, payload map[string]any) {
	if executionPolicy == nil || len(payload) == 0 {
		return
	}
	for _, key := range []string{"deliver", "label", "spawned_by", "input_provenance"} {
		if value, ok := payload[key]; ok && value != nil {
			executionPolicy[key] = value
		}
	}
	if value, ok := payload["attachments"]; ok && value != nil {
		executionPolicy["attachments"] = value
	}
}

func sanitizeEnvSegment(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	var builder strings.Builder
	builder.Grow(len(value))
	for _, ch := range value {
		switch {
		case ch >= 'a' && ch <= 'z':
			builder.WriteRune(ch - 32)
		case ch >= 'A' && ch <= 'Z', ch >= '0' && ch <= '9':
			builder.WriteRune(ch)
		default:
			builder.WriteRune('_')
		}
	}
	return strings.Trim(builder.String(), "_")
}

func stringifyEnvValue(value any) string {
	switch typed := value.(type) {
	case string:
		return typed
	case bool, int, int32, int64, float32, float64:
		return fmt.Sprintf("%v", typed)
	default:
		body, err := json.Marshal(typed)
		if err != nil {
			return fmt.Sprintf("%v", typed)
		}
		return string(body)
	}
}

func instanceKeyFromPayload(workspaceID uint, agentID *string, payload map[string]any, policy RunExecutionPolicy) string {
	if instanceKey := stringFromPayload(payload, types.AgentExecutionMetaKeyInstanceKey); instanceKey != "" {
		return instanceKey
	}
	lane := strPtrMaybe(stringFromPayload(payload, "lane"))
	return ExecutionClassKey(workspaceID, agentID, lane, policy)
}

func stringFromPayload(payload map[string]any, key string) string {
	v, ok := payload[key]
	if !ok || v == nil {
		return ""
	}
	switch t := v.(type) {
	case string:
		return t
	default:
		return fmt.Sprintf("%v", t)
	}
}

func intFromPayload(payload map[string]any, key string, fallback int) int {
	v, ok := payload[key]
	if !ok || v == nil {
		return fallback
	}
	switch t := v.(type) {
	case int:
		return t
	case uint:
		return int(t)
	case uint32:
		return int(t)
	case uint64:
		return int(t)
	case int32:
		return int(t)
	case int64:
		return int(t)
	case float32:
		return int(t)
	case float64:
		return int(t)
	default:
		return fallback
	}
}

func uintPtrFromPayload(payload map[string]any, key string) *uint {
	value := intFromPayload(payload, key, 0)
	if value <= 0 {
		return nil
	}
	v := uint(value)
	return &v
}

func strPtr(v string) *string {
	return &v
}

func strPtrMaybe(v string) *string {
	if strings.TrimSpace(v) == "" {
		return nil
	}
	return &v
}

func boolPtr(v bool) *bool {
	return &v
}

func runPolicyFromRun(run *types.AgentRun) RunExecutionPolicy {
	if run == nil {
		return DefaultRunExecutionPolicy()
	}
	retry := retryPolicyFromDelivery(run.DeliveryJSON)
	resources := map[string]any{}
	if len(run.DeliveryJSON) > 0 {
		if rawResources, ok := run.DeliveryJSON[types.AgentExecutionMetaKeyResources]; ok {
			if typedResources, ok := rawResources.(map[string]any); ok {
				resources = cloneAnyMap(typedResources)
			}
		}
	}
	return RunExecutionPolicy{
		Host:            ExecHost(run.ExecHost),
		Security:        ExecSecurity(run.ExecSecurity),
		Ask:             ExecAsk(run.ExecAsk),
		RuntimeType:     run.RuntimeType,
		WorkspaceAccess: run.WorkspaceAccess,
		NetworkEnabled:  run.NetworkEnabled,
		Interactive:     run.Interactive,
		Resources:       resources,
		Retry:           &retry,
	}
}

func retryPolicyFromDelivery(delivery map[string]any) RunRetryPolicy {
	retry := RunRetryPolicy{}
	if len(delivery) == 0 {
		return NormalizeRunRetryPolicy(retry)
	}
	retry.MaxAttempts = intFromAny(delivery[types.AgentExecutionMetaKeyRetryMaxAttempts])
	retry.DelayMs = intFromAny(delivery[types.AgentExecutionMetaKeyRetryDelayMs])

	if nested, ok := delivery[types.AgentExecutionMetaKeyRetry].(map[string]any); ok {
		if retry.MaxAttempts == 0 {
			retry.MaxAttempts = intFromAny(nested["max_attempts"])
		}
		if retry.DelayMs == 0 {
			retry.DelayMs = intFromAny(nested["delay_ms"])
		}
	}
	return NormalizeRunRetryPolicy(retry)
}

func buildRunDelivery(instanceKey string, policy RunExecutionPolicy, targetRunID *string) map[string]any {
	policy = NormalizeRunExecutionPolicy(policy)
	retryPolicy := RetryPolicyOrDefault(policy.Retry)
	delivery := map[string]any{
		types.AgentExecutionMetaKeyInstanceKey:      instanceKey,
		types.AgentExecutionMetaKeyRetryMaxAttempts: retryPolicy.MaxAttempts,
		types.AgentExecutionMetaKeyRetryDelayMs:     retryPolicy.DelayMs,
		types.AgentExecutionMetaKeyResources:        cloneAnyMap(policy.Resources),
	}
	if targetRunID != nil && strings.TrimSpace(*targetRunID) != "" {
		delivery["target_run_id"] = strings.TrimSpace(*targetRunID)
	}
	return delivery
}

func applyDeliveryMetadata(delivery map[string]any, payload map[string]any, routing map[string]any) {
	if delivery == nil {
		return
	}
	for _, key := range []string{"deliver", "label", "spawned_by", "input_provenance"} {
		if value, ok := payload[key]; ok && value != nil {
			delivery[key] = value
		}
	}
	if value, ok := payload["attachments"]; ok && value != nil {
		delivery["attachments"] = value
	}
	if len(routing) > 0 {
		delivery["routing"] = cloneAnyMap(routing)
	}
}

func applyResumeDeliveryMetadata(delivery map[string]any, payload map[string]any) {
	if delivery == nil || len(payload) == 0 {
		return
	}
	for _, key := range []string{
		types.OrchestrationOutboxPayloadDispatchAttempt,
		types.OrchestrationOutboxPayloadResumeExcludeRunID,
		types.OrchestrationOutboxPayloadResumeCheckpointRunID,
	} {
		if value, ok := payload[key]; ok && value != nil {
			delivery[key] = value
		}
	}
	if boolFromAny(payload[types.OrchestrationOutboxPayloadResumeSession]) {
		delivery[types.OrchestrationOutboxPayloadResumeSession] = true
	}
}

func intFromAny(value any) int {
	switch typed := value.(type) {
	case int:
		return typed
	case int32:
		return int(typed)
	case int64:
		return int(typed)
	case float32:
		return int(typed)
	case float64:
		return int(typed)
	case string:
		var parsed int
		if _, err := fmt.Sscanf(strings.TrimSpace(typed), "%d", &parsed); err == nil {
			return parsed
		}
		return 0
	default:
		return 0
	}
}

func boolFromAny(value any) bool {
	switch typed := value.(type) {
	case bool:
		return typed
	case int:
		return typed != 0
	case int32:
		return typed != 0
	case int64:
		return typed != 0
	case float32:
		return typed != 0
	case float64:
		return typed != 0
	case string:
		switch strings.ToLower(strings.TrimSpace(typed)) {
		case "1", "true", "yes", "on":
			return true
		default:
			return false
		}
	default:
		return false
	}
}

func executionInstanceKeyFromRun(run *types.AgentRun) string {
	if run == nil {
		return ""
	}
	if run.DeliveryJSON != nil {
		if raw, ok := run.DeliveryJSON[types.AgentExecutionMetaKeyInstanceKey]; ok {
			if s, ok := raw.(string); ok && s != "" {
				return s
			}
		}
	}
	return ExecutionClassKey(run.WorkspaceID, run.AgentID, nil, runPolicyFromRun(run))
}
