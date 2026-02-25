package orchestration

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/beam-cloud/airstore/pkg/common"
	"github.com/beam-cloud/airstore/pkg/repository"
	"github.com/beam-cloud/airstore/pkg/types"
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
)

type AgentService struct {
	backend            repository.BackendRepository
	taskQueue          repository.TaskQueue
	orchestrationStore *repository.OrchestrationStore
	terminalIO         repository.TerminalIORepository
	s2                 *common.S2Client
	defaultImage       string
	queueRouter        *TaskQueueRouter
	instanceController *ExecutionInstanceController
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
		queueRouter:        NewTaskQueueRouter(orchestrationStore),
		instanceController: NewExecutionInstanceController(ctx, backend, orchestrationStore, common.Keys.AgentInstanceLock),
	}
}

func (s *AgentService) Start(ctx context.Context) {
	go s.dispatchLoop(ctx)
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
	existing, err := s.backend.GetTaskByIdempotency(ctx, workspaceID, params.AgentID, params.IdempotencyKey)
	if err == nil {
		return existing, true, nil
	}

	runPolicy := DefaultRunExecutionPolicy()
	if params.Policy != nil {
		runPolicy = NormalizeRunExecutionPolicy(*params.Policy)
	}
	instanceKey := ExecutionClassKey(workspaceID, params.AgentID, params.Lane, runPolicy)
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
		agentConfig = cloneAnyMap(profile.ConfigJSON)
		agentProvider = agentConfigString(agentConfig, "provider", "llm_provider")
		agentModel = agentConfigString(agentConfig, "model", "default_model", "llm_model")
	}

	payload := map[string]any{
		"message":                              params.Message,
		"session_id":                           params.SessionID,
		"session_key":                          params.SessionKey,
		"agent_id":                             params.AgentID,
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
	}
	if len(agentConfig) > 0 {
		payload["agent_config"] = agentConfig
	}
	if agentProvider != "" {
		payload["provider"] = agentProvider
	}
	if agentModel != "" {
		payload["model"] = agentModel
	}

	task := &types.AgentTask{
		WorkspaceID:    workspaceID,
		AgentID:        params.AgentID,
		Kind:           types.AgentTaskKindAgentCommand,
		QueueMode:      types.AgentQueueModeQueue,
		State:          types.AgentTaskStateAccepted,
		IdempotencyKey: params.IdempotencyKey,
		PayloadJSON:    payload,
		RoutingJSON:    routingToMap(params.Routing),
	}
	if err := s.backend.CreateTask(ctx, task); err != nil {
		if existing, lookupErr := s.backend.GetTaskByIdempotency(ctx, workspaceID, params.AgentID, params.IdempotencyKey); lookupErr == nil {
			return existing, true, nil
		}
		return nil, false, err
	}

	if err := s.queueRouter.Enqueue(ctx, task, instanceKey); err != nil {
		return nil, false, err
	}
	return task, false, nil
}

func (s *AgentService) AcceptRunInput(
	ctx context.Context,
	workspaceID uint,
	targetRunID string,
	queueMode types.AgentQueueMode,
	message string,
	idempotencyKey string,
) (*types.AgentTask, bool, error) {
	if strings.TrimSpace(message) == "" {
		return nil, false, fmt.Errorf("message is required")
	}
	idempotencyKey = normalizeGeneratedID(idempotencyKey)

	run, err := s.backend.GetAgentRun(ctx, workspaceID, targetRunID)
	if err != nil {
		return nil, false, err
	}

	existing, err := s.backend.GetTaskByIdempotency(ctx, workspaceID, run.AgentID, idempotencyKey)
	if err == nil {
		return existing, true, nil
	}

	instanceKey := executionInstanceKeyFromRun(run)
	payload := map[string]any{
		"message":                              message,
		"session_id":                           run.SessionID,
		"session_key":                          run.SessionKey,
		"agent_id":                             run.AgentID,
		"timeout_ms":                           run.TimeoutMs,
		types.AgentExecutionMetaKeyInstanceKey: instanceKey,
	}
	task := &types.AgentTask{
		WorkspaceID:    workspaceID,
		AgentID:        run.AgentID,
		Kind:           types.AgentTaskKindRunInput,
		QueueMode:      queueMode,
		State:          types.AgentTaskStateAccepted,
		IdempotencyKey: idempotencyKey,
		PayloadJSON:    payload,
		RoutingJSON:    map[string]any{},
		TargetRunID:    &targetRunID,
	}
	if err := s.backend.CreateTask(ctx, task); err != nil {
		if existing, lookupErr := s.backend.GetTaskByIdempotency(ctx, workspaceID, run.AgentID, idempotencyKey); lookupErr == nil {
			return existing, true, nil
		}
		return nil, false, err
	}
	if err := s.queueRouter.Enqueue(ctx, task, instanceKey); err != nil {
		return nil, false, err
	}
	return task, false, nil
}

func (s *AgentService) dispatchLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		token, err := s.queueRouter.Pop(ctx, 2*time.Second)
		if err != nil {
			log.Warn().Err(err).Msg("orchestration dispatch pop failed")
			continue
		}
		if token == "" {
			continue
		}

		taskID, err := s.queueRouter.ResolveTaskID(ctx, token)
		if err != nil {
			log.Warn().Err(err).Str("token", token).Msg("resolve dispatch token failed")
			continue
		}
		if taskID == "" {
			continue
		}

		if err := s.dispatchTask(ctx, taskID); err != nil {
			log.Warn().Err(err).Str("task_id", taskID).Msg("dispatch task failed")
			if requeueErr := s.requeueIfDispatchable(ctx, taskID); requeueErr != nil {
				log.Warn().Err(requeueErr).Str("task_id", taskID).Msg("failed to requeue task after dispatch error")
			}
		}
	}
}

func (s *AgentService) dispatchTask(ctx context.Context, taskID string) error {
	task, err := s.backend.GetTaskByID(ctx, taskID)
	if err != nil {
		return err
	}
	if task.State != types.AgentTaskStateQueued && task.State != types.AgentTaskStateAccepted {
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
		return s.backend.UpdateTaskState(ctx, task.ID, types.AgentTaskStateDropped, &reason, nil)
	}

	run, err := s.backend.GetAgentRunByID(ctx, *task.TargetRunID)
	if err != nil {
		return err
	}

	attempts, _ := s.backend.ListAgentRunAttempts(ctx, run.ID)
	for _, attempt := range attempts {
		if attempt.ExecutionID != nil && attempt.Status.IsInFlight() {
			_ = s.backend.CancelRunExecution(ctx, *attempt.ExecutionID)
		}
	}
	if task.Kind == types.AgentTaskKindRunInput {
		_ = s.publishRunEvent(ctx, run.ID, types.AgentRunEventInterrupted, map[string]any{
			"task_id": task.ID,
			"action":  "cancel_then_continue",
		})
		if err := s.backend.UpdateAgentRunLifecycle(ctx, run.ID, types.AgentRunStatusAccepted, nil, nil, nil); err != nil {
			return err
		}
		return s.handleRunInputTask(ctx, task)
	}

	now := time.Now()
	errMsg := "interrupted by queued input"
	if err := s.backend.UpdateAgentRunLifecycle(ctx, run.ID, types.AgentRunStatusCancelled, nil, &now, &errMsg); err != nil {
		return err
	}
	_ = s.appendRunSnapshot(ctx, run.ID, types.AgentRunStatusCancelled, nil, &now, &errMsg, map[string]any{"cause": "interrupt"})
	_ = s.publishRunEvent(ctx, run.ID, types.AgentRunEventInterrupted, map[string]any{"task_id": task.ID})
	return s.backend.UpdateTaskState(ctx, task.ID, types.AgentTaskStateDispatched, nil, task.TargetRunID)
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

	if task.Kind == types.AgentTaskKindRunInput && task.QueueMode == types.AgentQueueModeSteer && task.TargetRunID != nil {
		steered, err := s.trySteerRunInputTask(ctx, task)
		if err != nil {
			return err
		}
		if steered {
			return nil
		}
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
		// Capacity is currently saturated for this execution class.
		if err := s.queueRouter.RequeueTask(ctx, task.ID); err != nil {
			return err
		}
		return nil
	}

	if task.Kind == types.AgentTaskKindRunInput && task.TargetRunID != nil {
		return s.handleRunInputTask(ctx, task)
	}

	run, runPolicy, prompt, err := s.materializeRun(ctx, task)
	if err != nil {
		reason := types.AgentTaskDropReasonRunMaterializationFail
		_ = s.backend.UpdateTaskState(ctx, task.ID, types.AgentTaskStateDropped, &reason, task.TargetRunID)
		return err
	}

	_, err = s.createAttemptExecutionTask(
		ctx,
		run,
		runPolicy,
		prompt,
		mapFromPayload(task.PayloadJSON, "agent_config"),
		task.PayloadJSON,
	)
	return err
}

func (s *AgentService) trySteerRunInputTask(ctx context.Context, task *types.AgentTask) (bool, error) {
	if task == nil || task.TargetRunID == nil || s.terminalIO == nil {
		return false, nil
	}

	run, err := s.backend.GetAgentRun(ctx, task.WorkspaceID, *task.TargetRunID)
	if err != nil {
		return false, err
	}
	if !run.Status.IsSteerEligible() {
		return false, nil
	}

	prompt := runInputPrompt(task.PayloadJSON)
	if prompt == "" {
		return false, nil
	}

	attempts, err := s.backend.ListAgentRunAttempts(ctx, run.ID)
	if err != nil {
		return false, err
	}

	var activeAttempt *types.AgentRunAttempt
	for i := len(attempts) - 1; i >= 0; i-- {
		attempt := attempts[i]
		if attempt == nil {
			continue
		}
		if attempt.Status != types.AgentAttemptStatusRunning || attempt.ExecutionID == nil {
			continue
		}
		if strings.TrimSpace(*attempt.ExecutionID) == "" {
			continue
		}
		activeAttempt = attempt
		break
	}
	if activeAttempt == nil || activeAttempt.ExecutionID == nil {
		return false, nil
	}

	execTask, err := s.backend.GetRunExecution(ctx, *activeAttempt.ExecutionID)
	if err != nil {
		return false, nil
	}
	if execTask == nil || !execTask.IsInteractive() || execTask.IsTerminal() {
		return false, nil
	}

	input := []byte(prompt)
	if !strings.HasSuffix(prompt, "\n") {
		input = append(input, '\n')
	}
	if err := s.terminalIO.PublishInput(ctx, execTask.ExternalId, input); err != nil {
		return false, nil
	}

	if err := s.backend.UpdateTaskState(ctx, task.ID, types.AgentTaskStateDispatched, nil, task.TargetRunID); err != nil {
		return false, err
	}

	_ = s.publishRunEvent(ctx, run.ID, types.AgentRunEventInputSteered, map[string]any{
		"task_id":      task.ID,
		"queue_mode":   task.QueueMode,
		"execution_id": execTask.ExternalId,
		"attempt_id":   activeAttempt.ID,
	})
	return true, nil
}

func (s *AgentService) handleRunInputTask(ctx context.Context, task *types.AgentTask) error {
	if task.TargetRunID == nil {
		reason := types.AgentTaskDropReasonRunInputMissingTarget
		return s.backend.UpdateTaskState(ctx, task.ID, types.AgentTaskStateDropped, &reason, nil)
	}

	run, err := s.backend.GetAgentRun(ctx, task.WorkspaceID, *task.TargetRunID)
	if err != nil {
		return err
	}
	if run.Status.IsTerminal() {
		reason := types.AgentTaskDropReasonRunInputTerminalTarget
		return s.backend.UpdateTaskState(ctx, task.ID, types.AgentTaskStateDropped, &reason, task.TargetRunID)
	}

	prompt := runInputPrompt(task.PayloadJSON)
	if prompt == "" {
		reason := types.AgentTaskDropReasonRunInputMissingMessage
		return s.backend.UpdateTaskState(ctx, task.ID, types.AgentTaskStateDropped, &reason, task.TargetRunID)
	}

	runPolicy := runPolicyFromRun(run)
	agentConfig := mapFromPayload(task.PayloadJSON, "agent_config")
	if len(agentConfig) == 0 && run.AgentID != nil {
		if profile, err := s.backend.GetAgentProfile(ctx, task.WorkspaceID, *run.AgentID); err == nil {
			agentConfig = cloneAnyMap(profile.ConfigJSON)
		}
	}
	if _, err := s.createAttemptExecutionTask(
		ctx,
		run,
		runPolicy,
		prompt,
		agentConfig,
		task.PayloadJSON,
	); err != nil {
		return err
	}
	if err := s.backend.UpdateTaskState(ctx, task.ID, types.AgentTaskStateDispatched, nil, task.TargetRunID); err != nil {
		return err
	}
	eventType := types.AgentRunEventInputDispatched
	if task.QueueMode == types.AgentQueueModeSteer {
		eventType = types.AgentRunEventSteerFallbackDispatched
	}
	_ = s.publishRunEvent(ctx, run.ID, eventType, map[string]any{
		"task_id":    task.ID,
		"queue_mode": task.QueueMode,
		"mode":       "followup_attempt",
	})
	return nil
}

func runInputPrompt(payload map[string]any) string {
	prompt := strings.TrimSpace(stringFromPayload(payload, "message"))
	if prompt == "" {
		prompt = strings.TrimSpace(stringFromPayload(payload, "prompt"))
	}
	return prompt
}

func (s *AgentService) materializeRun(
	ctx context.Context,
	task *types.AgentTask,
) (*types.AgentRun, RunExecutionPolicy, string, error) {
	payload := task.PayloadJSON
	prompt := strings.TrimSpace(stringFromPayload(payload, "message"))
	if prompt == "" {
		prompt = strings.TrimSpace(stringFromPayload(payload, "prompt"))
	}
	if prompt == "" {
		return nil, RunExecutionPolicy{}, "", fmt.Errorf("missing prompt/message in task payload")
	}
	if extraSystemPrompt := strings.TrimSpace(stringFromPayload(payload, "extra_system_prompt")); extraSystemPrompt != "" {
		prompt = extraSystemPrompt + "\n\n" + prompt
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
	if err := ValidateRunExecutionPolicy(runPolicy); err != nil {
		return nil, RunExecutionPolicy{}, "", err
	}

	sessionKey := strPtrMaybe(stringFromPayload(payload, "session_key"))
	agentID := task.AgentID
	instanceKey := instanceKeyFromPayload(task.WorkspaceID, agentID, payload, runPolicy)
	provider, model := providerModelFromPayload(payload)

	run := &types.AgentRun{
		WorkspaceID:     task.WorkspaceID,
		AgentID:         agentID,
		OriginTaskID:    task.ID,
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

	if err := s.backend.CreateAgentRun(ctx, run); err != nil {
		return nil, RunExecutionPolicy{}, "", err
	}
	if err := s.backend.UpdateTaskState(ctx, task.ID, types.AgentTaskStateDispatched, nil, &run.ID); err != nil {
		return nil, RunExecutionPolicy{}, "", err
	}
	if err := s.appendRunSnapshot(ctx, run.ID, types.AgentRunStatusAccepted, nil, nil, nil, map[string]any{"task_id": task.ID}); err != nil {
		log.Warn().Err(err).Str("run_id", run.ID).Msg("failed to append accepted snapshot")
	}
	_ = s.publishRunEvent(ctx, run.ID, types.AgentRunEventAccepted, map[string]any{"task_id": task.ID})
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
		executionPolicy["provider"] = *run.Provider
	}
	if run.Model != nil {
		executionPolicy["model"] = *run.Model
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
		TimeoutMs:         &run.TimeoutMs,
		ExecHost:          strPtr(run.ExecHost),
		ExecSecurity:      strPtr(run.ExecSecurity),
		ExecAsk:           strPtr(run.ExecAsk),
		RuntimeType:       strPtr(run.RuntimeType),
		WorkspaceAccess:   strPtr(run.WorkspaceAccess),
		NetworkEnabled:    boolPtr(run.NetworkEnabled),
		ExecutionPolicy:   executionPolicy,
		CreatedByMemberId: nil,
	}
	if err := s.backend.CreateRunExecution(ctx, execTask); err != nil {
		return nil, err
	}
	if err := s.backend.BindAttemptExecutionTask(ctx, attempt.ID, execTask.ExternalId); err != nil {
		return nil, err
	}
	if err := s.taskQueue.Push(ctx, execTask); err != nil {
		return nil, err
	}
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

func providerModelFromPayload(payload map[string]any) (*string, *string) {
	provider := strPtrMaybe(stringFromPayload(payload, "provider"))
	model := strPtrMaybe(stringFromPayload(payload, "model"))
	if provider != nil && model != nil {
		return provider, model
	}
	agentConfig := mapFromPayload(payload, "agent_config")
	if provider == nil {
		provider = strPtrMaybe(agentConfigString(agentConfig, "provider", "llm_provider"))
	}
	if model == nil {
		model = strPtrMaybe(agentConfigString(agentConfig, "model", "default_model", "llm_model"))
	}
	return provider, model
}

func applyRunRuntimeEnv(env map[string]string, run *types.AgentRun) {
	if env == nil || run == nil {
		return
	}
	env["AIRSTORE_RUN_ID"] = strings.TrimSpace(run.ID)
	env["AIRSTORE_ORIGIN_TASK_ID"] = strings.TrimSpace(run.OriginTaskID)
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
}

func applyPayloadRuntimeEnv(env map[string]string, payload map[string]any) {
	if env == nil || len(payload) == 0 {
		return
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

func (s *AgentService) requeueIfDispatchable(ctx context.Context, taskID string) error {
	task, err := s.backend.GetTaskByID(ctx, taskID)
	if err != nil {
		return err
	}
	if task.State != types.AgentTaskStateAccepted && task.State != types.AgentTaskStateQueued {
		return nil
	}
	return s.queueRouter.RequeueTask(ctx, task.ID)
}
