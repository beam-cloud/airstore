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
	"github.com/rs/zerolog/log"
)

type Service struct {
	backend            repository.BackendRepository
	taskQueue          repository.TaskQueue
	redis              *common.RedisClient
	s2                 *common.S2Client
	defaultImage       string
	queueRouter        *EnvelopeQueueRouter
	instanceController *ExecutionInstanceController
}

func NewService(
	ctx context.Context,
	backend repository.BackendRepository,
	taskQueue repository.TaskQueue,
	redis *common.RedisClient,
	s2 *common.S2Client,
	defaultImage string,
) *Service {
	queueStore := repository.NewAgentEnvelopeQueueStore(backend, redis)
	instanceLocker := repository.NewAgentInstanceDispatchLocker(redis)
	return &Service{
		backend:            backend,
		taskQueue:          taskQueue,
		redis:              redis,
		s2:                 s2,
		defaultImage:       defaultImage,
		queueRouter:        NewEnvelopeQueueRouter(queueStore),
		instanceController: NewExecutionInstanceController(ctx, backend, instanceLocker, common.Keys.AgentInstanceLock),
	}
}

func (s *Service) Start(ctx context.Context) {
	go s.dispatchLoop(ctx)
}

func (s *Service) AcceptAgentCommand(
	ctx context.Context,
	workspaceID uint,
	params AgentCommandParams,
) (*types.AgentTaskEnvelope, bool, error) {
	if err := ValidateAgentCommandParams(&params); err != nil {
		return nil, false, err
	}
	existing, err := s.backend.GetAgentTaskEnvelopeByIdempotency(ctx, workspaceID, params.AgentID, params.IdempotencyKey)
	if err == nil {
		return existing, true, nil
	}

	runPolicy := DefaultRunExecutionPolicy()
	instanceKey := ExecutionClassKey(workspaceID, params.AgentID, params.Lane, runPolicy)

	payload := map[string]any{
		"message":             params.Message,
		"session_id":          params.SessionID,
		"session_key":         params.SessionKey,
		"agent_id":            params.AgentID,
		"timeout_ms":          timeoutOrDefault(params.TimeoutMs, 600000),
		"policy":              runPolicy,
		"lane":                params.Lane,
		"extra_system_prompt": params.ExtraSystemPrompt,
		"input_provenance":    params.InputProvenance,
		"deliver":             params.Deliver,
		"attachments":         params.Attachments,
		"instance_key":        instanceKey,
	}

	envelope := &types.AgentTaskEnvelope{
		WorkspaceID:    workspaceID,
		AgentID:        params.AgentID,
		Kind:           types.AgentEnvelopeKindAgentCommand,
		QueueMode:      types.AgentQueueModeQueue,
		State:          types.AgentEnvelopeStateAccepted,
		IdempotencyKey: params.IdempotencyKey,
		PayloadJSON:    payload,
		RoutingJSON:    routingToMap(params.Routing),
	}
	if err := s.backend.CreateAgentTaskEnvelope(ctx, envelope); err != nil {
		if existing, lookupErr := s.backend.GetAgentTaskEnvelopeByIdempotency(ctx, workspaceID, params.AgentID, params.IdempotencyKey); lookupErr == nil {
			return existing, true, nil
		}
		return nil, false, err
	}

	if err := s.queueRouter.Enqueue(ctx, envelope, instanceKey); err != nil {
		return nil, false, err
	}
	return envelope, false, nil
}

func (s *Service) AcceptRunInput(
	ctx context.Context,
	workspaceID uint,
	targetRunID string,
	queueMode types.AgentQueueMode,
	message string,
	idempotencyKey string,
) (*types.AgentTaskEnvelope, bool, error) {
	if strings.TrimSpace(message) == "" {
		return nil, false, fmt.Errorf("message is required")
	}
	if strings.TrimSpace(idempotencyKey) == "" {
		return nil, false, fmt.Errorf("idempotency_key is required")
	}

	run, err := s.backend.GetAgentRun(ctx, workspaceID, targetRunID)
	if err != nil {
		return nil, false, err
	}

	existing, err := s.backend.GetAgentTaskEnvelopeByIdempotency(ctx, workspaceID, run.AgentID, idempotencyKey)
	if err == nil {
		return existing, true, nil
	}

	instanceKey := executionInstanceKeyFromRun(run)
	payload := map[string]any{
		"message":      message,
		"session_id":   run.SessionID,
		"session_key":  run.SessionKey,
		"agent_id":     run.AgentID,
		"timeout_ms":   run.TimeoutMs,
		"instance_key": instanceKey,
	}
	envelope := &types.AgentTaskEnvelope{
		WorkspaceID:    workspaceID,
		AgentID:        run.AgentID,
		Kind:           types.AgentEnvelopeKindRunInput,
		QueueMode:      queueMode,
		State:          types.AgentEnvelopeStateAccepted,
		IdempotencyKey: idempotencyKey,
		PayloadJSON:    payload,
		RoutingJSON:    map[string]any{},
		TargetRunID:    &targetRunID,
	}
	if err := s.backend.CreateAgentTaskEnvelope(ctx, envelope); err != nil {
		if existing, lookupErr := s.backend.GetAgentTaskEnvelopeByIdempotency(ctx, workspaceID, run.AgentID, idempotencyKey); lookupErr == nil {
			return existing, true, nil
		}
		return nil, false, err
	}
	if err := s.queueRouter.Enqueue(ctx, envelope, instanceKey); err != nil {
		return nil, false, err
	}
	return envelope, false, nil
}

func (s *Service) dispatchLoop(ctx context.Context) {
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

		envelopeID, err := s.queueRouter.ResolveEnvelopeID(ctx, token)
		if err != nil {
			log.Warn().Err(err).Str("token", token).Msg("resolve dispatch token failed")
			continue
		}
		if envelopeID == "" {
			continue
		}

		if err := s.dispatchEnvelope(ctx, envelopeID); err != nil {
			log.Warn().Err(err).Str("envelope_id", envelopeID).Msg("dispatch envelope failed")
		}
	}
}

func (s *Service) dispatchEnvelope(ctx context.Context, envelopeID string) error {
	envelope, err := s.backend.GetAgentTaskEnvelopeByID(ctx, envelopeID)
	if err != nil {
		return err
	}
	if envelope.State != types.AgentEnvelopeStateQueued && envelope.State != types.AgentEnvelopeStateAccepted {
		return nil
	}

	switch envelope.QueueMode {
	case types.AgentQueueModeInterrupt:
		return s.handleInterruptEnvelope(ctx, envelope)
	default:
		return s.handleExecutionEnvelope(ctx, envelope)
	}
}

func (s *Service) handleInterruptEnvelope(ctx context.Context, envelope *types.AgentTaskEnvelope) error {
	if envelope.TargetRunID == nil {
		reason := "interrupt_missing_target"
		return s.backend.UpdateAgentTaskEnvelopeState(ctx, envelope.ID, types.AgentEnvelopeStateDropped, &reason, nil)
	}

	run, err := s.backend.GetAgentRunByID(ctx, *envelope.TargetRunID)
	if err != nil {
		return err
	}

	now := time.Now()
	errMsg := "interrupted by queued input"
	if err := s.backend.UpdateAgentRunLifecycle(ctx, run.ID, types.AgentRunStatusCancelled, nil, &now, &errMsg); err != nil {
		return err
	}
	_ = s.appendRunSnapshot(ctx, run.ID, types.AgentRunStatusCancelled, nil, &now, &errMsg, map[string]any{"cause": "interrupt"})
	_ = s.publishRunEvent(ctx, run.ID, "interrupted", map[string]any{"envelope_id": envelope.ID})

	attempts, _ := s.backend.ListAgentRunAttempts(ctx, run.ID)
	for _, attempt := range attempts {
		if attempt.ExecutionTaskExternalID != nil && (attempt.Status == types.AgentAttemptStatusPending || attempt.Status == types.AgentAttemptStatusRunning) {
			_ = s.backend.CancelTask(ctx, *attempt.ExecutionTaskExternalID)
		}
	}
	return s.backend.UpdateAgentTaskEnvelopeState(ctx, envelope.ID, types.AgentEnvelopeStateDispatched, nil, envelope.TargetRunID)
}

func (s *Service) handleExecutionEnvelope(ctx context.Context, envelope *types.AgentTaskEnvelope) error {
	instanceKey := stringFromPayload(envelope.PayloadJSON, "instance_key")
	if instanceKey == "" {
		p := DefaultRunExecutionPolicy()
		if rawPolicy, ok := envelope.PayloadJSON["policy"]; ok {
			body, _ := json.Marshal(rawPolicy)
			_ = json.Unmarshal(body, &p)
			p = NormalizeRunExecutionPolicy(p)
		}
		lane := strPtrMaybe(stringFromPayload(envelope.PayloadJSON, "lane"))
		instanceKey = ExecutionClassKey(envelope.WorkspaceID, envelope.AgentID, lane, p)
	}

	if _, err := s.instanceController.EnsureInstance(ExecutionInstanceConfig{
		InstanceKey:            instanceKey,
		WorkspaceID:            envelope.WorkspaceID,
		AgentID:                envelope.AgentID,
		Lane:                   nil,
		ExecutionClassKey:      strings.TrimPrefix(instanceKey, "execclass_"),
		FailedAttemptThreshold: 5,
		InstanceLockKey:        common.Keys.AgentInstanceLock(instanceKey),
	}); err == nil {
		_ = s.instanceController.RouteDispatchTarget(instanceKey, 1)
	}

	if instance, err := s.backend.GetExecutionInstanceByKey(ctx, instanceKey); err == nil {
		desired := instance.DesiredDispatchConcurrency
		if desired <= 0 {
			desired = 1
			_ = s.instanceController.RouteDispatchTarget(instanceKey, desired)
		}
		if instance.RunningAttempts >= desired {
			// Capacity is currently saturated for this execution class.
			_ = s.queueRouter.RequeueEnvelope(ctx, envelope.ID)
			return nil
		}
	}

	run, runPolicy, instanceKey, prompt, err := s.materializeRun(ctx, envelope)
	if err != nil {
		reason := "run_materialization_failed"
		_ = s.backend.UpdateAgentTaskEnvelopeState(ctx, envelope.ID, types.AgentEnvelopeStateDropped, &reason, envelope.TargetRunID)
		return err
	}

	attempt := &types.AgentRunAttempt{
		RunID:           run.ID,
		AttemptNo:       1,
		Status:          types.AgentAttemptStatusPending,
		Strategy:        "primary",
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
		return err
	}

	if attempt.Status == types.AgentAttemptStatusBlocked {
		return nil
	}

	_, memberToken, err := s.backend.EnsureWorkspaceServiceToken(ctx, run.WorkspaceID)
	if err != nil {
		return err
	}

	execTask := &types.Task{
		WorkspaceId:       run.WorkspaceID,
		MemberToken:       memberToken,
		Status:            types.TaskStatusPending,
		Type:              ToTaskType(runPolicy),
		Prompt:            prompt,
		Image:             s.defaultImage,
		Entrypoint:        []string{},
		Env:               map[string]string{},
		Resources:         ToTaskResources(runPolicy),
		RunAttemptID:      &attempt.ID,
		TimeoutMs:         &run.TimeoutMs,
		ExecHost:          strPtr(run.ExecHost),
		ExecSecurity:      strPtr(run.ExecSecurity),
		ExecAsk:           strPtr(run.ExecAsk),
		RuntimeType:       strPtr(run.RuntimeType),
		WorkspaceAccess:   strPtr(run.WorkspaceAccess),
		NetworkEnabled:    boolPtr(run.NetworkEnabled),
		ExecutionPolicy:   map[string]any{"host": run.ExecHost, "security": run.ExecSecurity, "ask": run.ExecAsk, "runtime_type": run.RuntimeType, "workspace_access": run.WorkspaceAccess, "network_enabled": run.NetworkEnabled, "interactive": run.Interactive},
		CreatedByMemberId: nil,
	}
	if err := s.backend.CreateTask(ctx, execTask); err != nil {
		return err
	}
	if err := s.backend.BindAttemptExecutionTask(ctx, attempt.ID, execTask.ExternalId); err != nil {
		return err
	}
	if err := s.taskQueue.Push(ctx, execTask); err != nil {
		return err
	}
	return nil
}

func (s *Service) materializeRun(
	ctx context.Context,
	envelope *types.AgentTaskEnvelope,
) (*types.AgentRun, RunExecutionPolicy, string, string, error) {
	payload := envelope.PayloadJSON
	prompt := strings.TrimSpace(stringFromPayload(payload, "message"))
	if prompt == "" {
		prompt = strings.TrimSpace(stringFromPayload(payload, "prompt"))
	}
	if prompt == "" {
		return nil, RunExecutionPolicy{}, "", "", fmt.Errorf("missing prompt/message in envelope payload")
	}

	sessionID := stringFromPayload(payload, "session_id")
	if sessionID == "" {
		return nil, RunExecutionPolicy{}, "", "", fmt.Errorf("missing session_id in payload")
	}
	timeoutMs := intFromPayload(payload, "timeout_ms", 600000)
	if timeoutMs < 0 {
		return nil, RunExecutionPolicy{}, "", "", fmt.Errorf("timeout_ms must be >= 0")
	}

	runPolicy := DefaultRunExecutionPolicy()
	if rawPolicy, ok := payload["policy"]; ok {
		b, _ := json.Marshal(rawPolicy)
		_ = json.Unmarshal(b, &runPolicy)
		runPolicy = NormalizeRunExecutionPolicy(runPolicy)
	}
	if err := ValidateRunExecutionPolicy(runPolicy); err != nil {
		return nil, RunExecutionPolicy{}, "", "", err
	}

	sessionKey := strPtrMaybe(stringFromPayload(payload, "session_key"))
	agentID := envelope.AgentID
	instanceKey := stringFromPayload(payload, "instance_key")
	if instanceKey == "" {
		lane := strPtrMaybe(stringFromPayload(payload, "lane"))
		instanceKey = ExecutionClassKey(envelope.WorkspaceID, agentID, lane, runPolicy)
	}

	var provider *string
	if v := stringFromPayload(payload, "provider"); v != "" {
		provider = &v
	}
	var model *string
	if v := stringFromPayload(payload, "model"); v != "" {
		model = &v
	}

	run := &types.AgentRun{
		WorkspaceID:      envelope.WorkspaceID,
		AgentID:          agentID,
		OriginEnvelopeID: envelope.ID,
		Status:           types.AgentRunStatusAccepted,
		SessionID:        sessionID,
		SessionKey:       sessionKey,
		Provider:         provider,
		Model:            model,
		ExecHost:         string(runPolicy.Host),
		ExecSecurity:     string(runPolicy.Security),
		ExecAsk:          string(runPolicy.Ask),
		RuntimeType:      runPolicy.RuntimeType,
		WorkspaceAccess:  runPolicy.WorkspaceAccess,
		NetworkEnabled:   runPolicy.NetworkEnabled,
		Interactive:      runPolicy.Interactive,
		TimeoutMs:        timeoutMs,
		UsageJSON:        map[string]any{},
		DeliveryJSON:     map[string]any{"instance_key": instanceKey},
	}
	if envelope.Kind == types.AgentEnvelopeKindRunInput && envelope.TargetRunID != nil {
		targetRun, err := s.backend.GetAgentRunByID(ctx, *envelope.TargetRunID)
		if err != nil {
			return nil, RunExecutionPolicy{}, "", "", err
		}
		run.AgentID = targetRun.AgentID
		run.SessionID = targetRun.SessionID
		run.SessionKey = targetRun.SessionKey
		run.Provider = targetRun.Provider
		run.Model = targetRun.Model
		run.ExecHost = targetRun.ExecHost
		run.ExecSecurity = targetRun.ExecSecurity
		run.ExecAsk = targetRun.ExecAsk
		run.RuntimeType = targetRun.RuntimeType
		run.WorkspaceAccess = targetRun.WorkspaceAccess
		run.NetworkEnabled = targetRun.NetworkEnabled
		run.Interactive = targetRun.Interactive
		run.TimeoutMs = targetRun.TimeoutMs
		runPolicy = RunExecutionPolicy{
			Host:            ExecHost(targetRun.ExecHost),
			Security:        ExecSecurity(targetRun.ExecSecurity),
			Ask:             ExecAsk(targetRun.ExecAsk),
			RuntimeType:     targetRun.RuntimeType,
			WorkspaceAccess: targetRun.WorkspaceAccess,
			NetworkEnabled:  targetRun.NetworkEnabled,
			Interactive:     targetRun.Interactive,
			Resources:       map[string]any{},
		}
		instanceKey = executionInstanceKeyFromRun(targetRun)
		run.DeliveryJSON = map[string]any{"instance_key": instanceKey, "target_run_id": targetRun.ID}
	}

	if err := s.backend.CreateAgentRun(ctx, run); err != nil {
		return nil, RunExecutionPolicy{}, "", "", err
	}
	if err := s.backend.UpdateAgentTaskEnvelopeState(ctx, envelope.ID, types.AgentEnvelopeStateDispatched, nil, &run.ID); err != nil {
		return nil, RunExecutionPolicy{}, "", "", err
	}
	if err := s.appendRunSnapshot(ctx, run.ID, types.AgentRunStatusAccepted, nil, nil, nil, map[string]any{"envelope_id": envelope.ID}); err != nil {
		log.Warn().Err(err).Str("run_id", run.ID).Msg("failed to append accepted snapshot")
	}
	_ = s.publishRunEvent(ctx, run.ID, "accepted", map[string]any{"envelope_id": envelope.ID})
	return run, runPolicy, instanceKey, prompt, nil
}

func (s *Service) appendRunSnapshot(
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

func (s *Service) publishRunEvent(ctx context.Context, runID, eventType string, payload map[string]any) error {
	event := map[string]any{
		"run_id":     runID,
		"event_type": eventType,
		"ts":         time.Now().UnixMilli(),
		"payload":    payload,
	}
	if s.s2 != nil && s.s2.Enabled() {
		if err := s.s2.AppendRunEvent(ctx, runID, eventType, payload); err != nil {
			log.Warn().Err(err).Str("run_id", runID).Msg("failed to append run event to s2")
		}
	}
	if s.redis != nil {
		body, _ := json.Marshal(event)
		pipe := s.redis.Pipeline()
		pipe.Publish(ctx, common.Keys.AgentRunEventsChannel(runID), body)
		pipe.RPush(ctx, common.Keys.AgentRunEventsBuffer(runID), body)
		pipe.Expire(ctx, common.Keys.AgentRunEventsBuffer(runID), 24*time.Hour)
		_, err := pipe.Exec(ctx)
		return err
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

func executionInstanceKeyFromRun(run *types.AgentRun) string {
	if run == nil {
		return ""
	}
	if run.DeliveryJSON != nil {
		if raw, ok := run.DeliveryJSON["instance_key"]; ok {
			if s, ok := raw.(string); ok && s != "" {
				return s
			}
		}
	}
	return ExecutionClassKey(run.WorkspaceID, run.AgentID, nil, RunExecutionPolicy{
		Host:            ExecHost(run.ExecHost),
		Security:        ExecSecurity(run.ExecSecurity),
		Ask:             ExecAsk(run.ExecAsk),
		RuntimeType:     run.RuntimeType,
		WorkspaceAccess: run.WorkspaceAccess,
		NetworkEnabled:  run.NetworkEnabled,
		Interactive:     run.Interactive,
		Resources:       map[string]any{},
	})
}
