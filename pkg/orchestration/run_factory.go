package orchestration

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/beam-cloud/airstore/pkg/common"
	"github.com/beam-cloud/airstore/pkg/repository"
	"github.com/beam-cloud/airstore/pkg/types"
	"github.com/rs/zerolog/log"
)

type RunFactoryConfig struct {
	Backend           repository.BackendRepository
	TaskQueue         repository.TaskQueue
	TerminalIO        repository.TerminalIORepository
	S2                *common.S2Client
	Lifecycle         *TaskLifecycle
	ResumeBarrier     *ResumeBarrier
	DefaultImage      string
	PublishTaskUpdate func(context.Context, uint, string)
}

type RunFactory struct {
	backend           repository.BackendRepository
	taskQueue         repository.TaskQueue
	terminalIO        repository.TerminalIORepository
	s2                *common.S2Client
	lifecycle         *TaskLifecycle
	resumeBarrier     *ResumeBarrier
	defaultImage      string
	publishTaskUpdate func(context.Context, uint, string)
}

func NewRunFactory(cfg RunFactoryConfig) *RunFactory {
	return &RunFactory{
		backend:           cfg.Backend,
		taskQueue:         cfg.TaskQueue,
		terminalIO:        cfg.TerminalIO,
		s2:                cfg.S2,
		lifecycle:         cfg.Lifecycle,
		resumeBarrier:     cfg.ResumeBarrier,
		defaultImage:      cfg.DefaultImage,
		publishTaskUpdate: cfg.PublishTaskUpdate,
	}
}

func (f *RunFactory) ResolveRunAgentConfig(
	ctx context.Context,
	run *types.AgentRun,
	payload map[string]any,
) map[string]any {
	agentConfig := mapFromPayload(payload, agentPayloadKeyAgentConfig)
	if len(agentConfig) == 0 {
		agentConfig = f.agentConfigForRun(ctx, run)
	}
	agentConfig = cloneAnyMap(agentConfig)
	if skills := skillNamesFromConfig(agentConfig); len(skills) > 0 {
		injectSkillsSection(agentConfig, skills)
	} else {
		strengthenSkillDirectives(agentConfig)
	}
	return agentConfig
}

func (f *RunFactory) MaterializeRun(
	ctx context.Context,
	task *types.AgentTask,
) (*types.AgentRun, RunExecutionPolicy, string, error) {
	payloadSpec := parseTaskCommandPayload(task.PayloadJSON)
	prompt := payloadSpec.PromptText()
	if prompt == "" {
		return nil, RunExecutionPolicy{}, "", fmt.Errorf("missing prompt/message in task payload")
	}
	if payloadSpec.ExtraSystemPrompt != nil && strings.TrimSpace(*payloadSpec.ExtraSystemPrompt) != "" {
		task.PayloadJSON["_extra_system_prompt_resolved"] = strings.TrimSpace(*payloadSpec.ExtraSystemPrompt)
	}

	sessionID := strings.TrimSpace(payloadSpec.SessionID)
	if sessionID == "" {
		return nil, RunExecutionPolicy{}, "", fmt.Errorf("missing session_id in payload")
	}
	timeoutMs := payloadSpec.TimeoutMs
	if timeoutMs < 0 {
		return nil, RunExecutionPolicy{}, "", fmt.Errorf("timeout_ms must be >= 0")
	}

	runPolicy := NormalizeRunExecutionPolicy(payloadSpec.Policy)
	provider := payloadSpec.Provider
	model := payloadSpec.Model
	if provider == nil {
		return nil, RunExecutionPolicy{}, "", fmt.Errorf("agent provider is required in task payload")
	}
	if !isSupportedProvider(*provider) {
		return nil, RunExecutionPolicy{}, "", fmt.Errorf("agent provider %q is not supported", *provider)
	}
	runPolicy.Interactive = true
	if err := ValidateRunExecutionPolicy(runPolicy); err != nil {
		return nil, RunExecutionPolicy{}, "", err
	}

	if payloadSpec.Resume.Enabled {
		if f.resumeBarrier != nil {
			if err := f.resumeBarrier.WaitForResume(
				ctx,
				task.WorkspaceID,
				sessionID,
				payloadSpec.Resume.CheckpointRunID,
				payloadSpec.Resume.excludeRunIDs()...,
			); err != nil {
				return nil, RunExecutionPolicy{}, "", err
			}
		}
	} else if f.resumeBarrier != nil {
		if err := f.resumeBarrier.ensureSessionAvailableForNewRun(ctx, task.WorkspaceID, sessionID); err != nil {
			return nil, RunExecutionPolicy{}, "", err
		}
	}

	sessionKey := payloadSpec.SessionKey
	agentID := task.AgentID
	instanceKey := instanceKeyFromPayload(task.WorkspaceID, agentID, task.PayloadJSON, runPolicy)

	run := &types.AgentRun{
		WorkspaceID:     task.WorkspaceID,
		AgentID:         agentID,
		OriginTaskID:    task.ID,
		HookID:          payloadSpec.HookID,
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
	applyDeliveryMetadata(run.DeliveryJSON, task.PayloadJSON, task.RoutingJSON)
	applyResumeDeliveryMetadata(run.DeliveryJSON, task.PayloadJSON)

	if err := f.backend.CreateAgentRun(ctx, run); err != nil {
		return nil, RunExecutionPolicy{}, "", err
	}
	if f.lifecycle != nil {
		if err := f.lifecycle.Dispatch(ctx, task.ID, run.ID); err != nil {
			return nil, RunExecutionPolicy{}, "", err
		}
	}
	if f.publishTaskUpdate != nil {
		f.publishTaskUpdate(ctx, task.WorkspaceID, task.ID)
	}
	if err := appendRunSnapshotWithBackend(ctx, f.backend, run.ID, types.AgentRunStatusAccepted, nil, nil, nil, map[string]any{
		types.AgentRunEventPayloadKeyTaskID: task.ID,
	}); err != nil {
		return nil, RunExecutionPolicy{}, "", err
	}
	return run, runPolicy, prompt, nil
}

func (f *RunFactory) CreateAttemptExecutionTask(
	ctx context.Context,
	run *types.AgentRun,
	runPolicy RunExecutionPolicy,
	prompt string,
	agentConfig map[string]any,
	payload map[string]any,
) (*types.AgentRunAttempt, error) {
	nextAttemptNo, err := f.nextRunAttemptNo(ctx, run.ID)
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
	if err := f.backend.CreateAgentRunAttempt(ctx, attempt); err != nil {
		return nil, err
	}
	if attempt.Status == types.AgentAttemptStatusBlocked {
		return attempt, nil
	}
	failProvisioning := func(err error) (*types.AgentRunAttempt, error) {
		markAttemptProvisioningFailed(ctx, f.backend, run, attempt, err)
		return nil, err
	}

	var memberToken string
	_, memberToken, err = f.backend.EnsureWorkspaceServiceToken(ctx, run.WorkspaceID)
	if err != nil {
		return failProvisioning(err)
	}
	taskEnv := map[string]string{}
	applyRunExecutionContextEnv(taskEnv, run, attempt.ID)
	applyAgentConfigEnv(taskEnv, agentConfig)
	applyPayloadRuntimeEnv(taskEnv, payload)
	ensureRuntimeSystemPromptEnv(taskEnv)
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

	applyViewRuntimeContext(ctx, f.backend, f.s2, taskEnv, executionPolicy, run, payload)
	applyAgentMailRuntimeContext(ctx, f.backend, taskEnv, run)

	execTask := &types.RunExecution{
		WorkspaceId:       run.WorkspaceID,
		MemberToken:       memberToken,
		Status:            types.RunExecutionStatusPending,
		Type:              ToRunExecutionType(runPolicy),
		Prompt:            prompt,
		Image:             f.defaultImage,
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
	err = f.backend.CreateRunExecution(ctx, execTask)
	if err != nil {
		return failProvisioning(err)
	}
	err = f.backend.BindAttemptExecutionTask(ctx, attempt.ID, execTask.ExternalId)
	if err != nil {
		return failProvisioning(err)
	}
	if f.taskQueue == nil {
		return failProvisioning(fmt.Errorf("task queue is unavailable"))
	}
	err = f.taskQueue.Push(ctx, execTask)
	if err != nil {
		return failProvisioning(err)
	}
	if f.publishTaskUpdate != nil {
		f.publishTaskUpdate(ctx, run.WorkspaceID, run.OriginTaskID)
	}
	return attempt, nil
}

func (f *RunFactory) nextRunAttemptNo(ctx context.Context, runID string) (int, error) {
	attempts, err := f.backend.ListAgentRunAttempts(ctx, runID)
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

func (f *RunFactory) agentConfigForRun(ctx context.Context, run *types.AgentRun) map[string]any {
	if run == nil || run.AgentID == nil || strings.TrimSpace(*run.AgentID) == "" {
		return map[string]any{}
	}
	profile, err := f.backend.GetAgentProfile(ctx, run.WorkspaceID, *run.AgentID)
	if err != nil {
		return map[string]any{}
	}
	return cloneAnyMap(profile.ConfigJSON)
}

func appendRunSnapshotWithBackend(
	ctx context.Context,
	backend repository.BackendRepository,
	runID string,
	status types.AgentRunStatus,
	startedAt *time.Time,
	endedAt *time.Time,
	errorMsg *string,
	payload map[string]any,
) error {
	seq, err := backend.IncrementAgentRunSnapshotSeq(ctx, runID)
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
	return backend.AppendAgentRunSnapshot(ctx, &types.AgentRunSnapshot{
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

func markAttemptProvisioningFailed(
	ctx context.Context,
	backend repository.BackendRepository,
	run *types.AgentRun,
	attempt *types.AgentRunAttempt,
	cause error,
) {
	if backend == nil || run == nil || attempt == nil || cause == nil {
		return
	}
	errText := strings.TrimSpace(cause.Error())
	if errText == "" {
		errText = "failed to provision run attempt"
	}
	now := time.Now()
	if err := backend.UpdateAgentRunAttemptResult(
		ctx,
		attempt.ID,
		types.AgentAttemptStatusError,
		nil,
		now,
		&errText,
	); err != nil {
		log.Warn().
			Err(err).
			Str("run_id", run.ID).
			Str("attempt_id", attempt.ID).
			Msg("failed to mark provisioning attempt as errored")
	}
	if err := appendRunSnapshotWithBackend(
		ctx,
		backend,
		run.ID,
		types.AgentRunStatusError,
		nil,
		&now,
		&errText,
		map[string]any{
			types.AgentRunEventPayloadKeyTaskID: run.OriginTaskID,
		},
	); err != nil {
		log.Warn().
			Err(err).
			Str("run_id", run.ID).
			Str("attempt_id", attempt.ID).
			Msg("failed to append provisioning failure snapshot")
	}
}

type ResumeBarrier struct {
	backend    repository.BackendRepository
	terminalIO repository.TerminalIORepository
}

func NewResumeBarrier(backend repository.BackendRepository, terminalIO repository.TerminalIORepository) *ResumeBarrier {
	return &ResumeBarrier{
		backend:    backend,
		terminalIO: terminalIO,
	}
}

func (b *ResumeBarrier) WaitForResume(
	ctx context.Context,
	workspaceID uint,
	sessionID string,
	expectedCheckpointRunID string,
	excludeRunIDs ...string,
) error {
	log.Debug().
		Uint("workspace_id", workspaceID).
		Str("session_id", strings.TrimSpace(sessionID)).
		Str("checkpoint_run_id", strings.TrimSpace(expectedCheckpointRunID)).
		Strs("exclude_run_ids", excludeRunIDs).
		Msg("resume barrier: waiting for session state")
	if err := b.waitForSessionLeaseDrain(ctx, workspaceID, sessionID); err != nil {
		return err
	}
	log.Debug().
		Uint("workspace_id", workspaceID).
		Str("session_id", strings.TrimSpace(sessionID)).
		Msg("resume barrier: session lease drained")
	if err := b.waitForSessionCheckpoint(ctx, workspaceID, sessionID, expectedCheckpointRunID); err != nil {
		return err
	}
	log.Debug().
		Uint("workspace_id", workspaceID).
		Str("session_id", strings.TrimSpace(sessionID)).
		Str("checkpoint_run_id", strings.TrimSpace(expectedCheckpointRunID)).
		Msg("resume barrier: session checkpoint confirmed")
	return b.ensureSessionAvailableForNewRun(ctx, workspaceID, sessionID, excludeRunIDs...)
}

func (b *ResumeBarrier) waitForSessionCheckpoint(
	ctx context.Context,
	workspaceID uint,
	sessionID string,
	expectedRunID string,
) error {
	if b == nil || b.terminalIO == nil {
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
		checkpoint, err := getSessionCheckpointWithTimeout(ctx, b.terminalIO, workspaceID, sessionID)
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

func (b *ResumeBarrier) waitForSessionLeaseDrain(ctx context.Context, workspaceID uint, sessionID string) error {
	if b == nil || b.terminalIO == nil || sessionID == "" {
		return nil
	}
	deadline := time.After(sessionDrainMaxWait)
	tick := time.NewTicker(sessionDrainPollStep)
	defer tick.Stop()

	reconciled := false
	for {
		owner, err := getSessionLeaseOwnerWithTimeout(ctx, b.terminalIO, workspaceID, sessionID)
		if err != nil {
			return fmt.Errorf("check session lease: %w", err)
		}
		if owner == "" {
			return nil
		}
		if !reconciled {
			if b.tryReconcileStaleSessionLease(ctx, workspaceID, sessionID, owner) {
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

func (b *ResumeBarrier) ensureSessionAvailableForNewRun(
	ctx context.Context,
	workspaceID uint,
	sessionID string,
	excludeRunIDs ...string,
) error {
	if b == nil || b.backend == nil {
		return nil
	}
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" {
		return nil
	}

	log.Debug().
		Uint("workspace_id", workspaceID).
		Str("session_id", sessionID).
		Strs("exclude_run_ids", excludeRunIDs).
		Msg("resume barrier: validating session availability")
	if b.terminalIO != nil {
		if owner, _ := getSessionLeaseOwnerWithTimeout(ctx, b.terminalIO, workspaceID, sessionID); owner != "" {
			if !b.tryReconcileStaleSessionLease(ctx, workspaceID, sessionID, owner) {
				return fmt.Errorf("session ID %s is already in use (lease: %s)", sessionID, owner)
			}
		}
	}

	conflicts, err := b.backend.ListActiveRunsBySession(ctx, workspaceID, sessionID, excludeRunIDs, 5)
	if err != nil {
		return err
	}
	if len(conflicts) > 0 {
		return fmt.Errorf("session ID %s is already in use (run: %s)", sessionID, conflicts[0].ID)
	}
	return nil
}

func (b *ResumeBarrier) tryReconcileStaleSessionLease(ctx context.Context, workspaceID uint, sessionID, owner string) bool {
	if b == nil {
		return false
	}
	return ReconcileStaleSessionLease(ctx, b.backend, b.terminalIO, workspaceID, sessionID, owner)
}
