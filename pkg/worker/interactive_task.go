package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/beam-cloud/airstore/pkg/repository"
	"github.com/beam-cloud/airstore/pkg/types"
	agentsignal "github.com/beam-cloud/airstore/pkg/worker/agentsignal/baml_client"
	signaltypes "github.com/beam-cloud/airstore/pkg/worker/agentsignal/baml_client/types"
	"github.com/rs/zerolog/log"
)

const DefaultBetweenTurnsTimeout = 60 * time.Second

// mountFlushGracePeriod is a short wait after sandbox deletion but before
// unmounting the VFS. This gives the async writer time to flush pending
// writes (e.g. Claude session state) to object storage so the next
// resume finds a complete conversation history.
const mountFlushGracePeriod = 10 * time.Second
const sessionLeaseTTL = 30 * time.Second
const sessionLeaseRenewInterval = 10 * time.Second
const runInteractionTTL = 30 * time.Minute

func (w *Worker) setRunInteractionState(ctx context.Context, task types.RunExecution, state types.RunInteractionState) {
	if w == nil || w.terminalIO == nil {
		return
	}
	executionCtx := executionContextFromTask(task)
	if strings.TrimSpace(executionCtx.runID) == "" {
		return
	}
	activeExecutionID := ""
	if state != types.RunInteractionStateClosed {
		activeExecutionID = task.ExternalId
	}
	if err := w.terminalIO.SetRunInteraction(
		ctx,
		task.WorkspaceId,
		executionCtx.runID,
		types.RunInteraction{
			State:             state,
			ActiveExecutionID: activeExecutionID,
		},
		runInteractionTTL,
	); err != nil {
		addTaskExecutionContext(
			log.Warn().
				Err(err).
				Str("run_id", executionCtx.runID).
				Str("interaction_state", string(state)),
			task,
		).Msg("failed to persist run interaction state")
	}
}

// setOriginTaskState transitions the origin task's state via the gateway.
// Used to eagerly reflect waiting/running in the UI during a live session.
func (w *Worker) setOriginTaskState(ctx context.Context, task types.RunExecution, state types.AgentTaskState, inputKind types.InputKind, waitingSummary ...string) {
	execCtx := executionContextFromTask(task)
	if execCtx.originTaskID == "" || execCtx.runID == "" {
		return
	}
	var summary string
	if len(waitingSummary) > 0 {
		summary = waitingSummary[0]
	}
	if err := w.gatewayClient.UpdateTaskState(ctx, execCtx.originTaskID, string(state), execCtx.runID, string(inputKind), summary); err != nil {
		addTaskExecutionContext(log.Warn().Err(err).Str("target_state", string(state)), task).
			Msg("failed to eagerly update origin task state")
	}
}

func shouldRecordSessionCheckpoint(result *types.RunExecutionResult) bool {
	return result != nil && strings.TrimSpace(result.Error) == ""
}

func (w *Worker) recordSessionCheckpoint(ctx context.Context, task types.RunExecution, mountSource string, env map[string]string) error {
	if w == nil || w.terminalIO == nil {
		return nil
	}
	sessionID := strings.TrimSpace(env[agentSessionIDEnvKey])
	if sessionID == "" {
		return nil
	}
	execCtx := executionContextFromTask(task)
	if execCtx.runID == "" {
		return nil
	}
	checkpoint := &types.SessionCheckpoint{
		RunID:       execCtx.runID,
		ExecutionID: task.ExternalId,
		UpdatedAt:   time.Now().UnixMilli(),
	}
	if err := writeClaudeSessionCheckpoint(mountSource, env, checkpoint); err != nil {
		return err
	}
	return w.terminalIO.SetSessionCheckpoint(ctx, task.WorkspaceId, sessionID, checkpoint, 0)
}

func (w *Worker) runInteractiveTask(ctx context.Context, task types.RunExecution) (*types.RunExecutionResult, error) {
	if w.terminalIO == nil {
		return nil, fmt.Errorf("terminal transport is not configured")
	}
	runCtx, runCancel := context.WithCancel(ctx)
	defer runCancel()
	defer func() {
		finalCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		w.setRunInteractionState(finalCtx, task, types.RunInteractionStateClosed)
	}()

	sessionID := strings.TrimSpace(task.Env[agentSessionIDEnvKey])
	ownerID := fmt.Sprintf("%s:%s", strings.TrimSpace(w.workerId), task.ExternalId)
	releaseSessionLease := func() {}

	if sessionID != "" {
		acquired, err := w.terminalIO.AcquireSessionLease(runCtx, task.WorkspaceId, sessionID, ownerID, sessionLeaseTTL)
		if err != nil {
			return nil, fmt.Errorf("failed to acquire session lease: %w", err)
		}
		if !acquired {
			currentOwner, _ := w.terminalIO.GetSessionLeaseOwner(runCtx, task.WorkspaceId, sessionID)
			return nil, fmt.Errorf("session ID %s is already in use (owner: %s)", sessionID, currentOwner)
		}
		addTaskExecutionContext(log.Info().Str("session_id", sessionID), task).Msg("acquired session lease")

		leaseCtx, leaseCancel := context.WithCancel(runCtx)
		go w.heartbeatSessionLease(leaseCtx, task, sessionID, ownerID, runCancel)
		var releaseOnce sync.Once
		releaseSessionLease = func() {
			releaseOnce.Do(func() {
				leaseCancel()
				releaseCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				if err := w.terminalIO.ReleaseSessionLease(releaseCtx, task.WorkspaceId, sessionID, ownerID); err != nil {
					addTaskExecutionContext(log.Warn().Err(err).Str("session_id", sessionID), task).Msg("failed to release session lease")
				} else {
					addTaskExecutionContext(log.Info().Str("session_id", sessionID), task).Msg("released session lease")
				}
			})
		}
		defer releaseSessionLease()
	}

	sandboxID := fmt.Sprintf("task-%s", task.ExternalId)
	env := w.sandboxManager.copyTaskEnv(task)
	if claudeRunner, ok := w.sandboxManager.ResolveRunner(task, env).(*ClaudeCodeRunner); ok {
		claudeRunner.injectEnv(env)
	}
	taskMountSource := w.sandboxManager.mountFilesystem(runCtx, task)
	cfg := w.sandboxManager.buildTaskSandboxConfig(task, []string{"sleep", "infinity"}, env, taskMountSource)

	if _, err := w.sandboxManager.Create(cfg); err != nil {
		w.sandboxManager.cleanupMount(task.ExternalId)
		return nil, fmt.Errorf("failed to create interactive sandbox: %w", err)
	}

	if err := w.sandboxManager.Start(sandboxID); err != nil {
		w.sandboxManager.publishStatus(ctx, task.ExternalId, types.RunExecutionStatusFailed, nil, err.Error())
		w.sandboxManager.Delete(sandboxID, true)
		w.sandboxManager.cleanupMount(task.ExternalId)
		return nil, fmt.Errorf("failed to start interactive sandbox: %w", err)
	}

	// Configure git inside the sandbox: writes credential helper, gitconfig,
	// and resolves the real GitHub user's name/email via the github tool.
	// Runs synchronously so the config is ready before the first turn.
	setupGitInsideSandbox(runCtx, w.sandboxManager.runtime, sandboxID, env)

	w.sandboxManager.publishStatus(runCtx, task.ExternalId, types.RunExecutionStatusRunning, nil, "")
	w.setRunInteractionState(runCtx, task, types.RunInteractionStateWorking)

	result := w.runInteractiveSession(runCtx, task, sandboxID, taskMountSource)

	// Mark the interaction closed immediately so that any user input arriving
	// after the session ends sees "closed" and triggers a restart instead of
	// being published to a dead pubsub channel. The defer is still the
	// safety net for panics.
	w.setRunInteractionState(runCtx, task, types.RunInteractionStateClosed)

	// Report result to the gateway immediately, before cleanup. The task
	// state transitions now (~1s) instead of after the 10s flush grace
	// period. The later call in finishTask is idempotent (outbox dedupe).
	w.reportTaskResult(task, result)

	if err := w.sandboxManager.Delete(sandboxID, true); err != nil {
		addTaskExecutionContext(log.Warn().Err(err), task).Msg("interactive sandbox delete failed during cleanup")
	}

	time.Sleep(mountFlushGracePeriod)

	if shouldRecordSessionCheckpoint(result) {
		checkpointCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		if err := w.recordSessionCheckpoint(checkpointCtx, task, taskMountSource, env); err != nil {
			addTaskExecutionContext(log.Warn().Err(err).Str("session_id", sessionID), task).
				Msg("failed to persist durable session checkpoint")
		} else {
			addTaskExecutionContext(log.Info().Str("session_id", sessionID), task).
				Msg("persisted durable session checkpoint")
		}
		cancel()
	}

	releaseSessionLease()
	w.sandboxManager.cleanupMount(task.ExternalId)
	addTaskExecutionContext(log.Info(), task).Msg("interactive sandbox cleanup complete")

	return result, nil
}

func (w *Worker) heartbeatSessionLease(ctx context.Context, task types.RunExecution, sessionID, ownerID string, onLost func()) {
	ticker := time.NewTicker(sessionLeaseRenewInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			renewed, err := w.terminalIO.RenewSessionLease(ctx, task.WorkspaceId, sessionID, ownerID, sessionLeaseTTL)
			if err != nil {
				addTaskExecutionContext(log.Warn().Err(err).Str("session_id", sessionID), task).
					Msg("session lease renewal failed; canceling interactive task")
				if onLost != nil {
					onLost()
				}
				return
			} else if !renewed {
				addTaskExecutionContext(log.Warn().Str("session_id", sessionID), task).
					Msg("session lease lost; canceling interactive task")
				if onLost != nil {
					onLost()
				}
				return
			}
		}
	}
}

func (w *Worker) runInteractiveSession(ctx context.Context, task types.RunExecution, sandboxID string, mountSource string) *types.RunExecutionResult {
	sessionCtx, sessionCancel := context.WithCancel(ctx)
	defer sessionCancel()

	executionCtx := executionContextFromTask(task)

	idleTimeout := w.config.Sandbox.GetInteractiveIdleTimeout()
	var idleTimedOut atomic.Bool
	activityCh := make(chan struct{}, 1)

	env := w.sandboxManager.copyTaskEnv(task)
	runner := w.sandboxManager.ResolveRunner(task, env)

	var checkHeartbeat func() bool
	var touchHeartbeat func()
	var checkNeedsInput func() (bool, types.InputKind, string)
	var needsInputRunner NeedsInputRunner
	var needsInputMarkerPath string
	var terminalWriter *terminalOutputWriter
	bamlEnv := w.sandboxManager.BamlEnv()

	if heartbeatRunner, ok := runner.(HeartbeatRunner); ok && mountSource != "" {
		heartbeatPath, err := heartbeatRunner.SetupHeartbeat(mountSource, env)
		if err != nil {
			addTaskExecutionContext(log.Warn().Err(err).Str("runner", runner.Name()), task).
				Msg("failed to install heartbeat hooks")
		} else {
			checkHeartbeat = func() bool {
				return heartbeatRunner.CheckHeartbeat(heartbeatPath)
			}
			touchHeartbeat = func() {
				_ = os.WriteFile(heartbeatPath, []byte(time.Now().Format(time.RFC3339Nano)), 0o644)
			}
			addTaskExecutionContext(log.Info().Str("runner", runner.Name()).Str("heartbeat", heartbeatPath), task).
				Msg("heartbeat enabled via VFS")
		}
	}
	if ir, ok := runner.(NeedsInputRunner); ok && mountSource != "" {
		markerPath, err := ir.SetupNeedsInput(mountSource, env)
		if err != nil {
			addTaskExecutionContext(log.Warn().Err(err).Str("runner", runner.Name()), task).
				Msg("failed to install needs-input hook")
		} else {
			needsInputRunner = ir
			needsInputMarkerPath = markerPath
			checkNeedsInput = func() (bool, types.InputKind, string) {
				msg := ir.ReadLastMessage(markerPath)
				if msg == "" {
					return false, "", ""
				}
				classification, err := agentsignal.ClassifyTurn(ctx, msg, agentsignal.WithEnv(bamlEnv))
				if err != nil {
					addTaskExecutionContext(log.Warn().Err(err), task).
						Msg("BAML ClassifyTurn failed, defaulting to complete")
					return false, "", ""
				}
				if classification.Outcome != signaltypes.TurnOutcomeNEEDS_INPUT {
					return false, "", ""
				}
				inputKind := types.InputKindFreeText
				if classification.Input_kind != nil {
					inputKind = types.InputKind(strings.ToLower(string(*classification.Input_kind)))
				}

				var waitingSummary string
				if inputKind == types.InputKindApproveReject && terminalWriter.ringBuf != nil {
					assistantText := extractAssistantText(terminalWriter.ringBuf.Bytes(), 4000)
					if assistantText != "" {
						summary, extractErr := agentsignal.ExtractApprovalSummary(ctx, assistantText, agentsignal.WithEnv(bamlEnv))
						if extractErr != nil {
							addTaskExecutionContext(log.Warn().Err(extractErr), task).
								Msg("BAML ExtractApprovalSummary failed, proceeding without summary")
						} else {
							waitingSummary = marshalApprovalSummary(summary)
						}
					}
				}

				return true, inputKind, waitingSummary
			}

			addTaskExecutionContext(log.Info().Str("runner", runner.Name()), task).
				Msg("needs-input detection enabled")
		}
	}

	if idleTimeout > 0 {
		go monitorInteractiveSessionIdle(
			sessionCtx,
			task.ExternalId,
			executionCtx,
			sessionCancel,
			idleTimeout,
			activityCh,
			&idleTimedOut,
			checkHeartbeat,
		)
	}

	cancelCleanup := w.watchTaskCancellation(sessionCtx, task, func() {
		addTaskExecutionContext(log.Info(), task).Msg("received cancel signal for interactive task")
		sessionCancel()
		w.sandboxManager.Stop(sandboxID, true)
	})
	defer cancelCleanup()

	interactiveMirror := NewTaskOutput(task.ExternalId, "stdout", w.sandboxManager.taskOutputWriters(sessionCtx, task, env)...)
	defer interactiveMirror.Flush()

	terminalWriter = &terminalOutputWriter{
		ctx:          sessionCtx,
		taskID:       task.ExternalId,
		terminalIO:   w.terminalIO,
		executionCtx: executionCtx,
		onActivity: func() {
			signalActivity(activityCh)
			if touchHeartbeat != nil {
				touchHeartbeat()
			}
		},
		mirror:  interactiveMirror,
		ringBuf: newRingBuffer(terminalRingBufSize),
	}

	start := time.Now()

	var runErr error
	var needsInput bool
	if tr, ok := runner.(TurnRunner); ok {
		runErr, needsInput = w.runTurnSession(sessionCtx, task, sandboxID, tr, env, terminalWriter, activityCh, checkNeedsInput)
	} else {
		runErr = w.runGenericPTYSession(sessionCtx, task, sandboxID, terminalWriter, activityCh)
	}

	addTaskExecutionContext(
		log.Info().Dur("session_duration", time.Since(start)).Bool("needs_input", needsInput),
		task,
	).Msg("interactive session finished")

	exitCode, errMsg, status := interactiveResult(runErr, idleTimedOut.Load())
	w.sandboxManager.publishStatus(ctx, task.ExternalId, status, &exitCode, errMsg)

	var wakeSignal *types.RunExecutionWakeSignal
	if !needsInput && runErr == nil && needsInputRunner != nil && needsInputMarkerPath != "" {
		if msg := needsInputRunner.ReadLastMessage(needsInputMarkerPath); msg != "" {
			followUp, err := agentsignal.ClassifyFollowUp(ctx, msg, agentsignal.WithEnv(bamlEnv))
			if err != nil {
				addTaskExecutionContext(log.Warn().Err(err), task).
					Msg("BAML ClassifyFollowUp failed, treating as no follow-up")
			} else if followUp.Intent == signaltypes.FollowUpIntentFOLLOW_UP {
				wakeSignal = &types.RunExecutionWakeSignal{
					DelayMinutes: int(followUp.Delay_minutes),
				}
				if followUp.Reason != nil {
					wakeSignal.Reason = *followUp.Reason
				}
				if followUp.Follow_up_prompt != nil {
					wakeSignal.FollowUpPrompt = *followUp.Follow_up_prompt
				}
				addTaskExecutionContext(
					log.Info().Int("delay_minutes", wakeSignal.DelayMinutes).Str("reason", wakeSignal.Reason),
					task,
				).Msg("agent requested follow-up")
			}
		}
	}

	return &types.RunExecutionResult{
		ID:              task.ExternalId,
		ExitCode:        exitCode,
		Error:           errMsg,
		Duration:        time.Since(start),
		WaitingForInput: needsInput,
		WakeSignal:      wakeSignal,
	}
}

// runTurnSession executes an interactive session as a series of per-turn
// Exec calls. Each turn runs claude --print via ExecPTY (no stdin pipe).
// After each turn the Stop hook's marker file is checked: if the agent
// asked a question (needs input), the state is set to waiting_for_input
// and the worker blocks on Redis pubsub. Otherwise the session ends
// cleanly — the agent's work is done.
func (w *Worker) runTurnSession(
	ctx context.Context,
	task types.RunExecution,
	sandboxID string,
	runner TurnRunner,
	env map[string]string,
	stdout io.Writer,
	activityCh chan<- struct{},
	checkNeedsInput func() (bool, types.InputKind, string),
) (error, bool) {
	prompt := strings.TrimSpace(task.Prompt)
	isFirstTurn := true
	sessionEnv := cloneTurnEnv(env)
	firstTurnStrategies := buildFirstTurnStrategies(sessionEnv)

	for prompt != "" {
		if ctx.Err() != nil {
			return ctx.Err(), false
		}
		w.setRunInteractionState(ctx, task, types.RunInteractionStateWorking)
		if !isFirstTurn {
			w.setOriginTaskState(ctx, task, types.AgentTaskStateRunning, "")
		}

		if isFirstTurn {
			nextEnv, err := w.executeFirstTurnWithStrategy(
				ctx, task, sandboxID, runner, sessionEnv, stdout, prompt, firstTurnStrategies,
			)
			if err != nil {
				return err, false
			}
			sessionEnv = nextEnv
			isFirstTurn = false
		} else {
			if err := w.executeTurn(ctx, task, sandboxID, runner, sessionEnv, stdout, prompt, TurnArgModeFollowup, 1, 1, ""); err != nil {
				return err, false
			}
		}

		signalActivity(activityCh)

		if checkNeedsInput == nil {
			return nil, false
		}
		needsInput, inputKind, waitingSummary := checkNeedsInput()
		if !needsInput {
			return nil, false
		}

		w.setRunInteractionState(ctx, task, types.RunInteractionStateWaitingForInput)
		w.setOriginTaskState(ctx, task, types.AgentTaskStateWaiting, inputKind, waitingSummary)
		addTaskExecutionContext(log.Info(), task).Msg("turn complete, agent is waiting for input")
		prompt = w.waitForFollowupInput(ctx, task, DefaultBetweenTurnsTimeout, activityCh)
	}

	return nil, true
}

// waitForFollowupInput claims the next durable task_input via the gateway,
// using Redis pubsub as a low-latency wake hint. Returns the trimmed prompt
// or "" if the session should end.
func (w *Worker) waitForFollowupInput(
	ctx context.Context,
	task types.RunExecution,
	timeout time.Duration,
	activityCh chan<- struct{},
) string {
	execCtx := executionContextFromTask(task)
	originTaskID := execCtx.originTaskID
	runID := execCtx.runID
	executionID := task.ExternalId

	if originTaskID == "" || runID == "" {
		<-ctx.Done()
		return ""
	}

	var timeoutCh <-chan time.Time
	var timer *time.Timer
	if timeout > 0 {
		timer = time.NewTimer(timeout)
		timeoutCh = timer.C
		defer timer.Stop()
	}

	tryClaim := func() string {
		resp, err := w.gatewayClient.ClaimTaskInput(ctx, originTaskID, runID, executionID)
		if err != nil {
			addTaskExecutionContext(log.Warn().Err(err), task).Msg("failed to claim task input")
			return ""
		}
		if !resp.Found {
			return ""
		}
		prompt := strings.TrimSpace(resp.Message)
		if prompt == "" {
			_ = w.gatewayClient.AckTaskInput(ctx, resp.InputId)
			return ""
		}
		if err := w.gatewayClient.AckTaskInput(ctx, resp.InputId); err != nil {
			addTaskExecutionContext(log.Warn().Err(err).Str("input_id", resp.InputId), task).
				Msg("failed to ack task input")
		}
		return prompt
	}

	if prompt := tryClaim(); prompt != "" {
		signalActivity(activityCh)
		return prompt
	}

	var wakeCh <-chan struct{}
	var wakeCleanup func()
	if w.terminalIO != nil {
		var err error
		wakeCh, wakeCleanup, err = w.terminalIO.SubscribeInputWake(ctx, executionID)
		if err != nil {
			addTaskExecutionContext(log.Warn().Err(err), task).Msg("failed to subscribe to input wake")
		}
	}
	if wakeCleanup != nil {
		defer wakeCleanup()
	}
	if wakeCh == nil {
		wakeCh = make(chan struct{})
	}

	pollInterval := 2 * time.Second

	for {
		select {
		case <-ctx.Done():
			return ""
		case <-timeoutCh:
			return ""
		case <-wakeCh:
			if prompt := tryClaim(); prompt != "" {
				signalActivity(activityCh)
				return prompt
			}
		case <-time.After(pollInterval):
			if prompt := tryClaim(); prompt != "" {
				signalActivity(activityCh)
				return prompt
			}
		}
	}
}

func shouldContinueFromFirstTurn(env map[string]string) bool {
	if len(env) == 0 {
		return false
	}

	switch strings.ToLower(strings.TrimSpace(env[agentResumeSessionEnvKey])) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}

type firstTurnStrategy struct {
	mode             TurnArgMode
	transitionReason string
}

func buildFirstTurnStrategies(env map[string]string) []firstTurnStrategy {
	if !shouldContinueFromFirstTurn(env) {
		return []firstTurnStrategy{
			{
				mode:             TurnArgModeFirstStart,
				transitionReason: "resume not requested",
			},
		}
	}

	if strings.TrimSpace(env[agentSessionIDEnvKey]) != "" {
		return []firstTurnStrategy{
			{
				mode:             TurnArgModeFirstResumeByID,
				transitionReason: "resume requested with explicit session id",
			},
			{
				mode:             TurnArgModeFirstResumeLatest,
				transitionReason: "resume fallback using latest local VFS state",
			},
		}
	}
	return []firstTurnStrategy{
		{
			mode:             TurnArgModeFirstResumeLatest,
			transitionReason: "resume requested; no explicit session id",
		},
	}
}

func cloneTurnEnv(env map[string]string) map[string]string {
	if len(env) == 0 {
		return map[string]string{}
	}
	cloned := make(map[string]string, len(env))
	for key, value := range env {
		cloned[key] = value
	}
	return cloned
}

func (w *Worker) executeFirstTurnWithStrategy(
	ctx context.Context,
	task types.RunExecution,
	sandboxID string,
	runner TurnRunner,
	baseEnv map[string]string,
	stdout io.Writer,
	prompt string,
	strategies []firstTurnStrategy,
) (map[string]string, error) {
	if len(strategies) == 0 {
		return baseEnv, fmt.Errorf("no first-turn strategy configured")
	}

	totalAttempts := len(strategies)
	for idx, strategy := range strategies {
		attempt := idx + 1
		err := w.executeTurn(
			ctx,
			task,
			sandboxID,
			runner,
			baseEnv,
			stdout,
			prompt,
			strategy.mode,
			attempt,
			totalAttempts,
			strategy.transitionReason,
		)
		if err == nil {
			return baseEnv, nil
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return baseEnv, err
		}
		if attempt < totalAttempts {
			addTaskExecutionContext(
				log.Warn().
					Err(err).
					Str("turn_mode", string(strategy.mode)).
					Str("next_turn_mode", string(strategies[idx+1].mode)).
					Int("resume_attempt", attempt).
					Int("resume_attempts_total", totalAttempts),
				task,
			).Msg("first-turn strategy failed, trying next strategy")
			continue
		}

		// Emit a single clear failure when first-turn fallback is exhausted.
		if totalAttempts > 1 {
			addTaskExecutionContext(
				log.Error().
					Err(err).
					Str("turn_mode", string(strategy.mode)).
					Int("resume_attempt", attempt).
					Int("resume_attempts_total", totalAttempts),
				task,
			).Msg("session resume exhausted first-turn fallback")
			return baseEnv, fmt.Errorf("session resume exhausted first-turn fallback: %w", err)
		}
		return baseEnv, err
	}

	return baseEnv, fmt.Errorf("failed to execute first turn")
}

func (w *Worker) executeTurn(
	ctx context.Context,
	task types.RunExecution,
	sandboxID string,
	runner TurnRunner,
	env map[string]string,
	stdout io.Writer,
	prompt string,
	mode TurnArgMode,
	attempt int,
	totalAttempts int,
	transitionReason string,
) error {
	args := runner.BuildTurnArgs(prompt, env, mode)
	logger := addTaskExecutionContext(
		log.Info().
			Bool("first_turn", mode != TurnArgModeFollowup).
			Str("turn_mode", string(mode)).
			Bool("session_id_present", strings.TrimSpace(env[agentSessionIDEnvKey]) != "").
			Str("prompt", prompt[:min(50, len(prompt))]),
		task,
	)
	if totalAttempts > 1 {
		logger = logger.Int("resume_attempt", attempt).Int("resume_attempts_total", totalAttempts)
	}
	if strings.TrimSpace(transitionReason) != "" {
		logger = logger.Str("transition_reason", transitionReason)
	}
	logger.Msg("executing turn")

	return w.sandboxManager.ExecPTY(ctx, sandboxID, args, env, stdout)
}

// runGenericPTYSession handles interactive sessions for runners that don't
// support per-turn execution. Stdin is not forwarded; follow-up input uses
// the durable task_input inbox via the turn-based path.
func (w *Worker) runGenericPTYSession(
	ctx context.Context,
	task types.RunExecution,
	sandboxID string,
	stdout io.Writer,
	activityCh chan<- struct{},
) error {
	addTaskExecutionContext(log.Info(), task).Msg("starting generic PTY session")
	return w.sandboxManager.AttachPTY(ctx, sandboxID, nil, stdout)
}

func interactiveResult(err error, idleTimedOut bool) (int, string, types.RunExecutionStatus) {
	if err == nil {
		return 0, "", types.RunExecutionStatusComplete
	}

	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		if idleTimedOut {
			return 0, "", types.RunExecutionStatusComplete
		}
		return -1, "interactive session cancelled", types.RunExecutionStatusCancelled
	}

	return -1, err.Error(), types.RunExecutionStatusFailed
}

const terminalRingBufSize = 256 * 1024 // 256KB

type terminalOutputWriter struct {
	ctx          context.Context
	taskID       string
	terminalIO   repository.TerminalIORepository
	executionCtx taskExecutionContext
	onActivity   func()
	mirror       io.Writer
	ringBuf      *ringBuffer
}

func (w *terminalOutputWriter) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}

	if w.ringBuf != nil {
		_, _ = w.ringBuf.Write(p)
	}

	if w.mirror != nil {
		_, _ = w.mirror.Write(p)
	}
	if w.onActivity != nil {
		w.onActivity()
	}

	if err := w.terminalIO.PublishOutput(w.ctx, w.taskID, append([]byte(nil), p...)); err != nil {
		addTaskExecutionContextByID(log.Warn().Err(err), w.taskID, w.executionCtx).Msg("failed to publish terminal output")
	}

	return len(p), nil
}

func marshalApprovalSummary(s signaltypes.ApprovalSummary) string {
	b, err := json.Marshal(map[string]string{
		"summary": s.Summary,
		"details": s.Details,
	})
	if err != nil {
		return ""
	}
	return string(b)
}

func signalActivity(activityCh chan<- struct{}) {
	select {
	case activityCh <- struct{}{}:
	default:
	}
}

func monitorInteractiveSessionIdle(
	ctx context.Context,
	taskID string,
	executionCtx taskExecutionContext,
	cancel context.CancelFunc,
	idleTimeout time.Duration,
	activityCh <-chan struct{},
	idleTimedOut *atomic.Bool,
	checkHeartbeat func() bool,
) {
	timer := time.NewTimer(idleTimeout)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-activityCh:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(idleTimeout)
		case <-timer.C:
			if checkHeartbeat != nil && checkHeartbeat() {
				addTaskExecutionContextByID(
					log.Debug().Dur("idle_timeout", idleTimeout),
					taskID,
					executionCtx,
				).Msg("interactive idle timeout deferred due runner heartbeat")
				timer.Reset(idleTimeout)
				continue
			}
			idleTimedOut.Store(true)
			addTaskExecutionContextByID(
				log.Info().Dur("idle_timeout", idleTimeout),
				taskID,
				executionCtx,
			).Msg("interactive session idle timeout reached, stopping task")
			cancel()
			return
		}
	}
}
