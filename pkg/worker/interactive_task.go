package worker

import (
	"context"
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
	turnclass "github.com/beam-cloud/airstore/pkg/worker/turnclass/baml_client"
	turntypes "github.com/beam-cloud/airstore/pkg/worker/turnclass/baml_client/types"
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
		state,
		activeExecutionID,
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

func (w *Worker) runInteractiveTask(ctx context.Context, task types.RunExecution) (*types.RunExecutionResult, error) {
	if w.terminalIO == nil {
		return nil, fmt.Errorf("terminal transport is not configured")
	}
	defer func() {
		finalCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		w.setRunInteractionState(finalCtx, task, types.RunInteractionStateClosed)
	}()

	sessionID := strings.TrimSpace(task.Env[agentSessionIDEnvKey])
	ownerID := fmt.Sprintf("%s:%s", strings.TrimSpace(w.workerId), task.ExternalId)
	releaseSessionLease := func() {}

	if sessionID != "" {
		acquired, err := w.terminalIO.AcquireSessionLease(ctx, task.WorkspaceId, sessionID, ownerID, sessionLeaseTTL)
		if err != nil {
			return nil, fmt.Errorf("failed to acquire session lease: %w", err)
		}
		if !acquired {
			currentOwner, _ := w.terminalIO.GetSessionLeaseOwner(ctx, task.WorkspaceId, sessionID)
			return nil, fmt.Errorf("session ID %s is already in use (owner: %s)", sessionID, currentOwner)
		}
		addTaskExecutionContext(log.Info().Str("session_id", sessionID), task).Msg("acquired session lease")

		leaseCtx, leaseCancel := context.WithCancel(ctx)
		go w.heartbeatSessionLease(leaseCtx, task.WorkspaceId, sessionID, ownerID)
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
	taskMountSource := w.sandboxManager.mountFilesystem(ctx, task)
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
	setupGitInsideSandbox(ctx, w.sandboxManager.runtime, sandboxID, env)

	w.sandboxManager.publishStatus(ctx, task.ExternalId, types.RunExecutionStatusRunning, nil, "")
	w.setRunInteractionState(ctx, task, types.RunInteractionStateWorking)

	result := w.runInteractiveSession(ctx, task, sandboxID, taskMountSource)
	w.setRunInteractionState(ctx, task, types.RunInteractionStateClosed)
	releaseSessionLease()

	if err := w.sandboxManager.Delete(sandboxID, true); err != nil {
		addTaskExecutionContext(log.Warn().Err(err), task).Msg("interactive sandbox delete failed during cleanup")
	}

	time.Sleep(mountFlushGracePeriod)

	w.sandboxManager.cleanupMount(task.ExternalId)
	addTaskExecutionContext(log.Info(), task).Msg("interactive sandbox cleanup complete")

	return result, nil
}

func (w *Worker) heartbeatSessionLease(ctx context.Context, workspaceID uint, sessionID, ownerID string) {
	ticker := time.NewTicker(sessionLeaseRenewInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			renewed, err := w.terminalIO.RenewSessionLease(ctx, workspaceID, sessionID, ownerID, sessionLeaseTTL)
			if err != nil {
				log.Warn().Err(err).Str("session_id", sessionID).Msg("session lease renewal failed")
			} else if !renewed {
				log.Warn().Str("session_id", sessionID).Msg("session lease lost (owned by another)")
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
	var checkNeedsInput func() bool
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
	if inputRunner, ok := runner.(NeedsInputRunner); ok && mountSource != "" {
		markerPath, err := inputRunner.SetupNeedsInput(mountSource, env)
		if err != nil {
			addTaskExecutionContext(log.Warn().Err(err).Str("runner", runner.Name()), task).
				Msg("failed to install needs-input hook")
		} else {
			bamlEnv := w.bamlEnvForRunner(runner, env)
			checkNeedsInput = func() bool {
				msg := inputRunner.ReadLastMessage(markerPath)
				if msg == "" {
					return false
				}
				outcome, err := turnclass.ClassifyTurn(ctx, msg, turnclass.WithEnv(bamlEnv))
				if err != nil {
					addTaskExecutionContext(log.Warn().Err(err), task).
						Msg("BAML ClassifyTurn failed, defaulting to complete")
					return false
				}
				return outcome == turntypes.TurnOutcomeNEEDS_INPUT
			}
			addTaskExecutionContext(log.Info().Str("runner", runner.Name()), task).
				Msg("needs-input detection enabled (Stop hook + BAML)")
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

	interactiveMirror := NewTaskOutput(
		task.ExternalId,
		"stdout",
		NewS2Writer(sessionCtx, w.sandboxManager.s2, task.ExternalId, "stdout"),
		NewConsoleWriter(task.ExternalId, "stdout"),
	)
	defer interactiveMirror.Flush()

	terminalWriter := &terminalOutputWriter{
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
		mirror: interactiveMirror,
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
	return &types.RunExecutionResult{
		ID:              task.ExternalId,
		ExitCode:        exitCode,
		Error:           errMsg,
		Duration:        time.Since(start),
		WaitingForInput: needsInput,
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
	checkNeedsInput func() bool,
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

		if checkNeedsInput == nil || !checkNeedsInput() {
			return nil, false
		}

		w.setRunInteractionState(ctx, task, types.RunInteractionStateWaitingForInput)
		addTaskExecutionContext(log.Info(), task).Msg("turn complete, agent is waiting for input")
		prompt = w.waitForFollowupInput(ctx, task.ExternalId, DefaultBetweenTurnsTimeout, activityCh)
	}

	return nil, true
}

// waitForFollowupInput blocks until follow-up input arrives via Redis
// pubsub, the per-turn timeout expires, or the context is cancelled
// (e.g. idle timeout). Returns the trimmed prompt, or "" if the
// session should end.
func (w *Worker) waitForFollowupInput(
	ctx context.Context,
	taskID string,
	timeout time.Duration,
	activityCh chan<- struct{},
) string {
	if w.terminalIO == nil {
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

	for {
		inputCh, cleanup, err := w.terminalIO.SubscribeInput(ctx, taskID)
		if err != nil {
			select {
			case <-ctx.Done():
				return ""
			case <-timeoutCh:
				return ""
			case <-time.After(250 * time.Millisecond):
				continue
			}
		}

		select {
		case <-ctx.Done():
			cleanup()
			return ""
		case <-timeoutCh:
			cleanup()
			return ""
		case data, ok := <-inputCh:
			cleanup()
			if !ok {
				select {
				case <-ctx.Done():
					return ""
				case <-timeoutCh:
					return ""
				case <-time.After(250 * time.Millisecond):
					continue
				}
			}
			prompt := strings.TrimSpace(string(data))
			if prompt == "" {
				continue
			}
			signalActivity(activityCh)
			return prompt
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
// support per-turn execution. Falls back to the stdin-pipe approach.
func (w *Worker) runGenericPTYSession(
	ctx context.Context,
	task types.RunExecution,
	sandboxID string,
	stdout io.Writer,
	activityCh chan<- struct{},
) error {
	stdinReader, stdinWriter := io.Pipe()
	defer stdinReader.Close()

	go forwardTerminalInput(
		ctx, stdinWriter, task.ExternalId,
		executionContextFromTask(task),
		w.terminalIO,
		func() { signalActivity(activityCh) },
	)

	addTaskExecutionContext(log.Info(), task).Msg("starting generic PTY session")
	return w.sandboxManager.AttachPTY(ctx, sandboxID, stdinReader, stdout)
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

type terminalOutputWriter struct {
	ctx          context.Context
	taskID       string
	terminalIO   repository.TerminalIORepository
	executionCtx taskExecutionContext
	onActivity   func()
	mirror       io.Writer
}

func (w *terminalOutputWriter) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
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

// forwardTerminalInput pipes Redis pubsub input to a writer. Used by
// generic PTY sessions that accept stdin.
func forwardTerminalInput(
	ctx context.Context,
	stdinWriter *io.PipeWriter,
	taskID string,
	executionCtx taskExecutionContext,
	terminalIO repository.TerminalIORepository,
	onActivity func(),
) {
	defer stdinWriter.Close()

	for {
		if terminalIO == nil {
			<-ctx.Done()
			return
		}

		inputCh, cleanup, err := terminalIO.SubscribeInput(ctx, taskID)
		if err != nil {
			select {
			case <-ctx.Done():
				return
			case <-time.After(250 * time.Millisecond):
				continue
			}
		}

		channelClosed := false
		for !channelClosed {
			select {
			case <-ctx.Done():
				cleanup()
				return
			case data, ok := <-inputCh:
				if !ok {
					channelClosed = true
					continue
				}
				if len(data) == 0 {
					continue
				}
				if _, err := stdinWriter.Write(data); err != nil {
					cleanup()
					return
				}
				if onActivity != nil {
					onActivity()
				}
			}
		}
		cleanup()
		if ctx.Err() != nil {
			return
		}
		addTaskExecutionContextByID(log.Warn(), taskID, executionCtx).Msg("terminal input subscription closed; retrying")
		select {
		case <-ctx.Done():
			return
		case <-time.After(250 * time.Millisecond):
			continue
		}
	}
}

func signalActivity(activityCh chan<- struct{}) {
	select {
	case activityCh <- struct{}{}:
	default:
	}
}

// bamlEnvForRunner builds a minimal env map for BAML calls by extracting
// the API key from the runner (which holds it from worker config).
func (w *Worker) bamlEnvForRunner(runner AgentExecutionRunner, taskEnv map[string]string) map[string]string {
	env := map[string]string{}
	if cr, ok := runner.(*ClaudeCodeRunner); ok && cr.anthropicAPIKey != "" {
		env["ANTHROPIC_API_KEY"] = cr.anthropicAPIKey
	} else if key := taskEnv["ANTHROPIC_API_KEY"]; key != "" {
		env["ANTHROPIC_API_KEY"] = key
	}
	return env
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
