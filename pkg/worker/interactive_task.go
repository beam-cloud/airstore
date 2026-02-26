package worker

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync/atomic"
	"time"

	"github.com/beam-cloud/airstore/pkg/repository"
	"github.com/beam-cloud/airstore/pkg/types"
	"github.com/rs/zerolog/log"
)

const DefaultBetweenTurnsTimeout = 60 * time.Second

// mountFlushGracePeriod is a short wait after sandbox deletion but before
// unmounting the VFS. This gives the async writer time to flush pending
// writes (e.g. Claude session state) to object storage so the next
// resume finds a complete conversation history.
const mountFlushGracePeriod = 3 * time.Second

func (w *Worker) runInteractiveTask(ctx context.Context, task types.RunExecution) (*types.RunExecutionResult, error) {
	if w.terminalIO == nil {
		return nil, fmt.Errorf("terminal transport is not configured")
	}

	sandboxID := fmt.Sprintf("task-%s", task.ExternalId)

	env := w.sandboxManager.copyTaskEnv(task)

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

	w.sandboxManager.publishStatus(ctx, task.ExternalId, types.RunExecutionStatusRunning, nil, "")

	result := w.runInteractiveSession(ctx, task, sandboxID)

	if err := w.sandboxManager.Delete(sandboxID, true); err != nil {
		addTaskExecutionContext(log.Warn().Err(err), task).Msg("interactive sandbox delete failed during cleanup")
	}
	time.Sleep(mountFlushGracePeriod)
	w.sandboxManager.cleanupMount(task.ExternalId)
	addTaskExecutionContext(log.Info(), task).Msg("interactive sandbox cleanup complete")

	return result, nil
}

func (w *Worker) runInteractiveSession(ctx context.Context, task types.RunExecution, sandboxID string) *types.RunExecutionResult {
	sessionCtx, sessionCancel := context.WithCancel(ctx)
	defer sessionCancel()

	executionCtx := executionContextFromTask(task)

	idleTimeout := w.config.Sandbox.GetInteractiveIdleTimeout()
	var idleTimedOut atomic.Bool
	activityCh := make(chan struct{}, 1)

	if idleTimeout > 0 {
		go monitorInteractiveSessionIdle(sessionCtx, task.ExternalId, executionCtx, sessionCancel, idleTimeout, activityCh, &idleTimedOut)
	}

	cancelCh, cancelCleanup, err := w.terminalIO.SubscribeCancel(sessionCtx, task.ExternalId)
	if err != nil {
		return interactiveErrorResult(task.ExternalId, err)
	}
	defer cancelCleanup()

	go func() {
		select {
		case <-sessionCtx.Done():
		case _, ok := <-cancelCh:
			if !ok {
				return
			}
			addTaskExecutionContext(log.Info(), task).Msg("received cancel signal for interactive task")
			sessionCancel()
			w.sandboxManager.Stop(sandboxID, true)
		}
	}()

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
		onActivity:   func() { signalActivity(activityCh) },
		mirror:       interactiveMirror,
	}

	start := time.Now()
	env := w.sandboxManager.copyTaskEnv(task)
	runner := w.sandboxManager.ResolveRunner(task, env)

	var runErr error
	if tr, ok := runner.(TurnRunner); ok {
		runErr = w.runTurnSession(sessionCtx, task, sandboxID, tr, env, terminalWriter, activityCh)
	} else {
		runErr = w.runGenericPTYSession(sessionCtx, task, sandboxID, terminalWriter, activityCh)
	}

	addTaskExecutionContext(
		log.Info().Dur("session_duration", time.Since(start)).Err(runErr),
		task,
	).Msg("interactive session finished")

	exitCode, errMsg, status := interactiveResult(runErr, idleTimedOut.Load())
	w.sandboxManager.publishStatus(ctx, task.ExternalId, status, &exitCode, errMsg)
	return &types.RunExecutionResult{
		ID:       task.ExternalId,
		ExitCode: exitCode,
		Error:    errMsg,
		Duration: time.Since(start),
	}
}

// runTurnSession executes an interactive session as a series of per-turn
// Exec calls. Each turn runs the runner's CLI (e.g. claude --print) as a
// separate process, with the prompt passed via command-line args. Between
// turns, the worker waits for follow-up input via Redis pubsub. The idle
// monitor handles timeout; no shell loop or stdin pipe is involved.
func (w *Worker) runTurnSession(
	ctx context.Context,
	task types.RunExecution,
	sandboxID string,
	runner TurnRunner,
	env map[string]string,
	stdout io.Writer,
	activityCh chan<- struct{},
) error {
	prompt := strings.TrimSpace(task.Prompt)
	isFirstTurn := true
	sessionEnv := cloneTurnEnv(env)
	firstTurnStrategies := buildFirstTurnStrategies(sessionEnv)

	for prompt != "" {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		if isFirstTurn {
			nextEnv, err := w.executeFirstTurnWithStrategy(
				ctx,
				task,
				sandboxID,
				runner,
				sessionEnv,
				stdout,
				activityCh,
				prompt,
				firstTurnStrategies,
			)
			if err != nil {
				return err
			}
			sessionEnv = nextEnv
			isFirstTurn = false
		} else {
			err := w.executeTurn(
				ctx,
				task,
				sandboxID,
				runner,
				sessionEnv,
				stdout,
				activityCh,
				prompt,
				TurnArgModeFollowup,
				1,
				1,
				"",
			)
			if err != nil {
				return err
			}
		}

		addTaskExecutionContext(log.Info(), task).Msg("turn complete, waiting for follow-up input")
		prompt = w.waitForFollowupInput(ctx, task.ExternalId, DefaultBetweenTurnsTimeout)
	}

	return nil
}

// waitForFollowupInput blocks until follow-up input arrives via Redis
// pubsub, the per-turn timeout expires, or the context is cancelled
// (e.g. idle timeout). Returns the trimmed prompt, or "" if the
// session should end.
func (w *Worker) waitForFollowupInput(ctx context.Context, taskID string, timeout time.Duration) string {
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

	strategies := []firstTurnStrategy{
		{
			mode:             TurnArgModeFirstResumeLatest,
			transitionReason: "resume requested; prefer local VFS state",
		},
	}
	if strings.TrimSpace(env[agentSessionIDEnvKey]) != "" {
		strategies = append(
			strategies,
			firstTurnStrategy{
				mode:             TurnArgModeFirstResumeByID,
				transitionReason: "resume fallback using explicit session id",
			},
		)
	}
	strategies = append(
		strategies,
		firstTurnStrategy{
			mode:             TurnArgModeFirstFreshNoSession,
			transitionReason: "final fallback to fresh session without explicit id",
		},
	)
	return strategies
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

func envForFirstTurnStrategy(base map[string]string, strategy firstTurnStrategy) map[string]string {
	env := cloneTurnEnv(base)
	if strategy.mode == TurnArgModeFirstFreshNoSession {
		delete(env, agentSessionIDEnvKey)
		delete(env, agentResumeSessionEnvKey)
	}
	return env
}

func (w *Worker) executeFirstTurnWithStrategy(
	ctx context.Context,
	task types.RunExecution,
	sandboxID string,
	runner TurnRunner,
	baseEnv map[string]string,
	stdout io.Writer,
	activityCh chan<- struct{},
	prompt string,
	strategies []firstTurnStrategy,
) (map[string]string, error) {
	if len(strategies) == 0 {
		return baseEnv, fmt.Errorf("no first-turn strategy configured")
	}

	totalAttempts := len(strategies)
	for idx, strategy := range strategies {
		attempt := idx + 1
		attemptEnv := envForFirstTurnStrategy(baseEnv, strategy)
		err := w.executeTurn(
			ctx,
			task,
			sandboxID,
			runner,
			attemptEnv,
			stdout,
			activityCh,
			prompt,
			strategy.mode,
			attempt,
			totalAttempts,
			strategy.transitionReason,
		)
		if err == nil {
			return attemptEnv, nil
		}
		if attempt < totalAttempts {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return baseEnv, err
			}
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

		// Emit a single clear failure when a resumed session exhausts bounded retries.
		if totalAttempts > 1 {
			addTaskExecutionContext(
				log.Error().
					Err(err).
					Str("turn_mode", string(strategy.mode)).
					Int("resume_attempt", attempt).
					Int("resume_attempts_total", totalAttempts),
				task,
			).Msg("session resume exhausted all first-turn strategies")
			return baseEnv, fmt.Errorf("session resume exhausted all first-turn strategies: %w", err)
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
	activityCh chan<- struct{},
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

	signalActivity(activityCh)
	err := w.sandboxManager.ExecPTY(ctx, sandboxID, args, env, stdout)
	signalActivity(activityCh)
	return err
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

func interactiveErrorResult(taskID string, err error) *types.RunExecutionResult {
	return &types.RunExecutionResult{
		ID:       taskID,
		ExitCode: -1,
		Error:    err.Error(),
	}
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

// forwardTerminalInput pipes Redis pubsub input to a writer (for generic
// PTY sessions). Turn-based sessions don't use this — they call
// waitForFollowupInput instead.
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

func monitorInteractiveSessionIdle(
	ctx context.Context,
	taskID string,
	executionCtx taskExecutionContext,
	cancel context.CancelFunc,
	idleTimeout time.Duration,
	activityCh <-chan struct{},
	idleTimedOut *atomic.Bool,
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
