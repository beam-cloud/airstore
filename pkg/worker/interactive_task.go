package worker

import (
	"bytes"
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
	agentsignal "github.com/beam-cloud/airstore/pkg/worker/agentsignal/baml_client"
	signaltypes "github.com/beam-cloud/airstore/pkg/worker/agentsignal/baml_client/types"
	"github.com/rs/zerolog/log"
)

const (
	DefaultBetweenTurnsTimeout = 60 * time.Second
	mountFlushGracePeriod      = 10 * time.Second
	sessionLeaseTTL            = 30 * time.Second
	sessionLeaseRenewInterval  = 10 * time.Second
	runInteractionTTL          = 30 * time.Minute
	subagentPollInterval       = 10 * time.Second
	subagentMaxWait            = 30 * time.Minute
	subagentProbeTimeout       = 15 * time.Second
	terminalRingBufSize        = 256 * 1024
)

// subagentWaitOutcome describes why waitForSubagents returned.
type subagentWaitOutcome int

const (
	subagentNoneDetected subagentWaitOutcome = iota
	subagentFinished
	subagentMaxWaitReached
	subagentSessionCancelled
)

func (o subagentWaitOutcome) String() string {
	switch o {
	case subagentNoneDetected:
		return "none_detected"
	case subagentFinished:
		return "finished"
	case subagentMaxWaitReached:
		return "max_wait_reached"
	case subagentSessionCancelled:
		return "session_cancelled"
	default:
		return "unknown"
	}
}

// subagentProbeArgs detects real background Claude processes while excluding
// helper hooks that live under .claude/ (e.g. dump-stop-message.js) and the
// probe shell itself.
var subagentProbeArgs = []string{"/bin/sh", "-c",
	`mypid=$$; for pid in $(pgrep -f claude 2>/dev/null); do ` +
		`[ "$pid" = "$mypid" ] && continue; ` +
		`cmdline=$(tr '\0' ' ' < /proc/$pid/cmdline 2>/dev/null); ` +
		`case "$cmdline" in */.claude/*) continue;; *) exit 0;; esac; ` +
		`done; exit 1`,
}

// ---------------------------------------------------------------------------
// Interaction & task state helpers
// ---------------------------------------------------------------------------

func (w *Worker) setRunInteractionState(ctx context.Context, task types.RunExecution, state types.RunInteractionState) {
	if w == nil || w.terminalIO == nil {
		return
	}
	ec := executionContextFromTask(task)
	if ec.runID == "" {
		return
	}
	activeID := ""
	if state != types.RunInteractionStateClosed {
		activeID = task.ExternalId
	}
	if err := w.terminalIO.SetRunInteraction(ctx, task.WorkspaceId, ec.runID, types.RunInteraction{
		State: state, ActiveExecutionID: activeID,
	}, runInteractionTTL); err != nil {
		addTaskExecutionContext(log.Warn().Err(err).Str("state", string(state)), task).
			Msg("failed to persist run interaction state")
	}
}

func (w *Worker) setOriginTaskState(ctx context.Context, task types.RunExecution, state types.AgentTaskState, inputKind types.InputKind, waitingSummary string) {
	ec := executionContextFromTask(task)
	if ec.originTaskID == "" || ec.runID == "" {
		return
	}
	if err := w.gatewayClient.UpdateTaskState(ctx, ec.originTaskID, string(state), ec.runID, string(inputKind), waitingSummary); err != nil {
		addTaskExecutionContext(log.Warn().Err(err).Str("target_state", string(state)), task).
			Msg("failed to update origin task state")
	}
}

// ---------------------------------------------------------------------------
// Session checkpoint
// ---------------------------------------------------------------------------

func (w *Worker) recordSessionCheckpoint(ctx context.Context, task types.RunExecution, mountSource string, env map[string]string) error {
	if w == nil || w.terminalIO == nil {
		return nil
	}
	sessionID := strings.TrimSpace(env[agentSessionIDEnvKey])
	ec := executionContextFromTask(task)
	if sessionID == "" || ec.runID == "" {
		return nil
	}
	cp := &types.SessionCheckpoint{RunID: ec.runID, ExecutionID: task.ExternalId, UpdatedAt: time.Now().UnixMilli()}
	if err := writeClaudeSessionCheckpoint(mountSource, env, cp); err != nil {
		return err
	}
	return w.terminalIO.SetSessionCheckpoint(ctx, task.WorkspaceId, sessionID, cp, 0)
}

// ---------------------------------------------------------------------------
// Main entry point: run an interactive task
// ---------------------------------------------------------------------------

func (w *Worker) runInteractiveTask(ctx context.Context, task types.RunExecution) (*types.RunExecutionResult, error) {
	if w.terminalIO == nil {
		return nil, fmt.Errorf("terminal transport is not configured")
	}
	runCtx, runCancel := context.WithCancel(ctx)
	defer runCancel()
	defer func() {
		c, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		w.setRunInteractionState(c, task, types.RunInteractionStateClosed)
	}()

	sessionID := strings.TrimSpace(task.Env[agentSessionIDEnvKey])
	release, leaseErr := w.acquireSessionLease(runCtx, task, sessionID, runCancel)
	if release == nil && sessionID != "" {
		if leaseErr != nil {
			return nil, fmt.Errorf("session lease: %w", leaseErr)
		}
		return nil, fmt.Errorf("session %s already in use", sessionID)
	}
	defer release()

	sandboxID := fmt.Sprintf("task-%s", task.ExternalId)
	env := w.sandboxManager.copyTaskEnv(task)
	if cr, ok := w.sandboxManager.ResolveRunner(task, env).(*ClaudeCodeRunner); ok {
		cr.injectEnv(env)
	}
	mountSource := w.sandboxManager.mountFilesystem(runCtx, task)
	cfg := w.sandboxManager.buildTaskSandboxConfig(task, []string{"sleep", "infinity"}, env, mountSource)

	if _, err := w.sandboxManager.Create(cfg); err != nil {
		w.sandboxManager.cleanupMount(task.ExternalId)
		return nil, fmt.Errorf("create sandbox: %w", err)
	}
	if err := w.sandboxManager.Start(sandboxID); err != nil {
		w.sandboxManager.publishStatus(ctx, task.ExternalId, types.RunExecutionStatusFailed, nil, err.Error())
		w.sandboxManager.Delete(sandboxID, true)
		w.sandboxManager.cleanupMount(task.ExternalId)
		return nil, fmt.Errorf("start sandbox: %w", err)
	}

	setupGitInsideSandbox(runCtx, w.sandboxManager.runtime, sandboxID, env)
	w.sandboxManager.publishStatus(runCtx, task.ExternalId, types.RunExecutionStatusRunning, nil, "")
	w.setRunInteractionState(runCtx, task, types.RunInteractionStateWorking)

	result := w.runInteractiveSession(runCtx, task, sandboxID, mountSource)
	w.setRunInteractionState(runCtx, task, types.RunInteractionStateClosed)

	_ = w.sandboxManager.Delete(sandboxID, true)
	time.Sleep(mountFlushGracePeriod)

	if result != nil && result.Error == "" {
		c, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		if err := w.recordSessionCheckpoint(c, task, mountSource, env); err != nil {
			addTaskExecutionContext(log.Warn().Err(err), task).Msg("checkpoint failed")
		}
		cancel()
	}

	release()
	w.sandboxManager.cleanupMount(task.ExternalId)
	return result, nil
}

// acquireSessionLease returns a release func (call it when done) and an error.
// Returns a no-op func and nil error if sessionID is empty.
// Returns (nil, err) on infrastructure failure, (nil, nil) if the lease is contested.
func (w *Worker) acquireSessionLease(ctx context.Context, task types.RunExecution, sessionID string, onLost func()) (func(), error) {
	if sessionID == "" {
		return func() {}, nil
	}
	ownerID := fmt.Sprintf("%s:%s", strings.TrimSpace(w.workerId), task.ExternalId)
	acquired, err := w.terminalIO.AcquireSessionLease(ctx, task.WorkspaceId, sessionID, ownerID, sessionLeaseTTL)
	if err != nil {
		return nil, fmt.Errorf("acquire session lease: %w", err)
	}
	if !acquired {
		return nil, nil
	}
	leaseCtx, leaseCancel := context.WithCancel(ctx)
	go w.heartbeatSessionLease(leaseCtx, task, sessionID, ownerID, onLost)
	var once sync.Once
	return func() {
		once.Do(func() {
			leaseCancel()
			c, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = w.terminalIO.ReleaseSessionLease(c, task.WorkspaceId, sessionID, ownerID)
		})
	}, nil
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
			if err != nil || !renewed {
				addTaskExecutionContext(log.Warn().Err(err), task).Msg("session lease lost")
				if onLost != nil {
					onLost()
				}
				return
			}
		}
	}
}

// ---------------------------------------------------------------------------
// Interactive session: orchestrates turns, input, subagents
// ---------------------------------------------------------------------------

func (w *Worker) runInteractiveSession(ctx context.Context, task types.RunExecution, sandboxID, mountSource string) *types.RunExecutionResult {
	sessionCtx, sessionCancel := context.WithCancel(ctx)
	defer sessionCancel()

	env := w.sandboxManager.copyTaskEnv(task)
	runner := w.sandboxManager.ResolveRunner(task, env)
	bamlEnv := w.sandboxManager.BamlEnv()
	activityCh := make(chan struct{}, 1)
	var idleTimedOut atomic.Bool

	// Heartbeat
	var touchHeartbeat func()
	var checkHeartbeat func() bool
	if hr, ok := runner.(HeartbeatRunner); ok && mountSource != "" {
		if hbPath, err := hr.SetupHeartbeat(mountSource, env); err == nil {
			checkHeartbeat = func() bool { return hr.CheckHeartbeat(hbPath) }
			touchHeartbeat = func() { _ = os.WriteFile(hbPath, []byte(time.Now().Format(time.RFC3339Nano)), 0o644) }
		}
	}

	// Needs-input detection
	var needsInputRunner NeedsInputRunner
	var needsInputPath string
	if ir, ok := runner.(NeedsInputRunner); ok && mountSource != "" {
		if p, err := ir.SetupNeedsInput(mountSource, env); err == nil {
			needsInputRunner = ir
			needsInputPath = p
		}
	}

	// Idle monitor
	if timeout := w.config.Sandbox.GetInteractiveIdleTimeout(); timeout > 0 {
		go monitorInteractiveSessionIdle(sessionCtx, task.ExternalId, executionContextFromTask(task), sessionCancel, timeout, activityCh, &idleTimedOut, checkHeartbeat)
	}

	// Cancellation watcher
	cancelCleanup := w.watchTaskCancellation(sessionCtx, task, func() {
		sessionCancel()
		w.sandboxManager.Stop(sandboxID, true)
	})
	defer cancelCleanup()

	// Output writers
	outputPipeline := w.sandboxManager.taskOutputPipeline(sessionCtx, task, env)
	mirror := NewTaskOutput(task.ExternalId, "stdout", outputPipeline.writers...)
	defer outputPipeline.Wait()
	defer mirror.Flush()
	tw := &terminalOutputWriter{
		ctx: sessionCtx, taskID: task.ExternalId,
		terminalIO: w.terminalIO, executionCtx: executionContextFromTask(task),
		onActivity: func() {
			signalActivity(activityCh)
			if touchHeartbeat != nil {
				touchHeartbeat()
			}
		},
		mirror: mirror, ringBuf: newRingBuffer(terminalRingBufSize),
	}

	// Build the needs-input checker
	var checkNeedsInput func(string) (bool, types.InputKind, string)
	if needsInputRunner != nil {
		checkNeedsInput = w.buildNeedsInputChecker(sessionCtx, task, needsInputRunner, needsInputPath, tw, bamlEnv)
	}

	start := time.Now()
	var runErr error
	var needsInput bool
	var lastPrompt string

	if tr, ok := runner.(TurnRunner); ok {
		runErr, needsInput, lastPrompt = w.runTurnSession(sessionCtx, task, sandboxID, tr, env, tw, activityCh, checkNeedsInput, bamlEnv)
	} else {
		runErr = w.runGenericPTYSession(sessionCtx, task, sandboxID, tw, activityCh)
	}

	mirror.Flush()
	outputPipeline.Wait()

	if !needsInput && runErr == nil && tw.ringBuf != nil {
		var assistantMessage string
		if extractor, ok := runner.(ResponseExtractor); ok {
			assistantMessage = extractor.ExtractResponseText(tw.ringBuf.Bytes(), 24000)
		} else {
			assistantMessage = extractAssistantText(tw.ringBuf.Bytes(), 24000)
		}
		if assistantMessage != "" {
			var userMessage *string
			if trimmed := strings.TrimSpace(lastPrompt); trimmed != "" {
				userMessage = &trimmed
			}
			if err := persistFinalResponseOutput(
				sessionCtx,
				w.gatewayClient,
				task,
				outputPipeline.tracker,
				userMessage,
				assistantMessage,
				bamlEnv,
				nil,
			); err != nil {
				addTaskExecutionContext(log.Warn().Err(err), task).Msg("failed to persist final response output")
			}
		}
	}

	exitCode, errMsg, st := interactiveResult(runErr, idleTimedOut.Load())
	w.sandboxManager.publishStatus(ctx, task.ExternalId, st, &exitCode, errMsg)

	var agentMsg string
	if needsInputRunner != nil && needsInputPath != "" {
		agentMsg = needsInputRunner.ReadLastMessage(needsInputPath)
	}

	var wakeSignal *types.RunExecutionWakeSignal
	if !needsInput && runErr == nil && agentMsg != "" {
		wakeSignal = w.classifyFollowUp(ctx, agentMsg, lastPrompt, mountSource, env, bamlEnv)
	}

	var subtaskReqs []*types.SubtaskRequest
	if wakeSignal != nil && outputPipeline.tracker != nil {
		subtaskReqs = w.classifySubtasks(ctx, outputPipeline.tracker, agentMsg, lastPrompt, bamlEnv)
	}

	return &types.RunExecutionResult{
		ID: task.ExternalId, ExitCode: exitCode, Error: errMsg,
		Duration: time.Since(start), WaitingForInput: needsInput,
		WakeSignal: wakeSignal, SubtaskRequests: subtaskReqs,
	}
}

func (w *Worker) buildNeedsInputChecker(
	ctx context.Context, task types.RunExecution,
	runner NeedsInputRunner, markerPath string,
	tw *terminalOutputWriter, bamlEnv map[string]string,
) func(string) (bool, types.InputKind, string) {
	return func(currentPrompt string) (bool, types.InputKind, string) {
		msg := runner.ReadLastMessage(markerPath)
		if msg == "" {
			return false, "", ""
		}
		cls, err := agentsignal.ClassifyTurn(ctx, msg, agentsignal.WithEnv(bamlEnv))
		if err != nil {
			return false, "", ""
		}

		if cls.Outcome != signaltypes.TurnOutcomeNEEDS_INPUT {
			return false, "", ""
		}

		kind := types.InputKindFreeText
		if cls.Input_kind != nil {
			kind = types.InputKind(strings.ToLower(string(*cls.Input_kind)))
		}

		var summary string
		if kind == types.InputKindApproveReject && tw.ringBuf != nil {
			text := extractAssistantText(tw.ringBuf.Bytes(), 4000)
			summary = w.tryBuildApprovalSummary(ctx, text, bamlEnv)
		}
		return true, kind, summary
	}
}

// ---------------------------------------------------------------------------
// Turn session: the core turn loop
// ---------------------------------------------------------------------------

func (w *Worker) runTurnSession(
	ctx context.Context,
	task types.RunExecution,
	sandboxID string,
	runner TurnRunner,
	env map[string]string,
	stdout io.Writer,
	activityCh chan<- struct{},
	checkNeedsInput func(string) (bool, types.InputKind, string),
	bamlEnv map[string]string,
) (error, bool, string) {
	prompt := strings.TrimSpace(task.Prompt)
	sessionEnv := cloneMap(env)
	isFirst := true

	outputParser, _ := runner.(OutputParsingRunner)
	var turnBuf bytes.Buffer

	for prompt != "" {
		if ctx.Err() != nil {
			return ctx.Err(), false, ""
		}

		w.setRunInteractionState(ctx, task, types.RunInteractionStateWorking)
		if !isFirst {
			w.setOriginTaskState(ctx, task, types.AgentTaskStateRunning, "", "")
		}

		turnOut := stdout
		if outputParser != nil {
			turnBuf.Reset()
			turnOut = io.MultiWriter(stdout, &turnBuf)
		}

		if isFirst {
			if err := w.executeFirstTurn(ctx, task, sandboxID, runner, sessionEnv, turnOut, prompt); err != nil {
				return err, false, ""
			}
			isFirst = false
		} else {
			if err := w.executeTurn(ctx, task, sandboxID, runner, sessionEnv, turnOut, prompt, TurnArgModeFollowup); err != nil {
				return err, false, ""
			}
		}
		signalActivity(activityCh)

		if w.waitForSubagents(ctx, task, sandboxID, activityCh) == subagentFinished {
			if err := w.executeTurn(ctx, task, sandboxID, runner, sessionEnv, turnOut,
				"Your background tasks / subagents have completed. Please collect and report their results.",
				TurnArgModeFollowup); err != nil {
				return err, false, ""
			}
			signalActivity(activityCh)
		}

		// Determine if the agent needs user input before continuing.
		var needsInput bool
		var inputKind types.InputKind
		var waitingSummary string

		if outputParser != nil {
			var err error
			needsInput, inputKind, waitingSummary, err = outputParser.ParseTurnOutput(turnBuf.Bytes())
			if err != nil {
				addTaskExecutionContext(log.Warn().Err(err), task).Msg("failed to parse turn output")
			}
			if needsInput && inputKind == types.InputKindApproveReject {
				if s := w.tryBuildApprovalSummary(ctx, waitingSummary, bamlEnv); s != "" {
					waitingSummary = s
				}
			}
		} else if checkNeedsInput != nil {
			needsInput, inputKind, waitingSummary = checkNeedsInput(prompt)
		}

		if !needsInput {
			if pending := w.claimPendingInput(ctx, task); pending != "" {
				prompt = pending
				continue
			}
			return nil, false, prompt
		}

		w.setRunInteractionState(ctx, task, types.RunInteractionStateWaitingForInput)
		w.setOriginTaskState(ctx, task, types.AgentTaskStateWaiting, inputKind, waitingSummary)
		prompt = w.waitForFollowupInput(ctx, task, DefaultBetweenTurnsTimeout, activityCh)
		if prompt == "" {
			return nil, true, ""
		}
	}
	return nil, true, ""
}

// ---------------------------------------------------------------------------
// Subagent monitoring
// ---------------------------------------------------------------------------

func (w *Worker) waitForSubagents(ctx context.Context, task types.RunExecution, sandboxID string, activityCh chan<- struct{}) subagentWaitOutcome {
	return w.waitForSubagentsWithTiming(ctx, task, sandboxID, activityCh, subagentPollInterval, subagentMaxWait, subagentProbeTimeout)
}

func (w *Worker) waitForSubagentsWithTiming(
	ctx context.Context, task types.RunExecution, sandboxID string, activityCh chan<- struct{},
	pollInterval, maxWait, probeTimeout time.Duration,
) subagentWaitOutcome {
	probeCtx, probeCancel := context.WithTimeout(ctx, probeTimeout)
	err := w.sandboxManager.ExecCheck(probeCtx, sandboxID, subagentProbeArgs)
	probeCancel()
	if err != nil {
		return subagentNoneDetected
	}

	addTaskExecutionContext(log.Info(), task).Msg("waiting for subagent processes")
	deadline := time.After(maxWait)
	for {
		select {
		case <-ctx.Done():
			addTaskExecutionContext(log.Info(), task).
				Str("outcome", subagentSessionCancelled.String()).
				Msg("subagent wait ended")
			return subagentSessionCancelled
		case <-deadline:
			addTaskExecutionContext(log.Warn(), task).
				Str("outcome", subagentMaxWaitReached.String()).
				Dur("max_wait", maxWait).
				Msg("subagent wait ended")
			return subagentMaxWaitReached
		case <-time.After(pollInterval):
			signalActivity(activityCh)
			probeCtx, probeCancel := context.WithTimeout(ctx, probeTimeout)
			err := w.sandboxManager.ExecCheck(probeCtx, sandboxID, subagentProbeArgs)
			probeTimedOut := probeCtx.Err() != nil && ctx.Err() == nil
			probeCancel()
			if probeTimedOut {
				addTaskExecutionContext(log.Warn(), task).Msg("subagent probe timed out, will retry")
				continue
			}
			if err != nil {
				addTaskExecutionContext(log.Info(), task).
					Str("outcome", subagentFinished.String()).
					Msg("subagent wait ended")
				return subagentFinished
			}
		}
	}
}

// ---------------------------------------------------------------------------
// Input: claim pending + wait for follow-up
// ---------------------------------------------------------------------------

func (w *Worker) claimPendingInput(ctx context.Context, task types.RunExecution) string {
	ec := executionContextFromTask(task)
	if ec.originTaskID == "" || ec.runID == "" {
		return ""
	}
	return w.tryClaimInput(ctx, ec.originTaskID, ec.runID, task.ExternalId)
}

func (w *Worker) tryClaimInput(ctx context.Context, taskID, runID, execID string) string {
	resp, err := w.gatewayClient.ClaimTaskInput(ctx, taskID, runID, execID)
	if err != nil || !resp.Found {
		return ""
	}
	prompt := strings.TrimSpace(resp.Message)
	if prompt == "" {
		_ = w.gatewayClient.AckTaskInput(ctx, resp.InputId)
		return ""
	}
	_ = w.gatewayClient.AckTaskInput(ctx, resp.InputId)
	return prompt
}

func (w *Worker) waitForFollowupInput(ctx context.Context, task types.RunExecution, timeout time.Duration, activityCh chan<- struct{}) string {
	ec := executionContextFromTask(task)
	if ec.originTaskID == "" || ec.runID == "" {
		<-ctx.Done()
		return ""
	}

	// Try immediately
	if p := w.tryClaimInput(ctx, ec.originTaskID, ec.runID, task.ExternalId); p != "" {
		signalActivity(activityCh)
		return p
	}

	// Subscribe to Redis wake channel
	var wakeCh <-chan struct{}
	if w.terminalIO != nil {
		if ch, cleanup, err := w.terminalIO.SubscribeInputWake(ctx, task.ExternalId); err == nil {
			wakeCh = ch
			defer cleanup()
		}
	}
	if wakeCh == nil {
		wakeCh = make(chan struct{})
	}

	var timeoutCh <-chan time.Time
	if timeout > 0 {
		t := time.NewTimer(timeout)
		defer t.Stop()
		timeoutCh = t.C
	}

	for {
		select {
		case <-ctx.Done():
			return ""
		case <-timeoutCh:
			return ""
		case <-wakeCh:
		case <-time.After(2 * time.Second):
		}
		if p := w.tryClaimInput(ctx, ec.originTaskID, ec.runID, task.ExternalId); p != "" {
			signalActivity(activityCh)
			return p
		}
	}
}

// ---------------------------------------------------------------------------
// Turn execution
// ---------------------------------------------------------------------------

func (w *Worker) executeFirstTurn(
	ctx context.Context, task types.RunExecution, sandboxID string,
	runner TurnRunner, env map[string]string, stdout io.Writer, prompt string,
) error {
	strategies := buildFirstTurnStrategies(env)
	for i, s := range strategies {
		err := w.executeTurn(ctx, task, sandboxID, runner, env, stdout, prompt, s.mode)
		if err == nil {
			return nil
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return err
		}
		if i == len(strategies)-1 {
			return err
		}
		addTaskExecutionContext(log.Warn().Err(err).Str("mode", string(s.mode)), task).
			Msg("first-turn strategy failed, trying next")
		if s.mode == TurnArgModeFirstResumeByID {
			delete(env, agentSessionIDEnvKey)
		}
	}
	return fmt.Errorf("no first-turn strategies")
}

func (w *Worker) executeTurn(
	ctx context.Context, task types.RunExecution, sandboxID string,
	runner TurnRunner, env map[string]string, stdout io.Writer,
	prompt string, mode TurnArgMode,
) error {
	return w.sandboxManager.ExecPTY(ctx, sandboxID, runner.BuildTurnArgs(prompt, env, mode), env, stdout)
}

func (w *Worker) runGenericPTYSession(ctx context.Context, task types.RunExecution, sandboxID string, stdout io.Writer, _ chan<- struct{}) error {
	return w.sandboxManager.AttachPTY(ctx, sandboxID, nil, stdout)
}

type firstTurnStrategy struct {
	mode TurnArgMode
}

func buildFirstTurnStrategies(env map[string]string) []firstTurnStrategy {
	resume := strings.ToLower(strings.TrimSpace(env[agentResumeSessionEnvKey]))
	if resume != "1" && resume != "true" && resume != "yes" && resume != "on" {
		return []firstTurnStrategy{{mode: TurnArgModeFirstStart}}
	}
	if strings.TrimSpace(env[agentSessionIDEnvKey]) != "" {
		return []firstTurnStrategy{
			{mode: TurnArgModeFirstResumeByID},
			{mode: TurnArgModeFirstResumeLatest},
		}
	}
	return []firstTurnStrategy{{mode: TurnArgModeFirstResumeLatest}}
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

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

func cloneMap(m map[string]string) map[string]string {
	out := make(map[string]string, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

func signalActivity(ch chan<- struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}

// ---------------------------------------------------------------------------
// Terminal output writer
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// Idle monitor
// ---------------------------------------------------------------------------

func monitorInteractiveSessionIdle(
	ctx context.Context, taskID string, ec taskExecutionContext,
	cancel context.CancelFunc, timeout time.Duration,
	activityCh <-chan struct{}, idleTimedOut *atomic.Bool,
	checkHeartbeat func() bool,
) {
	timer := time.NewTimer(timeout)
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
			timer.Reset(timeout)
		case <-timer.C:
			if checkHeartbeat != nil && checkHeartbeat() {
				timer.Reset(timeout)
				continue
			}
			idleTimedOut.Store(true)
			addTaskExecutionContextByID(log.Info().Dur("idle_timeout", timeout), taskID, ec).
				Msg("idle timeout reached, stopping")
			cancel()
			return
		}
	}
}
